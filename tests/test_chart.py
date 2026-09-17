"""Offline contract tests; no Kubernetes, Docker or backend connections."""
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "charts" / "sink"
BASE = {"stores": {"mongo": {"state": "active"}}}
KAFKA = {"brokers": ["kafka:9092"], "topic": "mongo", "consumerGroup": "mongo-workers", "partitions": 8}


def render(values, *options):
    with tempfile.TemporaryDirectory() as directory:
        filename = Path(directory) / "values.yaml"
        filename.write_text(yaml.safe_dump(values))
        command = [os.environ.get("HELM", "helm"), "template", "test", str(CHART),
                   "--namespace", "sink", "--kube-version", "1.30.0", "-f", str(filename), *options]
        return subprocess.run(command, capture_output=True, text=True, check=False)


class ChartTests(unittest.TestCase):
    def manifests(self, values=None, *options):
        result = render(BASE if values is None else values, *options)
        self.assertEqual(result.returncode, 0, result.stderr)
        return {(doc["kind"], doc["metadata"]["name"]): doc
                for doc in yaml.safe_load_all(result.stdout) if doc}

    def test_empty_and_staged_cluster(self):
        empty = self.manifests({})
        self.assertEqual(list(empty), [("ConfigMap", "test-sink-inventory")])
        staged = self.manifests({"stores": {"mongo": {"state": "staged"}}})
        self.assertIn(("Deployment", "test-sink-mongo-engine"), staged)
        self.assertNotIn(("Deployment", "test-sink-gateway"), staged)

    def test_discovery_shutdown_security_and_memory(self):
        docs = self.manifests()
        for role, name, delay, grace in [("engine", "test-sink-mongo-engine", 95, 255),
                                         ("gateway", "test-sink-gateway", 100, 260)]:
            deployment = docs["Deployment", name]
            pod = deployment["spec"]["template"]
            container = pod["spec"]["containers"][0]
            self.assertEqual(container["args"], ["--config", "/etc/sink/sink.yaml"])
            self.assertEqual(container["lifecycle"]["preStop"]["sleep"]["seconds"], delay)
            self.assertEqual(pod["spec"]["terminationGracePeriodSeconds"], grace)
            self.assertEqual(deployment["spec"]["strategy"]["rollingUpdate"], {"maxUnavailable": 0, "maxSurge": 1})
            selector = deployment["spec"]["selector"]["matchLabels"]
            self.assertEqual(docs["Service", name]["spec"]["selector"], selector)
            self.assertEqual(docs["PodDisruptionBudget", name]["spec"]["selector"]["matchLabels"], selector)
            self.assertFalse(pod["spec"]["automountServiceAccountToken"])
            self.assertTrue(container["securityContext"]["readOnlyRootFilesystem"])
            self.assertEqual(container["livenessProbe"]["httpGet"]["path"], "/livez")
            self.assertNotIn("command", container.get("lifecycle", {}).get("preStop", {}))
        engine = docs["Deployment", "test-sink-mongo-engine"]
        container = engine["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(engine["spec"]["minReadySeconds"], 55)
        self.assertEqual(container["readinessProbe"]["httpGet"]["path"], "/readyz?service=sink.storage.mongo")
        self.assertEqual(container["env"][0]["value"], str(1024**3 * 80 // 100))
        self.assertEqual(docs["Service", "test-sink-mongo-engine"]["spec"]["clusterIP"], "None")
        self.assertFalse(docs["Service", "test-sink-mongo-engine"]["spec"]["publishNotReadyAddresses"])
        self.assertFalse(any(kind == "Secret" for kind, _ in docs))

    def test_routes_are_only_active_and_config_changes_roll_gateway(self):
        values = copy.deepcopy(BASE)
        values["stores"].update({"archive": {"state": "staged"}, "old": {"state": "retiring"}})
        first = self.manifests(values)
        config = yaml.safe_load(first["ConfigMap", "test-sink-gateway"]["data"]["sink.yaml"])
        self.assertEqual(config["gateway"]["routes"], [{"store": "mongo", "target": "dns:///test-sink-mongo-engine.sink.svc.cluster.local:8080", "tls": {"insecure": True}}])
        values["stores"]["archive"]["state"] = "active"
        second = self.manifests(values)
        a = first["Deployment", "test-sink-gateway"]["spec"]["template"]["metadata"]["annotations"]
        b = second["Deployment", "test-sink-gateway"]["spec"]["template"]["metadata"]["annotations"]
        self.assertNotEqual(a["sink.batchstream.io/config-checksum"], b["sink.batchstream.io/config-checksum"])
        self.assertEqual(first["Deployment", "test-sink-mongo-engine"], second["Deployment", "test-sink-mongo-engine"])

    def test_per_store_merge_does_not_leak(self):
        values = copy.deepcopy(BASE)
        values["stores"]["archive"] = {"state": "staged", "engine": {"config": {"existingSecret": "archive", "key": "complete.yaml"}, "pod": {"resources": {"limits": {"memory": "2Gi"}}, "configRevision": "v2"}}}
        docs = self.manifests(values)
        archive = docs["Deployment", "test-sink-archive-engine"]["spec"]["template"]["spec"]
        mongo = docs["Deployment", "test-sink-mongo-engine"]["spec"]["template"]["spec"]
        self.assertEqual(archive["containers"][0]["resources"]["limits"]["memory"], "2Gi")
        self.assertEqual(mongo["containers"][0]["resources"]["limits"]["memory"], "1Gi")
        self.assertEqual(archive["volumes"][0]["secret"], {"secretName": "archive", "items": [{"key": "complete.yaml", "path": "sink.yaml"}]})

    def test_budget_recalculates_with_dns(self):
        values = {**BASE, "discovery": {"dnsRefreshSeconds": 40, "dnsCacheSeconds": 60}}
        doc = self.manifests(values)["Deployment", "test-sink-mongo-engine"]
        container = doc["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["lifecycle"]["preStop"]["sleep"]["seconds"], 155)
        self.assertEqual(doc["spec"]["template"]["spec"]["terminationGracePeriodSeconds"], 315)
        self.assertEqual(doc["spec"]["minReadySeconds"], 115)

    def test_controller_owns_replicas_and_hpa_behavior(self):
        values = {**BASE, "engineDefaults": {"autoscaling": {"mode": "hpa", "minReplicas": 3, "maxReplicas": 7}}}
        docs = self.manifests(values)
        self.assertNotIn("replicas", docs["Deployment", "test-sink-mongo-engine"]["spec"])
        hpa = docs["HorizontalPodAutoscaler", "test-sink-mongo-engine"]["spec"]
        self.assertEqual((hpa["minReplicas"], hpa["maxReplicas"]), (3, 7))
        self.assertEqual(hpa["behavior"]["scaleDown"]["policies"], [{"type": "Pods", "value": 1, "periodSeconds": 300}])
        self.assertEqual(docs["Deployment", "test-sink-gateway"]["spec"]["replicas"], 2)

    def test_keda_worker_zero_and_partition_trigger(self):
        values = {"stores": {"mongo": {"state": "active", "kafka": KAFKA,
                  "worker": {"enabled": True, "allowScaleToZero": True, "disruptionBudget": {"enabled": False},
                             "autoscaling": {"mode": "keda", "minReplicas": 0, "maxReplicas": 8,
                                             "keda": {"authenticationRef": {"name": "auth"}, "fallback": {"failureThreshold": 3, "replicas": 2}}}}}}}
        docs = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        name = "test-sink-mongo-worker"
        deployment = docs["Deployment", name]
        self.assertNotIn("replicas", deployment["spec"])
        self.assertNotIn("lifecycle", deployment["spec"]["template"]["spec"]["containers"][0])
        self.assertNotIn(("Service", name), docs)
        self.assertNotIn(("PodDisruptionBudget", name), docs)
        spec = docs["ScaledObject", name]["spec"]
        self.assertEqual(spec["minReplicaCount"], 0)
        trigger = spec["triggers"][0]
        self.assertEqual(trigger["metadata"]["topic"], "mongo")
        self.assertEqual(trigger["metadata"]["consumerGroup"], "mongo-workers")
        self.assertEqual(trigger["metadata"]["allowIdleConsumers"], "false")
        self.assertEqual(trigger["authenticationRef"], {"name": "auth"})

    def test_keda_nonzero_ranges_omit_inapplicable_zero_controls(self):
        values = {**BASE, "gateway": {"autoscaling": {"mode": "keda"}}}
        docs = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        spec = docs["ScaledObject", "test-sink-gateway"]["spec"]
        self.assertNotIn("pollingInterval", spec)
        self.assertNotIn("cooldownPeriod", spec)
        values["gateway"]["autoscaling"]["keda"] = {"triggers": [
            {"type": "prometheus", "useCachedMetrics": True,
             "metadata": {"serverAddress": "http://prometheus", "query": "sum(queue_depth)", "threshold": "10"}}]}
        docs = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        self.assertEqual(docs["ScaledObject", "test-sink-gateway"]["spec"]["pollingInterval"], 30)

    def test_metrics_are_separate_from_public_service(self):
        values = {**BASE, "gateway": {"service": {"type": "LoadBalancer"}}, "metrics": {"enabled": True, "serviceMonitor": {"enabled": True}}}
        docs = self.manifests(values, "--api-versions", "monitoring.coreos.com/v1/ServiceMonitor")
        self.assertEqual([p["port"] for p in docs["Service", "test-sink-gateway"]["spec"]["ports"]], [8080])
        self.assertEqual(docs["Service", "test-sink-gateway-metrics"]["spec"]["type"], "ClusterIP")
        self.assertIn(("ServiceMonitor", "test-sink"), docs)

    def test_long_names_remain_unique(self):
        values = {"fullnameOverride": "x" * 80, "stores": {"first": {"state": "active"}, "second": {"state": "active"}}}
        docs = self.manifests(values)
        for _, name in docs:
            self.assertLessEqual(len(name), 63)
        self.assertEqual(len([1 for kind, _ in docs if kind == "Deployment"]), 3)

    def test_examples(self):
        self.manifests({}, "-f", str(ROOT / "examples/cluster-values.yaml"))
        self.manifests({}, "-f", str(ROOT / "examples/cluster-values.yaml"), "-f", str(ROOT / "examples/keda-values.yaml"), "--api-versions", "keda.sh/v1alpha1/ScaledObject")

    def test_unsafe_and_misspelled_settings_fail(self):
        cases = [
            "mode=engine", "engineDefaults.replicaCount=1", "gateway.replicaCount=0",
            "engineDefaults.pod.preStopSeconds=94", "gateway.pod.terminationGracePeriodSeconds=100",
            "engineDefaults.pod.shutdownBudgetSeconds=60", "requestTimeoutSeconds=301",
            "stores.Bad.state=active", "stores.mongo.state=deleted", "engineDefaults.replicaCont=3",
            "engineDefaults.pod.resources.limits.memory=0Gi", "engineDefaults.pod.resources.requests.memory=2Gi",
            "engineDefaults.autoscaling.mode=keda", "metrics.serviceMonitor.enabled=true",
            "gateway.runtime.gateway.dns_refresh_interval=1s", "gateway.runtime.service.request.timeout=1s",
            "engineDefaults.autoscaling.mode=hpa,engineDefaults.autoscaling.minReplicas=5,engineDefaults.autoscaling.maxReplicas=4",
            "engineDefaults.autoscaling.mode=hpa,engineDefaults.autoscaling.scaleDown.periodSeconds=60",
            "engineDefaults.autoscaling.mode=hpa,engineDefaults.autoscaling.cpuUtilization=0",
            "stores.mongo.worker.enabled=true",
            "engineDefaults.pod.env[0].name=GOMEMLIMIT,engineDefaults.pod.env[0].value=100MiB",
        ]
        for settings in cases:
            with self.subTest(settings=settings):
                result = render(BASE, "--set", settings)
                self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_worker_overpartition_and_implicit_zero_fail(self):
        values = {"stores": {"mongo": {"state": "active", "kafka": KAFKA, "worker": {"enabled": True, "replicaCount": 9}}}}
        self.assertIn("exceeds Kafka partitions", render(values).stderr)
        values["stores"]["mongo"]["worker"]["replicaCount"] = 0
        self.assertIn("requires at least 1", render(values).stderr)

    def test_old_kubernetes_is_rejected(self):
        result = render(BASE, "--kube-version", "1.29.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("kubeVersion", result.stderr)


if __name__ == "__main__":
    unittest.main()
