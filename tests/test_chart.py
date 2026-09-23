"""Offline contract tests; no Kubernetes, Docker or backend connections."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "charts" / "sink"
STORAGE = {"driver": "mongodb", "mongodb": {"uriSecretRef": {"name": "mongo-v1", "key": "uri"}}}
BASE = {
    "stores": {"mongo": {"storage": STORAGE, "phase": "active"}},
    "defaults": {"worker": {"config": {"kafkaConsumer": {"groupId": "mongo-workers"}}}},
}
KAFKA = {"brokers": ["kafka:9092"], "topic": {"name": "mongo"}, "topicPolicy": {"partitions": 8}}


def render(values, *options):
    with tempfile.TemporaryDirectory() as directory:
        filename = Path(directory) / "values.yaml"
        filename.write_text(yaml.safe_dump(values))
        command = [
            os.environ.get("HELM", "helm"),
            "template",
            "test",
            str(CHART),
            "--namespace",
            "sink",
            "--kube-version",
            "1.30.0",
            "-f",
            str(filename),
            *options,
        ]
        return subprocess.run(command, capture_output=True, text=True, check=False)


class ChartTests(unittest.TestCase):
    def manifests(self, values=None, *options):
        result = render(BASE if values is None else values, *options)
        self.assertEqual(result.returncode, 0, result.stderr)
        manifests = {
            (doc["kind"], doc["metadata"]["name"]): doc for doc in yaml.safe_load_all(result.stdout) if doc
        }
        return manifests

    def test_default_image_tag_matches_app_version(self):
        metadata = yaml.safe_load((CHART / "Chart.yaml").read_text())
        values = yaml.safe_load((CHART / "values.yaml").read_text())
        self.assertEqual(values["image"]["tag"], metadata["appVersion"])

    def test_tuning_overlay_renders_application_contract(self):
        values = {}
        docs = self.manifests(
            values,
            "-f",
            str(ROOT / "examples/cluster-values.yaml"),
            "-f",
            str(ROOT / "examples/tuning-values.yaml"),
            "-f",
            str(ROOT / "examples/logging-values.yaml"),
        )
        configs = {
            role: yaml.safe_load(docs["ConfigMap", name]["data"]["sink.yaml"])
            for role, name in [
                ("gateway", "test-sink-gateway"),
                ("engine", "test-sink-mongo-engine"),
                ("worker", "test-sink-mongo-worker"),
            ]
        }
        request = {"max_operations": 1000}
        self.assertEqual(configs["gateway"]["request"], request)
        self.assertEqual(configs["gateway"]["forwarding"]["max_fanout"], 8)
        self.assertEqual(configs["gateway"]["forwarding"]["max_connections"], 256)
        self.assertEqual(configs["gateway"]["forwarding"]["idle_timeout"], "5m")
        merge = {
            "max_attempts": 3,
            "lua": {
                "timeout": "100ms",
                "max_source_bytes": "64KiB",
                "max_result_bytes": "16MiB",
                "max_cached_programs": 256,
                "max_instructions": 1000000,
            },
        }
        execution = {"merge": merge}
        for role in ["engine", "worker"]:
            self.assertEqual(configs[role]["execution"], execution)
            self.assertNotIn("merge", configs[role])
        grpc = {"address": ":8080", "max_receive_message_bytes": "64MiB", "max_send_message_bytes": "64MiB"}
        for role in ["gateway", "engine"]:
            self.assertEqual(configs[role]["grpc"], grpc)
        batching = {
            "max_wait": "2ms",
            "max_operations": 32,
            "max_bytes": "16MiB",
            "queue": {"max_operations": 10000, "max_bytes": "128MiB"},
        }
        self.assertEqual(configs["engine"]["batching"], batching)
        producer = {"max_buffered_bytes": "64MiB"}
        self.assertEqual(configs["engine"]["producer"], producer)
        consumer = {
            "group_id": "mongo-workers",
            "max_poll_records": 500,
            "processing_timeout": "20s",
            "retry": {"max_attempts": 10, "backoff": "100ms", "max_backoff": "10s"},
        }
        self.assertEqual(configs["worker"]["consumer"], consumer)

    def test_kafka_scaler_settings_do_not_change_store_or_pods(self):
        values = copy.deepcopy(BASE)
        worker = {"enabled": True}
        values["stores"]["mongo"].update(kafka=KAFKA, worker=worker)
        values["defaults"]["worker"]["autoscaling"] = {
            "mode": "keda",
            "keda": {"kafkaLag": {"targetLag": 500, "authenticationRef": {"name": "auth"}}},
        }
        original = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        values["stores"]["mongo"]["worker"]["autoscaling"] = {
            "keda": {"kafkaLag": {"targetLag": 25, "useCachedMetrics": False}},
        }
        changed = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        trigger = changed["ScaledObject", "test-sink-mongo-worker"]["spec"]["triggers"][0]
        self.assertEqual(trigger["metadata"]["lagThreshold"], "25")
        self.assertEqual(trigger["metadata"]["consumerGroup"], "mongo-workers")
        authentication = {"name": "auth"}
        self.assertEqual(trigger["authenticationRef"], authentication)
        self.assertNotIn("useCachedMetrics", trigger)
        for identity, doc in original.items():
            if identity[0] != "ScaledObject":
                self.assertEqual(doc, changed[identity], identity)
        inventory = json.loads(changed["ConfigMap", "test-sink-inventory"]["data"]["stores"])
        self.assertEqual(inventory["mongo"]["phase"], "active")
        self.assertEqual(inventory["mongo"]["kafka"]["deadLetterTopic"], "mongo.dlq")

    def test_removed_and_wrong_role_fields_are_rejected(self):
        cases = [
            "engineDefaults.replicas=2",
            "workerDefaults.enabled=false",
            "clusterDomain=other.local",
            "discovery.dnsRefreshSeconds=10",
            "lifecycle.enforceTransitions=false",
            "gateway.runtime.memory.max_bytes=512MiB",
            "gateway.replicaCount=2",
            "gateway.config.request.maxOperations=10",
            "gateway.config.grpc.address=:9000",
            "gateway.config.forwarding.dnsRefreshInterval=10s",
            "gateway.config.kafkaConsumer.groupId=x",
            "gateway.autoscaling.allowScaleToZero=false",
            "defaults.engine.autoscaling.keda.kafkaLag.targetLag=1",
            "defaults.engine.config.execution.merge.maxAttempts=3",
            "defaults.engine.pod.shutdownBudgetSeconds=150",
            "defaults.worker.config.batching.maxWait=2ms",
            "defaults.worker.config.grpc.maxSendMessageBytes=1MiB",
            "stores.mongo.state=active",
            "stores.mongo.kafka.lagThreshold=10",
            "stores.mongo.storage.mongodb.maxConcurrentWrites=64",
            "stores.mongo.storage.mongodb.maxConcurrentGroups=16",
            "defaults.engine.config.memory.high_watermark_percent=80",
            "defaults.engine.config.logging.failure_body=false",
        ]
        for setting in cases:
            with self.subTest(setting=setting):
                result = render(BASE, "--set", setting)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Additional property", result.stderr)

    def test_nested_scaling_overrides_validate_after_inheritance(self):
        values = copy.deepcopy(BASE)
        worker = {"enabled": True}
        values["stores"]["mongo"].update(kafka=KAFKA, worker=worker)
        values["defaults"]["worker"]["autoscaling"] = {
            "mode": "keda",
            "keda": {
                "fallback": {"failureThreshold": 3, "replicas": 2},
                "kafkaLag": {"authenticationRef": {"name": "auth"}},
            },
        }
        worker["autoscaling"] = {
            "behavior": {"scaleDown": {"stabilizationWindowSeconds": 600}},
            "keda": {
                "fallback": {"replicas": 3},
                "kafkaLag": {"authenticationRef": {"kind": "ClusterTriggerAuthentication"}},
            },
        }
        docs = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        spec = docs["ScaledObject", "test-sink-mongo-worker"]["spec"]
        expected_fallback = {"failureThreshold": 3, "replicas": 3}
        self.assertEqual(spec["fallback"], expected_fallback)
        expected_auth = {"name": "auth", "kind": "ClusterTriggerAuthentication"}
        self.assertEqual(spec["triggers"][0]["authenticationRef"], expected_auth)
        scale_down = spec["advanced"]["horizontalPodAutoscalerConfig"]["behavior"]["scaleDown"]
        self.assertEqual(scale_down["stabilizationWindowSeconds"], 600)
        self.assertEqual(scale_down["policies"][0]["periodSeconds"], 300)
        deleted_rules = copy.deepcopy(values)
        deleted_rules["defaults"]["worker"]["autoscaling"]["behavior"] = {"scaleDown": {"policies": None}}
        result = render(deleted_rules, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        self.assertIn("requires complete scaleDown rules", result.stderr)
        del values["defaults"]["worker"]["autoscaling"]["keda"]["fallback"]
        result = render(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        self.assertIn("fallback requires failureThreshold and replicas", result.stderr)
        del worker["autoscaling"]["keda"]["fallback"]
        del values["defaults"]["worker"]["autoscaling"]["keda"]["kafkaLag"]["authenticationRef"]
        result = render(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        self.assertIn("authenticationRef requires name", result.stderr)

    def test_cluster_domain_rollout_and_service_account_overrides(self):
        values = copy.deepcopy(BASE)
        values["cluster"] = {"domain": "example.internal"}
        values["defaults"]["engine"] = {
            "rollout": {
                "readinessCheck": "process",
                "restartRevision": "reload-2",
                "preStopDelaySeconds": 80,
            },
            "serviceAccount": {"annotations": {"example.org/identity": "engine"}},
            "pod": {"annotations": {"example.org/runtime_key": "preserved"}},
        }
        docs = self.manifests(values)
        config = yaml.safe_load(docs["ConfigMap", "test-sink-gateway"]["data"]["sink.yaml"])
        self.assertIn(".svc.example.internal:8080", config["forwarding"]["routes"][0]["target"])
        pod = docs["Deployment", "test-sink-mongo-engine"]["spec"]["template"]
        self.assertEqual(pod["metadata"]["annotations"]["sink.batchstream.io/config-revision"], "reload-2")
        self.assertEqual(pod["metadata"]["annotations"]["example.org/runtime_key"], "preserved")
        self.assertEqual(pod["spec"]["terminationGracePeriodSeconds"], 240)
        self.assertEqual(pod["spec"]["containers"][0]["readinessProbe"]["httpGet"]["path"], "/readyz")
        annotations = {"example.org/identity": "engine"}
        self.assertEqual(
            docs["ServiceAccount", "test-sink-mongo-engine"]["metadata"]["annotations"], annotations
        )

    def test_integration_scenarios_use_valid_values(self):
        spec = importlib.util.spec_from_file_location(
            "chart_qualification", ROOT / "tests/integration/run.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            module.ROOT = Path(directory)
            for scenario in ["all", "upgrades"]:
                args = SimpleNamespace(
                    cluster="sink-chart-values-test", kubeconfig=None, sink_image=None, scenario=scenario
                )
                qualification = module.Qualification(args, directory)
                self.manifests(qualification.values)

    def test_logging_inheritance_outputs_and_rollout(self):
        values = copy.deepcopy(BASE)
        worker_settings = {"enabled": True}
        values["stores"]["mongo"].update(kafka=KAFKA, worker=worker_settings)
        values["stores"]["archive"] = {"storage": STORAGE, "phase": "staged"}
        original = self.manifests(values)
        for doc in original.values():
            if doc["kind"] == "ConfigMap" and "sink.yaml" in doc.get("data", {}):
                self.assertNotIn("logging", yaml.safe_load(doc["data"]["sink.yaml"]))
        logging = {
            "level": "warn",
            "console": {"enabled": True, "format": "json"},
            "labels": {"environment": "test", "cluster": "fixture"},
            "includeFailureBody": True,
            "maxBodyBytes": "32KiB",
            "otlp": {
                "enabled": True,
                "protocol": "grpc",
                "endpoint": "collector:4317",
                "tls": {"enabled": True},
                "queueSize": 256,
                "batchSize": 32,
                "flushInterval": "500ms",
                "exportTimeout": "2s",
                "shutdownTimeout": "5s",
            },
        }
        for role in ["gateway", "engine", "worker"]:
            settings = (
                values.setdefault("gateway", {})
                if role == "gateway"
                else values["defaults"].setdefault(role, {})
            )
            settings.update({"config": {"logging": copy.deepcopy(logging)}})
            if role == "worker":
                settings["config"]["kafkaConsumer"] = {"groupId": "mongo-workers"}
        engine_override = {
            "includeFailureBody": False,
            "console": {"enabled": False},
            "otlp": {"tls": {"enabled": False}},
        }
        values["stores"]["mongo"]["engine"] = {"config": {"logging": engine_override}}
        worker_override = {"otlp": {"enabled": False}, "level": "error"}
        values["stores"]["mongo"]["worker"]["config"] = {"logging": worker_override}
        docs = self.manifests(values)
        expected_logging = {
            "level": "warn",
            "console": {"enabled": True, "format": "json"},
            "labels": {"environment": "test", "cluster": "fixture"},
            "failure_body": True,
            "max_body_bytes": "32KiB",
            "otlp": {
                "enabled": True,
                "protocol": "grpc",
                "endpoint": "collector:4317",
                "tls": {"enabled": True},
                "queue_size": 256,
                "batch_size": 32,
                "flush_interval": "500ms",
                "export_timeout": "2s",
                "shutdown_timeout": "5s",
            },
        }
        engine = copy.deepcopy(expected_logging)
        engine["failure_body"] = False
        engine["console"]["enabled"] = False
        engine["otlp"]["tls"]["enabled"] = False
        worker = copy.deepcopy(expected_logging)
        worker["otlp"]["enabled"] = False
        worker["level"] = "error"
        for name, expected in [
            ("test-sink-gateway", expected_logging),
            ("test-sink-archive-engine", expected_logging),
            ("test-sink-mongo-engine", engine),
            ("test-sink-mongo-worker", worker),
        ]:
            config = yaml.safe_load(docs["ConfigMap", name]["data"]["sink.yaml"])
            self.assertEqual(config["logging"], expected)
            before = original["Deployment", name]["spec"]["template"]["metadata"]["annotations"]
            after = docs["Deployment", name]["spec"]["template"]["metadata"]["annotations"]
            self.assertNotEqual(
                before["sink.batchstream.io/config-checksum"], after["sink.batchstream.io/config-checksum"]
            )
        values["stores"]["mongo"]["engine"]["config"]["logging"]["level"] = "debug"
        changed = self.manifests(values)
        for name in ["test-sink-gateway", "test-sink-mongo-worker", "test-sink-archive-engine"]:
            self.assertEqual(docs["Deployment", name], changed["Deployment", name])

    def test_logging_identity_uses_downward_api_without_duplicate_overrides(self):
        values = copy.deepcopy(BASE)
        worker_settings = {"enabled": True}
        values["stores"]["mongo"].update(kafka=KAFKA, worker=worker_settings)
        values["defaults"]["engine"] = {"pod": {"env": [{"name": "POD_NAME", "value": "explicit-pod"}]}}
        docs = self.manifests(values)
        fields = {
            "POD_UID": "metadata.uid",
            "POD_NAME": "metadata.name",
            "POD_NAMESPACE": "metadata.namespace",
            "NODE_NAME": "spec.nodeName",
        }
        for name in ["test-sink-gateway", "test-sink-mongo-engine", "test-sink-mongo-worker"]:
            env = docs["Deployment", name]["spec"]["template"]["spec"]["containers"][0]["env"]
            indexed = {entry["name"]: entry for entry in env}
            self.assertEqual(len(indexed), len(env))
            for key, field in fields.items():
                if key == "POD_NAME" and name.endswith("engine"):
                    self.assertEqual(indexed[key]["value"], "explicit-pod")
                else:
                    self.assertEqual(indexed[key]["valueFrom"]["fieldRef"]["fieldPath"], field)

    def test_logging_rejects_invalid_settings(self):
        invalid = [
            {"level": "trace"},
            {"components": {"rpc": "debug"}},
            {"components": {}},
            {"console": {"format": "yaml"}},
            {"console": {"enabled": False}},
            {"unknown": True},
            {"labels": {"tenant": "test"}},
            {"labels": {"pod": "line\nbreak"}},
            {"labels": {"pod": "雪" * 86}},
            {"labels": {"cluster": 42}},
            {"includeFailureBody": "true"},
            {"maxBodyBytes": 1023},
            {"maxBodyBytes": 65537},
            {"maxBodyBytes": "0.5KiB"},
            {"maxBodyBytes": "65KiB"},
            {"maxBodyBytes": "1.0001KiB"},
            {"otlp": {"enabled": True}},
            {"otlp": {"endpoint": "https://collector:4317"}},
            {"otlp": {"endpoint": "user@collector:4317"}},
            {"otlp": {"endpoint": "collector:0"}},
            {"otlp": {"endpoint": "collector:65536"}},
            {"otlp": {"endpoint": "collector:4317/logs"}},
            {"otlp": {"headers": {"authorization": "unsupported"}}},
            {"otlp": {"protocol": "http"}},
            {"otlp": {"tls": {"insecure": True}}},
            {"otlp": {"queueSize": 0}},
            {"otlp": {"queueSize": 8193}},
            {"otlp": {"batchSize": 513}},
            {"otlp": {"queueSize": 127}},
            {"otlp": {"batchSize": 0}},
            {"otlp": {"flushInterval": "61s"}},
            {"otlp": {"exportTimeout": "0s"}},
            {"otlp": {"shutdownTimeout": "30s1ns"}},
            {"otlp": {"exportTimeout": "1m"}},
            {"otlp": {"flushInterval": "garbage"}},
            {"otlp": {"exportTimeout": "-1s"}},
        ]
        for logging in invalid:
            with self.subTest(logging=logging):
                values = copy.deepcopy(BASE)
                values["defaults"]["engine"] = {"config": {"logging": logging}}
                result = render(values)
                self.assertNotEqual(result.returncode, 0, logging)
                self.assertIn("logging", result.stderr)
        for role in ["gateway", "engine", "worker"]:
            values = copy.deepcopy(BASE)
            worker_settings = {"enabled": True}
            values["stores"]["mongo"].update(kafka=KAFKA, worker=worker_settings)
            settings = (
                values.setdefault("gateway", {})
                if role == "gateway"
                else values["defaults"].setdefault(role, {})
            )
            settings.update({"config": {"logging": {"otlp": {"enabled": True}}}})
            self.assertIn("logging.otlp.endpoint", render(values).stderr)

    def test_logging_accepts_units_protocols_and_effective_store_settings(self):
        values = copy.deepcopy(BASE)
        logging = {
            "console": {"enabled": False},
            "maxBodyBytes": "1.5 KiB",
            "labels": {"pod": "雪" * 85},
            "otlp": {
                "enabled": True,
                "endpoint": "[::1]:4318",
                "protocol": "http/protobuf",
                "tls": {"enabled": False},
                "flushInterval": "1m",
                "exportTimeout": "+1.5s",
                "shutdownTimeout": "1s500ms",
                "queueSize": 16,
            },
        }
        values["defaults"]["engine"] = {"config": {"logging": logging}}
        values["stores"]["mongo"]["engine"] = {"config": {"logging": {"otlp": {"batchSize": 8}}}}
        docs = self.manifests(values)
        config = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-engine"]["data"]["sink.yaml"])
        self.assertEqual(config["logging"]["otlp"]["batch_size"], 8)
        values["stores"]["mongo"]["engine"]["config"]["logging"]["otlp"]["enabled"] = False
        self.assertIn("logging requires console or OTLP", render(values).stderr)
        values["defaults"]["engine"]["config"]["logging"]["console"]["enabled"] = True
        values["stores"]["mongo"]["engine"]["config"]["logging"]["otlp"]["endpoint"] = ""
        self.manifests(values)

    def test_logging_shutdown_budget_includes_exporter_flush(self):
        values = copy.deepcopy(BASE)
        values["gateway"] = {
            "pod": {},
            "config": {
                "logging": {
                    "otlp": {"enabled": True, "endpoint": "collector:4317", "shutdownTimeout": "5.5s"}
                }
            },
            "rollout": {"shutdownBudgetSeconds": 125},
        }
        self.assertIn("plus logging.otlp.shutdownTimeout (126 seconds)", render(values).stderr)
        values["gateway"]["rollout"]["shutdownBudgetSeconds"] = 126
        self.manifests(values)
        values["gateway"]["config"]["logging"]["otlp"]["enabled"] = False
        values["gateway"]["rollout"]["shutdownBudgetSeconds"] = 120
        self.manifests(values)

    def test_shared_image_and_pull_secrets_apply_to_every_workload(self):
        values = copy.deepcopy(BASE)
        worker = {"enabled": True}
        values["stores"]["mongo"].update(kafka=KAFKA, worker=worker)
        values["stores"]["archive"] = {"storage": STORAGE, "phase": "staged"}
        original = self.manifests(values)
        pull_secrets = [{"name": "registry-primary"}, {"name": "registry-backup"}]
        values["image"] = {
            "repository": "registry.example.invalid/sink",
            "tag": "candidate",
            "pullPolicy": "Always",
            "pullSecrets": pull_secrets,
        }
        changed = self.manifests(values)
        deployments = [identity for identity in changed if identity[0] == "Deployment"]
        self.assertEqual(len(deployments), 4)
        for identity in deployments:
            before = original[identity]["spec"]["template"]["spec"]
            pod = changed[identity]["spec"]["template"]["spec"]
            self.assertNotIn("imagePullSecrets", before)
            self.assertEqual(pod["imagePullSecrets"], pull_secrets)
            self.assertEqual(pod["containers"][0]["image"], "registry.example.invalid/sink:candidate")
            self.assertEqual(pod["containers"][0]["imagePullPolicy"], "Always")
        values["image"]["pullSecrets"] = []
        without_secrets = self.manifests(values)
        for identity in deployments:
            self.assertNotIn("imagePullSecrets", without_secrets[identity]["spec"]["template"]["spec"])
        del values["image"]
        self.assertEqual(self.manifests(values), original)

    def test_shared_image_digest_takes_precedence_over_tag(self):
        values = copy.deepcopy(BASE)
        worker = {"enabled": True}
        values["stores"]["mongo"].update(kafka=KAFKA, worker=worker)
        digest = "sha256:" + "2" * 64
        values["image"] = {
            "repository": "registry.example.invalid/sink",
            "tag": "candidate",
            "digest": digest,
        }
        docs = self.manifests(values)
        for identity, doc in docs.items():
            if identity[0] == "Deployment":
                self.assertEqual(
                    doc["spec"]["template"]["spec"]["containers"][0]["image"],
                    "registry.example.invalid/sink@" + digest,
                )
        values["image"]["digest"] = ""
        docs = self.manifests(values)
        for identity, doc in docs.items():
            if identity[0] == "Deployment":
                self.assertEqual(
                    doc["spec"]["template"]["spec"]["containers"][0]["image"],
                    "registry.example.invalid/sink:candidate",
                )
        values["image"]["digest"] = "invalid"
        self.assertNotEqual(render(values).returncode, 0)

    def test_component_images_and_top_level_pull_secrets_are_rejected(self):
        for path in [
            ("gateway",),
            ("defaults", "engine"),
            ("defaults", "worker"),
            ("stores", "mongo", "engine"),
            ("stores", "mongo", "worker"),
        ]:
            with self.subTest(path=path):
                values = copy.deepcopy(BASE)
                component = values
                for key in path:
                    if key not in component:
                        component[key] = {}
                    component = component[key]
                component["image"] = {}
                result = render(values)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Additional property image", result.stderr)
        values = copy.deepcopy(BASE)
        values["imagePullSecrets"] = []
        result = render(values)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Additional property imagePullSecrets", result.stderr)
        del values["imagePullSecrets"]
        for refs in [[{}], [{"name": ""}], [{"name": "registry", "namespace": "other"}], "registry"]:
            with self.subTest(refs=refs):
                values["image"] = {"pullSecrets": refs}
                self.assertNotEqual(render(values).returncode, 0)

    def test_memory_capacity_is_optional_and_configurable_per_role(self):
        defaults = self.manifests()
        default_config = yaml.safe_load(defaults["ConfigMap", "test-sink-gateway"]["data"]["sink.yaml"])
        self.assertNotIn("memory", default_config)
        values = copy.deepcopy(BASE)
        values["gateway"] = {"config": {"memory": {"highWatermarkPercent": 90}}}
        values["defaults"]["engine"] = {
            "config": {"memory": {"maxBytes": "768MiB", "highWatermarkPercent": 80}}
        }
        values["stores"]["mongo"]["engine"] = {"config": {"memory": {"lowWatermarkPercent": 65}}}
        docs = self.manifests(values)
        gateway = yaml.safe_load(docs["ConfigMap", "test-sink-gateway"]["data"]["sink.yaml"])
        engine = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-engine"]["data"]["sink.yaml"])
        gateway_expected = {"high_watermark_percent": 90}
        self.assertEqual(gateway["memory"], gateway_expected)
        engine_expected = {"max_bytes": "768MiB", "high_watermark_percent": 80, "low_watermark_percent": 65}
        self.assertEqual(engine["memory"], engine_expected)
        values["gateway"]["config"]["memory"]["highWatermarkPercent"] = 100
        self.assertNotEqual(render(values).returncode, 0)
        values["gateway"]["config"]["memory"]["highWatermarkPercent"] = 70
        self.assertNotEqual(render(values).returncode, 0)

    def test_empty_and_staged_cluster(self):
        empty = self.manifests({})
        self.assertEqual(list(empty), [("ConfigMap", "test-sink-inventory")])
        staged = self.manifests({"stores": {"mongo": {"storage": STORAGE, "phase": "staged"}}})
        self.assertIn(("Deployment", "test-sink-mongo-engine"), staged)
        self.assertNotIn(("Deployment", "test-sink-gateway"), staged)

    def test_discovery_shutdown_security_and_memory(self):
        docs = self.manifests()
        for role, name, delay, grace in [
            ("engine", "test-sink-mongo-engine", 65, 225),
            ("gateway", "test-sink-gateway", 70, 230),
        ]:
            deployment = docs["Deployment", name]
            pod = deployment["spec"]["template"]
            container = pod["spec"]["containers"][0]
            self.assertEqual(
                container["args"],
                ["--config", "/etc/sink/sink.yaml"]
                + (["--store-config", "/etc/sink/store.yaml"] if role == "engine" else []),
            )
            self.assertEqual(container["lifecycle"]["preStop"]["sleep"]["seconds"], delay)
            self.assertEqual(pod["spec"]["terminationGracePeriodSeconds"], grace)
            self.assertEqual(
                deployment["spec"]["strategy"]["rollingUpdate"], {"maxUnavailable": 0, "maxSurge": 1}
            )
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
        headless = docs["Service", "test-sink-gateway-headless"]
        self.assertEqual(headless["spec"]["clusterIP"], "None")
        self.assertFalse(headless["spec"]["publishNotReadyAddresses"])
        self.assertEqual(
            headless["spec"]["selector"], docs["Service", "test-sink-gateway"]["spec"]["selector"]
        )
        self.assertFalse(any(kind == "Secret" for kind, _ in docs))

    def test_routes_are_only_active_and_config_changes_roll_gateway(self):
        values = copy.deepcopy(BASE)
        values["stores"].update(
            {
                "archive": {"storage": STORAGE, "phase": "staged"},
                "old": {"storage": STORAGE, "phase": "retiring"},
            }
        )
        first = self.manifests(values)
        config = yaml.safe_load(first["ConfigMap", "test-sink-gateway"]["data"]["sink.yaml"])
        self.assertEqual(
            config["forwarding"]["routes"],
            [
                {
                    "store": "mongo",
                    "target": "dns:///test-sink-mongo-engine.sink.svc.cluster.local:8080",
                    "tls": {"insecure": True},
                }
            ],
        )
        values["stores"]["archive"]["phase"] = "active"
        second = self.manifests(values)
        a = first["Deployment", "test-sink-gateway"]["spec"]["template"]["metadata"]["annotations"]
        b = second["Deployment", "test-sink-gateway"]["spec"]["template"]["metadata"]["annotations"]
        self.assertNotEqual(
            a["sink.batchstream.io/config-checksum"], b["sink.batchstream.io/config-checksum"]
        )
        self.assertEqual(
            first["Deployment", "test-sink-mongo-engine"], second["Deployment", "test-sink-mongo-engine"]
        )

    def test_per_store_merge_does_not_leak(self):
        values = copy.deepcopy(BASE)
        values["stores"]["archive"] = {
            "storage": STORAGE,
            "phase": "staged",
            "engine": {
                "pod": {"resources": {"limits": {"memory": "2Gi"}}},
                "rollout": {"restartRevision": "v2"},
            },
        }
        docs = self.manifests(values)
        archive = docs["Deployment", "test-sink-archive-engine"]["spec"]["template"]["spec"]
        mongo = docs["Deployment", "test-sink-mongo-engine"]["spec"]["template"]["spec"]
        self.assertEqual(archive["containers"][0]["resources"]["limits"]["memory"], "2Gi")
        self.assertEqual(mongo["containers"][0]["resources"]["limits"]["memory"], "1Gi")
        self.assertEqual(
            archive["volumes"][0]["projected"]["sources"],
            [
                {"configMap": {"name": "test-sink-archive-engine"}},
                {"configMap": {"name": "test-sink-archive-store"}},
            ],
        )

    def test_shared_secret_projection_and_runtime(self):
        values = copy.deepcopy(BASE)
        async_config = {"kafka": KAFKA, "worker": {"enabled": True}}
        values["stores"]["mongo"].update(async_config)
        docs = self.manifests(values)
        configs = []
        for role in ["engine", "worker"]:
            name = f"test-sink-mongo-{role}"
            pod = docs["Deployment", name]["spec"]["template"]
            projection = pod["spec"]["volumes"][1]["projected"]
            self.assertEqual(projection["defaultMode"], 0o440)
            self.assertEqual(
                projection["sources"],
                [
                    {
                        "secret": {
                            "name": "mongo-v1",
                            "optional": False,
                            "items": [{"key": "uri", "path": "mongodb-uri"}],
                        }
                    }
                ],
            )
            config = yaml.safe_load(docs["ConfigMap", name]["data"]["sink.yaml"])
            self.assertEqual(config["mode"], role)
            self.assertNotIn("storage", config)
            self.assertNotIn("request", config)
            self.assertNotIn("service", config)
            if role == "worker":
                self.assertEqual(config["consumer"]["group_id"], "mongo-workers")
                self.assertNotIn("grpc", config)
            sources = pod["spec"]["volumes"][0]["projected"]["sources"]
            self.assertEqual(sources[1]["configMap"]["name"], "test-sink-mongo-store")
            shared = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-store"]["data"]["store.yaml"])
            self.assertEqual(shared["name"], "mongo")
            self.assertEqual(shared["storage"]["mongodb"]["uri_file"], "/etc/sink-secrets/mongodb-uri")
            self.assertNotIn("max_concurrent_writes", shared["storage"]["mongodb"])
            self.assertNotIn("max_concurrent_groups", shared["storage"]["mongodb"])
            self.assertNotIn("consumer", shared["kafka"])
            self.assertEqual(shared["kafka"]["replication_factor"], 3)
            self.assertEqual(shared["kafka"]["min_insync_replicas"], 1)
            self.assertEqual(shared["kafka"]["topic"], {"name": "mongo"})
            configs.append(shared)
        self.assertEqual(*configs)
        gateway = docs["Deployment", "test-sink-gateway"]
        self.assertEqual(len(gateway["spec"]["template"]["spec"]["volumes"]), 1)
        values["stores"]["mongo"]["credentialRevision"] = "v2"
        rotated = self.manifests(values)
        for role in ["engine", "worker"]:
            name = f"test-sink-mongo-{role}"
            self.assertNotEqual(
                docs["Deployment", name]["spec"]["template"], rotated["Deployment", name]["spec"]["template"]
            )
        self.assertEqual(gateway, rotated["Deployment", "test-sink-gateway"])
        values["stores"]["mongo"]["storage"]["mongodb"]["uriSecretRef"]["name"] = "mongo-v2"
        versioned = self.manifests(values)
        for role in ["engine", "worker"]:
            name = f"test-sink-mongo-{role}"
            self.assertNotEqual(
                rotated["Deployment", name]["spec"]["template"],
                versioned["Deployment", name]["spec"]["template"],
            )
        values["stores"]["mongo"]["storage"]["mongodb"]["metadataField"] = "custom_metadata"
        tuned = self.manifests(values)
        shared = yaml.safe_load(tuned["ConfigMap", "test-sink-mongo-store"]["data"]["store.yaml"])
        self.assertEqual(shared["storage"]["mongodb"]["metadata_field"], "custom_metadata")
        for role in ["engine", "worker"]:
            name = f"test-sink-mongo-{role}"
            config = yaml.safe_load(tuned["ConfigMap", name]["data"]["sink.yaml"])
            self.assertNotIn("mongodb", config.get("execution", {}))
            self.assertNotEqual(
                versioned["Deployment", name]["spec"]["template"]["metadata"]["annotations"],
                tuned["Deployment", name]["spec"]["template"]["metadata"]["annotations"],
            )

    def test_advanced_tuning_is_generated_from_values(self):
        values = copy.deepcopy(BASE)
        values["metrics"] = {"enabled": True}
        values["defaults"]["engine"] = {
            "config": {"grpc": {"maxReceiveMessageBytes": "8MiB"}, "merge": {"maxAttempts": 5}}
        }
        values["stores"]["mongo"]["engine"] = {"config": {"batching": {"maxWait": "5ms"}}}
        values["stores"]["mongo"]["worker"] = {
            "enabled": True,
            "config": {"kafkaConsumer": {"processingTimeout": "20s"}, "merge": {"maxAttempts": 7}},
        }
        values["stores"]["mongo"]["kafka"] = {**KAFKA, "deadLetterTopic": {"retention": "720h"}}
        docs = self.manifests(values)
        engine = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-engine"]["data"]["sink.yaml"])
        worker = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-worker"]["data"]["sink.yaml"])
        self.assertEqual(engine["execution"]["merge"]["max_attempts"], 5)
        self.assertEqual(engine["batching"]["max_wait"], "5ms")
        self.assertEqual(worker["execution"]["merge"]["max_attempts"], 7)
        self.assertNotIn("batching", worker)
        self.assertEqual(engine["grpc"]["max_receive_message_bytes"], "8MiB")
        self.assertEqual(worker["consumer"]["processing_timeout"], "20s")
        self.assertTrue(engine["prometheus"]["enabled"])
        self.assertTrue(worker["prometheus"]["enabled"])

    def test_search_credentials_and_rejected_plaintext(self):
        for driver in ["elasticsearch", "opensearch"]:
            for auth in [
                {},
                {"username": "sink", "passwordSecretRef": {"name": "search", "key": "password"}},
                {
                    "usernameSecretRef": {"name": "search", "key": "user"},
                    "passwordSecretRef": {"name": "search", "key": "pass"},
                },
                {"apiKeySecretRef": {"name": "search", "key": "api-key"}},
            ]:
                search = {"endpoints": ["https://search:9200"], **auth}
                values = {
                    "stores": {"search": {"phase": "active", "storage": {"driver": driver, "search": search}}}
                }
                docs = self.manifests(values)
                config = yaml.safe_load(docs["ConfigMap", "test-sink-search-store"]["data"]["store.yaml"])
                self.assertNotIn("SecretRef", json.dumps(config))
                expected = {"endpoints": ["https://search:9200"]}
                if "username" in auth:
                    expected["username"] = "sink"
                for key, runtime, filename in [
                    ("usernameSecretRef", "username_file", "search-username"),
                    ("passwordSecretRef", "password_file", "search-password"),
                    ("apiKeySecretRef", "api_key_file", "search-api-key"),
                ]:
                    if key in auth:
                        expected[runtime] = f"/etc/sink-secrets/{filename}"
                self.assertEqual(config["storage"]["search"], expected)
                sources = docs["Deployment", "test-sink-search-engine"]["spec"]["template"]["spec"]["volumes"]
                self.assertEqual(len(sources), 2 if auth else 1)
        bad_storage = [
            {"driver": "opensearch", "search": {"endpoints": ["https://user:password@search"]}},
            {"driver": "mongodb", "mongodb": {"uri": "mongodb://inline"}},
            {"driver": "mongodb", "mongodb": {"uriSecretRef": {"name": "mongo"}}},
            {"driver": "mongodb", "mongodb": {"uriSecretRef": {"name": "mongo", "key": "../uri"}}},
            {
                "driver": "mongodb",
                "mongodb": {"uriSecretRef": {"name": "mongo", "key": "uri", "namespace": "other"}},
            },
            {"driver": "opensearch", "search": {"endpoints": ["http://search"], "password": "inline"}},
            {"driver": "opensearch", "search": {"endpoints": ["http://search"], "username": "sink"}},
            {
                "driver": "opensearch",
                "search": {
                    "endpoints": ["http://search"],
                    "username": "sink",
                    "usernameSecretRef": {"name": "s", "key": "u"},
                },
            },
            {
                "driver": "opensearch",
                "search": {
                    "endpoints": ["http://search"],
                    "username": "sink",
                    "passwordSecretRef": {"name": "s", "key": "p"},
                    "apiKeySecretRef": {"name": "s", "key": "a"},
                },
            },
        ]
        for storage in bad_storage:
            values = {"stores": {"test": {"phase": "staged", "storage": storage}}}
            result = render(values)
            self.assertNotEqual(result.returncode, 0, storage)
        for override in [
            "defaults.engine.config.existingSecret=old",
            "defaults.engine.config.grpc.address=:9999",
            "defaults.worker.config.service.requestLimits.timeout=1s",
        ]:
            values = copy.deepcopy(BASE)
            async_config = {"kafka": KAFKA, "worker": {"enabled": True}}
            values["stores"]["mongo"].update(async_config)
            self.assertNotEqual(render(values, "--set", override).returncode, 0)

    def test_budget_recalculates_with_dns(self):
        values = {**BASE, "cluster": {"discovery": {"dnsRefreshSeconds": 40, "dnsCacheTTLSeconds": 60}}}
        doc = self.manifests(values)["Deployment", "test-sink-mongo-engine"]
        container = doc["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["lifecycle"]["preStop"]["sleep"]["seconds"], 125)
        self.assertEqual(doc["spec"]["template"]["spec"]["terminationGracePeriodSeconds"], 285)
        self.assertEqual(doc["spec"]["minReadySeconds"], 115)

    def test_controller_owns_replicas_and_hpa_behavior(self):
        behavior = {
            "scaleUp": {
                "stabilizationWindowSeconds": 0,
                "selectPolicy": "Max",
                "policies": [{"type": "Percent", "value": 100, "periodSeconds": 60}],
            },
            "scaleDown": {
                "stabilizationWindowSeconds": 600,
                "selectPolicy": "Min",
                "policies": [{"type": "Pods", "value": 1, "periodSeconds": 300}],
            },
        }
        values = {
            **BASE,
            "defaults": {
                **BASE["defaults"],
                "engine": {
                    "autoscaling": {"mode": "hpa", "minReplicas": 3, "maxReplicas": 7, "behavior": behavior}
                },
            },
        }
        docs = self.manifests(values)
        self.assertNotIn("replicas", docs["Deployment", "test-sink-mongo-engine"]["spec"])
        hpa = docs["HorizontalPodAutoscaler", "test-sink-mongo-engine"]["spec"]
        self.assertEqual((hpa["minReplicas"], hpa["maxReplicas"]), (3, 7))
        self.assertEqual(
            hpa["behavior"]["scaleDown"]["policies"], [{"type": "Pods", "value": 1, "periodSeconds": 300}]
        )
        self.assertEqual(hpa["behavior"]["scaleDown"]["stabilizationWindowSeconds"], 600)
        self.assertEqual(docs["Deployment", "test-sink-gateway"]["spec"]["replicas"], 2)

    def test_single_engine_is_explicit_and_never_zero(self):
        values = {
            **BASE,
            "defaults": {
                **BASE["defaults"],
                "engine": {"replicas": 1, "podDisruptionBudget": {"maxUnavailable": 0}},
            },
        }
        docs = self.manifests(values)
        self.assertEqual(docs["Deployment", "test-sink-mongo-engine"]["spec"]["replicas"], 1)
        self.assertEqual(docs["PodDisruptionBudget", "test-sink-mongo-engine"]["spec"]["maxUnavailable"], 0)
        values["defaults"]["engine"]["replicas"] = 0
        self.assertIn("engine/mongo requires at least 1 replicas", render(values).stderr)

    def test_scale_down_policy_must_cover_termination(self):
        values = copy.deepcopy(BASE)
        values["defaults"]["engine"] = {
            "autoscaling": {
                "mode": "hpa",
                "behavior": {
                    "scaleUp": {
                        "stabilizationWindowSeconds": 0,
                        "selectPolicy": "Max",
                        "policies": [{"type": "Pods", "value": 2, "periodSeconds": 60}],
                    },
                    "scaleDown": {
                        "stabilizationWindowSeconds": 300,
                        "selectPolicy": "Min",
                        "policies": [{"type": "Pods", "value": 1, "periodSeconds": 60}],
                    },
                },
            }
        }
        result = render(values)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("scaleDown policy periodSeconds must cover termination grace (225)", result.stderr)

    def test_keda_worker_zero_and_partition_trigger(self):
        values = {
            "stores": {
                "mongo": {
                    "storage": STORAGE,
                    "phase": "active",
                    "kafka": KAFKA,
                    "worker": {
                        "enabled": True,
                        "podDisruptionBudget": {"enabled": False},
                        "autoscaling": {
                            "mode": "keda",
                            "minReplicas": 0,
                            "maxReplicas": 8,
                            "keda": {
                                "initialCooldownPeriod": 60,
                                "restoreToOriginalReplicaCount": True,
                                "fallback": {
                                    "failureThreshold": 3,
                                    "replicas": 2,
                                    "behavior": "currentReplicasIfHigher",
                                },
                                "kafkaLag": {
                                    "name": "pse-search-lag",
                                    "useCachedMetrics": True,
                                    "authenticationRef": {"name": "auth"},
                                },
                            },
                            "allowScaleToZero": True,
                        },
                        "config": {"kafkaConsumer": {"groupId": "mongo-workers"}},
                    },
                }
            }
        }
        docs = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        name = "test-sink-mongo-worker"
        deployment = docs["Deployment", name]
        self.assertNotIn("replicas", deployment["spec"])
        self.assertNotIn("lifecycle", deployment["spec"]["template"]["spec"]["containers"][0])
        self.assertNotIn(("Service", name), docs)
        self.assertNotIn(("PodDisruptionBudget", name), docs)
        spec = docs["ScaledObject", name]["spec"]
        self.assertEqual(spec["minReplicaCount"], 0)
        self.assertEqual(spec["initialCooldownPeriod"], 60)
        self.assertTrue(spec["advanced"]["restoreToOriginalReplicaCount"])
        self.assertEqual(spec["fallback"]["behavior"], "currentReplicasIfHigher")
        trigger = spec["triggers"][0]
        self.assertEqual(trigger["name"], "pse-search-lag")
        self.assertTrue(trigger["useCachedMetrics"])
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
        values["gateway"]["autoscaling"]["keda"] = {
            "triggers": [
                {
                    "type": "prometheus",
                    "useCachedMetrics": True,
                    "metadata": {
                        "serverAddress": "http://prometheus",
                        "query": "sum(queue_depth)",
                        "threshold": "10",
                    },
                }
            ]
        }
        docs = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        self.assertEqual(docs["ScaledObject", "test-sink-gateway"]["spec"]["pollingInterval"], 30)

    def test_keda_production_controls_and_mixed_fallback(self):
        triggers = [
            {"type": "cpu", "name": "cpu", "metricType": "Utilization", "metadata": {"value": "70"}},
            {"type": "memory", "name": "memory", "metricType": "Utilization", "metadata": {"value": "80"}},
            {
                "type": "prometheus",
                "name": "execution-budget",
                "metricType": "AverageValue",
                "useCachedMetrics": True,
                "metadata": {
                    "serverAddress": "http://prometheus",
                    "query": "sum(sink_memory_used_bytes)",
                    "threshold": "134217728",
                    "ignoreNullValues": "false",
                },
            },
        ]
        values = {
            **BASE,
            "defaults": {
                **BASE["defaults"],
                "engine": {
                    "autoscaling": {
                        "mode": "keda",
                        "minReplicas": 2,
                        "maxReplicas": 10,
                        "keda": {
                            "annotations": {"autoscaling.keda.sh/paused": "false"},
                            "hpaName": "sink-mongo",
                            "restoreToOriginalReplicaCount": True,
                            "triggers": triggers,
                            "fallback": {
                                "failureThreshold": 3,
                                "replicas": 2,
                                "behavior": "currentReplicasIfHigher",
                            },
                        },
                    }
                },
            },
        }
        docs = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        scaled = docs["ScaledObject", "test-sink-mongo-engine"]
        self.assertEqual(scaled["metadata"]["annotations"], {"autoscaling.keda.sh/paused": "false"})
        self.assertEqual(scaled["spec"]["advanced"]["horizontalPodAutoscalerConfig"]["name"], "sink-mongo")
        self.assertTrue(scaled["spec"]["advanced"]["restoreToOriginalReplicaCount"])
        self.assertEqual(scaled["spec"]["fallback"]["behavior"], "currentReplicasIfHigher")
        self.assertEqual(
            [trigger["name"] for trigger in scaled["spec"]["triggers"]], ["cpu", "memory", "execution-budget"]
        )

    def test_keda_fallback_requires_supported_trigger(self):
        values = {
            **BASE,
            "defaults": {
                **BASE["defaults"],
                "engine": {
                    "autoscaling": {
                        "mode": "keda",
                        "minReplicas": 2,
                        "maxReplicas": 6,
                        "keda": {
                            "triggers": [
                                {"type": "cpu", "metricType": "Utilization", "metadata": {"value": "70"}},
                                {"type": "memory", "metricType": "Utilization", "metadata": {"value": "80"}},
                            ],
                            "fallback": {"failureThreshold": 3, "replicas": 2},
                        },
                    }
                },
            },
        }
        result = render(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fallback requires at least one Value/AverageValue trigger", result.stderr)

    def test_metrics_are_separate_from_public_service(self):
        values = {
            **BASE,
            "gateway": {"service": {"type": "LoadBalancer"}},
            "metrics": {"enabled": True, "serviceMonitor": {"enabled": True}},
        }
        docs = self.manifests(values, "--api-versions", "monitoring.coreos.com/v1/ServiceMonitor")
        self.assertEqual([p["port"] for p in docs["Service", "test-sink-gateway"]["spec"]["ports"]], [8080])
        self.assertEqual(docs["Service", "test-sink-gateway-metrics"]["spec"]["type"], "ClusterIP")
        self.assertIn(("ServiceMonitor", "test-sink"), docs)

    def test_long_names_remain_unique(self):
        values = {
            "fullnameOverride": "x" * 80,
            "stores": {
                "first": {"storage": STORAGE, "phase": "active"},
                "second": {"storage": STORAGE, "phase": "active"},
            },
        }
        docs = self.manifests(values)
        for _, name in docs:
            self.assertLessEqual(len(name), 63)
        self.assertEqual(len([1 for kind, _ in docs if kind == "Deployment"]), 3)

    def test_kafka_dead_letter_topics_cannot_overlap(self):
        for topic in ["mongo", "mongo.dlq", " mongo ", " mongo.dlq "]:
            values = copy.deepcopy(BASE)
            values["stores"]["mongo"]["kafka"] = copy.deepcopy(KAFKA)
            kafka = {**KAFKA, "topic": {"name": topic}}
            values["stores"]["archive"] = {"phase": "staged", "storage": STORAGE, "kafka": kafka}
            self.assertNotEqual(render(values).returncode, 0)
        values = copy.deepcopy(BASE)
        values["stores"]["mongo"]["kafka"] = {**KAFKA, "deadLetterTopic": {"name": "mongo"}}
        self.assertNotEqual(render(values).returncode, 0)

    def test_shared_kafka_policy_and_independent_topics(self):
        values = copy.deepcopy(BASE)
        values["stores"]["mongo"]["kafka"] = {
            **KAFKA,
            "topicPolicy": {**KAFKA["topicPolicy"], "replicationFactor": 3, "minInSyncReplicas": 2},
            "maxRecordBytes": "2MiB",
            "topic": {"name": KAFKA["topic"]["name"], "retention": "48h"},
            "deadLetterTopic": {"name": "rejected", "retention": "240h"},
        }
        docs = self.manifests(values)
        shared = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-store"]["data"]["store.yaml"])
        self.assertEqual(shared["kafka"]["replication_factor"], 3)
        self.assertEqual(shared["kafka"]["min_insync_replicas"], 2)
        self.assertEqual(shared["kafka"]["max_record_bytes"], "2MiB")
        self.assertEqual(shared["kafka"]["topic"], {"name": "mongo", "retention": "48h"})
        self.assertEqual(shared["kafka"]["dead_letter"], {"name": "rejected", "retention": "240h"})
        for runtime in [
            {"topic": {"partitions": 4}},
            {"topic": {"replication_factor": 2}},
            {"topic": {"min_insync_replicas": 1}},
            {"topic": {"maxRecordBytes": "2MiB"}},
            {"deadLetterTopic": {"topic": "rejected"}},
        ]:
            values["stores"]["mongo"]["kafka"] = {**KAFKA, **runtime}
            self.assertNotEqual(render(values).returncode, 0)

    def test_examples(self):
        values = {}
        self.manifests(
            values,
            "-f",
            str(ROOT / "examples/cluster-values.yaml"),
            "-f",
            str(ROOT / "examples/memory-keda-values.yaml"),
            "--api-versions",
            "keda.sh/v1alpha1/ScaledObject",
        )
        self.manifests({}, "-f", str(ROOT / "examples/search-values.yaml"))
        self.manifests({}, "-f", str(ROOT / "examples/cluster-values.yaml"))
        self.manifests(
            {},
            "-f",
            str(ROOT / "examples/cluster-values.yaml"),
            "-f",
            str(ROOT / "examples/keda-values.yaml"),
            "--api-versions",
            "keda.sh/v1alpha1/ScaledObject",
        )

    def test_unsafe_and_misspelled_settings_fail(self):
        cases = [
            "mode=engine",
            "defaults.engine.replicas=0",
            "gateway.replicas=0",
            "defaults.engine.rollout.preStopDelaySeconds=64",
            "gateway.rollout.terminationGracePeriodSeconds=100",
            "defaults.engine.rollout.shutdownBudgetSeconds=60",
            "requestTimeoutSeconds=301",
            "stores.Bad.phase=active",
            "stores.mongo.phase=deleted",
            "defaults.engine.replicaCont=3",
            "defaults.engine.pod.resources.limits.memory=0Gi",
            "defaults.engine.pod.resources.requests.memory=2Gi",
            "defaults.engine.autoscaling.mode=keda",
            "metrics.serviceMonitor.enabled=true",
            "gateway.config.gateway.dnsRefreshInterval=1s",
            "gateway.config.service.requestLimits.timeout=1s",
            "defaults.engine.autoscaling.mode=hpa,defaults.engine.autoscaling.minReplicas=5,defaults.engine.autoscaling.maxReplicas=4",
            "defaults.engine.autoscaling.mode=hpa,defaults.engine.autoscaling.scaleDown.periodSeconds=60",
            "defaults.engine.autoscaling.mode=hpa,defaults.engine.autoscaling.targetCPUUtilizationPercent=0",
            "stores.mongo.worker.enabled=true",
            "defaults.engine.pod.env[0].name=GOMEMLIMIT,defaults.engine.pod.env[0].value=100MiB",
        ]
        for settings in cases:
            with self.subTest(settings=settings):
                result = render(BASE, "--set", settings)
                self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_worker_overpartition_and_implicit_zero_fail(self):
        values = {
            "stores": {
                "mongo": {
                    "storage": STORAGE,
                    "phase": "active",
                    "kafka": KAFKA,
                    "worker": {
                        "enabled": True,
                        "replicas": 9,
                        "config": {"kafkaConsumer": {"groupId": "mongo-workers"}},
                    },
                }
            }
        }
        self.assertIn("exceeds Kafka partitions", render(values).stderr)
        values["stores"]["mongo"]["worker"]["replicas"] = 0
        self.assertIn("requires at least 1", render(values).stderr)

    def test_old_kubernetes_is_rejected(self):
        result = render(BASE, "--kube-version", "1.29.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("kubeVersion", result.stderr)


if __name__ == "__main__":
    unittest.main()
