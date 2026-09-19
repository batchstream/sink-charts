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
STORAGE = {"driver": "mongodb", "mongodb": {"uriSecretRef": {"name": "mongo-v1", "key": "uri"}}}
BASE = {"workerDefaults": {"runtime": {"consumer": {"group_id": "mongo-workers"}}}, "stores": {"mongo": {"storage": STORAGE, "state": "active"}}}
KAFKA = {"brokers": ["kafka:9092"], "topic": "mongo", "partitions": 8}


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

    def test_logging_inheritance_outputs_and_rollout(self):
        values = copy.deepcopy(BASE)
        worker_settings = {"enabled": True}
        values["stores"]["mongo"].update(kafka=KAFKA, worker=worker_settings)
        values["stores"]["archive"] = {"storage": STORAGE, "state": "staged"}
        original = self.manifests(values)
        for doc in original.values():
            if doc["kind"] == "ConfigMap" and "sink.yaml" in doc.get("data", {}):
                self.assertNotIn("logging", yaml.safe_load(doc["data"]["sink.yaml"]))
        logging = {"level": "warn",
                   "console": {"enabled": True, "format": "json"},
                   "labels": {"environment": "test", "cluster": "fixture"},
                   "failure_body": True, "max_body_bytes": "32KiB",
                   "otlp": {"enabled": True, "protocol": "grpc", "endpoint": "collector:4317",
                            "tls": {"enabled": True}, "queue_size": 256, "batch_size": 32,
                            "flush_interval": "500ms", "export_timeout": "2s", "shutdown_timeout": "5s"}}
        for role in ["gateway", "engineDefaults", "workerDefaults"]:
            values[role] = {"runtime": {"logging": copy.deepcopy(logging)}}
            if role == "workerDefaults":
                values[role]["runtime"]["consumer"] = {"group_id": "mongo-workers"}
        engine_override = {"failure_body": False, "console": {"enabled": False},
                           "otlp": {"tls": {"enabled": False}}}
        values["stores"]["mongo"]["engine"] = {"runtime": {"logging": engine_override}}
        worker_override = {"otlp": {"enabled": False}, "level": "error"}
        values["stores"]["mongo"]["worker"]["runtime"] = {"logging": worker_override}
        docs = self.manifests(values)
        engine = copy.deepcopy(logging)
        engine["failure_body"] = False
        engine["console"]["enabled"] = False
        engine["otlp"]["tls"]["enabled"] = False
        worker = copy.deepcopy(logging)
        worker["otlp"]["enabled"] = False
        worker["level"] = "error"
        for name, expected in [("test-sink-gateway", logging), ("test-sink-archive-engine", logging),
                               ("test-sink-mongo-engine", engine), ("test-sink-mongo-worker", worker)]:
            config = yaml.safe_load(docs["ConfigMap", name]["data"]["sink.yaml"])
            self.assertEqual(config["logging"], expected)
            before = original["Deployment", name]["spec"]["template"]["metadata"]["annotations"]
            after = docs["Deployment", name]["spec"]["template"]["metadata"]["annotations"]
            self.assertNotEqual(before["sink.batchstream.io/config-checksum"], after["sink.batchstream.io/config-checksum"])
        values["stores"]["mongo"]["engine"]["runtime"]["logging"]["level"] = "debug"
        changed = self.manifests(values)
        for name in ["test-sink-gateway", "test-sink-mongo-worker", "test-sink-archive-engine"]:
            self.assertEqual(docs["Deployment", name], changed["Deployment", name])

    def test_logging_identity_uses_downward_api_without_duplicate_overrides(self):
        values = copy.deepcopy(BASE)
        worker_settings = {"enabled": True}
        values["stores"]["mongo"].update(kafka=KAFKA, worker=worker_settings)
        values["engineDefaults"] = {"pod": {"env": [{"name": "POD_NAME", "value": "explicit-pod"}]}}
        docs = self.manifests(values)
        fields = {"POD_UID": "metadata.uid", "POD_NAME": "metadata.name",
                  "POD_NAMESPACE": "metadata.namespace", "NODE_NAME": "spec.nodeName"}
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
            {"level": "trace"}, {"components": {"rpc": "debug"}}, {"components": {}},
            {"console": {"format": "yaml"}}, {"console": {"enabled": False}}, {"unknown": True},
            {"labels": {"tenant": "test"}}, {"labels": {"pod": "line\nbreak"}},
            {"labels": {"pod": "雪" * 86}}, {"labels": {"cluster": 42}},
            {"failure_body": "true"}, {"max_body_bytes": 1023}, {"max_body_bytes": 65537},
            {"max_body_bytes": "0.5KiB"}, {"max_body_bytes": "65KiB"}, {"max_body_bytes": "1.0001KiB"},
            {"otlp": {"enabled": True}}, {"otlp": {"endpoint": "https://collector:4317"}},
            {"otlp": {"endpoint": "user@collector:4317"}}, {"otlp": {"endpoint": "collector:0"}},
            {"otlp": {"endpoint": "collector:65536"}}, {"otlp": {"endpoint": "collector:4317/logs"}},
            {"otlp": {"headers": {"authorization": "unsupported"}}}, {"otlp": {"protocol": "http"}},
            {"otlp": {"tls": {"insecure": True}}}, {"otlp": {"queue_size": 0}},
            {"otlp": {"queue_size": 8193}}, {"otlp": {"batch_size": 513}},
            {"otlp": {"queue_size": 127}}, {"otlp": {"batch_size": 0}},
            {"otlp": {"flush_interval": "61s"}}, {"otlp": {"export_timeout": "0s"}},
            {"otlp": {"shutdown_timeout": "30s1ns"}}, {"otlp": {"export_timeout": "1m"}},
            {"otlp": {"flush_interval": "garbage"}}, {"otlp": {"export_timeout": "-1s"}},
        ]
        for logging in invalid:
            with self.subTest(logging=logging):
                values = copy.deepcopy(BASE)
                values["engineDefaults"] = {"runtime": {"logging": logging}}
                result = render(values)
                self.assertNotEqual(result.returncode, 0, logging)
                self.assertIn("logging", result.stderr)
        for role in ["gateway", "engineDefaults", "workerDefaults"]:
            values = copy.deepcopy(BASE)
            worker_settings = {"enabled": True}
            values["stores"]["mongo"].update(kafka=KAFKA, worker=worker_settings)
            values[role] = {"runtime": {"logging": {"otlp": {"enabled": True}}}}
            self.assertIn("logging.otlp.endpoint", render(values).stderr)

    def test_logging_accepts_units_protocols_and_effective_store_settings(self):
        values = copy.deepcopy(BASE)
        logging = {"console": {"enabled": False}, "max_body_bytes": "1.5 KiB",
                   "labels": {"pod": "雪" * 85},
                   "otlp": {"enabled": True, "endpoint": "[::1]:4318", "protocol": "http/protobuf",
                            "tls": {"enabled": False}, "flush_interval": "1m", "export_timeout": "+1.5s",
                            "shutdown_timeout": "1s500ms", "queue_size": 16}}
        values["engineDefaults"] = {"runtime": {"logging": logging}}
        values["stores"]["mongo"]["engine"] = {"runtime": {"logging": {"otlp": {"batch_size": 8}}}}
        docs = self.manifests(values)
        config = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-engine"]["data"]["sink.yaml"])
        self.assertEqual(config["logging"]["otlp"]["batch_size"], 8)
        values["stores"]["mongo"]["engine"]["runtime"]["logging"]["otlp"]["enabled"] = False
        self.assertIn("logging requires console or OTLP", render(values).stderr)
        values["engineDefaults"]["runtime"]["logging"]["console"]["enabled"] = True
        values["stores"]["mongo"]["engine"]["runtime"]["logging"]["otlp"]["endpoint"] = ""
        self.manifests(values)

    def test_logging_shutdown_budget_includes_exporter_flush(self):
        values = copy.deepcopy(BASE)
        values["gateway"] = {"runtime": {"logging": {"otlp": {"enabled": True, "endpoint": "collector:4317",
                                                                  "shutdown_timeout": "5.5s"}}},
                             "pod": {"shutdownBudgetSeconds": 125}}
        self.assertIn("plus logging.otlp.shutdown_timeout (126 seconds)", render(values).stderr)
        values["gateway"]["pod"]["shutdownBudgetSeconds"] = 126
        self.manifests(values)
        values["gateway"]["runtime"]["logging"]["otlp"]["enabled"] = False
        values["gateway"]["pod"]["shutdownBudgetSeconds"] = 120
        self.manifests(values)

    def test_role_images_allow_engine_first_upgrade_and_gateway_first_rollback(self):
        values = copy.deepcopy(BASE)
        values["stores"]["mongo"].update(kafka=KAFKA, worker={"enabled": True})
        original = self.manifests(values)
        new_digest = "sha256:" + "1" * 64
        values["engineDefaults"] = {"image": {"digest": new_digest}}
        engines = self.manifests(values)
        for name in ["test-sink-gateway", "test-sink-mongo-worker"]:
            self.assertEqual(original["Deployment", name], engines["Deployment", name])
        engine_pod = engines["Deployment", "test-sink-mongo-engine"]["spec"]["template"]["spec"]
        self.assertEqual(engine_pod["containers"][0]["image"], "ghcr.io/batchstream/sink@" + new_digest)
        values["gateway"] = {"image": {"digest": new_digest}}
        gateways = self.manifests(values)
        self.assertEqual(engines["Deployment", "test-sink-mongo-engine"], gateways["Deployment", "test-sink-mongo-engine"])
        gateway_pod = gateways["Deployment", "test-sink-gateway"]["spec"]["template"]["spec"]
        self.assertEqual(gateway_pod["containers"][0]["image"], engine_pod["containers"][0]["image"])
        del values["gateway"]
        self.assertEqual(self.manifests(values), engines)
        del values["engineDefaults"]
        self.assertEqual(self.manifests(values), original)

    def test_image_tag_and_digest_precedence_at_each_layer(self):
        values = copy.deepcopy(BASE)
        values["engineDefaults"] = {"image": {"digest": "sha256:" + "2" * 64}}
        values["stores"]["mongo"]["engine"] = {"image": {"tag": "per-store", "pullPolicy": "Always"}}
        values["stores"]["archive"] = {"storage": STORAGE, "state": "staged"}
        values["gateway"] = {"image": {"tag": "gateway-only"}}
        docs = self.manifests(values)
        expected = {"test-sink-mongo-engine": "ghcr.io/batchstream/sink:per-store",
                    "test-sink-archive-engine": "ghcr.io/batchstream/sink@sha256:" + "2" * 64,
                    "test-sink-gateway": "ghcr.io/batchstream/sink:gateway-only"}
        for name, image in expected.items():
            self.assertEqual(docs["Deployment", name]["spec"]["template"]["spec"]["containers"][0]["image"], image)
        self.assertEqual(docs["Deployment", "test-sink-mongo-engine"]["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"], "Always")
        values["stores"]["mongo"]["engine"]["image"]["digest"] = "sha256:" + "3" * 64
        docs = self.manifests(values)
        self.assertEqual(docs["Deployment", "test-sink-mongo-engine"]["spec"]["template"]["spec"]["containers"][0]["image"], "ghcr.io/batchstream/sink@sha256:" + "3" * 64)
        values["gateway"]["image"]["digest"] = "invalid"
        self.assertNotEqual(render(values).returncode, 0)

    def test_memory_capacity_is_optional_and_configurable_per_role(self):
        defaults = self.manifests()
        default_config = yaml.safe_load(defaults["ConfigMap", "test-sink-gateway"]["data"]["sink.yaml"])
        self.assertNotIn("memory", default_config)
        values = copy.deepcopy(BASE)
        values["gateway"] = {"runtime": {"memory": {"burst_percent": 15}}}
        values["engineDefaults"] = {"runtime": {"memory": {"max_bytes": "256MiB", "burst_percent": 10}}}
        values["stores"]["mongo"]["engine"] = {"runtime": {"memory": {"wait_timeout": "500ms"}}}
        docs = self.manifests(values)
        gateway = yaml.safe_load(docs["ConfigMap", "test-sink-gateway"]["data"]["sink.yaml"])
        engine = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-engine"]["data"]["sink.yaml"])
        gateway_expected = {"burst_percent": 15}
        self.assertEqual(gateway["memory"], gateway_expected)
        engine_expected = {"max_bytes": "256MiB", "burst_percent": 10, "wait_timeout": "500ms"}
        self.assertEqual(engine["memory"], engine_expected)
        values["gateway"]["runtime"]["memory"]["burst_percent"] = 100
        self.assertNotEqual(render(values).returncode, 0)

    def test_empty_and_staged_cluster(self):
        empty = self.manifests({})
        self.assertEqual(list(empty), [("ConfigMap", "test-sink-inventory")])
        staged = self.manifests({"stores": {"mongo": {"storage": STORAGE, "state": "staged"}}})
        self.assertIn(("Deployment", "test-sink-mongo-engine"), staged)
        self.assertNotIn(("Deployment", "test-sink-gateway"), staged)

    def test_discovery_shutdown_security_and_memory(self):
        docs = self.manifests()
        for role, name, delay, grace in [("engine", "test-sink-mongo-engine", 65, 225),
                                         ("gateway", "test-sink-gateway", 70, 230)]:
            deployment = docs["Deployment", name]
            pod = deployment["spec"]["template"]
            container = pod["spec"]["containers"][0]
            self.assertEqual(container["args"], ["--config", "/etc/sink/sink.yaml"] + (["--store-config", "/etc/sink/store.yaml"] if role == "engine" else []))
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
        headless = docs["Service", "test-sink-gateway-headless"]
        self.assertEqual(headless["spec"]["clusterIP"], "None")
        self.assertFalse(headless["spec"]["publishNotReadyAddresses"])
        self.assertEqual(headless["spec"]["selector"], docs["Service", "test-sink-gateway"]["spec"]["selector"])
        self.assertFalse(any(kind == "Secret" for kind, _ in docs))

    def test_routes_are_only_active_and_config_changes_roll_gateway(self):
        values = copy.deepcopy(BASE)
        values["stores"].update({"archive": {"storage": STORAGE, "state": "staged"}, "old": {"storage": STORAGE, "state": "retiring"}})
        first = self.manifests(values)
        config = yaml.safe_load(first["ConfigMap", "test-sink-gateway"]["data"]["sink.yaml"])
        self.assertEqual(config["forwarding"]["routes"], [{"store": "mongo", "target": "dns:///test-sink-mongo-engine.sink.svc.cluster.local:8080", "tls": {"insecure": True}}])
        values["stores"]["archive"]["state"] = "active"
        second = self.manifests(values)
        a = first["Deployment", "test-sink-gateway"]["spec"]["template"]["metadata"]["annotations"]
        b = second["Deployment", "test-sink-gateway"]["spec"]["template"]["metadata"]["annotations"]
        self.assertNotEqual(a["sink.batchstream.io/config-checksum"], b["sink.batchstream.io/config-checksum"])
        self.assertEqual(first["Deployment", "test-sink-mongo-engine"], second["Deployment", "test-sink-mongo-engine"])

    def test_per_store_merge_does_not_leak(self):
        values = copy.deepcopy(BASE)
        values["stores"]["archive"] = {"storage": STORAGE, "state": "staged", "engine": {"pod": {"resources": {"limits": {"memory": "2Gi"}}, "configRevision": "v2"}}}
        docs = self.manifests(values)
        archive = docs["Deployment", "test-sink-archive-engine"]["spec"]["template"]["spec"]
        mongo = docs["Deployment", "test-sink-mongo-engine"]["spec"]["template"]["spec"]
        self.assertEqual(archive["containers"][0]["resources"]["limits"]["memory"], "2Gi")
        self.assertEqual(mongo["containers"][0]["resources"]["limits"]["memory"], "1Gi")
        self.assertEqual(archive["volumes"][0]["projected"]["sources"], [{"configMap": {"name": "test-sink-archive-engine"}}, {"configMap": {"name": "test-sink-archive-store"}}])

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
            self.assertEqual(projection["sources"], [{"secret": {"name": "mongo-v1", "optional": False,
                             "items": [{"key": "uri", "path": "mongodb-uri"}]}}])
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
            self.assertNotIn("consumer", shared["kafka"])
            self.assertEqual(shared["kafka"]["topic"]["replication_factor"], 3)
            configs.append(shared)
        self.assertEqual(*configs)
        gateway = docs["Deployment", "test-sink-gateway"]
        self.assertEqual(len(gateway["spec"]["template"]["spec"]["volumes"]), 1)
        values["stores"]["mongo"]["credentialRevision"] = "v2"
        rotated = self.manifests(values)
        for role in ["engine", "worker"]:
            name = f"test-sink-mongo-{role}"
            self.assertNotEqual(docs["Deployment", name]["spec"]["template"], rotated["Deployment", name]["spec"]["template"])
        self.assertEqual(gateway, rotated["Deployment", "test-sink-gateway"])
        values["stores"]["mongo"]["storage"]["mongodb"]["uriSecretRef"]["name"] = "mongo-v2"
        versioned = self.manifests(values)
        for role in ["engine", "worker"]:
            name = f"test-sink-mongo-{role}"
            self.assertNotEqual(rotated["Deployment", name]["spec"]["template"], versioned["Deployment", name]["spec"]["template"])
        values["stores"]["mongo"]["engine"] = {"runtime": {"execution": {"mongodb": {"max_concurrent_writes": 32}}}}
        tuned = self.manifests(values)
        name = "test-sink-mongo-engine"
        config = yaml.safe_load(tuned["ConfigMap", name]["data"]["sink.yaml"])
        self.assertEqual(config["execution"]["mongodb"]["max_concurrent_writes"], 32)
        self.assertEqual(versioned["Deployment", "test-sink-mongo-worker"], tuned["Deployment", "test-sink-mongo-worker"])
        self.assertEqual(versioned["ConfigMap", "test-sink-mongo-store"], tuned["ConfigMap", "test-sink-mongo-store"])
        self.assertNotEqual(versioned["Deployment", name]["spec"]["template"]["metadata"]["annotations"],
                            tuned["Deployment", name]["spec"]["template"]["metadata"]["annotations"])

    def test_advanced_tuning_is_generated_from_values(self):
        values = copy.deepcopy(BASE)
        values["metrics"] = {"enabled": True}
        values["engineDefaults"] = {"runtime": {"execution": {"max_output_bytes": "16MiB"},
                                               "grpc": {"max_receive_message_bytes": "8MiB"}}}
        values["stores"]["mongo"]["engine"] = {"runtime": {"batching": {"max_wait": "5ms"}}}
        values["stores"]["mongo"]["worker"] = {"enabled": True, "runtime": {"execution": {"max_output_bytes": "4MiB"}, "consumer": {"processing_timeout": "20s"}}}
        values["stores"]["mongo"]["kafka"] = {**KAFKA, "runtime": {"dead_letter": {"retention": "720h"}}}
        docs = self.manifests(values)
        engine = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-engine"]["data"]["sink.yaml"])
        worker = yaml.safe_load(docs["ConfigMap", "test-sink-mongo-worker"]["data"]["sink.yaml"])
        self.assertEqual(engine["execution"]["max_output_bytes"], "16MiB")
        self.assertEqual(engine["batching"]["max_wait"], "5ms")
        self.assertEqual(worker["execution"]["max_output_bytes"], "4MiB")
        self.assertNotIn("batching", worker)
        self.assertEqual(engine["grpc"]["max_receive_message_bytes"], "8MiB")
        self.assertEqual(worker["consumer"]["processing_timeout"], "20s")
        self.assertTrue(engine["prometheus"]["enabled"])
        self.assertTrue(worker["prometheus"]["enabled"])

    def test_search_credentials_and_rejected_plaintext(self):
        for driver in ["elasticsearch", "opensearch"]:
            for auth in [{}, {"username": "sink", "passwordSecretRef": {"name": "search", "key": "password"}},
                         {"usernameSecretRef": {"name": "search", "key": "user"}, "passwordSecretRef": {"name": "search", "key": "pass"}},
                         {"apiKeySecretRef": {"name": "search", "key": "api-key"}}]:
                search = {"endpoints": ["https://search:9200"], **auth}
                values = {"stores": {"search": {"state": "active", "storage": {"driver": driver, "search": search}}}}
                docs = self.manifests(values)
                config = yaml.safe_load(docs["ConfigMap", "test-sink-search-store"]["data"]["store.yaml"])
                self.assertNotIn("SecretRef", json.dumps(config))
                expected = {"endpoints": ["https://search:9200"]}
                if "username" in auth:
                    expected["username"] = "sink"
                for key, runtime, filename in [("usernameSecretRef", "username_file", "search-username"),
                                                ("passwordSecretRef", "password_file", "search-password"),
                                                ("apiKeySecretRef", "api_key_file", "search-api-key")]:
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
            {"driver": "mongodb", "mongodb": {"uriSecretRef": {"name": "mongo", "key": "uri", "namespace": "other"}}},
            {"driver": "opensearch", "search": {"endpoints": ["http://search"], "password": "inline"}},
            {"driver": "opensearch", "search": {"endpoints": ["http://search"], "username": "sink"}},
            {"driver": "opensearch", "search": {"endpoints": ["http://search"], "username": "sink", "usernameSecretRef": {"name": "s", "key": "u"}}},
            {"driver": "opensearch", "search": {"endpoints": ["http://search"], "username": "sink", "passwordSecretRef": {"name": "s", "key": "p"}, "apiKeySecretRef": {"name": "s", "key": "a"}}},
        ]
        for storage in bad_storage:
            values = {"stores": {"test": {"state": "staged", "storage": storage}}}
            result = render(values)
            self.assertNotEqual(result.returncode, 0, storage)
        for override in ["engineDefaults.config.existingSecret=old", "engineDefaults.runtime.grpc.address=:9999",
                         "workerDefaults.runtime.service.request.timeout=1s"]:
            values = copy.deepcopy(BASE)
            async_config = {"kafka": KAFKA, "worker": {"enabled": True}}
            values["stores"]["mongo"].update(async_config)
            self.assertNotEqual(render(values, "--set", override).returncode, 0)

    def test_budget_recalculates_with_dns(self):
        values = {**BASE, "discovery": {"dnsRefreshSeconds": 40, "dnsCacheSeconds": 60}}
        doc = self.manifests(values)["Deployment", "test-sink-mongo-engine"]
        container = doc["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["lifecycle"]["preStop"]["sleep"]["seconds"], 125)
        self.assertEqual(doc["spec"]["template"]["spec"]["terminationGracePeriodSeconds"], 285)
        self.assertEqual(doc["spec"]["minReadySeconds"], 115)

    def test_controller_owns_replicas_and_hpa_behavior(self):
        behavior = {"scaleUp": {"stabilizationWindowSeconds": 0, "selectPolicy": "Max",
                                "policies": [{"type": "Percent", "value": 100, "periodSeconds": 60}]},
                    "scaleDown": {"stabilizationWindowSeconds": 600, "selectPolicy": "Min",
                                  "policies": [{"type": "Pods", "value": 1, "periodSeconds": 300}]}}
        values = {**BASE, "engineDefaults": {"autoscaling": {"mode": "hpa", "minReplicas": 3, "maxReplicas": 7,
                                                               "behavior": behavior}}}
        docs = self.manifests(values)
        self.assertNotIn("replicas", docs["Deployment", "test-sink-mongo-engine"]["spec"])
        hpa = docs["HorizontalPodAutoscaler", "test-sink-mongo-engine"]["spec"]
        self.assertEqual((hpa["minReplicas"], hpa["maxReplicas"]), (3, 7))
        self.assertEqual(hpa["behavior"]["scaleDown"]["policies"], [{"type": "Pods", "value": 1, "periodSeconds": 300}])
        self.assertEqual(hpa["behavior"]["scaleDown"]["stabilizationWindowSeconds"], 600)
        self.assertEqual(docs["Deployment", "test-sink-gateway"]["spec"]["replicas"], 2)

    def test_single_engine_is_explicit_and_never_zero(self):
        values = {**BASE, "engineDefaults": {"replicaCount": 1, "disruptionBudget": {"maxUnavailable": 0}}}
        docs = self.manifests(values)
        self.assertEqual(docs["Deployment", "test-sink-mongo-engine"]["spec"]["replicas"], 1)
        self.assertEqual(docs["PodDisruptionBudget", "test-sink-mongo-engine"]["spec"]["maxUnavailable"], 0)
        values["engineDefaults"]["replicaCount"] = 0
        self.assertIn("engine/mongo requires at least 1 replicas", render(values).stderr)

    def test_scale_down_policy_must_cover_termination(self):
        values = copy.deepcopy(BASE)
        values["engineDefaults"] = {"autoscaling": {"mode": "hpa", "behavior": {
            "scaleUp": {"stabilizationWindowSeconds": 0, "selectPolicy": "Max",
                        "policies": [{"type": "Pods", "value": 2, "periodSeconds": 60}]},
            "scaleDown": {"stabilizationWindowSeconds": 300, "selectPolicy": "Min",
                          "policies": [{"type": "Pods", "value": 1, "periodSeconds": 60}]},
        }}}
        result = render(values)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("scaleDown policy periodSeconds must cover termination grace (225)", result.stderr)

    def test_keda_worker_zero_and_partition_trigger(self):
        values = {"stores": {"mongo": {"storage": STORAGE, "state": "active", "kafka": KAFKA,
                  "worker": {"enabled": True, "runtime": {"consumer": {"group_id": "mongo-workers"}}, "allowScaleToZero": True, "disruptionBudget": {"enabled": False},
                             "autoscaling": {"mode": "keda", "minReplicas": 0, "maxReplicas": 8,
                                             "keda": {"initialCooldownPeriod": 60,
                                                      "restoreToOriginalReplicaCount": True,
                                                      "authenticationRef": {"name": "auth"},
                                                      "fallback": {"failureThreshold": 3, "replicas": 2,
                                                                   "behavior": "currentReplicasIfHigher"},
                                                      "kafkaTrigger": {"name": "pse-search-lag", "useCachedMetrics": True}}}}}}}
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
        values["gateway"]["autoscaling"]["keda"] = {"triggers": [
            {"type": "prometheus", "useCachedMetrics": True,
             "metadata": {"serverAddress": "http://prometheus", "query": "sum(queue_depth)", "threshold": "10"}}]}
        docs = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        self.assertEqual(docs["ScaledObject", "test-sink-gateway"]["spec"]["pollingInterval"], 30)

    def test_keda_production_controls_and_mixed_fallback(self):
        triggers = [
            {"type": "cpu", "name": "cpu", "metricType": "Utilization", "metadata": {"value": "70"}},
            {"type": "memory", "name": "memory", "metricType": "Utilization", "metadata": {"value": "80"}},
            {"type": "prometheus", "name": "execution-budget", "metricType": "AverageValue",
             "useCachedMetrics": True,
             "metadata": {"serverAddress": "http://prometheus", "query": "sum(sink_execution_store_bytes)",
                          "threshold": "134217728", "ignoreNullValues": "false"}},
        ]
        values = {**BASE, "engineDefaults": {"autoscaling": {
            "mode": "keda", "minReplicas": 2, "maxReplicas": 10,
            "keda": {"annotations": {"autoscaling.keda.sh/paused": "false"}, "hpaName": "sink-mongo",
                     "restoreToOriginalReplicaCount": True, "triggers": triggers,
                     "fallback": {"failureThreshold": 3, "replicas": 2,
                                  "behavior": "currentReplicasIfHigher"}}}}}
        docs = self.manifests(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        scaled = docs["ScaledObject", "test-sink-mongo-engine"]
        self.assertEqual(scaled["metadata"]["annotations"], {"autoscaling.keda.sh/paused": "false"})
        self.assertEqual(scaled["spec"]["advanced"]["horizontalPodAutoscalerConfig"]["name"], "sink-mongo")
        self.assertTrue(scaled["spec"]["advanced"]["restoreToOriginalReplicaCount"])
        self.assertEqual(scaled["spec"]["fallback"]["behavior"], "currentReplicasIfHigher")
        self.assertEqual([trigger["name"] for trigger in scaled["spec"]["triggers"]],
                         ["cpu", "memory", "execution-budget"])

    def test_keda_fallback_requires_supported_trigger(self):
        values = {**BASE, "engineDefaults": {"autoscaling": {
            "mode": "keda", "minReplicas": 2, "maxReplicas": 6,
            "keda": {"triggers": [
                {"type": "cpu", "metricType": "Utilization", "metadata": {"value": "70"}},
                {"type": "memory", "metricType": "Utilization", "metadata": {"value": "80"}},
            ], "fallback": {"failureThreshold": 3, "replicas": 2}},
        }}}
        result = render(values, "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fallback requires at least one Value/AverageValue trigger", result.stderr)

    def test_metrics_are_separate_from_public_service(self):
        values = {**BASE, "gateway": {"service": {"type": "LoadBalancer"}}, "metrics": {"enabled": True, "serviceMonitor": {"enabled": True}}}
        docs = self.manifests(values, "--api-versions", "monitoring.coreos.com/v1/ServiceMonitor")
        self.assertEqual([p["port"] for p in docs["Service", "test-sink-gateway"]["spec"]["ports"]], [8080])
        self.assertEqual(docs["Service", "test-sink-gateway-metrics"]["spec"]["type"], "ClusterIP")
        self.assertIn(("ServiceMonitor", "test-sink"), docs)

    def test_long_names_remain_unique(self):
        values = {"fullnameOverride": "x" * 80, "stores": {"first": {"storage": STORAGE, "state": "active"}, "second": {"storage": STORAGE, "state": "active"}}}
        docs = self.manifests(values)
        for _, name in docs:
            self.assertLessEqual(len(name), 63)
        self.assertEqual(len([1 for kind, _ in docs if kind == "Deployment"]), 3)

    def test_kafka_dead_letter_topics_cannot_overlap(self):
        for topic in ["mongo", "mongo.dlq", " mongo ", " mongo.dlq "]:
            values = copy.deepcopy(BASE)
            values["stores"]["mongo"]["kafka"] = copy.deepcopy(KAFKA)
            kafka = {**KAFKA, "topic": topic}
            values["stores"]["archive"] = {"state": "staged", "storage": STORAGE, "kafka": kafka}
            self.assertNotEqual(render(values).returncode, 0)
        values = copy.deepcopy(BASE)
        values["stores"]["mongo"]["kafka"] = {**KAFKA, "runtime": {"dead_letter": {"topic": "mongo"}}}
        self.assertNotEqual(render(values).returncode, 0)

    def test_examples(self):
        values = {}
        self.manifests(values, "-f", str(ROOT / "examples/cluster-values.yaml"), "-f", str(ROOT / "examples/memory-keda-values.yaml"), "--api-versions", "keda.sh/v1alpha1/ScaledObject")
        self.manifests({}, "-f", str(ROOT / "examples/search-values.yaml"))
        self.manifests({}, "-f", str(ROOT / "examples/cluster-values.yaml"))
        self.manifests({}, "-f", str(ROOT / "examples/cluster-values.yaml"), "-f", str(ROOT / "examples/keda-values.yaml"), "--api-versions", "keda.sh/v1alpha1/ScaledObject")

    def test_unsafe_and_misspelled_settings_fail(self):
        cases = [
            "mode=engine", "engineDefaults.replicaCount=0", "gateway.replicaCount=0",
            "engineDefaults.pod.preStopSeconds=64", "gateway.pod.terminationGracePeriodSeconds=100",
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
        values = {"stores": {"mongo": {"storage": STORAGE, "state": "active", "kafka": KAFKA, "worker": {"enabled": True, "runtime": {"consumer": {"group_id": "mongo-workers"}}, "replicaCount": 9}}}}
        self.assertIn("exceeds Kafka partitions", render(values).stderr)
        values["stores"]["mongo"]["worker"]["replicaCount"] = 0
        self.assertIn("requires at least 1", render(values).stderr)

    def test_old_kubernetes_is_rejected(self):
        result = render(BASE, "--kube-version", "1.29.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("kubeVersion", result.stderr)


if __name__ == "__main__":
    unittest.main()
