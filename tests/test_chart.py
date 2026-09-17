import os
from pathlib import Path
import subprocess
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "charts" / "sink"


def render(*options):
    command = [
        os.environ.get("HELM", "helm"), "template", "test", str(CHART),
        "--namespace", "sink", "--kube-version", "1.30.0", *options,
    ]
    return subprocess.run(command, capture_output=True, text=True, check=False)


class ChartTests(unittest.TestCase):
    def manifests(self, *options):
        result = render(*options)
        self.assertEqual(result.returncode, 0, result.stderr)
        documents = list(yaml.safe_load_all(result.stdout))
        manifests = {item["kind"]: item for item in documents if item}
        return manifests

    def test_engine_discovery_and_shutdown(self):
        documents = self.manifests()
        deployment = documents["Deployment"]
        service = documents["Service"]
        pod = deployment["spec"]["template"]
        container = pod["spec"]["containers"][0]
        self.assertEqual(service["spec"]["clusterIP"], "None")
        self.assertFalse(service["spec"].get("publishNotReadyAddresses", False))
        self.assertEqual(service["spec"]["selector"], pod["metadata"]["labels"])
        self.assertEqual(deployment["spec"]["selector"]["matchLabels"], pod["metadata"]["labels"])
        self.assertEqual(documents["PodDisruptionBudget"]["spec"]["selector"]["matchLabels"], pod["metadata"]["labels"])
        self.assertEqual(container["image"], "ghcr.io/liran/sink:0.15.0")
        self.assertEqual(container["lifecycle"]["preStop"]["sleep"]["seconds"], 110)
        self.assertEqual(pod["spec"]["terminationGracePeriodSeconds"], 180)
        self.assertEqual(container["readinessProbe"]["httpGet"]["path"], "/readyz")
        self.assertEqual(container["livenessProbe"]["httpGet"]["path"], "/livez")
        self.assertFalse(pod["spec"]["automountServiceAccountToken"])
        self.assertEqual(pod["spec"]["volumes"][0]["secret"]["secretName"], "test-sink-config")
        self.assertNotIn("Secret", documents)

    def test_gateway_service(self):
        documents = self.manifests("-f", str(ROOT / "examples/gateway-values.yaml"))
        self.assertEqual(documents["Deployment"]["metadata"]["name"], "sink-gateway")
        self.assertEqual(documents["Service"]["spec"]["type"], "ClusterIP")
        self.assertNotIn("clusterIP", documents["Service"]["spec"])
        external = self.manifests("--set", "mode=gateway,service.type=LoadBalancer")
        self.assertEqual(external["Service"]["spec"]["type"], "LoadBalancer")

    def test_worker_does_not_expose_business_grpc(self):
        documents = self.manifests("-f", str(ROOT / "examples/worker-values.yaml"))
        self.assertNotIn("Service", documents)
        container = documents["Deployment"]["spec"]["template"]["spec"]["containers"][0]
        self.assertNotIn("grpc", [port["name"] for port in container["ports"]])
        self.assertNotIn("lifecycle", container)
        metrics = self.manifests("--set", "mode=worker,metrics.enabled=true")
        self.assertEqual([port["name"] for port in metrics["Service"]["spec"]["ports"]], ["metrics"])

    def test_external_secret_key_and_service_account(self):
        documents = self.manifests(
            "--set", "config.existingSecret=external,config.key=config.yaml",
            "--set", "serviceAccount.create=false,serviceAccount.name=existing",
        )
        self.assertNotIn("ServiceAccount", documents)
        pod = documents["Deployment"]["spec"]["template"]["spec"]
        self.assertEqual(pod["serviceAccountName"], "existing")
        secret = pod["volumes"][0]["secret"]
        self.assertEqual(secret["secretName"], "external")
        self.assertEqual(secret["items"][0]["key"], "config.yaml")
        self.assertEqual(secret["items"][0]["path"], "sink.yaml")

    def test_image_digest_overrides_tag(self):
        digest = "sha256:" + "a" * 64
        documents = self.manifests("--set", f"image.digest={digest},image.tag=ignored")
        image = documents["Deployment"]["spec"]["template"]["spec"]["containers"][0]["image"]
        self.assertEqual(image, f"ghcr.io/liran/sink@{digest}")

    def test_metrics_and_custom_ports(self):
        documents = self.manifests("--set", "metrics.enabled=true,ports.grpc=9080,ports.health=9081,ports.metrics=9190,service.port=9080")
        container = documents["Deployment"]["spec"]["template"]["spec"]["containers"][0]
        container_ports = {port["name"]: port["containerPort"] for port in container["ports"]}
        service_ports = {port["name"]: port["port"] for port in documents["Service"]["spec"]["ports"]}
        self.assertEqual(container_ports["grpc"], 9080)
        self.assertEqual(container_ports["health"], 9081)
        self.assertEqual(service_ports["grpc"], 9080)
        self.assertEqual(service_ports["metrics"], 9190)

    def test_scaled_to_zero_and_disabled_pdb(self):
        documents = self.manifests("--set", "mode=worker,replicaCount=0,podDisruptionBudget.enabled=false")
        self.assertEqual(documents["Deployment"]["spec"]["replicas"], 0)
        self.assertNotIn("PodDisruptionBudget", documents)

    def test_invalid_values_fail_before_installation(self):
        invalid_values = [
            "mode=server", "replicaCount=-1", "ports.health=0", "image.digest=bad",
            "preStopDelaySeconds=-1", "config.key=", "replicaCont=3",
            "terminationGracePeriodSeconds=110", "service.type=LoadBalancer",
            "service.port=9080",
        ]
        for value in invalid_values:
            with self.subTest(value=value):
                result = render("--set", value)
                self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_old_kubernetes_is_rejected(self):
        result = render("--kube-version", "1.29.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("kubeVersion", result.stderr)


if __name__ == "__main__":
    unittest.main()
