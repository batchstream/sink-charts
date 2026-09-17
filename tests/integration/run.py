#!/usr/bin/env python3
"""Explicit local qualification. Never uses the user's default kubeconfig."""
import argparse
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

import yaml

ROOT = Path(__file__).resolve().parents[2]
NODE = "kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"


class Qualification:
    def __init__(self, args, directory):
        self.args = args
        self.directory = Path(directory)
        self.cluster = args.cluster or f"sink-chart-{int(time.time())}"
        if not self.cluster.startswith("sink-chart-"):
            raise ValueError("only explicitly owned sink-chart-* Kind clusters are allowed")
        self.kubeconfig = str(Path(args.kubeconfig).resolve()) if args.kubeconfig else str(self.directory / "kubeconfig")
        self.namespace = "sink-chart-test"
        self.image = f"sink-chart-probe:{self.cluster}"
        self.report_dir = ROOT / ".reports" / self.cluster
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.kubectl = ["kubectl", "--kubeconfig", self.kubeconfig, "--context", f"kind-{self.cluster}", "-n", self.namespace]
        self.helm = ["helm", "--kubeconfig", self.kubeconfig, "--kube-context", f"kind-{self.cluster}"]
        self.values = {
            "gateway": {"pod": {"resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"memory": "512Mi"}}}},
            "engineDefaults": {"pod": {"resources": {"requests": {"cpu": "100m", "memory": "256Mi"}, "limits": {"memory": "512Mi"}}}},
            "workerDefaults": {"pod": {"resources": {"requests": {"cpu": "100m", "memory": "256Mi"}, "limits": {"cpu": "200m", "memory": "512Mi"}}}},
            "stores": {"mongo": {"state": "active", "engine": {"config": {"existingSecret": "mongo-engine-config"}},
                "worker": {"enabled": True, "replicaCount": 0, "allowScaleToZero": True, "config": {"existingSecret": "mongo-worker-config"}, "disruptionBudget": {"enabled": False}},
                "kafka": {"brokers": ["kafka:9092"], "topic": "mongo-mutations", "consumerGroup": "mongo-workers", "partitions": 4, "lagThreshold": 100}}},
        }
        self.events = []
        self.owned = False
        self.image_built = False
        self.traffic_active = False

    def command(self, args, **kwargs):
        result = subprocess.run(args, text=True, capture_output=True, timeout=1200, **kwargs)
        if result.returncode:
            raise RuntimeError(f"{' '.join(args)}\n{result.stdout}\n{result.stderr}")
        return result.stdout

    def log(self, event):
        print(time.strftime("%H:%M:%S"), event, flush=True)
        self.events.append({"time": time.time(), "event": event})
        (self.report_dir / "events.json").write_text(json.dumps(self.events, indent=2))

    def apply(self, documents):
        self.command(self.kubectl + ["apply", "-f", "-"], input=yaml.safe_dump_all(documents))

    def get(self, kind, name=None):
        return json.loads(self.command(self.kubectl + ["get", kind] + ([name] if name else []) + ["-o", "json"]))

    def wait(self, description, predicate, timeout=600):
        self.log(description)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.traffic_active and self.get("pod", "traffic")["status"].get("phase") == "Failed":
                raise RuntimeError(self.command(self.kubectl + ["logs", "traffic"]))
            if predicate():
                return
            time.sleep(3)
        raise RuntimeError(f"timed out: {description}")

    def available(self, name):
        deployment = self.get("deployment", name)
        spec = deployment["spec"]
        status = deployment.get("status", {})
        desired = spec.get("replicas", 1)
        return (status.get("observedGeneration", 0) >= deployment["metadata"]["generation"]
                and status.get("availableReplicas", 0) == desired
                and status.get("updatedReplicas", 0) == desired)

    def no_terminating(self):
        return all(not pod["metadata"].get("deletionTimestamp") for pod in self.get("pods")["items"])

    def upgrade(self, values=None, rejection=None):
        filename = self.directory / "values.yaml"
        filename.write_text(yaml.safe_dump(self.values if values is None else values))
        args = self.helm + ["upgrade", "--install", "qual", str(ROOT / "charts/sink"), "-n", self.namespace,
                            "-f", str(filename), "--wait", "--timeout", "15m"]
        result = subprocess.run(args, text=True, capture_output=True)
        if rejection:
            if result.returncode == 0 or rejection not in result.stderr:
                raise RuntimeError(f"expected rejection {rejection!r}: {result.stdout} {result.stderr}")
            self.log(f"guard rejected: {rejection}")
        elif result.returncode:
            raise RuntimeError(result.stdout + result.stderr)

    def probe(self, name, options):
        pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name}, "spec": {
            "restartPolicy": "Never", "securityContext": {"runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532},
            "containers": [{"name": "probe", "image": self.image, "imagePullPolicy": "Never", "args": options,
                            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"memory": "512Mi"}},
                            "volumeMounts": [{"name": "output", "mountPath": "/output"}]}],
            "volumes": [{"name": "output", "emptyDir": {}}]}}
        self.apply([pod])

    def probe_result(self, name):
        self.wait(f"business probe {name} completes", lambda: self.get("pod", name)["status"].get("phase") in ("Succeeded", "Failed"))
        logs = self.command(self.kubectl + ["logs", name])
        (self.report_dir / f"{name}.json").write_text(logs)
        result = json.loads(logs)
        if self.get("pod", name)["status"]["phase"] != "Succeeded" or result["errors"]:
            raise RuntimeError(f"probe failed: {logs}")
        return result

    def setup(self):
        clusters = self.command(["kind", "get", "clusters"]).splitlines()
        if self.args.kubeconfig:
            # Reuse is only for an explicitly created disposable cluster during development.
            if self.cluster not in clusters:
                raise RuntimeError("requested owned Kind cluster does not exist")
            expected_nodes = set(self.command(["kind", "get", "nodes", "--name", self.cluster]).splitlines())
            actual_nodes = {node["metadata"]["name"] for node in self.get("nodes")["items"]}
            if actual_nodes != expected_nodes:
                raise RuntimeError("kubeconfig does not identify the owned Kind cluster")
            self.owned = True
        else:
            if self.cluster in clusters:
                raise RuntimeError("cluster already exists; refusing to adopt or delete it")
            self.owned = True
            self.command(["kind", "create", "cluster", "--name", self.cluster, "--image", NODE,
                          "--kubeconfig", self.kubeconfig, "--wait", "120s"])
        self.log("owned Kind cluster ready")
        # Reuse local public image cache without downloading or deleting shared images.
        cached = []
        for image in ["ghcr.io/liran/sink:0.15.0", "mongo:8.2", "apache/kafka:4.2.1",
                      "ghcr.io/kedacore/keda:2.20.0", "ghcr.io/kedacore/keda-metrics-apiserver:2.20.0",
                      "ghcr.io/kedacore/keda-admission-webhooks:2.20.0"]:
            inspect = subprocess.run(["docker", "image", "inspect", image], capture_output=True)
            if inspect.returncode == 0:
                cached.append(image)
        if cached:
            self.command(["kind", "load", "docker-image", *cached, "--name", self.cluster])
        namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": self.namespace}}
        self.apply([namespace])
        architecture = self.command(["docker", "info", "--format", "{{.Architecture}}"]).strip()
        arch = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}[architecture]
        environment = {**os.environ, "GOWORK": "off", "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": arch}
        self.command(["go", "build", "-trimpath", "-o", str(self.directory / "probe"), "."], cwd=ROOT / "tests/integration/probe", env=environment)
        (self.directory / "Dockerfile").write_text("FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab\nCOPY probe /probe\nENTRYPOINT [\"/probe\"]\n")
        self.command(["docker", "build", "-t", self.image, str(self.directory)])
        self.image_built = True
        self.command(["kind", "load", "docker-image", self.image, "--name", self.cluster])
        self.command(self.helm + ["upgrade", "--install", "keda", "keda", "--repo", "https://kedacore.github.io/charts", "--version", "2.20.0", "-n", self.namespace, "--wait", "--timeout", "10m"])
        self.command(self.kubectl + ["apply", "-f", str(ROOT / "tests/integration/backends.yaml")])
        self.wait("isolated Mongo and Kafka ready", lambda: self.available("mongo") and self.available("kafka"))
        self.command(self.kubectl + ["exec", "deployment/mongo", "--", "mongosh", "--quiet", "--eval",
            'try { rs.status() } catch (e) { rs.initiate({_id:"rs0",members:[{_id:0,host:"mongo:27017"}]}) }'])
        for store in ["mongo", "archive"]:
            for role in ["engine", "worker"]:
                config = {"mode": role, "health": {"address": ":8081"}, "shutdown_timeout": "30s", "service": {"request": {"timeout": "30s"}},
                    "storage": {"name": store, "driver": "mongodb", "mongodb": {"uri": "mongodb://mongo:27017/?replicaSet=rs0"}}}
                if role == "engine":
                    config["grpc"] = {"address": ":8080"}
                if store == "mongo":
                    config["storage"]["kafka"] = {"enabled": True, "brokers": ["kafka:9092"],
                        "topic": {"name": "mongo-mutations", "partitions": 4, "replication_factor": 1, "min_insync_replicas": 1},
                        "consumer": {"group_id": "mongo-workers"}}
                if role == "worker" and store == "archive":
                    continue
                secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": f"{store}-{role}-config"}, "stringData": {"sink.yaml": yaml.safe_dump(config)}}
                self.apply([secret])
        self.upgrade()
        self.wait("initial Engine discovery converges", lambda: self.available("qual-sink-mongo-engine"))

    def exercise(self):
        self.probe("traffic", ["-dataset", "rollout"])
        self.traffic_active = True
        self.wait("persistent business client starts", lambda: self.get("pod", "traffic")["status"].get("phase") == "Running")
        time.sleep(10)
        self.log("rolling Gateway and Engine with default drain budgets")
        self.values["gateway"]["pod"]["configRevision"] = "roll-1"
        self.values["stores"]["mongo"]["engine"]["pod"] = {"configRevision": "roll-1"}
        self.upgrade()
        self.wait("rolling Pods fully terminate", self.no_terminating)
        self.log("Engine scale 2 -> 3 -> 2 under traffic")
        self.values["stores"]["mongo"]["engine"]["replicaCount"] = 3
        self.upgrade()
        self.wait("third Engine becomes discoverable before scale-down", lambda: self.available("qual-sink-mongo-engine"))
        self.values["stores"]["mongo"]["engine"]["replicaCount"] = 2
        self.upgrade()
        self.wait("scaled-down Engine fully terminates", self.no_terminating)
        invalid = copy.deepcopy(self.values)
        invalid["stores"]["archive"] = {"state": "active"}
        self.upgrade(invalid, "must first be staged")
        self.values["stores"]["archive"] = {"state": "staged", "engine": {"config": {"existingSecret": "archive-engine-config"}}}
        self.upgrade()
        invalid = copy.deepcopy(self.values)
        invalid["stores"]["archive"]["state"] = "active"
        invalid["stores"]["archive"]["engine"]["pod"] = {"configRevision": "unverified"}
        self.upgrade(invalid, "activation must be a separate upgrade")
        invalid = copy.deepcopy(self.values)
        invalid["stores"]["archive"]["state"] = "active"
        if not self.available("qual-sink-archive-engine"):
            self.upgrade(invalid, "Engine is not fully available")
        self.wait("staged Engine passes the discovery availability window", lambda: self.available("qual-sink-archive-engine"))
        self.values["stores"]["archive"]["state"] = "active"
        self.upgrade()
        self.wait("new Store routing converges", self.no_terminating)
        self.probe("archive-check", ["-store", "archive", "-dataset", "activated", "-duration", "5s"])
        self.probe_result("archive-check")
        invalid = copy.deepcopy(self.values)
        del invalid["stores"]["archive"]
        self.upgrade(invalid, "must be retiring before removal")
        self.values["stores"]["archive"]["state"] = "retiring"
        self.upgrade()
        invalid = copy.deepcopy(self.values)
        invalid["stores"]["archive"]["state"] = "staged"
        self.upgrade(invalid, "cannot return to staged")
        invalid = copy.deepcopy(self.values)
        del invalid["stores"]["archive"]
        self.upgrade(invalid, "removal requires")
        invalid["lifecycle"] = {"removalApprovals": ["archive"]}
        # Retirement returns with newly ready Pods while old Gateways still drain.
        if not self.no_terminating():
            self.upgrade(invalid, "old or terminating Gateway Pods")
        self.wait("retired Store's old Gateway Pods drain", self.no_terminating)
        self.values = invalid
        self.upgrade()
        self.log("Store staged -> active -> retiring -> removed without disrupting mongo")
        self.command(self.kubectl + ["exec", "traffic", "--", "/probe", "-stop"])
        result = self.probe_result("traffic")
        self.log(f"continuous traffic reconciled {result['verified']} records without errors")
        self.log("publish Kafka backlog while Worker is zero")
        self.probe("publish", ["-mode", "publish", "-dataset", "async", "-count", "20000"])
        published = self.probe_result("publish")
        worker = self.values["stores"]["mongo"]["worker"]
        worker["autoscaling"] = {"mode": "keda", "minReplicas": 0, "maxReplicas": 3,
                                 "keda": {"pollingInterval": 5, "cooldownPeriod": 30}}
        self.upgrade()
        self.wait("KEDA scales backlog to multiple consumers", lambda: self.get("deployment", "qual-sink-mongo-worker")["spec"]["replicas"] >= 2, timeout=180)
        self.wait("multiple Worker Pods become ready", lambda: self.get("deployment", "qual-sink-mongo-worker").get("status", {}).get("readyReplicas", 0) >= 2, timeout=180)
        members = self.command(self.kubectl + ["exec", "deployment/kafka", "--", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "kafka:9092", "--describe", "--group", "mongo-workers", "--members", "--verbose"])
        (self.report_dir / "consumer-members.txt").write_text(members)
        self.probe("verify", ["-mode", "verify", "-dataset", "async", "-count", str(published["acknowledged"])])
        verified = self.probe_result("verify")
        self.wait("KEDA returns Workers to zero after lag drains", lambda: self.get("deployment", "qual-sink-mongo-worker")["spec"]["replicas"] == 0, timeout=600)
        self.wait("Kafka consumers finish graceful departure", self.no_terminating)
        self.log(f"KEDA 0 -> multiple -> 0 verified {verified['verified']} asynchronously accepted records")
        self.probe("publish-again", ["-mode", "publish", "-dataset", "async-again", "-count", "100"])
        self.probe_result("publish-again")
        self.probe("verify-again", ["-mode", "verify", "-dataset", "async-again", "-count", "100"])
        self.probe_result("verify-again")
        self.log("scale-from-zero reactivation verified")

    def diagnostics(self):
        if not self.owned:
            return
        for kind in ["pods", "deployments", "events", "scaledobjects", "hpa", "endpointslices"]:
            try:
                (self.report_dir / f"{kind}.json").write_text(json.dumps(self.get(kind), indent=2))
            except Exception as error:
                self.log(f"diagnostic {kind}: {error}")
        try:
            pods = self.get("pods")["items"]
            for pod in pods:
                name = pod["metadata"]["name"]
                try:
                    logs = self.command(self.kubectl + ["logs", name, "--all-containers", "--tail", "200"])
                    (self.report_dir / f"{name}.log").write_text(logs)
                except Exception as error:
                    self.log(f"log collection for {name}: {error}")
        except Exception as error:
            self.log(f"log collection: {error}")

    def cleanup(self):
        if not self.owned:
            return
        self.command(["kind", "delete", "cluster", "--name", self.cluster])
        if self.image_built:
            subprocess.run(["docker", "image", "rm", self.image], capture_output=True, check=False)
        self.log("owned cluster, namespace, controllers and probe image removed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", help="owned sink-chart-* name; defaults to a unique name")
    parser.add_argument("--kubeconfig", help="explicit owned Kind kubeconfig for development reuse; cluster is still deleted")
    args = parser.parse_args()
    for executable in ["kind", "kubectl", "helm", "docker", "go"]:
        if not shutil.which(executable):
            parser.error(f"required executable missing: {executable}")
    with tempfile.TemporaryDirectory(prefix="sink-chart-qual-") as directory:
        qualification = Qualification(args, directory)
        try:
            qualification.setup()
            qualification.exercise()
            qualification.log("PASS")
        finally:
            qualification.diagnostics()
            qualification.cleanup()


if __name__ == "__main__":
    main()
