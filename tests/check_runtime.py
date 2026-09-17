"""Opt-in released-image config check, called by CI (not unittest discovery)."""
from pathlib import Path
import subprocess
import tempfile

import yaml

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "ghcr.io/liran/sink@sha256:b240d56f3df686e9e42d863deb7ac7f52bddf321397b43ba919f665ddfbf21a7"


def main():
    rendered = subprocess.check_output(["helm", "template", "check", str(ROOT / "charts/sink"),
                                       "-f", str(ROOT / "examples/cluster-values.yaml")], text=True)
    gateway = next(doc for doc in yaml.safe_load_all(rendered)
                   if doc and doc["kind"] == "ConfigMap" and "sink.yaml" in doc.get("data", {}))
    with tempfile.TemporaryDirectory(prefix="sink-chart-config-") as directory:
        config_dir = Path(directory)
        (config_dir / "gateway.yaml").write_text(gateway["data"]["sink.yaml"])
        for role in ["engine", "worker"]:
            (config_dir / f"{role}.yaml").write_text((ROOT / f"examples/{role}-config.yaml").read_text())
        for role in ["engine", "worker", "gateway"]:
            subprocess.run(["docker", "run", "--rm", "--network", "none", "-v", f"{config_dir}:/config:ro", IMAGE,
                            "config", "check", "--config", f"/config/{role}.yaml"], check=True)


if __name__ == "__main__":
    main()
