"""Opt-in image config check with synthetic projected credentials; no backends."""
import argparse
from pathlib import Path
import subprocess
import tempfile

import yaml

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sink-image", help="explicit candidate image; defaults to the chart's pinned release")
    args = parser.parse_args()
    defaults = yaml.safe_load((ROOT / "charts/sink/values.yaml").read_text())["image"]
    image = args.sink_image or (f"{defaults['repository']}@{defaults['digest']}" if defaults["digest"] else f"{defaults['repository']}:{defaults['tag']}")
    for example in ["cluster-values.yaml", "search-values.yaml"]:
        rendered = subprocess.check_output(["helm", "template", "check", str(ROOT / "charts/sink"),
                                           "-f", str(ROOT / "examples" / example)], text=True)
        configs = [doc for doc in yaml.safe_load_all(rendered)
                   if doc and doc["kind"] == "ConfigMap" and "sink.yaml" in doc.get("data", {})]
        with tempfile.TemporaryDirectory(prefix="sink-chart-config-") as directory:
            root = Path(directory)
            root.chmod(0o755) # Public fixtures readable by the image's nonroot UID.
            for name, value in {"mongodb-uri": "mongodb://example.invalid:27017", "search-username": "fixture",
                                "search-password": "  fixture:$#quotes\"'\\\nunicode-雪 \n", "search-api-key": "fixture-key"}.items():
                (root / name).write_text(value)
            for config in configs:
                filename = config["metadata"]["name"] + ".yaml"
                (root / filename).write_text(config["data"]["sink.yaml"])
                subprocess.run(["docker", "run", "--rm", "--network", "none", "-v", f"{root}:/etc/sink:ro",
                                "-v", f"{root}:/etc/sink-secrets:ro", image,
                                "config", "check", "--config", f"/etc/sink/{filename}"], check=True)


if __name__ == "__main__":
    main()
