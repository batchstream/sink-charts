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
    parser.add_argument("--sink-binary", type=Path, help="local candidate binary; no Docker required")
    args = parser.parse_args()
    defaults = yaml.safe_load((ROOT / "charts/sink/values.yaml").read_text())["image"]
    image = args.sink_image or (f"{defaults['repository']}@{defaults['digest']}" if defaults["digest"] else f"{defaults['repository']}:{defaults['tag']}")
    examples = [["cluster-values.yaml"], ["search-values.yaml"],
                ["cluster-values.yaml", "logging-values.yaml"]]
    for files in examples:
        command = ["helm", "template", "check", str(ROOT / "charts/sink")]
        for example in files:
            command.extend(["-f", str(ROOT / "examples" / example)])
        rendered = subprocess.check_output(command, text=True)
        documents = [doc for doc in yaml.safe_load_all(rendered) if doc]
        configs = {doc["metadata"]["name"]: doc["data"] for doc in documents if doc["kind"] == "ConfigMap"}
        deployments = [doc for doc in documents if doc["kind"] == "Deployment"]
        with tempfile.TemporaryDirectory(prefix="sink-chart-config-") as directory:
            root = Path(directory)
            root.chmod(0o755) # Public fixtures readable by the image's nonroot UID.
            for name, value in {"mongodb-uri": "mongodb://example.invalid:27017", "search-username": "fixture",
                                "search-password": "  fixture:$#quotes\"'\\\nunicode-雪 \n", "search-api-key": "fixture-key"}.items():
                (root / name).write_text(value)
            for deployment in deployments:
                pod = deployment["spec"]["template"]["spec"]
                for source in pod["volumes"][0]["projected"]["sources"]:
                    for filename, content in configs[source["configMap"]["name"]].items():
                        if args.sink_binary:
                            content = content.replace("/etc/sink-secrets/", str(root) + "/")
                        (root / filename).write_text(content)
                arguments = pod["containers"][0]["args"]
                if args.sink_binary:
                    arguments = [arg.replace("/etc/sink/", str(root) + "/") for arg in arguments]
                    command = [str(args.sink_binary.resolve()), "config", "check", *arguments]
                else:
                    command = ["docker", "run", "--rm", "--network", "none", "-v", f"{root}:/etc/sink:ro",
                               "-v", f"{root}:/etc/sink-secrets:ro", image, "config", "check", *arguments]
                subprocess.run(command, check=True)



if __name__ == "__main__":
    main()
