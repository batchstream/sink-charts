# Sink Helm charts

[![Charts](https://github.com/batchstream/sink-charts/actions/workflows/ci.yaml/badge.svg)](https://github.com/batchstream/sink-charts/actions/workflows/ci.yaml)

Helm charts for [Sink](https://github.com/batchstream/sink), a gRPC data layer for
MongoDB, Elasticsearch, and OpenSearch. The initial `sink` chart is version
`0.1.0` and targets Sink `0.15.0`.

Each release deploys one Gateway, Engine, or Worker. Install a Gateway plus an
Engine release per Store; add a Worker release per Store when asynchronous
delivery is needed. Database and Kafka clusters are managed separately.

## Requirements

- Helm 3.17 or newer.
- Kubernetes 1.30 or newer, with the default `PodLifecycleSleepAction` feature
  enabled. The chart uses Kubernetes' [native sleep lifecycle hook](https://kubernetes.io/blog/2025/05/14/kubernetes-v1-33-updates-to-container-lifecycle/), which works with Sink's distroless image.
- An existing configuration Secret in the release namespace, containing a
  `sink.yaml` key. The chart references this Secret without storing backend
  credentials in Helm values or generating a Secret.

## Install from source

```shell
git clone https://github.com/batchstream/sink-charts.git
cd sink-charts
kubectl create namespace sink
```

Prepare a local configuration from `examples/engine-config.yaml`, set the real
backend address, and validate it with the matching Sink binary:

```shell
cp examples/engine-config.yaml engine.local.yaml
# Edit engine.local.yaml before continuing.
sink config check --config engine.local.yaml
kubectl -n sink create secret generic sink-primary-engine-config \
  --from-file=sink.yaml=engine.local.yaml
helm upgrade --install sink-primary-engine ./charts/sink \
  --namespace sink --values examples/engine-values.yaml --wait --timeout 10m
```

For Gateway, the supplied example routes Store `primary` to that Engine's
headless Service in the same namespace:

```shell
kubectl -n sink create secret generic sink-gateway-config \
  --from-file=sink.yaml=examples/gateway-config.yaml
helm upgrade --install sink-gateway ./charts/sink \
  --namespace sink --values examples/gateway-values.yaml --wait --timeout 10m
kubectl -n sink port-forward service/sink-gateway 8080:8080
```

The examples use plaintext traffic inside a trusted network. Apply your own
network access controls and transport protection before exposing the Gateway.
Use a namespace-qualified Engine DNS name for routes across namespaces.

For asynchronous delivery, prepare `examples/worker-config.yaml` with your
backend and Kafka addresses. Enable `storage.kafka` in the Engine configuration
with the same Store, brokers, and topic policy; the consumer group belongs only
in the Worker configuration. Create `sink-primary-worker-config` and install:

```shell
helm upgrade --install sink-primary-worker ./charts/sink \
  --namespace sink --values examples/worker-values.yaml --wait --timeout 10m
```

Full annotated configuration examples are maintained in
[Sink's versioned configs directory](https://github.com/batchstream/sink/tree/v0.15.0/configs).
Sink reads the mounted configuration directly and does not substitute `${ENV}`.

## Configuration

See [`values.yaml`](charts/sink/values.yaml) for all settings. A
[`values.schema.json`](charts/sink/values.schema.json) validates inputs before
rendering or installation.

| Value | Default | Purpose |
| --- | --- | --- |
| `mode` | `engine` | `gateway`, `engine`, or `worker`; must match the mounted configuration |
| `replicaCount` | `2` | Independent replica count for this release |
| `image.repository` | `ghcr.io/liran/sink` | Verified location of the 0.15.0 release image |
| `image.tag` | Chart app version | Override the Sink version |
| `image.digest` | Empty | Optional immutable digest, overriding the tag |
| `config.existingSecret` | `<fullname>-config` | Secret created by your secret-management process |
| `config.key` | `sink.yaml` | Secret key mounted as `/etc/sink/sink.yaml` |
| `service.type` | `ClusterIP` | Gateway Service type; Engine always uses a headless Service |
| `service.port` | `8080` | gRPC Service port; must equal `ports.grpc` for headless Engines |
| `ports` | `8080`, `8081`, `9090` | gRPC, health, and metrics container ports; must match Sink configuration |
| `metrics.enabled` | `false` | Expose metrics; also enable `prometheus` in Sink configuration |
| `preStopDelaySeconds` | `110` | Serving time before SIGTERM; role examples customize this |
| `terminationGracePeriodSeconds` | `180` | Total Pod termination budget |
| `podDisruptionBudget.minAvailable` | `1` | Minimum available replicas during voluntary disruptions |

Worker releases have no business gRPC port or Service. Enabling metrics creates
a metrics-only ClusterIP Service for a Worker. All roles use `/livez` and
`/readyz` on the dedicated health port. No Kubernetes API permissions are granted.

The image remains under `ghcr.io/liran/sink` because
[Sink 0.15.0](https://github.com/batchstream/sink/releases/tag/v0.15.0) was published
before the organization migration. Change the repository and version together
when selecting a later verified release.

## Operations

Sink loads configuration at startup. After changing an external Secret, restart
the corresponding Deployment, or change a revision in `podAnnotations` as part
of the Helm upgrade. Helm cannot detect externally managed Secret changes.

```shell
kubectl -n sink rollout restart deployment/sink-primary-engine
kubectl -n sink rollout status deployment/sink-primary-engine --timeout=10m
```

Keep a release's `mode`, names, and selectors stable across upgrades; use a new
release when changing a component's role. Tune resources, Go memory settings,
replicas, and disruption budgets together. A single replica with
`minAvailable: 1` blocks voluntary eviction. Configure `affinity` and
`topologySpreadConstraints` to spread production replicas across nodes and zones.

The default 110-second Engine pre-stop delay and 180-second termination grace
follow Sink's illustrative [rolling-upgrade budget](https://github.com/batchstream/sink/blob/v0.15.0/docs/rolling-upgrades.md).
Measure discovery/cache delays and request lifetimes in your cluster. The total
grace must also cover every sequential Sink shutdown phase; passing Helm
validation alone does not establish interruption-free rollouts.

## Development

```shell
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
make check PYTHON=.venv/bin/python
```

Checks cover strict Helm linting, role-specific rendering, discovery and port
contracts, Secret references, image selection, invalid values, and packaging.
CI additionally checks Kubernetes resource schemas and validates example
configurations offline with the released Sink image. These checks do not deploy
to a cluster or exercise backend connectivity.

Packaged charts are written to `dist/`. Bump `charts/sink/Chart.yaml`'s `version`
for chart changes and `appVersion` when updating the default Sink version. This
repository initially distributes chart source; no hosted chart index or OCI
chart release is configured.

## License

[MIT](LICENSE).
