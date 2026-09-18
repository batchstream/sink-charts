# Sink Helm charts

When using this chart, **values are the only runtime configuration source**.
Do not maintain a separate Sink config YAML. The chart renders every role's
configuration with shared Store settings and managed operational defaults;
external Secrets contain credential values only.

One release deploys a Sink cluster: a shared Gateway and independently configured
Engine and Worker Deployments for each Store. Chart `0.4.0` generates runtime
configuration and references individual credential keys in external Secrets. It targets Sink **0.16.0**, SDK **0.8.0**, and
Kubernetes **1.30+** with native lifecycle sleep hooks enabled.

- Stable Store names, generated headless discovery, staged activation and retirement.
- Stable Gateway ClusterIP and headless DNS names derived from the Helm release name.
- Per-Store resources, credential references, scheduling, replica ranges and HPA/KEDA.
- Calculated DNS withdrawal/request drain budgets, rolling surge, readiness and PDBs.
- Explicit Worker scale-to-zero, Kafka partition limits, lag triggers and authentication references.
- Unprivileged read-only containers, no Kubernetes token/RBAC, separate metrics Services.

The chart installs no database, Kafka, KEDA, metrics-server or Prometheus operator.
The default empty `stores` map creates only an inventory ConfigMap. Follow
[operations](docs/operations.md) before admitting production traffic; defaults are
budget assumptions, not a zero-error guarantee under arbitrary DNS/backend failures.

## Install

Create the release namespace and provision `sink-mongo-v1` and `sink-archive-v1`
Secrets there. Each example references a key named `uri` containing its complete
MongoDB URI. Use your secret manager; never commit credentials to values files.
Engine and Worker share one Store configuration and its Secret references. See
[credential handling and rotation](docs/operations.md#store-credentials) and
[search authentication examples](examples/search-values.yaml).

```sh
helm upgrade --install sink ./charts/sink --namespace sink --create-namespace \
  -f examples/cluster-values.yaml --wait --timeout 15m
```

The first `sink` is the Helm release name, not a version. Upgrading the same release
keeps `sink-sink-gateway` and `sink-sink-gateway-headless` stable. SDKs that need
client-side endpoint discovery can use
`dns:///sink-sink-gateway-headless.sink.svc.cluster.local:8080`.

Customize a copy of `cluster-values.yaml` for your actual backends. Its HPA example
requires metrics-server. Without it, use `autoscaling.mode: none`. To use Kafka lag
scaling, install KEDA first and add `-f examples/keda-values.yaml`. Authentication
uses existing TriggerAuthentication resources; scaler credentials are independent
of Store backend credentials. See the runbook for Sink Kafka authentication limits.

The image is pinned to a verified multi-architecture digest at
`ghcr.io/batchstream/sink`. Override `image.repository` **and** `image.digest`
together when moving artifacts. To select by tag, explicitly set `image.digest: ""`.
Chart 0.4 requires Sink 0.16+ and removes the complete-config Secret API from 0.2.
Move ordinary configuration into Store values and create per-field credential
Secrets before upgrading; the schema rejects the old `engine/worker.config` keys.

## Configure and operate

See [values.yaml](charts/sink/values.yaml) for defaults and
[the operational runbook](docs/operations.md) for rollout budgets, resource sizing,
Store lifecycle, controller handoff, GitOps, failure cases and configuration contracts.

```yaml
stores:
  orders:
    state: staged
    storage:
      driver: mongodb
      mongodb:
        uriSecretRef: {name: orders-mongo-v1, key: uri}
    engine:
      pod:
        resources:
          requests: {cpu: "1", memory: 1Gi}
          limits: {memory: 2Gi}
      autoscaling: {mode: hpa, minReplicas: 2, maxReplicas: 8}
```

## Validate

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
make check PYTHON=.venv/bin/python
```

Rendering tests are offline. `make integration` is an explicit Docker/Kind test;
see [tests/integration](tests/integration/README.md). CI also checks Kubernetes
schemas and complete example configurations against the pinned Sink image.

### Demand-based memory admission

Images that include the demand-based memory allocator (after Sink 0.16.0) accept
`gateway.runtime.memory`, `engineDefaults.runtime.memory`, and
`workerDefaults.runtime.memory`, with per-Store overrides under each role.
The map supports `max_bytes`, `burst_percent` (1–99), and `wait_timeout`.
Leave it empty for server automatic sizing and its measured 10% reserve default.
Empty maps are omitted from generated YAML so the chart remains compatible with
its pinned older image. Do not enable these fields on an older image.

[The memory KEDA overlay](examples/memory-keda-values.yaml) supplies managed-capacity
pressure and temporary-rejection signals with `metricType: Value`. Install KEDA,
configure Prometheus scraping and adjust job/namespace selectors to one Gateway
Deployment before enabling it. Engine selectors must also isolate the Store;
Workers should retain Kafka lag triggers. Missing metrics must remain a scaler
error rather than zero load. Upgrade Engines before Gateways for the private
framed-response protocol; roll back Gateways first. This example does not change
the pinned image or perform a cluster rollout.
