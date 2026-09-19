# Sink Helm charts

When using this chart, **values are the only runtime configuration source**.
Do not maintain a separate Sink config YAML. The chart renders every role's
configuration with shared Store settings and managed operational defaults;
external Secrets contain credential values only.

One release deploys a Sink cluster: a shared Gateway and independently configured
Engine and Worker Deployments for each Store. Chart `0.8.0` generates runtime
configuration and references individual credential keys in external Secrets. It targets Sink **0.19.0**, SDK **0.8.0**, and
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

This configuration requires Sink 0.19.0. Until that release is published, build
its matching candidate and override `image.repository`, `image.tag`, and
`image.digest: ""`. Do not pair Chart 0.8 with a 0.18 or older image.

Each Store gets one `store.yaml` ConfigMap shared by Engine and Worker. Both roles
load it using `--store-config`; their own `sink.yaml` holds only role tuning.
Use `gateway.runtime.forwarding` / `request`, `engineDefaults.runtime.execution` /
`batching` / `producer`, and `workerDefaults.runtime.execution` / `consumer`.
Consumer group belongs in `stores.<name>.worker.runtime.consumer.group_id`.
There is no service-wide request timeout; callers control request lifetime.


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
When upgrading an existing Sink 0.16 installation to Chart 0.7, explicitly keep
its current global image tag and digest in your values while advancing all Engines
with `engineDefaults.image`. Wait for every old Engine Pod to exit before advancing
`gateway.image`; advance Workers separately. See the
[staged upgrade procedure](docs/operations.md#staged-image-upgrades).
The new global default is intended for fresh installations or a completed staged upgrade.

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

### Diagnostic logging

Set `gateway.runtime.logging`, `engineDefaults.runtime.logging`, and
`workerDefaults.runtime.logging`. Store-specific `stores.<name>.engine.runtime.logging`
and `stores.<name>.worker.runtime.logging` merge into their role defaults, including
nested labels and OTLP settings. Explicit `false` overrides are preserved.
`logging.level` applies to all components. Empty maps omit the section and retain
the selected Sink image's logging defaults with OTLP disabled; keep them empty on
images older than Sink 0.18.0. The logging overlay explicitly selects text output.

The [logging overlay](examples/logging-values.yaml) documents every supported
field and can be applied after `cluster-values.yaml`. Replace its Collector
endpoint and deployment labels. It uses verified TLS; set `otlp.tls.enabled: false`
explicitly for a plaintext receiver. `grpc` and `http/protobuf` use a `host:port`
endpoint (typically 4317 and 4318), without a scheme or path. The chart does not
install a Collector. See the [Sink logging reference](https://github.com/batchstream/sink/blob/v0.18.0/docs/logging.md)
for event meanings and bounds. `failure_body` defaults to false; enabling it can
include document content in supported severe-failure logs.

Pods receive `POD_UID`, `POD_NAME`, `POD_NAMESPACE`, and `NODE_NAME` through the
Downward API. Explicit entries in a role's `pod.env` override these defaults;
`logging.labels` takes precedence over the matching environment values. Logging
configuration changes update the Pod config checksum and trigger a rolling restart.

Schema and template checks validate settings after role/Store merging, including
at least one output, OTLP endpoint/queue limits, bounded durations and body sizes.
When OTLP is enabled, `pod.shutdownBudgetSeconds` must cover four application
shutdown timeouts plus `logging.otlp.shutdown_timeout` (default 5s, rounded up to
whole seconds). The existing 150s budget covers the maximum 30s log flush with
default 30s application timeouts. Termination grace and autoscaler cooldown checks
continue to use the whole shutdown budget.

## Validate

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
make check PYTHON=.venv/bin/python
```

Rendering tests are offline. `make integration` is an explicit Docker/Kind test;
see [tests/integration](tests/integration/README.md). CI also checks Kubernetes
schemas and complete example configurations against the pinned Sink image.

### Process memory admission

Sink 0.19.0 uses high/low watermarks through `gateway.runtime.memory`,
`engineDefaults.runtime.memory`, and `workerDefaults.runtime.memory`, with per-Store
overrides. Supported fields are `max_bytes`, `high_watermark_percent` (default 80),
and `low_watermark_percent` (default 70). Require `0 < low < high < 100`.
An empty map selects server defaults. Startup panics when estimated minimum
working memory does not fit below the high watermark; larger gRPC/Lua/Kafka
buffers may require larger Pod limits. See [server sizing](https://github.com/batchstream/sink/blob/main/docs/design/demand-based-admission.md).

[The memory KEDA overlay](examples/memory-keda-values.yaml) uses observed memory
pressure and RPC rejection ratios with `metricType: Value`. Configure Prometheus
selectors for one role/Deployment/Store and retain Worker Kafka lag triggers.
Missing data remains a scaler error. Gateway and Engine must use matching private
protocol versions; incompatible versions require an isolated cutover.
