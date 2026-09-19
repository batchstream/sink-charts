# Sink cluster

When using this chart, **values are the only runtime configuration source**.
Do not maintain a separate Sink config YAML. The chart renders every role's
configuration with shared Store settings and managed operational defaults;
external Secrets contain credential values only.

Chart 0.7.0 deploys one Gateway and independent Engine/Worker workloads per Store.
It uses Sink 0.18.0 and requires Kubernetes 1.30+ with native lifecycle sleep hooks.
This replaces complete-config Secrets with ordinary values and per-field Secret references.

The default empty `stores` map creates an inventory only. Before installing,
provision externally managed credential Secrets in the release namespace.
Engine and Worker share `stores.<name>.storage`; runtime YAML is generated.
The chart does not install backend databases, Kafka, KEDA or monitoring operators.

Gateway clients can use the stable ClusterIP Service or the optional headless
Service. Their names derive from the Helm release name and therefore do not change
during upgrades of that release. For a release named `sink` in namespace `sink`,
the headless SDK address is
`dns:///sink-sink-gateway-headless.sink.svc.cluster.local:8080`.

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

Store keys are stable DNS labels of 1–32 lowercase letters/digits/hyphens. Each
Store inherits independent Engine and Worker defaults, with nested overrides.
`none`, `hpa` and `keda` are mutually exclusive replica controllers. HPA CPU metrics
require metrics-server; KEDA requires its operator/CRDs before installation.
Kafka Worker scaling includes a generated lag trigger, partition-aware replica
ceilings, optional authentication references and explicitly enabled scale-to-zero.

Default Engine preStop is 95 seconds; Gateway preStop is 100 seconds. These derive
from declared discovery, request and shutdown budgets. Smaller unsafe overrides
fail rendering. The budgets must be measured against your DNS caches, proxies,
load balancers, clients and backend shutdown behavior. Unbounded DNS outages and
forced termination remain outside a bounded graceful rollout guarantee.

Image overrides are available at `gateway.image`, `engineDefaults.image`,
`workerDefaults.image`, and `stores.<name>.engine.image` / `worker.image`.
Fields inherit from the global `image`; Store overrides take precedence over role
defaults. A tag override clears an inherited digest unless that override also
supplies a digest. See the [upgrade procedure](https://github.com/batchstream/sink-charts/blob/v0.7.0/docs/operations.md#staged-image-upgrades)
for Engine-first upgrades and Gateway-first rollbacks across protocol changes.
For an existing Sink 0.16 installation, explicitly retain its current global image
tag and digest while upgrading Engines, then Gateways, then Workers. Wait for old
Pods to exit at each stage before adopting the new global default. Keep memory
settings empty on roles still running 0.16.0.

Configure `gateway.runtime.logging`, `engineDefaults.runtime.logging`, and
`workerDefaults.runtime.logging`, with nested overrides at
`stores.<name>.engine.runtime.logging` / `worker.runtime.logging`.
Empty maps preserve Sink's warn-level JSON stderr defaults and keep OTLP disabled.
Nonempty logging settings require Sink 0.18+; leave them empty on older image
overrides. All fields are shown in the [logging overlay](https://github.com/batchstream/sink-charts/blob/v0.7.0/examples/logging-values.yaml).
Use an existing Collector for OTLP; the chart does not install one. TLS is enabled
by default. `failure_body` can expose document content and stays disabled by default.
Pods receive identity through Downward API environment variables; explicit
`pod.env` entries override those defaults, and `logging.labels` overrides matching
environment labels. Configuration changes trigger rolling restarts.
The shutdown budget must also cover the configured OTLP shutdown flush (default
5s, maximum 30s); the default 150s budget already includes this allowance.

Use `staged -> active -> retiring -> remove` for Store changes. Live Helm upgrades
validate availability and drain transitions; offline GitOps rendering, rollback
and uninstall require the documented external checks. Wait for Available replicas
and terminating Pods, not only a successful `helm --wait`.

- [Complete deployment examples](https://github.com/batchstream/sink-charts/tree/v0.7.0/examples)
- [Operational runbook and configuration contract](https://github.com/batchstream/sink-charts/blob/v0.7.0/docs/operations.md)
- [Source and qualification tests](https://github.com/batchstream/sink-charts/tree/v0.7.0)

Search authentication supports `usernameSecretRef` + `passwordSecretRef`, or
`apiKeySecretRef`, each with `{name, key}`. Gateway never mounts Store credentials.
Secret bytes are read once at startup. Use versioned immutable Secret names to
roll both roles; `credentialRevision` forces a restart for in-place updates.
Keep both credentials valid until all old Pods have terminated. See the runbook
for projection, missing-key failure behavior, rotation, rollback and Kafka limits.
