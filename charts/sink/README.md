# Sink cluster

Chart 0.2.0 deploys one Gateway and independent Engine/Worker workloads per Store.
It uses Sink 0.15.0 and requires Kubernetes 1.30+ with native lifecycle sleep hooks.
This is a new values interface, replacing the earlier single-role chart.

The default empty `stores` map creates an inventory only. Before installing,
create external Secrets containing complete role-specific Sink configurations.
The chart does not install backend databases, Kafka, KEDA or monitoring operators.

```yaml
stores:
  orders:
    state: staged
    engine:
      config: {existingSecret: orders-engine-config}
      pod:
        configRevision: "v1"
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

Use `staged -> active -> retiring -> remove` for Store changes. Live Helm upgrades
validate availability and drain transitions; offline GitOps rendering, rollback
and uninstall require the documented external checks. Wait for Available replicas
and terminating Pods, not only a successful `helm --wait`.

- [Complete deployment examples](https://github.com/batchstream/sink-charts/tree/v0.2.0/examples)
- [Operational runbook and configuration contract](https://github.com/batchstream/sink-charts/blob/v0.2.0/docs/operations.md)
- [Source and qualification tests](https://github.com/batchstream/sink-charts/tree/v0.2.0)
