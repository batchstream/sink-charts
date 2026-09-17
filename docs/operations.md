# Operating a Sink cluster

## Configuration and initial admission

Chart 0.2 is a new cluster interface. Store map keys are stable Sink identities;
changing a key creates a different Store. Each Store gets an Engine headless Service
and Deployment, plus an optional Worker Deployment. Only `state: active` Stores
appear in the generated Gateway configuration. With no active Stores, no Gateway
is deployed. Engine and Worker Secrets are external and never created or deleted
by this chart. Backends, records and Kafka topics are never deleted by the chart.

Every external Secret must contain **complete** Sink YAML under `config.key`:

| Contract | Required value |
| --- | --- |
| `mode` | `engine` or `worker`, matching the workload |
| `storage.name` | Exact Store map key |
| `storage.driver` and backend configuration | Same durable backend for the matching Engine and Worker |
| `grpc.address` (Engine) | `:8080` |
| `health.address` | `:8081` |
| `prometheus.enabled/address` | `metrics.enabled` and `:9090` |
| `service.request.timeout` | No greater than chart `requestTimeoutSeconds` |
| `shutdown_timeout` | Component `pod.shutdownTimeoutSeconds` |
| Kafka brokers, source/DLQ topic, group and policy | Identical between Engine and Worker; scaler metadata must match |

The chart cannot inspect or prove the contents of an external Secret. Run
`sink config check --config FILE` in your secret delivery pipeline and verify this
contract before deployment. Neither `env` nor `envFrom` injects YAML values: Sink
has no environment substitution. Use `pod.configRevision` to roll Pods after
updating an external Secret; mounted file refresh alone does not reload Sink.
Versioned immutable Secrets plus a revision change make configuration rollback
reviewable. Do not combine a Store's activation with its configuration update.

Gateway listeners, routes, DNS interval and request timeout are chart-managed.
`gateway.runtime` accepts other supported Sink gateway/service/gRPC tuning. Check
the resulting configuration with the pinned server before applying new tuning.
Store capability readiness checks storage by default. `readiness: kafka` is useful
for an async-only Store; `process` deliberately admits without dependency checks.
A single dependency probe does not prove Mongo majority durability or search shard
availability. Before admitting traffic, run representative write/read and async
publish/application checks. Gateway `/readyz` proves process readiness, not every
routed Store. A Store outage should not restart otherwise healthy Gateway Pods.

Internal traffic is plaintext gRPC. Keep Engine and Worker networks private; for
untrusted networks use a separately configured service mesh/network policy and
validate its connection drain behavior. A public Gateway requires appropriately
configured TLS/authentication at your ingress or proxy. The chart does not create
those policies or silently expose a public LoadBalancer by default.

## Rolling upgrades and DNS

The defaults encode the measured failure mechanism from Sink's qualification:
a Gateway captures Engine membership for an accepted batch, so removing an endpoint
from DNS alone does not make it safe to close its listener.

```
Engine preStop >= endpoint publication + DNS cache + Gateway DNS refresh
                 + lookup/retry allowance + maximum remaining request + margin
Gateway preStop >= client/proxy/LB withdrawal + maximum remaining request + margin
Pod termination grace >= preStop + whole-process shutdown budget + margin
```

Default Engine: `5 + 30 + 10 + 10 + 30 + 10 = 95s` preStop, `255s` total grace.
Default Gateway: `60 + 30 + 10 = 100s` preStop, `260s` total grace.
Worker: no preStop wait, `160s` grace, allowing prompt consumer departure.
Each uses a 150s process shutdown budget and 10s final margin. The chart rejects
shorter overrides and a process budget smaller than four configured shutdown
timeouts. This lower bound is a guardrail, not proof that backend cleanup finishes
within it: `shutdown_timeout` bounds individual phases, not the entire process.
Measure worker settlement, gRPC drain, listener shutdown and backend disconnect.

Kubernetes marks terminating endpoints unready while native preStop sleep keeps
Sink accepting in-flight/stale-discovery calls. Engine discovery never publishes
unready addresses. `/livez` only checks the process; dependency failures do not
cause liveness restart loops. Startup gets up to five minutes. Engine
`minReadySeconds` is at least the DNS convergence allowance (55s by default), so a
surging replica has time to enter discovery before another old Pod is removed.
Rolling updates use `maxUnavailable: 0`, `maxSurge: 1`. Reserve surge capacity for
**each concurrently changing Deployment**, including terminating Pods; total Pod
count can temporarily exceed desired replicas plus one while old Pods drain.

Measure your CoreDNS and NodeLocal DNS positive/negative TTLs, EndpointSlice
publication, gRPC clients' re-resolution/backoff, proxies and load balancer target
deregistration. Direct Engine clients need their own discovery/connection budgets;
the Engine calculation above models Gateway callers. `dnsRefreshSeconds` does not
flush upstream caches. SERVFAIL,
stale answers or a partition can exceed any finite drain window. Abrupt node loss,
OOM/SIGKILL, forced deletion, backend quorum loss or clients with shorter deadlines
can still fail requests. Use deadlines and operation identifiers to reconcile
ambiguous mutation outcomes; do not blindly retry non-idempotent writes.

PDBs govern voluntary evictions, not Deployment rolling updates, manual scale-down,
node crashes or OOM. Soft hostname/zone spread allows scheduling in small clusters;
production HA needs enough actual nodes/zones and spare capacity. Override affinity
or spread constraints to make placement strict when that capacity is guaranteed.
Avoid simultaneously draining nodes and rolling/scaling all components.

## Store lifecycle

Use a full, version-controlled values file for each upgrade, with `--wait --timeout
15m`. Also check `availableReplicas` against desired replicas: Helm can return
when Pods are Ready before `minReadySeconds` elapses. Inspect terminating Pods as
well: Deployment availability does not mean all
old Pods have exited. Helm hooks do not delete data, and there is no resource keep
policy leaving orphaned Deployments behind.

1. **Stage:** add a new Store with `state: staged` and create its complete config
   Secrets. Engines/Workers start, but Gateway does not route to it. Wait for Engine
   available replicas and check the representative backend and Kafka operations
   directly through its headless Service. Unique topics and consumer groups prevent
   cross-Store delivery; consumer count cannot exceed topic partitions.
2. **Activate:** change only `state` to `active` in a separate upgrade. Live Helm
   rendering rejects activation until the existing Engine Deployment has observed
   its current generation and has all desired updated/available replicas. The
   unchanged Engine configuration hash prevents activating an untested new config.
   Gateway configuration changes roll its Pods. Wait for all Gateway Pods to carry
   the new checksum before expecting every client to reach the new Store.
3. **Retire:** first stop all writers/publishers to the Store, including direct
   Engine users. Wait for accepted calls. Change it to `retiring`. The Gateway route
   is removed; Engine and Worker resources remain. Requests from clients that still
   use the retired Store will correctly fail after withdrawal; the workflow cannot
   promise uninterrupted service to a deliberately removed Store. Other Stores
   continue serving during the Gateway rollout.
4. **Drain and remove:** wait until all old/terminating Gateway Pods have gone,
   verify source lag is zero, resolve/replay or explicitly retain DLQ records, and
   verify final data. Then delete the Store entry and include its name in
   `lifecycle.removalApprovals`. Live Helm rejects direct active Store deletion,
   missing approval and retirement while old Gateway Pods remain. Keep external
   Secrets/backends/topics until your retention/rollback requirements are satisfied.
   Clear completed removal approvals from the next values revision. A previously
   routed Store cannot return to `staged`, which would bypass retirement draining.

Disabling a Worker also requires retirement plus a removal approval after draining.
Kafka broker/topic/group/partition metadata is immutable while a Store exists;
lag thresholds can change. Topic partition changes can alter record affinity:
perform a drained migration to a new topic/group, never just raise the scaler cap.
Backend targets are private Secret content, so the chart cannot enforce their
immutability. Do not switch a Store to a different backend in place.

The live guard uses Helm `lookup` of the inventory ConfigMap, Deployments and Pods;
the Helm operator identity needs namespace read access to these objects. Workload
ServiceAccounts have no RBAC or mounted API token. An initial fresh install may
contain active Stores; admit traffic only after every required workload and backend
check passes. Subsequent Store additions must first be staged, including when the
release was initially empty.

`helm template` and client-side dry-run cannot execute these checks. For GitOps,
run the same staged/activation/retirement reviews in separate reconciliations and
verify live state externally; use a server-side Helm dry-run where supported.
`lifecycle.enforceTransitions: false` explicitly delegates these checks to your
release pipeline. Helm rollback replays stored manifests without re-running the
checks, and uninstall/direct kubectl changes bypass them entirely. Do not use
`--atomic` or unattended rollback across Store topology transitions. To uninstall,
stop producers, drain accepted calls and Kafka/DLQ, withdraw client traffic, wait
for discovery, then uninstall. Changing release/fullname/namespace is a migration,
not a supported way to bypass inventory checks.

## Resources and fixed-budget capacity

Requests drive scheduling and CPU HPA percentages. Every component has a memory
request/limit and CPU request. CPU limits are optional because throttling can harm
tail latency; set them when your tenancy policy requires hard CPU caps. Store
Engine/Worker overrides merge independently with their role defaults. Integer
memory quantities (`512Mi`, `2Gi`, decimal bytes, `M`/`G`) are supported; fractional
or milli-byte quantities are rejected. `GOMEMLIMIT` defaults to 80% of the container
memory limit, leaving headroom for non-Go memory. It is a soft Go runtime target,
not an OOM guarantee. Do not override it through `env`; use `goMemoryLimitPercent`.

Application queues, execution bytes, publish buffers, Kafka fetch sizes and gRPC
message limits must fit the memory budget too. Set these in the complete runtime
configuration and qualify representative maximum documents/fanout. Increasing
replicas cannot repair an overloaded database or Kafka partition. Budget peak
resources as desired replicas **plus surge and terminating replicas**, multiplied
by per-Pod requests, then add Kafka/backend/controllers and operational headroom.

The prior fixed-resource benchmark is workload-specific, not a cluster sizing
promise. Compare achieved throughput, p95/p99, rejection/error rate, queue age,
Kafka lag age and memory at a fixed total resource budget. Choose thresholds below
sustained saturation with room for one replica's withdrawal. Observe backend pool
and connection growth when scaling; more replicas may reduce useful throughput.

## Autoscaling

Exactly one `autoscaling.mode` owns replicas: `none`, `hpa`, or `keda`. When a
controller is enabled, the Deployment omits `replicas`, preventing ordinary Helm
upgrades from resetting a controller's live count. Gateway/Engine minimum is two.
Workers can run one only with a compatible PDB; zero requires `allowScaleToZero`
and a disabled PDB. Zero means async application waits for polling and cold start.
Synchronous Engine operations remain independent of Worker count.

HPA supports CPU, memory and additional Kubernetes metric specifications.
KEDA Gateway/Engine defaults to CPU; custom triggers replace that CPU trigger.
Worker KEDA always includes a Kafka lag trigger and appends any custom triggers.
Kafka uses earliest offset bootstrap, explicit source topic and consumer group,
no idle consumers, no persistent-lag exclusion, and no invalid-offset zeroing.
The replica ceiling must be at most the partition count. Use `lagThreshold` as
records per consumer, measured against application time, not arbitrary CPU ratios.
Retries/poison records can hold lag high; do not mask them to force scale-down.

CPU HPA/KEDA needs metrics-server; external HPA metrics need an adapter. KEDA and
its CRDs must exist before enabling it. The chart references existing
TriggerAuthentication/ClusterTriggerAuthentication; match Kafka TLS/SASL policy to
the actual brokers. The scaler's access is separate from Sink's Kafka access.
Fallback can be enabled for supported AverageValue triggers; CPU/memory fallback
is rejected. Kafka scaler failure should retain capacity via a tested fallback,
not be assumed to mean zero lag.

Default HPA scale-down allows one Pod per 300s with a 300s stabilization window.
The period must cover Pod termination grace, limiting overlapping departures.
Scale-up allows two Pods per minute. Tune scale-up against partition rebalance and
backend capacity. KEDA `cooldownPeriod` covers **1 -> 0**; HPA behavior governs
nonzero scaling. KEDA zero transitions and manual replica edits are not protected
by the HPA scale-down rate. Worker graceful Kafka departure still needs its entire
termination budget. Choose cooldown longer than routine idle gaps to avoid churn.

Controller changes require an explicit handoff: record current desired count,
freeze application rollouts, switch to `none` with that replicaCount and wait for
the old HPA/ScaledObject (including KEDA-generated HPA) to disappear, then enable
the new mode. Do not run an external HPA against these Deployments concurrently.
A first install with autoscaling may briefly start at Kubernetes' default one Pod
until its controller reconciles; do not admit traffic until the configured minimum
is available. This is why live availability checks matter beyond Helm rendering.

## References

- [Sink rolling-upgrade contract](https://github.com/batchstream/sink/blob/v0.15.0/docs/rolling-upgrades.md)
- [Kubernetes termination flow](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination-flow)
- [Native lifecycle hooks](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/)
- [HPA behavior and replica ownership](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)
- [KEDA Kafka scaler](https://keda.sh/docs/2.20/scalers/apache-kafka/)
- [KEDA ScaledObject settings](https://keda.sh/docs/2.20/reference/scaledobject-spec/)
- [Helm lookup semantics](https://helm.sh/docs/chart_template_guide/functions_and_pipelines/#using-the-lookup-function)
