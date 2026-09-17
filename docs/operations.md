# Operating a Sink cluster

## Configuration and initial admission

Chart 0.4 uses a shared `stores.<name>.storage` object for each Engine/Worker
pair. **Maintain only values, not a separate Sink config YAML.** The chart generates
the complete configuration for every role. It requires Sink 0.16.0+. The previous complete configuration Secret interface
has been removed. Store map keys remain stable Sink identities; changing a key
creates a different Store. Only active Stores appear in the Gateway routes.

Common operational values are an abstraction over Sink configuration: listener
ports, time budgets, Store identity, Kafka policy and credentials are configured
once and rendered consistently for each role. MongoDB tuning uses
`metadataField` (default `__sink`), `maxConcurrentWrites` (64), and
`maxConcurrentGroups` (16). Resource/memory limits, discovery convergence, rolling
surge and graceful exit have the defaults described below. Advanced tuning remains
inside values under `runtime`; it never requires a separately maintained config file.

Gateway Service names derive from the Helm release name, not the chart version, so
upgrading one release keeps client addresses stable. A release named `sink` in the
`sink` namespace exposes `sink-sink-gateway.sink.svc.cluster.local` and, by default,
the headless discovery name `sink-sink-gateway-headless.sink.svc.cluster.local`.
The latter is suitable for SDK client-side endpoint discovery. Renaming a release
or namespace changes both names and is a cluster migration.

The chart generates ordinary runtime YAML in ConfigMaps. It manages role, Store
name, listener/metrics addresses, request and shutdown timeouts, and Kafka identity
from values. `engineDefaults.runtime`, `workerDefaults.runtime`, and their per-Store
overrides accept additional `service` and `grpc` tuning. `gateway.runtime` accepts
additional Gateway tuning. Managed fields cannot be overridden there. Kafka
brokers/topic/group/partitions and `replicationFactor` (default 3)/`minInSyncReplicas`
(default 2) are shared with both roles; `kafka.runtime` accepts other supported
topic/producer/consumer/dead_letter tuning. Validate tuning with the pinned Sink
before deployment. KEDA uses the same broker/topic/group metadata.

## Store credentials

Provision Secrets externally in the **release namespace**, using a secret manager,
External Secrets, Sealed Secrets, or your normal Kubernetes delivery pipeline.
The chart neither creates nor reads their values, nor deletes them on uninstall.
References follow Kubernetes `secretKeyRef` naming: `{name, key}`. There is no
cross-namespace reference or optional fallback. Only referenced keys are projected,
read-only with mode 0440 and Pod group 65532. Workloads need no Kubernetes API token
or Secret-reading RBAC. Gateway does not mount Store credentials.

```yaml
stores:
  orders:
    state: staged
    storage:
      driver: mongodb
      mongodb:
        uriSecretRef: {name: orders-mongo-v1, key: uri}
    worker:
      enabled: false
```

The `uri` key holds the **complete MongoDB connection URI**, including percent-encoded
credentials and required replicaSet/authSource/TLS options. Engine and Worker share
this reference. The generated config contains only
`uri_file: /etc/sink-secrets/mongodb-uri`. It does not interpolate a password into
a URI or put Secret data into Helm values/history, ConfigMaps, environment
variables, annotations, or command-line arguments.

Search Stores use `storage.search.endpoints` plus either:

- `username` (nonsecret) or `usernameSecretRef`, paired with `passwordSecretRef`;
- `apiKeySecretRef` for API-key authentication;
- neither for a deliberately unauthenticated development service.

The authentication modes are mutually exclusive. See
[search-values.yaml](../examples/search-values.yaml). Inline MongoDB URIs,
passwords and API keys are rejected by the chart schema. Do not embed credentials
in search endpoint URLs or other ordinary tuning values. Sink currently has no
Kafka SASL/TLS credential fields; KEDA's `authenticationRef` authenticates the
**scaler only** and does not configure Sink's Kafka clients.

Secret file bytes are preserved exactly, including spaces, `$`, quotes, Unicode
and newlines. Avoid accidental trailing newlines. Missing Secret/key prevents
container startup; an empty, unreadable, oversized or invalid file makes Sink fail
startup. `sink config check` needs the same mounted files and validates without
backend connections, but successful validation does not prove authentication.
Readiness and representative authenticated write/read/async checks are still needed.
Do not put production credentials into local fixture files or debug logs.

### Rotation and rollback

1. Create a new backend credential while keeping the old one valid. Use two users
   or the backend's overlapping-key mechanism; replacing the only valid password
   immediately cannot provide uninterrupted rotation. Verify the replacement can
   authenticate and has the required read/write/index-management permissions before
   changing the reference. A valid file or successful Ping is not a write-permission
   check; Pod readiness does not gate Kafka consumption inside an already running Worker.
2. Create a new preferably immutable Secret (for example `orders-mongo-v2`) with
   that credential. Change `uriSecretRef.name` or the relevant search reference
   in values. This changes **both Engine and Worker Pod templates** automatically.
3. Upgrade normally with the documented surge/discovery/drain budgets. Verify all
   desired Pods are available and **all old/terminating Engine and Worker Pods have
   exited**; `helm --wait` or new Pod readiness alone is insufficient. Check actual
   synchronous and asynchronous operations with the new credential.
4. Only then revoke the old credential. Keep its Secret for as long as rollback
   requires it; rollback also requires the backend credential to remain valid.

Sink loads credentials only at startup. Projected-volume refresh does not reload
connections. If updating a Secret in place, wait for propagation and explicitly
change `stores.<name>.credentialRevision` to roll both roles; prefer versioned
immutable names to avoid kubelet cache races and ambiguous rollback. A shared
Secret used by several Stores requires rotating every consuming Store. A Worker
at zero reads the selected Secret when KEDA next starts it; test that path before
revocation if asynchronous delivery is required. A malformed replacement can
leave a rollout stalled while old Pods remain healthy: keep the old credential
valid, correct the reference or roll back, and never force-delete healthy Pods.

Ordinary runtime ConfigMap changes automatically change the workload checksum.
Do not combine a Store's activation with its configuration or credential change;
stage and validate it first. The chart never deletes backend records or Kafka topics.

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

1. **Stage:** add a new Store with `state: staged` and provision its referenced credential
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
Kafka broker/source/DLQ topic/group/partition metadata is immutable while a Store exists;
lag thresholds and non-identity runtime tuning can change. Source and DLQ topics
must not overlap across Stores. Topic partition changes can alter record affinity:
perform a drained migration to a new topic/group, never just raise the scaler cap.
MongoDB targets are private Secret content; search targets are values. The chart
cannot verify backend identity or enforce its immutability. Do not switch a Store to a different backend in place.

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
message limits must fit the memory budget too. Set these in the supported `runtime.service`/`runtime.grpc`
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
upgrades from resetting a controller's live count. Gateway minimum is two. Engine
defaults to two, but an explicit fixed or minimum count of one is allowed with
`maxUnavailable: 0`; it has no replica redundancy during failure or termination.
Engine zero is rejected because synchronous Store operations need a routable Engine.
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
Fallback can be enabled when at least one trigger supports `Value` or `AverageValue`,
including Kafka and Prometheus. CPU and memory triggers do not participate in
fallback, but may coexist with a supported trigger on the same ScaledObject. Kafka
scaler failure should retain capacity via a tested fallback, not be assumed to mean
zero lag. The Worker Kafka trigger uses cached metrics by default so its configured
polling interval remains the scaler's query cadence. KEDA annotations, generated
HPA name, original-replica restoration and fallback behavior are explicit values.

Default scale-down allows one Pod per 300s with a 300s stabilization window. Every
configured scale-down policy period must cover Pod termination grace, limiting
overlapping departures. Scale-up selects the larger of 100% or two Pods per minute.
The complete `autoscaling.behavior` object is available for both HPA and KEDA's
generated HPA. Tune scale-up against partition rebalance and backend capacity. KEDA
`cooldownPeriod` covers **1 -> 0**; HPA behavior governs nonzero scaling.
`pollingInterval` is emitted for zero activation or cached metrics; nonzero HPA fetch
cadence is controlled by Kubernetes. Inapplicable zero controls are omitted to avoid
misleading KEDA settings. KEDA zero transitions and manual replica edits are not protected
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

- [Sink rolling-upgrade contract](https://github.com/batchstream/sink/blob/v0.16.0/docs/rolling-upgrades.md)
- [Kubernetes termination flow](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination-flow)
- [Native lifecycle hooks](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/)
- [HPA behavior and replica ownership](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)
- [KEDA Kafka scaler](https://keda.sh/docs/2.20/scalers/apache-kafka/)
- [KEDA ScaledObject settings](https://keda.sh/docs/2.20/reference/scaledobject-spec/)
- [Helm lookup semantics](https://helm.sh/docs/chart_template_guide/functions_and_pipelines/#using-the-lookup-function)
