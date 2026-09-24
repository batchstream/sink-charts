# Sink values reference

Chart **0.10.0** targets Sink **0.21.0** and Kubernetes **1.30+**. Values configure
a shared Gateway and a separate Engine/optional Worker for each Store. The chart
generates `sink.yaml` per role and a shared `store.yaml` per Store. There is no
separate application configuration file to maintain.

## Layout and inheritance

```yaml
cluster:
  domain: cluster.local
metrics:
  enabled: false
gateway:
  config:
    requestLimits: {maxOperations: 1000}
defaults:
  engine:
    config:
      batching: {maxOperations: 32, maxWait: 2ms}
      executionQueue: {maxTasks: 10000, maxBytes: 128MiB}
  worker:
    enabled: false
stores:
  orders:
    phase: active
    storage:
      driver: mongodb
      mongodb:
        uriSecretRef: {name: orders-mongo-v1, key: uri}
    engine:
      replicas: 2
```

`gateway` is configured directly. Each Store's `engine`/`worker` recursively
merges into `defaults.engine`/`defaults.worker`. Maps merge; lists replace;
explicit false and zero are retained. Empty maps retain inherited fields.
Omitted application fields retain Sink defaults. All components use the same
top-level `image`, including its pull policy and `pullSecrets`. A nonempty digest
takes precedence over the tag; set `image.digest: ""` to select by tag.

All Chart fields use camelCase. The chart translates application fields into
Sink YAML; opaque Kubernetes/KEDA objects keep their own keys. Unknown fields,
old paths and settings for the wrong role are rejected. References `{name, key}`
point to externally managed Secrets in the release namespace. The chart does not
read, create or delete Secret values.

## Unified Engine admission

`defaults.engine.config.executionQueue` (or the corresponding per-Store Engine
configuration) renders `execution.queue`. `maxTasks` limits ready batches and
individual Native calls in one FIFO; a batch is one task, not one task per
operation. `maxBytes` is shared waiting bytes across batch collection and
admission. It is not a per-method quota or an execution-concurrency limit.
`batching.queue` still bounds each collection queue; `stores.<name>.maxConcurrent`
is the independent adaptive execution ceiling. Worker and Gateway reject
`executionQueue`.

This setting requires the server implementation of unified admission. When
qualifying an unreleased candidate, override the image accordingly; do not enable
it against an older pinned image that does not accept `execution.queue`.

## Global and Store fields

Unless marked required, fields are optional. Defaults below are from this Chart
or the targeted Sink version; a different image version may have different application defaults.

| Field | Purpose | Default / allowed values |
| --- | --- | --- |
| `nameOverride` | Replace the Chart name in resource names | Empty |
| `fullnameOverride` | Replace the complete release resource prefix | Empty |
| `image.repository` | Container repository | `ghcr.io/batchstream/sink` |
| `image.tag` | Version tag | `0.21.0` |
| `image.digest` | Immutable image selector, preferred over tag | Empty or `sha256:<64 hex characters>` |
| `image.pullPolicy` | Kubernetes pull policy | `IfNotPresent`; `Always`, `IfNotPresent`, `Never` |
| `image.pullSecrets` | Registry Secret references for every component | `[]`; list of `{name}` |
| `cluster.domain` | Kubernetes service DNS suffix | `cluster.local` |
| `cluster.discovery.dnsRefreshSeconds` | Gateway Engine DNS refresh cadence | `10` seconds |
| `cluster.discovery.endpointPropagationSeconds` | Endpoint publication allowance | `5` seconds |
| `cluster.discovery.dnsCacheTTLSeconds` | Upstream positive/negative DNS cache allowance | `30` seconds |
| `cluster.discovery.dnsRetryBackoffSeconds` | Failed lookup/retry allowance | `10` seconds |
| `cluster.discovery.clientEndpointRemovalSeconds` | Client/proxy/LB Gateway withdrawal allowance | `60` seconds |
| `cluster.discovery.safetyMarginSeconds` | Margin added to withdrawal and termination | `10` seconds |
| `cluster.storeLifecycle.enforceTransitions` | Check live inventory, Deployments and Pods on Helm upgrades | `true`; offline rendering cannot perform live checks |
| `cluster.storeLifecycle.removalApprovals` | Store names approved after writer/Kafka/DLQ drain | `[]` |
| `metrics.enabled` | Enable metrics listeners and private Services | `false` |
| `metrics.serviceMonitor.enabled` | Create a ServiceMonitor | `false`; requires metrics and the CRD |
| `metrics.serviceMonitor.labels` | Additional ServiceMonitor labels | `{}` |
| `metrics.serviceMonitor.interval` | Scrape interval | `30s` |
| `metrics.serviceMonitor.scrapeTimeout` | Scrape timeout | `10s` |
| `stores.<name>` | Stable Store identity used by clients | Required for workloads; 1–32 lowercase letters/digits/hyphens |
| `stores.<name>.phase` | Admission phase | **Required**: `staged`, `active`, `retiring` |
| `stores.<name>.maxConcurrent` | Adaptive Store execution ceiling per Engine/Worker process | Omitted: Sink defaults to `64` for MongoDB or `128` for Elasticsearch/OpenSearch; explicit `1`–`4096` |
| `stores.<name>.storage.driver` | Storage driver | **Required**: `mongodb`, `elasticsearch`, `opensearch` |
| `storage.mongodb.uriSecretRef` | Complete MongoDB URI reference | **Required** for MongoDB: `{name, key}` |
| `storage.mongodb.metadataField` | Sink document metadata field | `__sink` |
| `storage.search.endpoints` | Search endpoint URLs | **Required** for search: nonempty URL list |
| `storage.search.username` | Nonsecret basic-auth username | Empty; mutually exclusive with username reference |
| `storage.search.usernameSecretRef` | Basic-auth username reference | `{name, key}` |
| `storage.search.passwordSecretRef` | Basic-auth password reference | Required with either username field |
| `storage.search.apiKeySecretRef` | API-key reference | `{name, key}`; cannot combine with basic auth |
| `stores.<name>.credentialRevision` | Restart both roles after an in-place credential change | Empty; versioned Secret names are preferred |
| `stores.<name>.engine` | Engine overrides | Inherits `defaults.engine` |
| `stores.<name>.worker` | Worker overrides | Inherits `defaults.worker` |

`storage.*` paths in the table are relative to `stores.<name>`. Only active Stores
enter Gateway routes; staged and retiring Stores retain their backend workloads.
An empty Store map creates only inventory. The chart does not install backends or
cluster controllers.

## Role deployment fields

These fields apply at `gateway`, `defaults.engine`, `defaults.worker`, or their
Store override. Gateway alone has `service`; Worker alone has `enabled` and
`autoscaling.allowScaleToZero`; Engine alone has `rollout.readinessCheck`.

| Field | Purpose | Default / allowed values |
| --- | --- | --- |
| `enabled` | Create the Worker | `false`; requires Store Kafka and consumer group |
| `replicas` | Fixed replicas when autoscaling mode is none | `2`; Gateway ≥2, Engine ≥1, Worker ≥1 unless zero explicitly allowed |
| `service.type` | Public Gateway exposure | `ClusterIP`; `ClusterIP`, `LoadBalancer` |
| `service.port` | Gateway Service port | `8080` |
| `service.annotations` | Gateway Service annotations | `{}` |
| `service.headless.enabled` | Stable Pod-address discovery for SDK clients | `true` |
| `service.headless.annotations` | Headless Service annotations | `{}` |
| `pod.resources.requests.cpu` | Scheduled CPU | Gateway `250m`, Engine/Worker `500m`; positive |
| `pod.resources.requests.memory` | Scheduled memory | Gateway `256Mi`, Engine/Worker `512Mi`; ≤ limit |
| `pod.resources.limits.cpu` | Optional CPU cap | Unset; ≥ request |
| `pod.resources.limits.memory` | Container memory ceiling | Gateway `512Mi`, Engine/Worker `1Gi` |
| `pod.goMemoryLimitPercent` | GOMEMLIMIT as percent of memory ceiling | `80`; 10–90 |
| `pod.env` | Additional/overridden container environment | `[]`; cannot override GOMEMLIMIT |
| `pod.envFrom` | Kubernetes environment sources | `[]` |
| `pod.labels` | Extra Pod labels | `{}`; reserved identity prefixes cannot be overridden |
| `pod.annotations` | Extra Pod annotations | `{}`; generated config annotations are reserved |
| `pod.nodeSelector` | Node selector | `{}` |
| `pod.tolerations` | Scheduling tolerations | `[]` |
| `pod.affinity` | Kubernetes affinity rules | `{}` |
| `pod.topologySpreadConstraints` | Pod spreading rules | `[]` enables soft node/zone spread |
| `pod.priorityClassName` | Existing PriorityClass | Empty |
| `serviceAccount.annotations` | Per-workload ServiceAccount annotations | `{}`; token automount is disabled |
| `rollout.restartRevision` | Force a Pod restart for external mounted-data updates | Empty |
| `rollout.minReadySeconds` | Ready time before availability | `10`; Engine effective floor is DNS convergence, normally `55` |
| `rollout.readinessCheck` | Engine readiness dependency | `storage`; `storage`, `kafka`, `process` |
| `rollout.shutdownTimeoutSeconds` | Timeout for each application cleanup phase | `30` |
| `rollout.shutdownBudgetSeconds` | Total SIGTERM-to-exit allowance | `150`; ≥4 phase timeouts plus enabled OTLP flush |
| `rollout.preStopDelaySeconds` | Delay before SIGTERM | `null`: Engine `65`, Gateway `70`, Worker `0`; smaller overrides fail |
| `rollout.terminationGracePeriodSeconds` | Kubernetes termination deadline | `null`: Engine `225`, Gateway `230`, Worker `160`; covers preStop + shutdown + margin |
| `podDisruptionBudget.enabled` | Create a PDB | `true`; disable for Worker zero |
| `podDisruptionBudget.maxUnavailable` | Voluntary unavailable replicas | `1`; must be less than minimum replicas |

Discovery and termination values are operational allowances to measure against
your cluster. Rollouts use zero unavailable replicas and one surge replica.
Service names derive from the release name. For release/namespace `sink`, the
headless address is `dns:///sink-sink-gateway-headless.sink.svc.cluster.local:8080`.

## Autoscaling

All paths below are relative to a role's `autoscaling`. KEDA fields keep the
corresponding controller semantics; polling and cooldown numbers are seconds.

| Field | Purpose | Default / allowed values |
| --- | --- | --- |
| `mode` | Replica controller | `none`; `none`, `hpa`, `keda` |
| `minReplicas` | Controller minimum | `2` |
| `maxReplicas` | Controller maximum | Gateway/Engine `6`, Worker `4`; Worker ≤ Kafka partitions |
| `allowScaleToZero` | Permit zero Workers and delayed processing | `false`; Worker only |
| `targetCPUUtilizationPercent` | CPU target relative to requests | `70`; `0` disables this HPA metric |
| `targetMemoryUtilizationPercent` | Memory target relative to requests | `0` disables this HPA metric |
| `extraMetrics` | Additional native HPA metric specifications | `[]`; HPA only |
| `behavior.scaleUp` | Native HPA scale-up rules | Larger of 100% or 2 Pods per 60s; zero stabilization |
| `behavior.scaleDown` | Native HPA scale-down rules | 1 Pod per 300s; 300s stabilization; periods must cover termination |
| `behavior.*.stabilizationWindowSeconds` | Native HPA stabilization window | Integer 0–3600 |
| `behavior.*.selectPolicy` | Policy selection | `Max`, `Min`, `Disabled` |
| `behavior.*.policies` | Native HPA scaling limits | List of `{type: Pods or Percent, value: positive integer, periodSeconds: 1–1800}` |
| `keda.pollingInterval` | Zero activation/cached-metric polling | `30` seconds |
| `keda.cooldownPeriod` | KEDA 1→0 cooldown | `300` seconds |
| `keda.initialCooldownPeriod` | Initial 1→0 delay | `0` seconds |
| `keda.annotations` | ScaledObject annotations | `{}` |
| `keda.hpaName` | Generated HPA name override | Empty |
| `keda.restoreToOriginalReplicaCount` | Restore original replica count on ScaledObject removal | `false` |
| `keda.triggers` | Native KEDA triggers | `[]`; replaces Gateway/Engine CPU trigger, appends to Worker Kafka trigger |
| `keda.fallback` | Capacity during scaler failures | `{}` disables; requires supported Value/AverageValue trigger |
| `keda.fallback.failureThreshold` | Failures before fallback | Required if fallback set; positive integer |
| `keda.fallback.replicas` | Fallback count | Required if fallback set; within min/max |
| `keda.fallback.behavior` | Fallback policy | `static`, `currentReplicas`, `currentReplicasIfHigher`, `currentReplicasIfLower` |
| `keda.kafkaLag.targetLag` | Unprocessed records per Worker | `1000`; Worker only |
| `keda.kafkaLag.name` | Generated Kafka trigger name | `kafka-lag`; Worker only |
| `keda.kafkaLag.useCachedMetrics` | Cache Kafka lag at polling cadence | `true`; Worker only |
| `keda.kafkaLag.authenticationRef` | Existing scaler authentication | `{}`; `{name, kind}` where kind is `TriggerAuthentication` or `ClusterTriggerAuthentication`; Worker only |

HPA CPU/memory and KEDA CPU require metrics-server. KEDA and ServiceMonitor need
their CRDs installed. Kafka scaler authentication is separate from Sink's backend
credentials. Controller handoffs and scale-to-zero require the operational checks
in the runbook.

## Application settings

All paths below are relative to a role's `config`. Each field is optional except
an enabled Worker's `kafkaConsumer.groupId`. Durations are positive Go duration
strings such as `2ms`, `100ms`, `20s`. Byte limits accept positive integer bytes or
`KiB`/`MiB`/`GiB`. Pod memory uses Kubernetes quantities (`Mi`, `Gi`) instead.

| Field | Role | Purpose / Sink default |
| --- | --- | --- |
| `memory.maxBytes` | All | Optional process memory ceiling, bounded by detected limits |
| `memory.highWatermarkPercent` | All | Reject new work at `80` percent; integer 1–99 |
| `memory.lowWatermarkPercent` | All | Resume below `70` percent; positive and below high watermark |
| `grpc.maxReceiveMessageBytes` | Gateway, Engine | Maximum received gRPC message; `64MiB` |
| `grpc.maxSendMessageBytes` | Gateway, Engine | Maximum sent gRPC message; `64MiB` |
| `requestLimits.maxOperations` | Gateway | Total operations across Stores in one public request; `1000` |
| `forwarding.idleTimeout` | Gateway | Idle Engine connection lifetime; `5m` |
| `forwarding.maxConnections` | Gateway | Engine connection capacity; `256` |
| `forwarding.maxFanout` | Gateway | Concurrent forwarding groups; `8` |
| `merge.maxAttempts` | Engine, Worker | Revision conflict attempts; `3` |
| `merge.lua.timeout` | Engine, Worker | Per-script execution limit; `100ms` |
| `merge.lua.maxSourceBytes` | Engine, Worker | Lua source bound; `64KiB` |
| `merge.lua.maxResultBytes` | Engine, Worker | Per-document current/input/result bound; `16MiB` |
| `merge.lua.maxCachedPrograms` | Engine, Worker | Compiled-program cache capacity; `256` |
| `merge.lua.maxInstructions` | Engine, Worker | Lua instruction limit; `1000000` |
| `batching.maxWait` | Engine | Batch collection delay; `2ms` |
| `batching.maxOperations` | Engine | Operations per execution batch; `32` |
| `batching.maxBytes` | Engine | Execution batch bytes; `16MiB` |
| `batching.queue.maxOperations` | Engine | Queued operations; `10000`; ≥ batch size |
| `batching.queue.maxBytes` | Engine | Queued bytes; `128MiB`; ≥ batch bytes |
| `kafkaProducer.maxBufferedBytes` | Engine | Pending Kafka producer bytes; `64MiB`; requires Store Kafka |
| `kafkaConsumer.groupId` | Worker | **Required when enabled**, unique Store consumer group; also used by KEDA |
| `kafkaConsumer.maxPollRecords` | Worker | Records per poll; `500` |
| `kafkaConsumer.processingTimeout` | Worker | Processing round limit; `20s`, at most 20s |
| `kafkaConsumer.retry.maxAttempts` | Worker | Attempts per processing round; `10` |
| `kafkaConsumer.retry.backoff` | Worker | Initial retry backoff; `100ms` |
| `kafkaConsumer.retry.maxBackoff` | Worker | Maximum retry backoff; `10s`; ≥ backoff |

Listeners, routes, mode, Store identity, metrics addresses and DNS refresh are
managed by the Chart. These cannot be set through `config`. Request lifetimes
are controlled by callers; there is no service-wide request timeout.

## Logging

Paths below are relative to `config.logging` on every role. Logging maps, including
labels and OTLP settings, merge through role defaults and Store overrides.

| Field | Purpose | Default / allowed values |
| --- | --- | --- |
| `level` | Minimum severity for all components | `warn`; `debug`, `info`, `warn`, `error` |
| `console.enabled` | Standard-error output | `true`; console or OTLP must be enabled |
| `console.format` | Console encoding | `text`; `text`, `json` |
| `labels.environment` | Deployment environment | Empty |
| `labels.cluster` | Deployment cluster label | Empty |
| `labels.namespace` | Kubernetes namespace label | From POD_NAMESPACE |
| `labels.pod` | Pod label | From POD_NAME |
| `labels.node` | Node label | From NODE_NAME |
| `includeFailureBody` | Include document content in supported severe-failure logs | `false` |
| `maxBodyBytes` | Failure-body byte bound | `16KiB`; 1KiB–64KiB; integer bytes or human-readable byte string |
| `otlp.enabled` | Direct log export to existing Collector | `false` |
| `otlp.protocol` | OTLP transport | `grpc`; `grpc`, `http/protobuf` |
| `otlp.endpoint` | Collector host:port, no scheme/path | Required when enabled |
| `otlp.tls.enabled` | Verify Collector TLS; false selects plaintext | `true` |
| `otlp.queueSize` | Pending log records | `1024`; 1–8192 |
| `otlp.batchSize` | Records per export | `128`; 1–512, ≤queueSize |
| `otlp.flushInterval` | Export cadence | `1s`; >0 and ≤60s |
| `otlp.exportTimeout` | Export attempt deadline | `3s`; >0 and ≤30s |
| `otlp.shutdownTimeout` | Additional final log flush | `5s`; >0 and ≤30s; included in rollout shutdown budget |

Labels are single-line strings of at most 256 UTF-8 bytes. Explicit nonempty labels
win over environment-derived identity. The chart supplies POD_UID, POD_NAME,
POD_NAMESPACE and NODE_NAME through the Downward API; explicit `pod.env` entries
replace matching defaults. Configuration changes trigger rolling restarts.

## Shared Kafka settings

Paths below are relative to `stores.<name>.kafka`. Omit the entire object for a
synchronous Store. Providing it enables Kafka publishing. Enable the Worker
separately when asynchronous operations should be consumed by this release.

| Field | Purpose | Default / allowed values |
| --- | --- | --- |
| `brokers` | Bootstrap servers | **Required**: nonempty list of host:port |
| `topic.name` | Mutation topic | **Required**, unique across Stores and dead-letter topics |
| `topic.retention` | Mutation retention | `72h` |
| `deadLetterTopic.name` | Failed-record topic | `<topic.name>.dlq` |
| `deadLetterTopic.retention` | Failed-record retention | `720h` |
| `topicPolicy.partitions` | Partitions for both topics | **Required**, positive integer; bounds Worker replicas |
| `topicPolicy.replicationFactor` | Replicas for both topics | `3` |
| `topicPolicy.minInSyncReplicas` | Minimum ISR for both topics | `1`; ≤ replicationFactor |
| `maxRecordBytes` | Shared Kafka record-size limit | `900KiB` |

Sink owns Topic creation and reconciliation. Do not rename Store keys, topics,
consumer groups or backend targets in place. Kafka lag thresholds live in
`worker.autoscaling.keda.kafkaLag`; producer/consumer tuning lives in role `config`.

See the source repository's [examples](https://github.com/batchstream/sink-charts/tree/main/examples)
and [operational runbook](https://github.com/batchstream/sink-charts/blob/main/docs/operations.md)
for credential rotation, discovery sizing, Store transitions and controller handoffs.
