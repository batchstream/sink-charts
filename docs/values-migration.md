# Values field changes

Chart 0.10 has one configuration schema. Old fields are rejected; there are no
aliases, automatic conversion or legacy merge rules. For a new installation,
start with the current examples. This table is a reference for rewriting values.

| Previous path | New path |
| --- | --- |
| `imagePullSecrets` | `image.pullSecrets` |
| `<role>.image` | Removed; every component uses the top-level `image` |
| `clusterDomain` | `cluster.domain` |
| `discovery` | `cluster.discovery` |
| `discovery.endpointPublicationSeconds` | `cluster.discovery.endpointPropagationSeconds` |
| `discovery.dnsCacheSeconds` | `cluster.discovery.dnsCacheTTLSeconds` |
| `discovery.lookupBackoffSeconds` | `cluster.discovery.dnsRetryBackoffSeconds` |
| `discovery.clientWithdrawalSeconds` | `cluster.discovery.clientEndpointRemovalSeconds` |
| `lifecycle` | `cluster.storeLifecycle` |
| `engineDefaults` / `workerDefaults` | `defaults.engine` / `defaults.worker` |
| `stores.<name>.state` | `stores.<name>.phase` |
| `<role>.replicaCount` | `<role>.replicas` |
| `<role>.runtime` | `<role>.config` |
| `<role>.runtime.execution.merge` | `<role>.config.merge` |
| `gateway.runtime.request` | `gateway.config.requestLimits` |
| `<engine>.runtime.producer` | `<engine>.config.kafkaProducer` |
| `<worker>.runtime.consumer` | `<worker>.config.kafkaConsumer` |
| `<role>.pod.minReadySeconds` | `<role>.rollout.minReadySeconds` |
| `<role>.pod.shutdownTimeoutSeconds` | `<role>.rollout.shutdownTimeoutSeconds` |
| `<role>.pod.shutdownBudgetSeconds` | `<role>.rollout.shutdownBudgetSeconds` |
| `<role>.pod.preStopSeconds` | `<role>.rollout.preStopDelaySeconds` |
| `<role>.pod.terminationGracePeriodSeconds` | `<role>.rollout.terminationGracePeriodSeconds` |
| `<role>.pod.configRevision` | `<role>.rollout.restartRevision` |
| `<role>.pod.serviceAccountAnnotations` | `<role>.serviceAccount.annotations` |
| `<engine>.readiness` | `<engine>.rollout.readinessCheck` |
| `<role>.disruptionBudget` | `<role>.podDisruptionBudget` |
| `<worker>.allowScaleToZero` | `<worker>.autoscaling.allowScaleToZero` |
| `<role>.autoscaling.cpuUtilization` | `<role>.autoscaling.targetCPUUtilizationPercent` |
| `<role>.autoscaling.memoryUtilization` | `<role>.autoscaling.targetMemoryUtilizationPercent` |
| `<worker>.autoscaling.keda.kafkaTrigger` | `<worker>.autoscaling.keda.kafkaLag` |
| `<worker>.autoscaling.keda.authenticationRef` | `<worker>.autoscaling.keda.kafkaLag.authenticationRef` |
| `stores.<name>.kafka.lagThreshold` | `stores.<name>.worker.autoscaling.keda.kafkaLag.targetLag` |
| `stores.<name>.kafka.topic` (string) | `stores.<name>.kafka.topic.name` |
| `stores.<name>.kafka.partitions` | `stores.<name>.kafka.topicPolicy.partitions` |
| `stores.<name>.kafka.replicationFactor` | `stores.<name>.kafka.topicPolicy.replicationFactor` |
| `stores.<name>.kafka.minInSyncReplicas` | `stores.<name>.kafka.topicPolicy.minInSyncReplicas` |
| `stores.<name>.kafka.runtime.topic.retention` | `stores.<name>.kafka.topic.retention` |
| `stores.<name>.kafka.runtime.dead_letter` | `stores.<name>.kafka.deadLetterTopic` |
| `stores.<name>.kafka.runtime.max_record_bytes` | `stores.<name>.kafka.maxRecordBytes` |
| `storage.mongodb.maxConcurrentWrites`, `maxConcurrentGroups` | Removed; current Sink rejects these fields |
| Sink `execution.store_max_concurrent` in an Engine/Worker config | `stores.<name>.maxConcurrent` in Chart values; it renders Store `max_concurrent` for both roles |

Application leaf fields now use camelCase, for example `max_operations` becomes
`maxOperations`, `group_id` becomes `groupId`, `high_watermark_percent` becomes
`highWatermarkPercent`, and `shutdown_timeout` under OTLP becomes `shutdownTimeout`.
The logging switch `failure_body` is named `includeFailureBody`.
`<role>` means `gateway`, `defaults.engine`, `defaults.worker`, or the corresponding
Store role override. Kafka scaler fields and scale-to-zero are Worker-only.

Image selection is shared across all components. Secret references, Service
identity, native Kubernetes/KEDA objects and the generated Sink file format keep
their established meaning.
This values redesign is not a live upgrade procedure for an older deployment or
inventory. Use a fresh release with the new values schema.
