# Sink Helm charts

One release deploys a shared Gateway and independent Engine/Worker Deployments
for each Store. Chart **0.10.0** targets Sink **0.21.0** and Kubernetes **1.30+**.
Values are the only configuration source: the chart generates role and Store
configuration, while existing Secrets provide credential values.

## Configuration structure

| Scope | Values path | Purpose |
| --- | --- | --- |
| Cluster | `cluster` | DNS domain, discovery timing, Store lifecycle checks |
| Shared image | `image` | One image, pull policy and registry credentials for every component |
| Monitoring | `metrics` | Metrics listeners and optional ServiceMonitor |
| Gateway | `gateway` | Public gRPC service and Gateway settings |
| Role defaults | `defaults.engine`, `defaults.worker` | Shared settings inherited by every Store |
| Store | `stores.<name>` | Lifecycle phase, storage, Kafka topics and role overrides |

Each role uses the same layout: `config` for application behavior, `pod` for
resources/environment/scheduling, `serviceAccount` for identity annotations,
`rollout` for readiness and shutdown, `autoscaling` for replicas managed by HPA or
KEDA, and `podDisruptionBudget` for voluntary eviction limits. `replicas` applies
when `autoscaling.mode: none`.

All components share the top-level `image`, including `image.pullSecrets` for
registry Secret references. Components do not expose image overrides.

Chart-owned fields use camelCase. For example, `config.kafkaConsumer.groupId`,
`config.memory.highWatermarkPercent`, and `config.logging.includeFailureBody`.
The chart translates these into Sink's configuration format. Kubernetes and KEDA
objects such as affinity, triggers, annotations and labels retain their native keys.
Unknown fields and settings belonging to another role fail schema validation.

## Start a cluster

Provision the referenced Secrets in the release namespace using your secret
manager. Copy [cluster-values.yaml](examples/cluster-values.yaml), then replace
the Store names, credential references and Kafka broker addresses.

```yaml
stores:
  orders:
    phase: active
    maxConcurrent: 96 # Optional per-process ceiling for Engine and Worker.
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

```sh
helm upgrade --install sink ./charts/sink --namespace sink --create-namespace \
  -f examples/cluster-values.yaml --wait --timeout 15m
```

The chart installs no databases, Kafka, KEDA, metrics-server or Prometheus operator.
The HPA example requires metrics-server. Use `autoscaling.mode: none` for fixed
replicas. The default empty `stores` map creates only an inventory ConfigMap.
For an existing release, add Stores as `phase: staged`, wait for Engines to become
available, then activate in a separate upgrade.

For release `sink` in namespace `sink`, SDKs can use
`dns:///sink-sink-gateway-headless.sink.svc.cluster.local:8080`. The service name
remains stable across upgrades. Set `cluster.domain` for a different cluster DNS suffix.

## Tune and operate

Maps merge recursively: `defaults.engine` → `stores.<name>.engine`, and
`defaults.worker` → `stores.<name>.worker`. Explicit `false` and `0` override their
defaults; lists replace the inherited list. An empty map keeps inherited fields.
Store overrides never affect another Store. Application settings omitted from the
effective `config` inherit Sink defaults.

| Example | Use |
| --- | --- |
| [Cluster](examples/cluster-values.yaml) | Active asynchronous Store and staged synchronous Store |
| [Application tuning](examples/tuning-values.yaml) | Request limits, merge/Lua, batching, Kafka producer/consumer |
| [Kafka lag scaling](examples/keda-values.yaml) | Worker scale-to-zero and scaler authentication |
| [Memory scaling](examples/memory-keda-values.yaml) | Prometheus memory pressure and rejection metrics |
| [Logging](examples/logging-values.yaml) | Console output, deployment labels and OTLP export |
| [Search](examples/search-values.yaml) | Elasticsearch/OpenSearch and credential references |

Kafka topic identity and shared policy belong to `stores.<name>.kafka`.
Consumer behavior belongs to `worker.config.kafkaConsumer`; the scaling target
belongs to `worker.autoscaling.keda.kafkaLag.targetLag`. Engine publishing buffers
use `engine.config.kafkaProducer`. Each enabled Worker needs its own consumer group.

[The packaged values reference](charts/sink/README.md) describes fields, defaults,
required settings and allowed values. [The operational runbook](docs/operations.md)
covers discovery budgets, Store lifecycle, credentials, controller handoff and
image rollouts. [The field migration table](docs/values-migration.md) identifies
renamed paths; the chart accepts only the new schema.

## Validate

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
make check PYTHON=.venv/bin/python
```

The rendering tests run offline. CI also validates Kubernetes resource schemas
and feeds generated files to the matching Sink configuration parser.
`make integration` explicitly starts Docker/Kind qualification with synthetic
backends; see [the integration instructions](tests/integration/README.md).
