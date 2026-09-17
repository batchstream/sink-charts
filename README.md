# Sink Helm charts

One release deploys a Sink cluster: a shared Gateway and independently configured
Engine and Worker Deployments for each Store. Chart `0.2.0` replaces the original
single-role values interface. It targets Sink **0.15.0**, SDK **0.8.0**, and
Kubernetes **1.30+** with native lifecycle sleep hooks enabled.

- Stable Store names, generated headless discovery, staged activation and retirement.
- Per-Store resources, configuration Secrets, scheduling, replica ranges and HPA/KEDA.
- Calculated DNS withdrawal/request drain budgets, rolling surge, readiness and PDBs.
- Explicit Worker scale-to-zero, Kafka partition limits, lag triggers and authentication references.
- Unprivileged read-only containers, no Kubernetes token/RBAC, separate metrics Services.

The chart installs no database, Kafka, KEDA, metrics-server or Prometheus operator.
The default empty `stores` map creates only an inventory ConfigMap. Follow
[operations](docs/operations.md) before admitting production traffic; defaults are
budget assumptions, not a zero-error guarantee under arbitrary DNS/backend failures.

## Install

Create a namespace and externally managed complete Engine/Worker configuration
Secrets, following [the examples](examples). Sink does **not** substitute environment
variables into its YAML. Never put credentials in committed values files. Use your
secret manager to create `sink-mongo-engine-config`, `sink-mongo-worker-config` and,
if using the staged archive example, `sink-archive-engine-config`, with key `sink.yaml`.

```sh
helm upgrade --install sink ./charts/sink --namespace sink --create-namespace \
  -f examples/cluster-values.yaml --wait --timeout 15m
```

Customize a copy of `cluster-values.yaml` for your actual backends. Its HPA example
requires metrics-server. Without it, use `autoscaling.mode: none`. To use Kafka lag
scaling, install KEDA first and add `-f examples/keda-values.yaml`. Authentication
uses existing TriggerAuthentication resources; scaler credentials are independent
of the Sink runtime Secret.

The image is pinned to the verified 0.15.0 multi-architecture digest at
`ghcr.io/liran/sink`, where that release was published before the organization
transfer. Override `image.repository` **and** `image.digest` together when moving
artifacts. To select by tag, explicitly set `image.digest: ""`.

## Configure and operate

See [values.yaml](charts/sink/values.yaml) for defaults and
[the operational runbook](docs/operations.md) for rollout budgets, resource sizing,
Store lifecycle, controller handoff, GitOps, failure cases and configuration contracts.

```yaml
stores:
  orders:
    state: staged
    engine:
      config: {existingSecret: orders-engine-config}
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
