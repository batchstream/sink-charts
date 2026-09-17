# Disposable Kubernetes qualification

Requires Docker (8 GiB recommended), Kind 0.33.0, kubectl, Helm 3.17+, Go 1.27 and
Python with `requirements-dev.txt`. Run explicitly:

```sh
make integration PYTHON=.venv/bin/python
```

The runner creates a unique `sink-chart-*` Kind cluster with Kubernetes 1.35.8,
uses its own temporary kubeconfig/context, builds a small SDK 0.8.0 business probe,
and puts the test release, Mongo, Kafka and KEDA 2.20 in `sink-chart-test` inside that
owned cluster. KEDA CRDs are installed only there. No current/default Kubernetes
context is used. The cluster and probe image are deleted in `finally`, including
on test failures. Logs and JSON reconciliation evidence remain in `.reports/`.
Interrupting the entire host/process can bypass cleanup; use `kind get clusters`
and delete only the recorded test cluster if that happens.

Coverage:

- Default DNS/termination budgets during simultaneous Gateway/Engine rolling updates.
- Engine 2 -> 3 -> 2 while a persistent SDK client performs unique creates and reads.
- Store staged -> active -> retiring -> removal; existing Store traffic continues.
- Live Helm lookup rejection of unsafe addition, activation, direct removal,
  missing drain approval and old Gateway Pods during retirement.
- Complete final reconciliation of every acknowledged synchronous record; no
  mutation retries and SDK reads configured for one attempt.
- Async acceptance with zero Workers, actual Kafka backlog, KEDA activation and
  multiple consumers, all accepted records applied, return to zero and reactivation.

The fixture uses one-node Mongo/Kafka to isolate deployment behavior. It is **not**
a backend HA, multi-zone, production capacity or EKS load-balancer qualification.
It uses actual default Chart DNS/drain timings, so allow roughly 15–25 minutes plus
image downloads. The async verification polls for application, which is expected
for durable asynchronous completion; synchronous rollout errors fail immediately.

For development only, an explicitly created disposable Kind cluster can be adopted
with `--cluster sink-chart-NAME --kubeconfig /absolute/owned/kubeconfig`. That cluster
is still deleted on completion. Never point this option at a shared cluster.
