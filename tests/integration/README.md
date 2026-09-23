# Disposable Kubernetes qualification

Requires Docker (8 GiB recommended), Kind 0.33.0, kubectl, Helm 3.17+, Go 1.27 and
Python with `requirements-dev.txt`. Run explicitly:

```sh
make integration PYTHON=.venv/bin/python
```

The runner creates a unique `sink-chart-*` Kind cluster with Kubernetes 1.35.8,
uses its own temporary kubeconfig/context, builds a business probe pinned to
sink-go v0.10.1 for the matching streaming protocol and typed request API,
and puts the test release, Mongo, Kafka and KEDA 2.20 in `sink-chart-test` inside that
owned cluster. Its CoreDNS TTL is set to 30 seconds to exercise the chart
cache budget; KEDA CRDs are installed only there. No current/default Kubernetes
context is used. The cluster and probe image are deleted in `finally`, including
on test failures. Logs and JSON reconciliation evidence remain in `.reports/`.
The `all` and `scaling` scenarios require Sink 0.20+ and enable chart-generated
logging on all roles with an unreachable local OTLP receiver. Business traffic,
readiness, and bounded shutdown must still work. The `upgrades` scenario exercises separate compatible image references; it does
not assert compatibility with the retired 0.18 configuration or forwarding protocol.
Interrupting the entire host/process can bypass cleanup; use `kind get clusters`
and delete only the recorded test cluster if that happens.

Coverage:

- Authenticated MongoDB with synthetic URI-reserved/Unicode credentials projected from Secrets.
- Both roles rotate to a new immutable Secret under traffic; revoke the old backend
  user after termination, then verify synchronous and asynchronous operations.
- A missing key during rotation leaves existing Engine/Worker capacity intact under traffic.
- Missing Secret and missing key block staged Engine startup and activation.

- Default DNS/termination budgets during simultaneous Gateway/Engine rolling updates,
  using the Gateway headless Service through a persistent SDK client.
- Engine 2 -> 3 -> 1 -> 2 while that client performs unique creates and reads.
- Store staged -> active -> retiring -> removal; existing Store traffic continues.
- Live Helm lookup rejection of unsafe addition, activation, direct removal,
  missing drain approval and old Gateway Pods during retirement.
- Complete final reconciliation of every acknowledged synchronous record; no
  mutation retries and SDK reads configured for one attempt.
- Async acceptance with zero Workers, actual Kafka backlog, KEDA activation and
  multiple consumers, all accepted records applied, return to zero and reactivation.
  Worker scheduling is held until HPA observes the backlog, modeling node provisioning
  and avoiding a fast consumer draining all lag before the first HPA sample. Replica
  changes are made by KEDA/HPA, never manually forced by the test.

The fixture uses one-node Mongo/Kafka to isolate deployment behavior. It is **not**
a backend HA, multi-zone, production capacity or EKS load-balancer qualification.
It uses actual default Chart DNS/drain timings, so allow roughly 20–30 minutes plus
image downloads. The async verification polls for application, which is expected
for durable asynchronous completion; synchronous rollout errors fail immediately.

For development only, an explicitly created disposable Kind cluster can be adopted
with `--cluster sink-chart-NAME --kubeconfig /absolute/owned/kubeconfig`. That cluster
is still deleted on completion. Never point this option at a shared cluster.

Use `--scenario scaling` to rerun only the Kafka/KEDA regression in a fresh cluster.

Use `--sink-image repository:tag` to qualify an explicitly built local candidate.
Only the disposable test cluster loads that image. Default deployment/scaling
runs require the chart's matching 0.20 release. Before release, pass a locally
built role-config candidate. `upgrades` requires an explicit baseline candidate.

## Shared image upgrades and rollback

CI builds two differently version-stamped images from the matching candidate and
runs `--scenario upgrades --sink-image sink-chart-candidate:a --upgrade-image sink-chart-candidate:b`
in a separate disposable Kind cluster. Each values revision changes only the
top-level `image`: all Gateways, Engines and Workers upgrade together, then roll
back together. Both revisions wait for old Pods to terminate, record each
Deployment's image, and verify fresh synchronous and asynchronous records.
A persistent SDK client continues creating and reading throughout both rollouts
without mutation retries; its complete acknowledged history is reconciled at
the end. Premature traffic completion fails the scenario.

Evidence includes `version-matrix.json`, per-stage business reports and the
continuous traffic report. The test qualifies the specific compatible image pair.
Use `--sink-image` and `--upgrade-image` to qualify another pair; incompatible
configuration, forwarding or Kafka formats require a separate cluster cutover.
