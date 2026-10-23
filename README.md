# fleetwave

A Kubernetes operator, written in Go, that rolls changes across a **fleet of
clusters** in health-gated waves and rolls the whole fleet back when a wave
goes bad.

You describe the change once, on a hub cluster:

```yaml
apiVersion: fleet.fleetwave.io/v1alpha1
kind: FleetRollout
metadata:
  name: checkout
spec:
  clusterSelector:
    matchLabels: {env: prod}
  strategy:
    waves: ["1", "25%", "100%"]   # canary, a quarter of the fleet, everyone
    bakeTime: 10m                 # each wave must stay healthy this long
    progressDeadline: 5m          # per cluster, from apply to healthy
    maxFailures: 0
    autoRollback: true
  resources:
    - apiVersion: apps/v1
      kind: Deployment
      metadata: {name: checkout, namespace: payments}
      spec: ...
```

and fleetwave:

- applies it to a **canary** cluster first, then widens wave by wave;
- decides "healthy" from the workloads themselves (Deployment, StatefulSet,
  DaemonSet, Job rollout status, `Ready` conditions), not from "the apply
  returned 200";
- holds each wave for a **bake time** and fails a cluster that regresses during it;
- on failure, **rolls every cluster that got the bad revision back** to the last
  revision that reached the whole fleet, and records why;
- prunes objects you removed from the spec, refuses to take over objects another
  rollout owns, and cleans up member clusters when the rollout is deleted.

The design note, with the state machine, failure modes and how it scales to
160+ clusters, is in [DESIGN.md](DESIGN.md).

## See it work

`make demo` boots a hub and four member clusters on your machine (each a real
kube-apiserver and etcd via envtest, no Docker needed), registers them, and
ships three releases of a `checkout` service. Release 2 crash-loops only in
`syd-1`. Real output:

```text
── release 2: checkout 1.5.0, which crash-loops in syd-1 only
   bkk-1  wave 1  Progressing rev e6994e2e10
   sgp-1  wave 2  Pending     rev a80c6c60ed
   sgp-2  wave 3  Pending     rev a80c6c60ed
   syd-1  wave 3  Pending     rev a80c6c60ed
Progressing wave 1 of 3 in progress
   bkk-1  wave 1  Healthy     rev e6994e2e10
Baking      wave 1 of 3 healthy; baking, 3s left
   sgp-1  wave 2  Progressing rev e6994e2e10
   sgp-1  wave 2  Healthy     rev e6994e2e10
Baking      wave 2 of 3 healthy; baking, 3s left
   sgp-2  wave 3  Progressing rev e6994e2e10
   syd-1  wave 3  Progressing rev e6994e2e10
   sgp-2  wave 3  Healthy     rev e6994e2e10
   bkk-1  wave 1  RolledBack  rev a80c6c60ed  rolled back with the rest of the rollout
   sgp-1  wave 2  RolledBack  rev a80c6c60ed  rolled back with the rest of the rollout
   sgp-2  wave 3  RolledBack  rev a80c6c60ed  rolled back with the rest of the rollout
   syd-1  wave 3  RolledBack  rev a80c6c60ed  progress deadline (6s) exceeded: Deployment/checkout: 0 of 3 updated replicas available
RolledBack  1 cluster(s) failed (budget 0); rolling back 4 cluster(s) to a80c6c60ed
   running in member clusters: bkk-1=1.4.0  sgp-1=1.4.0  sgp-2=1.4.0  syd-1=1.4.0

── release 3: checkout 1.5.1 with the fix
   ...
Succeeded   revision 776086a321 rolled out to 4 cluster(s)
   running in member clusters: bkk-1=1.5.1  sgp-1=1.5.1  sgp-2=1.5.1  syd-1=1.5.1
```

The demo uses short timings (3s bake, 6s deadline) and a simulated kubelet, so
the failure is injected with a test-only annotation. The controller code is the
same code that runs in a cluster.

## How it is built

| Path | What it does |
|---|---|
| `api/v1alpha1` | `MemberCluster` and `FleetRollout` types; CRDs and deepcopy are generated with controller-gen |
| `internal/planner` | The rollout state machine. Pure functions, no clients or clocks, so every rule is a table test |
| `internal/controller` | Reconcilers: probe members, gather health, call the planner, apply/prune/roll back with bounded parallelism, write status, events and metrics |
| `internal/health` | Health verdicts for workloads and anything with a `Ready` condition |
| `internal/revision` | Validation, content hashing, apply ordering, pruning inventories; snapshots stored as `ControllerRevision`s |
| `internal/remote` | Member clients built from kubeconfig Secrets, cached per Secret version so rotation needs no restart |
| `internal/testenv` | Hub + N member API servers and a kubelet simulator, shared by the tests and the demo |

A few decisions I would defend in review (more in DESIGN.md):

- **Server-side apply** with a dedicated field manager, so re-applying is
  idempotent and a restart mid-wave is safe.
- **No informers on member clusters.** At 160+ clusters, one watch per kind per
  member in the hub costs more than the GETs a rollout actually needs.
- **Unreachable clusters block a wave instead of being skipped.** Skipping
  silently is how a fleet ends up on two versions. Cordon a cluster to move on.
- **Halted and RolledBack are sticky** until the spec changes, so a known-bad
  revision is never retried by the controller; the fix is a Git commit.
- **Kubeconfig Secrets are never cached** on the hub; only their metadata is watched.

## Tests

```sh
make test        # unit tests + multi-cluster integration tests, with -race
make test-unit   # planner, health and revision only, no API servers
```

The integration suite runs a hub and three member clusters as separate API
servers and covers: waves respecting bake time, a region-specific bad release
rolled back fleet-wide, pruning, ownership conflicts, finalizer cleanup on all
members, pause, an unreachable cluster unblocked by cordoning, invalid specs,
and that the samples in `config/samples` pass CRD validation.

Coverage: planner 97%, revision 92%, health 89%, controller 84%.

## Run it on real clusters

Requires Go 1.24 and, for a real install, a hub cluster plus member clusters
(kind works).

```sh
make docker-build                       # fleetwave:dev
kind load docker-image fleetwave:dev --name hub
make deploy                             # CRDs, RBAC, manager (2 replicas, leader election)

# register members: kubeconfig under key "value" (the Cluster API convention).
# With kind, use --internal so the address is reachable from the hub's pods.
kind get kubeconfig --name bkk-1 --internal > bkk-1.kubeconfig
kubectl -n fleetwave-system create secret generic bkk-1-kubeconfig --from-file=value=bkk-1.kubeconfig
kubectl apply -f config/samples/memberclusters.yaml
kubectl get memberclusters

kubectl apply -f config/samples/fleetrollout.yaml
kubectl get fleetrollouts -w
kubectl describe fleetrollout checkout   # per-cluster state and events
```

Manager flags: `--poll-interval`, `--drift-interval`, `--probe-interval`,
`--apply-concurrency`, `--member-qps`, `--leader-elect`. Metrics are on `:8080`
(`fleetwave_rollout_phase`, `fleetwave_rollout_clusters`,
`fleetwave_member_cluster_ready`, `fleetwave_rollbacks_total`, ...).

## Status

Personal project, `v1alpha1`. Verified with the envtest suite and demo
above. The next things on the list (metric-based analysis gates, manual
approval between waves, Cluster API auto-registration) are in DESIGN.md.

## License

MIT
