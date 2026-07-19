---
title: "Topology-Aware Scheduling (TAS)"
linkTitle: "Topology-Aware Scheduling"
weight: 70
description: >
  Topology-Aware Scheduling (TAS)
type: docs
---

<!-- Written from a read of the source around the v0.19 cut. Links point at main (not a pinned commit), so files/directories stay resolvable as the code evolves; described behavior may drift in detail over time -- if something looks off, a fix or a removal is equally welcome, no need to reconcile the whole page. -->

KEP-2724; a top roadmap area. TAS makes Kueue place a workload's pods *compactly* within the datacenter network topology (block → rack → host) to minimize inter-pod hops for ML training.

## 7.1 The API surface

- **`Topology` CRD** ([`apis/kueue/v1beta2/topology_types.go`](https://github.com/kubernetes-sigs/kueue/blob/main/apis/kueue/v1beta2/topology_types.go)): an ordered list of `levels`, each a node label key, from widest to narrowest, e.g. `cloud.provider.com/block`, `.../rack`, `kubernetes.io/hostname`.
- A **ResourceFlavor** opts in by setting `.spec.topologyName` — that flavor's nodes are now tracked per topology domain.
- **Per-pod-set requests** via job annotations (topology_types.go:28–47), copied into `Workload.spec.podSets[*].topologyRequest`:
  - `kueue.x-k8s.io/podset-required-topology: rack-label` — all pods **must** fit in one domain at that level (else the flavor doesn't fit);
  - `kueue.x-k8s.io/podset-preferred-topology: ...` — best effort, relax upward level by level;
  - `kueue.x-k8s.io/podset-unconstrained-topology` — count capacity, ignore compactness.
- The scheduler's output is `topologyAssignment` in the admission: an explicit list of domains and pod counts per domain.

## 7.2 How placement is computed

The TAS cache ([`pkg/cache/scheduler/tas_cache.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/cache/scheduler/tas_cache.go), [`tas_flavor.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/cache/scheduler/tas_flavor.go), [`tas_nodes_cache.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/cache/scheduler/tas_nodes_cache.go)) watches Nodes and non-TAS pods ([`tas_non_tas_pod_cache.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/cache/scheduler/tas_non_tas_pod_cache.go) — usage by pods Kueue doesn't manage still consumes node capacity!) and builds, per TAS flavor, a tree of topology domains with free capacity per domain.

At snapshot time ([`tas_flavor_snapshot.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/cache/scheduler/tas_flavor_snapshot.go)), the flavor assigner asks: *can this pod set's count × requests fit into the domain tree under the requested level?* The algorithm greedily searches the fewest/lowest domains that fit (balanced placement variant behind `TASBalancedPlacement`, [`tas_balanced_placement.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/cache/scheduler/tas_balanced_placement.go)). If yes, the chosen domains are recorded in the assignment; capacity is consumed in the snapshot so co-scheduled workloads in the same cycle don't collide.

## 7.3 From assignment to pods: the ungater

TAS pods are created with a **scheduling gate** (`kueue.TopologySchedulingGate`). The **topology ungater** ([`pkg/controller/tas/topology_ungater.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/tas/topology_ungater.go)) watches admitted TAS workloads and their pods, assigns each pod to a concrete domain from `topologyAssignment` (injecting a node selector for that domain's label values), then removes the gate — only then does kube-scheduler place it. This is how Kueue "pins" pods to racks without being a scheduler itself.

Supporting controllers in [`pkg/controller/tas/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/controller/tas): [`topology_controller.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/tas/topology_controller.go) (Topology lifecycle), [`resource_flavor.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/tas/resource_flavor.go) (flavor↔topology wiring), [`node_controller.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/tas/node_controller.go) (node health; `NodeFailureDelay = 30s`), [`non_tas_usage_controller.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/tas/non_tas_usage_controller.go).

## 7.4 Failure handling — the active frontier

When a node in an assignment dies, the workload's pods can't just be rescheduled anywhere — the assignment names domains. Gates: `TASFailedNodeReplacement` (find a replacement domain), `TASFailedNodeReplacementFailFast`, `TASReplaceNodeOnPodTermination`, `TASReplaceNodeOnNodeTaints`. The scheduler participates via `handleFailedTASReplacement` / `evictWorkloadAfterFailedTASReplacement` ([`pkg/scheduler/scheduler.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/scheduler/scheduler.go), [`pkg/scheduler/scheduler.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/scheduler/scheduler.go)) and the second-pass queue; unhealthy nodes are tracked in `Workload.status.unhealthyNodes`. Roadmap: eviction on tainted nodes, TAS×elastic workloads, TAS×ResourceTransformations — all live exactly here.

**Where you'll engage:** placement algorithm improvements, multi-layer topologies (`TASMultiLayerTopology`), failure recovery, and the substantial test surfaces [`scheduler_tas_test.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/scheduler/scheduler_tas_test.go), [`test/integration/singlecluster/tas/`](https://github.com/kubernetes-sigs/kueue/tree/main/test/integration/singlecluster/tas), [`test/e2e/tas/`](https://github.com/kubernetes-sigs/kueue/tree/main/test/e2e/tas).
