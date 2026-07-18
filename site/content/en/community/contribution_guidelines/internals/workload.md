---
title: "The Workload: anatomy of Kueue's central object"
linkTitle: "The Workload"
weight: 10
description: >
  The Workload: anatomy of Kueue's central object
type: docs
---

<!-- Verified against main@2b494fec3 (the v0.19 cut). Re-verify each minor release. -->

Everything in Kueue converges on the `Workload` type (`apis/kueue/v1beta2/workload_types.go`). If you understand this one object completely, every subsystem becomes "code that reads or writes some part of a Workload."

## 1.1 Spec: what the job needs

```yaml
spec:
  queueName: team-a-queue        # LocalQueue; immutable once QuotaReserved
  priorityClassName: high        # WorkloadPriorityClass or k8s PriorityClass
  priority: 1000                 # resolved numeric priority (mutable → reprioritization)
  active: true                   # set false to deactivate/evict without deleting
  podSets:                       # 1..18 groups of identical pods (limit raised from 8 in v0.19)
  - name: main
    count: 4
    minCount: 2                  # partial admission (KEP-420), optional
    template: {...}              # full corev1.PodTemplateSpec
    topologyRequest: {...}       # TAS (see Chapter 7)
```

Key ideas:

- **PodSets are the resource-accounting unit.** A `batch/Job` maps to one pod set; a JobSet or RayJob maps to several (driver/workers, leader/replicas). The scheduler computes `count × per-pod-requests` per pod set — helpers in `pkg/podset/` and `pkg/workload/resources.go` (which also applies LimitRanges, runtime class overhead, and the `resources.transformations` from the configuration).
- **Priority is resolved at creation** into `.spec.priority` from either a `WorkloadPriorityClass` (label `kueue.x-k8s.io/priority-class`, Kueue-specific, doesn't affect pod preemption by kubelet — KEP-973) or the pod's Kubernetes PriorityClass. Code: `pkg/util/priority/`.
- **`active`** is the deactivation switch: the workload controller evicts a workload whose `active=false` (`WorkloadDeactivated` eviction reason) — used by users, by MultiKueue, and by the maximum-execution-time feature (label `kueue.x-k8s.io/max-exec-time-seconds`).

## 1.2 Status: what Kueue decided

```yaml
status:
  admission:                       # written by the scheduler on QuotaReserved
    clusterQueue: team-a-cq
    podSetAssignments:
    - name: main
      flavors: {cpu: spot, memory: spot}
      resourceUsage: {cpu: "4"}
      count: 4
      topologyAssignment: {...}    # TAS only
  admissionChecks: [...]           # per-check states (Pending/Ready/Retry/Rejected)
  conditions: [...]                # QuotaReserved, Admitted, Evicted, Requeued, Finished
  requeueState:                    # backoff bookkeeping after evictions
    count: 3
    requeueAt: "..."
```

The `admission` block is the scheduler's output and the contract with the job framework: `podSetAssignments[*].flavors` tells the reconciler which ResourceFlavors' `nodeLabels`/`tolerations` to inject into the job's pod templates before unsuspending.

## 1.3 The condition state machine

Defined around `workload_types.go:929–1013`; manipulated almost exclusively through helpers in `pkg/workload/workload.go` (never hand-roll condition updates — grep for an existing helper first):

```
 (pending) ──scheduler──▶ QuotaReserved ──all AdmissionChecks Ready──▶ Admitted ──job done──▶ Finished
     ▲                        │                                           │
     │                        │ check Rejected / preemption /             │ preemption, podsReady
     │                        │ CQ stopped                                │ timeout, deactivation,
     │                        ▼                                           ▼ TAS node failure...
     └──── Requeued ◀──── Evicted ◀───────────────────────────────────────┘
```

Contributor-critical details:

- **`Evicted` is a process, not just a flag.** Eviction is two-phase: the evictor sets `Evicted=True` with a reason (`pkg/workload/evict/`), then the workload controller (`pkg/controller/core/workload_controller.go`) finishes the job: suspends via the job framework, clears `.status.admission`, releases quota in the cache, sets `Requeued`, and re-enqueues with backoff. When touching eviction paths, always ask "who completes the second phase?"
- **Requeue backoff** (`.status.requeueState`, KEP-1282): evictions under `waitForPodsReady` increment a counter with exponential backoff; exceeding `backoffLimitCount` deactivates the workload. Note: `waitForPodsReady` is **enabled by default since v0.19** (30-minute timeout, `blockAdmission: false`; opt out via config or the `DisableWaitForPodsReady` gate), so this path is now live in default installations.
- **Eviction reasons are an API.** Each reason constant (Preempted, PodsReadyTimeout, AdmissionCheck, ClusterQueueStopped, Deactivated, NodeFailures…) shows up in conditions, events, and metrics. Reusing a reason for a semantically different operation is a review blocker (see the terminology rule in the architecture page).

## 1.4 `workload.Info`: the in-memory view

The scheduler and both caches never work with raw `*kueue.Workload` alone; they wrap it in `workload.Info` (`pkg/workload/workload.go:205`):

```go
type Info struct {
    Obj           *kueue.Workload
    TotalRequests []PodSetResources   // pre-computed count×requests per pod set
    ClusterQueue  kueue.ClusterQueueReference
    LastAssignment *AssignmentClusterQueueState  // remembers failed flavor attempts
    SchedulingHash string             // equivalence class for SchedulingEquivalenceHashing
    // + AFS usage, second-pass iteration, last evaluated generation
}
```

`TotalRequests` is where all resource math starts; `SchedulingHash` groups identically-shaped workloads so the flavor assigner can reuse results (`SchedulingEquivalenceHashing` gate). If you change anything that affects scheduling outcome, check whether it must be part of the hash.

## 1.5 Identity and lifecycle plumbing

- Name: `<job-type-prefix>-<job-name>-<suffix>` (`pkg/controller/jobframework/workload_names.go`; `ShortWorkloadNames` gate changes the scheme).
- `ownerReferences[0]` → the parent job; label `kueue.x-k8s.io/job-uid` → parent UID (indexed; the standard way to find a job's workload).
- A finalizer (`kueue.x-k8s.io/resource-in-use`) protects in-use objects; orphaned workloads (job deleted) are garbage-collected — feature `FinishOrphanedWorkloads` and `objectRetentionPolicies` in the configuration control terminal-object GC (KEP-1618).
- **Updates use SSA-style patching** through `pkg/workload/patching/` (see `PatchAdmissionStatus` usage in the scheduler). Never `Update()` a workload status directly in new code; conflicts and field ownership are handled centrally there.

**Where you'll engage:** almost every feature adds a condition, a reason, a status field, or a spec knob here — which means webhook validation (`pkg/webhooks/workload_webhook.go`), conversion from `v1beta1`, helper functions, and integration tests in `test/integration/singlecluster/controller/`.
