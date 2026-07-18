---
title: "Glossary"
linkTitle: "Glossary"
weight: 95
description: >
  Definitions of Kueue-specific terms used across code, docs, and KEPs
type: docs
---

<!-- Verified against main@2b494fec3 (the v0.19 cut). Re-verify each minor release. -->


Terms you'll meet in code, KEPs, and Slack, with where each lives. Cross-references in *italics*.

**Admission** — the decision that a workload may run. Two-phase: quota reservation by the scheduler (*QuotaReserved*), then *AdmissionChecks*. Result recorded in `Workload.status.admission`.

**AdmissionCheck** — an extensible second admission gate: an external controller must mark the check `Ready` on each workload before it becomes *Admitted*. Built-ins: ProvisioningRequest, *MultiKueue*. (`apis/kueue/v1beta2/admissioncheck_types.go`)

**AFS — Admission Fair Sharing** — ordering pending workloads by *historical* LocalQueue usage (decayed over time), not just current share. Gate `AdmissionFairSharing`. (`pkg/cache/queue/afs/`)

**Assume / rollback** — optimistic admission: usage is written into the live *scheduler cache* before the API-server patch; on patch failure the cache entry is deleted. (`assumeWorkload`, `pkg/scheduler/scheduler.go`)

**BestEffortFIFO / StrictFIFO** — *queueing strategies*: whether an inadmissible head workload is set aside (BestEffort, default) or blocks the whole ClusterQueue (Strict).

**Borrowing** — a ClusterQueue using unused quota of other queues in its *cohort*, up to its `borrowingLimit`.

**Cohort** — a (since KEP-79, hierarchical) grouping of ClusterQueues that can borrow from each other. Quota math over the tree: *resourceNode*.

**ClusterQueue (CQ)** — cluster-scoped quota pool with flavors, strategies, preemption policy. The admin-facing core object.

**DRA** — Dynamic Resource Allocation integration: quota for device classes rather than plain resource names. Gates `KueueDRAIntegration*`. (`pkg/dra/`)

**DRS — Dominant Resource Share** — fair-sharing value per CQ/cohort: max over resources of (usage above nominal ÷ lendable capacity), divided by fair weight. Lower share ⇒ admitted earlier, preempted later. (`pkg/cache/scheduler/fair_sharing.go`)

**Eviction** — taking a running workload off the cluster *without* deleting it: suspend job, clear admission, set `Evicted` then `Requeued`. Reasons: Preempted, PodsReadyTimeout, Deactivated, ClusterQueueStopped, NodeFailures… Distinct from *preemption* (one cause of eviction).

**Flavor** → *ResourceFlavor*.

**Flavor fungibility** — per-CQ policy for when the flavor search stops: settle for borrowing/preemption in this flavor (`MayStopSearch`) vs `TryNextFlavor`; preference `BorrowingOverPreemption` or the reverse.

**Head (of a queue)** — the next workload a ClusterQueue offers to the scheduler; a scheduling cycle takes the heads of all CQs (`Manager.Heads()`).

**Inadmissible workload** — a pending workload parked outside the heap after failing admission; re-queued only by relevant cluster events (quota freed, CQ/flavor updated…). (`pkg/cache/queue/inadmissible_workloads.go`)

**Job framework** — the abstraction (`GenericJob` interface + generic reconciler + base webhook) that lets ~16 job types share one management loop. (`pkg/controller/jobframework/`)

**KEP** — Kueue Enhancement Proposal; design doc under `keps/<issue-number>-<name>/`. Required for API/behavior changes.

**Lending limit** — cap on how much of a CQ's nominal quota its cohort siblings may borrow (KEP-1224).

**LocalQueue (LQ)** — namespaced pointer to a ClusterQueue; the user-facing submission target (`kueue.x-k8s.io/queue-name` label).

**ManagedBy** — field on Job/JobSet telling the regular controller to leave the object alone; how *MultiKueue* keeps the manager-cluster copy dormant while the worker copy runs.

**MultiKueue** — multi-cluster dispatching: manager cluster owns queues/quota; workloads are mirrored to worker clusters through an *AdmissionCheck*; first worker to admit wins. (`pkg/controller/admissionchecks/multikueue/`)

**Nominal quota** — the guaranteed quantity of a resource in a flavor for a CQ; usage beyond it is *borrowing*.

**Partial admission** — admitting a workload with fewer pods than requested, down to `podSets[*].minCount` (KEP-420).

**Pod group** — plain pods managed as one workload via `kueue.x-k8s.io/pod-group-name` annotations (KEP-976); basis of the serving integrations.

**PodSet** — a group of identical pods within a workload (count × template); the unit of resource accounting and flavor assignment.

**Preemption** — evicting admitted workloads to make room for a pending one, per the CQ's three policy dials (`withinClusterQueue`, `reclaimWithinCohort`, `borrowWithinCohort`). Victims get the `Preempted` event with preemptor UID/JobUID.

**Prebuilt workload** — a user-created Workload a job attaches to via the `kueue.x-k8s.io/prebuilt-workload-name` label, instead of Kueue generating one.

**Quota reservation** → *QuotaReserved* condition; phase one of admission, done by the scheduler.

**Reclaimable pods** — pods of a partially finished job whose resources are returned to the quota before the whole job finishes (KEP-78).

**Requeue / RequeueState** — post-eviction return to the pending queue, with exponential backoff counted in `status.requeueState` (KEP-1282).

**ResourceFlavor (RF)** — a named kind of capacity (spot, H100…) carrying node labels/taints/tolerations that get injected into admitted jobs.

**resourceNode** — the shared quota data structure of CQs and cohorts: `Quotas`, `SubtreeQuota`, `Usage`. (`pkg/cache/scheduler/resource_node.go`)

**Scheduler cache** — the *admitted* side: usage per CQ/cohort, snapshot source. Don't confuse with the *queue manager* (pending side). (`pkg/cache/scheduler/`)

**Scheduling cycle** — one iteration of `schedule()`: heads → snapshot → nominate → ordered admission → requeue. (`pkg/scheduler/scheduler.go`)

**Second pass** — a follow-up scheduling evaluation shortly after admission-related events (used by TAS and preemption timing). (`pkg/cache/queue/second_pass_queue.go`)

**Snapshot** — the point-in-time copy of the scheduler cache a cycle computes on; preemption *simulates* on it.

**Suspend** — the `spec.suspend`-style field of a job; Kueue's fundamental actuation lever: managed jobs start suspended, admission unsuspends, eviction re-suspends.

**TAS — Topology-Aware Scheduling** — placing a workload's pods compactly within a topology tree (block/rack/host) via `Topology` CRD + per-pod-set annotations; enforced by scheduling gates and the *ungater* (KEP-2724).

**Topology domain** — one node-set at a topology level (a specific rack, host…); TAS assignments enumerate domains and pod counts.

**Ungater** — the TAS controller that releases pods' `kueue.x-k8s.io/topology` scheduling gates one by one, pinning each to its assigned domain. (`pkg/controller/tas/topology_ungater.go`)

**Visibility API** — on-demand endpoints exposing pending-workload positions (`apis/visibility/`, KEP-168).

**Workload** — Kueue's central CRD: the framework-agnostic representation of a job that the scheduler operates on. If in doubt, start reading here.

**WorkloadPriorityClass** — Kueue-only priority for queueing/preemption ordering, decoupled from pod (kubelet) priority (KEP-973).

**Workload slice** — the elastic-workload mechanism: a resized job gets a new workload "slice" that replaces the old one without full stop (KEP-77, gate `ElasticJobsViaWorkloadSlices`).
