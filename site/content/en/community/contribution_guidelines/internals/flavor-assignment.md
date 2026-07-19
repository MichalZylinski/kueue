---
title: "Flavor assignment: deciding *how* a workload could fit"
linkTitle: "Flavor assignment"
weight: 40
description: >
  Flavor assignment: deciding *how* a workload could fit
type: docs
---

<!-- Written from a read of the source around the v0.19 cut. Links point at main (not a pinned commit), so files/directories stay resolvable as the code evolves; described behavior may drift in detail over time -- if something looks off, a fix or a removal is equally welcome, no need to reconcile the whole page. -->

Package: [`pkg/scheduler/flavorassigner/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/scheduler/flavorassigner). Given one workload and one ClusterQueue snapshot, produce an `Assignment`: for every pod set and every resource, *which flavor*, and *in which mode*.

## 4.1 The three modes

`FlavorAssignmentMode` (flavorassigner.go:411):

| Mode | Meaning |
|---|---|
| `Fit` | enough **unused** quota right now (possibly by borrowing from the cohort) |
| `Preempt` | would fit **within quota**, but only if some admitted workload(s) are evicted; borrowing may also be pending (`WhenCanBorrow` policy) |
| `NoFit` | can never fit in this flavor with current quotas — the reason string ends up in the workload status (`NoFitReason`) |

The **representative mode** of a whole assignment is the *worst* mode across all pod sets/resources (`RepresentativeMode()`): one `NoFit` resource makes the workload inadmissible in that configuration; one `Preempt` makes the whole admission contingent on preemption (Chapter 5 then computes the victims).

## 4.2 The search

For each pod set resource, the assigner iterates the CQ's resource groups and their flavors **in the order listed in the ClusterQueue spec** (order is meaningful API — "prefer spot, spill to on-demand"). For each flavor it checks:

1. **Node-affinity compatibility** — the flavor's `nodeLabels`/`nodeTaints` vs the pod template's affinity/tolerations (so a pod set that explicitly requires `gpu-type=h100` skips non-matching flavors). Kueue reuses scheduler plugin logic for this check.
2. **Quota arithmetic** — against the cohort tree (Chapter 3): fits under nominal? fits with borrowing? would fit if usage below nominal were reclaimed?
3. Feature adjuncts — TAS feasibility (Chapter 7), DRA device quota ([`pkg/dra/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/dra)), reclaimable pods (already-finished pods reduce the request — KEP-78).

Results per attempted flavor are recorded (`FlavorAssignmentAttempts`, with `NoFitReason`) — since recently these attempts are also surfaced in events (`FormatFlavorAssignmentAttemptsForEvents` in scheduler.go), a gift for debugging "why did it pick flavor X?"

## 4.3 Flavor fungibility: the policy knobs

`ClusterQueue.spec.flavorFungibility` (clusterqueue_types.go:439–453) controls when the search stops:

- `whenCanBorrow: MayStopSearch | TryNextFlavor` — settle for borrowing in this flavor, or keep looking for a flavor with free nominal quota?
- `whenCanPreempt: MayStopSearch | TryNextFlavor` — settle for preempting here, or prefer a later flavor that fits without violence?
- `preference: BorrowingOverPreemption | PreemptionOverBorrowing` — tie-break between modes.

Comparing candidate assignments across flavors happens in the internal `preemptionMode` ordering ([`pkg/scheduler/flavorassigner/flavorassigner.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/scheduler/flavorassigner/flavorassigner.go)). This is a subtle, table-test-heavy area: any change here needs cases in [`flavorassigner_test.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/scheduler/flavorassigner/flavorassigner_test.go) covering all policy combinations.

## 4.4 Assignment reuse and second chances

- `LastAssignment` on `workload.Info` remembers which flavors already failed, so BestEffortFIFO retries don't thrash.
- `updateAssignmentIfNeeded` ([`pkg/scheduler/scheduler.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/scheduler/scheduler.go)) revalidates a nominated entry's assignment if the snapshot changed mid-cycle (earlier entries in the same cycle may have consumed quota).
- With `SchedulingEquivalenceHashing`, identically-shaped workloads share results via `SchedulingHash`.

**Where you'll engage:** new resource semantics (DRA, resource transformations), fungibility policies, partial admission (`minCount` — the assigner may downsize pod sets), TAS interplay. Roadmap tie-in: "flavor assignment strategies (minimize cost vs minimize borrowing)" is a long-term goal — this package is where it will land.
