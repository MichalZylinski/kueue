---
title: "The Queue Manager: the pending side"
linkTitle: "The Queue Manager"
weight: 20
description: >
  The Queue Manager: the pending side
type: docs
---

<!-- Verified against main@2b494fec3 (the v0.19 cut). Re-verify each minor release. -->

Package: `pkg/cache/queue/`. This is Kueue's "waiting room": every workload that is *not yet admitted* lives here, ordered and ready for the scheduler to pop.

## 2.1 Structure

```
Manager (manager.go)                 one per process; a big mutex + a Broadcast cond-var
 ├── localQueues:  map[LocalQueueReference]*LocalQueue     (local_queue.go)
 ├── hm: hierarchy of ClusterQueues + Cohorts              (cohort.go + pkg/cache/hierarchy)
 │     └── *ClusterQueue (cluster_queue.go)
 │           ├── heap of pending workload.Info             ← the actual queue
 │           └── inadmissibleWorkloads                     (inadmissible_workloads.go)
 ├── secondPassQueue                                       (second_pass_queue.go)
 └── AFS penalty tracking                                  (afs/)
```

A workload's path: `AddOrUpdateWorkload` (manager.go:628) resolves LocalQueue → ClusterQueue, wraps it in `workload.Info`, and pushes it onto the ClusterQueue's heap.

## 2.2 Ordering: the queueing strategies

Heap order (see `cluster_queue.go`) is by **priority, then eviction/creation timestamp**. The `queueingStrategy` on the ClusterQueue changes *what happens when the head doesn't fit*:

- **`StrictFIFO`** — the queue is a strict line: if the head is inadmissible it blocks everything behind it (predictability over throughput). The head stays the head.
- **`BestEffortFIFO`** (default) — an inadmissible head is moved aside into `inadmissibleWorkloads`, letting smaller workloads behind it through.

Whether a workload re-enters the heap immediately or parks as inadmissible is decided by `RequeueWorkload(... reason)` (manager.go:675) — the `RequeueReason` distinguishes "failed after nomination" from "became eligible again."

## 2.3 The inadmissible set and event-driven retries

`inadmissibleWorkloads` is not polled on a timer. Parked workloads are re-queued when *something relevant changes*: the manager exposes `QueueAssociatedInadmissibleWorkloadsAfter` and cluster-event handlers so that quota release (a workload finished), a ClusterQueue/Cohort/ResourceFlavor update, a new AdmissionCheck state, etc. move affected workloads back into the heap and `Broadcast()` wakes the scheduler.

**This is a classic bug surface:** "my workload stays Pending forever even though quota freed up" is almost always a missing *requeue trigger* — some event type that should call back into the manager but doesn't. When you add any new admission precondition, you must also add the event path that retries inadmissible workloads when the precondition changes.

## 2.4 `Heads()`: the hand-off to the scheduler

`Heads(ctx)` (manager.go:810) is the single point of contact with the scheduler: it returns the head workload of *every* active ClusterQueue, **blocking on the condition variable while all heaps are empty**. That is why the scheduling cycle in `scheduler.go` "blocks while the queues are empty" — the block is here.

## 2.5 Odds and ends worth knowing

- **Second-pass queue** — some admissions need a follow-up scheduling pass shortly after (e.g. TAS after preemption victims actually vanish); `second_pass_queue.go` holds them.
- **StopPolicy / status checker** — `status_checker.go` answers "is this CQ active?"; workloads submitted to a held/stopped queue are parked with a reason surfaced in the workload status.
- **Dumper** — `dumper.go` prints queue contents; triggered via `hack/dump_cache.sh`. Extend it when you add state here.
- **`finishedWorkloads` LRU** — recently finished workloads are remembered to avoid re-adding them (races between the job reconciler and workload deletion).
- **Concurrent admission (KEP-8691)** — `ConcurrentAdmissionEnabled*` methods special-case "parent" workloads that fan out into variants; feature-gated by `ConcurrentAdmission`, an active development area.

**Where you'll engage:** queueing-order features (priority defaulting, AFS ordering), new requeue triggers, StrictFIFO/BestEffortFIFO semantics, visibility (pending-position APIs read from here). Tests: `pkg/cache/queue/*_test.go` + `test/integration/singlecluster/scheduler/`.
