---
title: "Design docs: the KEP process and a reading syllabus"
linkTitle: "KEPs & design docs"
weight: 40
description: >
  When you need a KEP, and which existing KEPs to read for each area of the code
type: docs
---

<!-- Written from a read of the source around the v0.19 cut. Links point at main (not a pinned commit), so files/directories stay resolvable as the code evolves; described behavior may drift in detail over time -- if something looks off, a fix or a removal is equally welcome, no need to reconcile the whole page. -->

## The KEP process for larger features

Anything that adds/changes API surface or introduces a significant behavior needs a **Kueue Enhancement Proposal** before code:

1. Open (or find) a feature issue; discuss viability in the issue / `#wg-batch` / the working group meeting.
2. Copy [`keps/NNNN-template/`](https://github.com/kubernetes-sigs/kueue/tree/main/keps/NNNN-template) to `keps/<issue-number>-<short-name>/` (`README.md` = the design doc: motivation, goals/non-goals, proposal, API, test plan, graduation criteria; `kep.yaml` = metadata).
3. Iterate on the KEP PR — expect substantial design discussion; this is where the real engineering happens.
4. Implement in follow-up PRs, gated alpha → beta → stable, updating the KEP's graduation criteria as you go.

Before designing anything, **search [`keps/`](https://github.com/kubernetes-sigs/kueue/tree/main/keps) first** — with 50+ merged KEPs, your idea likely touches an existing design (e.g. anything about preemption interacts with KEP-83, -1337, -1714, -7990). The KEP directory doubles as the best architecture documentation in the repo.


## Annotated KEP syllabus

Reading order per track. Directory names are real (`keps/<dir>/README.md`). Read the "Motivation", "Proposal", and "Drawbacks/Alternatives" sections; skim test plans on first pass.

### Track A — Core scheduling & quota (read these first, whatever your area)
1. **[`993-two-phase-admission`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/993-two-phase-admission/README.md)** — why admission is QuotaReserved → AdmissionChecks; the extension point everything else plugs into.
2. **[`79-hierarchical-cohorts`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/79-hierarchical-cohorts/README.md)** — the cohort tree and quota semantics; prerequisite for all borrowing/preemption reading.
3. **[`1224-lending-limit`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/1224-lending-limit/README.md)** — the other half of borrowing.
4. **[`582-preempt-based-on-flavor-order`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/582-preempt-based-on-flavor-order/README.md)** + **[`420-partial-admission`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/420-partial-admission/README.md)** — how flavor order and downsizing interact with admission.
5. **[`7513-quota-check-strategy`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/7513-quota-check-strategy/README.md)** and **[`6143-quota-release-strategy`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/6143-quota-release-strategy/README.md)** — recent refinements of when quota is checked/released; good view of how mature APIs evolve.

### Track B — Preemption & fair sharing
1. **[`83-workload-preemption`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/83-workload-preemption/README.md)** — the original model and the three policy dials.
2. **[`1337-preempt-within-cohort-while-borrowing`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/1337-preempt-within-cohort-while-borrowing/README.md)** — the trickiest policy corner.
3. **[`1714-fair-sharing`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/1714-fair-sharing/README.md)** — DRS, weights, preemption strategies.
4. **[`4136-admission-fair-sharing`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/4136-admission-fair-sharing/README.md)** — historical-usage ordering.
5. **[`7990-preemption-cost`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/7990-preemption-cost/README.md)** — open roadmap work; read last, contribute here.

### Track C — Job integrations & workload lifecycle
1. **[`369-job-interface`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/369-job-interface/README.md)** — the GenericJob contract; the integration author's constitution.
2. **[`976-plain-pods`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/976-plain-pods/README.md)** — pod groups; basis of serving support.
3. **[`973-workload-priority`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/973-workload-priority/README.md)** + **[`10765-workload-priority-class-defaulting`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/10765-workload-priority-class-defaulting/README.md)** — priority resolution.
4. **[`1282-pods-ready-requeue-strategy`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/1282-pods-ready-requeue-strategy/README.md)** + **[`349-all-or-nothing`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/349-all-or-nothing/README.md)** — waitForPodsReady semantics.
5. **[`78-dynamically-reclaiming-resources`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/78-dynamically-reclaiming-resources/README.md)** and **[`77-dynamically-sized-jobs`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/77-dynamically-sized-jobs/README.md)** — reclaimable pods and elastic slices.
6. **[`3589-manage-jobs-selectively`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/3589-manage-jobs-selectively/README.md)** — namespace-selector managedness.

### Track D — Topology-Aware Scheduling
1. **[`2724-topology-aware-scheduling`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/2724-topology-aware-scheduling/README.md)** — the foundation (long; worth it).
2. **[`6757-failure-recovery`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/6757-failure-recovery/README.md)** + gates `TASFailedNodeReplacement*` code — node-failure handling.
3. **[`2937-resource-transformer`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/2937-resource-transformer/README.md)** — interacts with TAS on the roadmap.

### Track E — MultiKueue
1. **[`693-multikueue`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/693-multikueue/README.md)** — architecture and the admission-check trick.
2. **[`2349-multikueue-external-custom-job-support`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/2349-multikueue-external-custom-job-support/README.md)** — adapters for arbitrary job types.
3. **[`9270-multikueue-incremental-step-size`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/9270-multikueue-incremental-step-size/README.md)** — the Incremental dispatcher.
4. **[`8303-multikueue-orchestrated-preemption`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/8303-multikueue-orchestrated-preemption/README.md)** + **[`9988-multikueue-manager-quota-automation`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/9988-multikueue-manager-quota-automation/README.md)** — active roadmap; open surface for contributors.

### Track F — UX, observability, APIs
1. **[`168-pending-workloads-visibility`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/168-pending-workloads-visibility/README.md)** (+ [`168-2-pending-workloads-visibility`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/168-2-pending-workloads-visibility/README.md)) — the visibility API story.
2. **[`2076-kueuectl`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/2076-kueuectl/README.md)** + **[`487-kubectl-plugin`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/487-kubectl-plugin/README.md)** — the CLI.
3. **[`1833-metrics-for-local-queue`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/1833-metrics-for-local-queue/README.md)**, **[`7066-custom-metric-labels`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/7066-custom-metric-labels/README.md)**, **[`10852-inadmissible-workloads-observability`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/10852-inadmissible-workloads-observability/README.md)** — metrics conventions in practice.
4. **[`2936-local-queue-defaulting`](https://github.com/kubernetes-sigs/kueue/blob/main/keps/2936-local-queue-defaulting/README.md)** — small, complete, great "anatomy of a simple KEP" example.
