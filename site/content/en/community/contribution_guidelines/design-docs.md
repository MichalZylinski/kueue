---
title: "Design docs: the KEP process and a reading syllabus"
linkTitle: "KEPs & design docs"
weight: 60
description: >
  When you need a KEP, and which existing KEPs to read for each area of the code
type: docs
---

<!-- Verified against main@2b494fec3 (the v0.19 cut). Re-verify each minor release. -->

## The KEP process for larger features

Anything that adds/changes API surface or introduces a significant behavior needs a **Kueue Enhancement Proposal** before code:

1. Open (or find) a feature issue; discuss viability in the issue / `#wg-batch` / the working group meeting.
2. Copy `keps/NNNN-template/` to `keps/<issue-number>-<short-name>/` (`README.md` = the design doc: motivation, goals/non-goals, proposal, API, test plan, graduation criteria; `kep.yaml` = metadata).
3. Iterate on the KEP PR — expect substantial design discussion; this is where the real engineering happens.
4. Implement in follow-up PRs, gated alpha → beta → stable, updating the KEP's graduation criteria as you go.

Before designing anything, **search `keps/` first** — with 50+ merged KEPs, your idea likely touches an existing design (e.g. anything about preemption interacts with KEP-83, -1337, -1714, -7990). The KEP directory doubles as the best architecture documentation in the repo.


## Annotated KEP syllabus

Reading order per track. Directory names are real (`keps/<dir>/README.md`). Read the "Motivation", "Proposal", and "Drawbacks/Alternatives" sections; skim test plans on first pass.

### Track A — Core scheduling & quota (read these first, whatever your area)
1. **`993-two-phase-admission`** — why admission is QuotaReserved → AdmissionChecks; the extension point everything else plugs into.
2. **`79-hierarchical-cohorts`** — the cohort tree and quota semantics; prerequisite for all borrowing/preemption reading.
3. **`1224-lending-limit`** — the other half of borrowing.
4. **`582-preempt-based-on-flavor-order`** + **`420-partial-admission`** — how flavor order and downsizing interact with admission.
5. **`7513-quota-check-strategy`** and **`6143-quota-release-strategy`** — recent refinements of when quota is checked/released; good view of how mature APIs evolve.

### Track B — Preemption & fair sharing
1. **`83-workload-preemption`** — the original model and the three policy dials.
2. **`1337-preempt-within-cohort-while-borrowing`** — the trickiest policy corner.
3. **`1714-fair-sharing`** — DRS, weights, preemption strategies.
4. **`4136-admission-fair-sharing`** — historical-usage ordering.
5. **`7990-preemption-cost`** — open roadmap work; read last, contribute here.

### Track C — Job integrations & workload lifecycle
1. **`369-job-interface`** — the GenericJob contract; the integration author's constitution.
2. **`976-plain-pods`** — pod groups; basis of serving support.
3. **`973-workload-priority`** + **`10765-workload-priority-class-defaulting`** — priority resolution.
4. **`1282-pods-ready-requeue-strategy`** + **`349-all-or-nothing`** — waitForPodsReady semantics.
5. **`78-dynamically-reclaiming-resources`** and **`77-dynamically-sized-jobs`** — reclaimable pods and elastic slices.
6. **`3589-manage-jobs-selectively`** — namespace-selector managedness.

### Track D — Topology-Aware Scheduling
1. **`2724-topology-aware-scheduling`** — the foundation (long; worth it).
2. **`6757-failure-recovery`** + gates `TASFailedNodeReplacement*` code — node-failure handling.
3. **`2937-resource-transformer`** — interacts with TAS on the roadmap.

### Track E — MultiKueue
1. **`693-multikueue`** — architecture and the admission-check trick.
2. **`2349-multikueue-external-custom-job-support`** — adapters for arbitrary job types.
3. **`9270-multikueue-incremental-step-size`** — the Incremental dispatcher.
4. **`8303-multikueue-orchestrated-preemption`** + **`9988-multikueue-manager-quota-automation`** — active roadmap; open surface for contributors.

### Track F — UX, observability, APIs
1. **`168-pending-workloads-visibility`** (+ `168-2`) — the visibility API story.
2. **`2076-kueuectl`** + **`487-kubectl-plugin`** — the CLI.
3. **`1833-metrics-for-local-queue`**, **`7066-custom-metric-labels`**, **`10852-inadmissible-workloads-observability`** — metrics conventions in practice.
4. **`2936-local-queue-defaulting`** — small, complete, great "anatomy of a simple KEP" example.
