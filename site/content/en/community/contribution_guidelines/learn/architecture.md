---
title: "Architecture: from job to running pods"
linkTitle: "Architecture"
weight: 10
description: >
  What Kueue is, how the repository is laid out, and the life of a workload end to end
type: docs
---

<!-- Verified against main@2b494fec3 (the v0.19 cut). Re-verify each minor release. -->

## What Kueue is (and is not)

Kueue is a **Kubernetes-native job queueing system**. It decides **when** a batch or ML workload should be admitted to run (based on quota, priority, and fair sharing) and **where** in terms of resource flavors (e.g., which GPU type, spot vs. on-demand) — but it deliberately does **not** replace the kube-scheduler, which still decides which node each pod lands on.

The mental model in one sentence:

> **Jobs arrive suspended → Kueue holds them in queues → when quota is available, Kueue admits the workload, injects node affinities for the chosen flavors, and unsuspends it → the kube-scheduler places the pods.**

Kueue's job is *admission control at the workload level*: quota reservation, queueing, preemption, fair sharing across teams, topology-aware placement hints, and multi-cluster dispatching (MultiKueue).

**What Kueue is not:**
- Not a pod scheduler (no bin-packing of pods to nodes — that's kube-scheduler).
- Not a workflow engine (no DAGs, no dependencies between jobs).
- Not a job controller (it doesn't create pods; the integrated job frameworks — batch/Job, JobSet, RayJob, etc. — do that themselves once unsuspended).

Kueue is a subproject of Kubernetes SIG Scheduling, developed under the **Batch Working Group (wg-batch)**.


## Prerequisites — what you need to know first

You don't need to know all of this on day one, but the learning curve flattens dramatically if you invest in these, in order:

### Must-have
| Topic | Why it matters in Kueue | Where to learn |
|---|---|---|
| **Go** (including generics, goroutines, mutexes) | The entire codebase is Go 1.26 | [go.dev/tour](https://go.dev/tour) |
| **Kubernetes basics** (Pods, Jobs, Deployments, labels, RBAC) | Kueue orchestrates these objects | [kubernetes.io/docs/tutorials](https://kubernetes.io/docs/tutorials/) |
| **Custom Resources & CRDs** | Every Kueue API object is a CRD | [K8s docs: CRDs](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) |
| **The controller/reconciler pattern** | All Kueue logic runs in reconcile loops | [Kubebuilder book](https://book.kubebuilder.io/) — read chapters 1–2 at minimum |
| **controller-runtime** | Kueue is built on it (managers, caches, watches, webhooks) | Same Kubebuilder book |

### Very useful
- **Admission webhooks** (mutating + validating) — Kueue sits in the admission path for every integrated job type.
- **`suspend` semantics of batch/Job** — the foundation of Kueue's admission model ([KEP-2926 in Kubernetes](https://kubernetes.io/docs/concepts/workloads/controllers/job/#suspending-a-job)).
- **Ginkgo/Gomega** — the integration and e2e test framework.
- **envtest** — runs a real kube-apiserver + etcd without nodes; used by all integration tests.
- **kind** — Kubernetes-in-Docker; used by all e2e tests.

### Read before writing code
- The user-facing concept docs at [kueue.sigs.k8s.io/docs/concepts](https://kueue.sigs.k8s.io/docs/concepts/) — sources live in `site/content/en/docs/concepts/`. Read at minimum: `workload.md`, `local_queue.md`, `cluster_queue.md`, `resource_flavor.md`, `admission.md`, `preemption.md`, `cohort.md`.
- The KEP template `keps/NNNN-template/` and one or two merged KEPs relevant to your area (see [§13](#13-kep-process)).


## The core concepts, in dependency order

Learn these in this order — each builds on the previous. API types live in `apis/kueue/v1beta2/`.

### 3.1 ResourceFlavor
A cluster-scoped label for a *kind* of capacity: "spot instances", "H100 GPUs", "ARM nodes". Carries node labels/taints/tolerations so that when a workload is admitted with this flavor, Kueue injects the right node affinity. Defined in `apis/kueue/v1beta2/resourceflavor_types.go`.

### 3.2 ClusterQueue
A cluster-scoped pool of quota. Defines, per resource (cpu, memory, `nvidia.com/gpu`, …) and per flavor: `nominalQuota`, `borrowingLimit`, `lendingLimit`. Also carries the queueing strategy, preemption policy, and namespace selector. This is the object admins care about most.

### 3.3 LocalQueue
A namespaced pointer to a ClusterQueue. Users submit jobs to a LocalQueue in their own namespace; multiple namespaces' LocalQueues can feed the same ClusterQueue (this is the tenancy boundary — remember it when debugging: the workload that preempted yours may live in a namespace you cannot see).

### 3.4 Workload
The heart of Kueue. For every job Kueue manages, it creates (or the user pre-creates) a `Workload` object that describes the job's pod sets and resource needs *in a job-framework-agnostic way*. **The scheduler never looks at Jobs, RayJobs, or JobSets — only at Workloads.** Everything in `pkg/scheduler` and `pkg/cache` operates on Workloads.

A Workload moves through **status conditions** (defined in `apis/kueue/v1beta2/workload_types.go`):

| Condition | Meaning |
|---|---|
| `QuotaReserved` | The scheduler found flavors and reserved quota in a ClusterQueue |
| `Admitted` | Quota reserved **and** all AdmissionChecks passed — the job may now run |
| `Evicted` | The workload was evicted (preemption, pods-ready timeout, deactivation, …) |
| `Requeued` | After eviction, the workload went back to the queue |
| `Finished` | The underlying job completed or failed — quota is released permanently |

### 3.5 Cohort
Groups ClusterQueues so they can **borrow** unused quota from each other. Since KEP-79, cohorts are hierarchical (a tree of Cohort objects, each possibly with its own quota). The hierarchy data structures live in `pkg/cache/hierarchy/`.

### 3.6 AdmissionCheck
A second, extensible admission gate (KEP-993 "two-phase admission"): after quota is reserved, external controllers (e.g., cluster-autoscaler `ProvisioningRequest` — KEP-1136, or MultiKueue) must also mark the workload ready before it's `Admitted`. Controllers for built-in checks live in `pkg/controller/admissionchecks/`.

### 3.7 Preemption and Fair Sharing
Within a ClusterQueue or cohort, higher-priority workloads can evict lower-priority ones (`pkg/scheduler/preemption/`). Fair Sharing (KEP-1714) uses dominant-resource-style share values to decide admission order and preemption targets across queues; Admission Fair Sharing (KEP-4136) factors in historical usage.

### 3.8 The big optional features
- **Topology-Aware Scheduling (TAS)** — KEP-2724. Admits workloads onto specific racks/blocks of nodes to minimize network hops; adds a `Topology` API and a large sub-cache (`pkg/cache/scheduler/tas_*.go`, `pkg/controller/tas/`).
- **MultiKueue** — KEP-693. A manager cluster dispatches workloads to worker clusters; each integration ships a MultiKueue adapter. See `pkg/controller/admissionchecks/multikueue/` and `pkg/controller/workloaddispatcher/`.
- **Elastic workloads / workload slices** — KEP-77. Jobs that can grow/shrink while running (`pkg/workloadslicing/`).
- **DRA integration** — KEP-2941. Quota for Dynamic Resource Allocation devices (`pkg/dra/`).

Every optional behavior hides behind a **feature gate** declared in `pkg/features/kube_features.go` — this file is the source of truth for all gates (~70 in v0.19), and doc tables are generated from it (see [§9](#9-codegen-and-verify)). Gates come and go between minor releases (v0.19 removed `LendingLimit` and `MultiKueueBatchJobWithManagedBy` as their features became unconditional) — always check the file, not your memory.


## Repository map

```
kueue/
├── apis/
│   ├── kueue/v1beta2/        # The Kueue API types (current version). v1beta1 kept for conversion.
│   ├── config/v1beta2/       # The controller-manager Configuration API (the config file format)
│   └── visibility/           # Visibility API (pending-workloads endpoints)
├── cmd/
│   ├── kueue/                # main() for the controller manager
│   ├── kueuectl/             # kubectl-kueue plugin (KEP-2076/487)
│   ├── importer/             # migrate pre-existing jobs into Kueue
│   ├── kueueviz/             # web UI backend/frontend
│   └── experimental/         # incubating tools; includes agent "skills" runbooks (see §14)
├── pkg/
│   ├── cache/
│   │   ├── queue/            # Queue Manager: LocalQueue/ClusterQueue heaps of *pending* workloads
│   │   ├── scheduler/        # Scheduler Cache: quota usage of *admitted* workloads + snapshots + TAS cache
│   │   └── hierarchy/        # Generic cohort-tree data structures shared by both
│   ├── scheduler/            # The scheduling cycle, flavor assignment, preemption
│   │   ├── flavorassigner/
│   │   └── preemption/       # incl. fairsharing/
│   ├── controller/
│   │   ├── core/             # Reconcilers for Workload, ClusterQueue, LocalQueue, Cohort, RF, AC, WPC
│   │   ├── jobframework/     # THE abstraction layer: GenericJob interface, generic reconciler, base webhook
│   │   ├── jobs/             # One package per integration: job, jobset, pod, ray*, kubeflow, mpijob, ...
│   │   ├── admissionchecks/  # ProvisioningRequest, MultiKueue controllers
│   │   ├── tas/              # Topology-aware scheduling controllers
│   │   └── workloaddispatcher/  # MultiKueue dispatching strategies
│   ├── webhooks/             # Validating/defaulting webhooks for Kueue's own CRDs
│   ├── workload/             # Helpers around the Workload type (conditions, usage, eviction, patching)
│   ├── features/             # kube_features.go — ALL feature gates, single source of truth
│   ├── metrics/              # Prometheus metrics definitions
│   ├── config/               # Loading/validation of the Configuration API
│   ├── podset, resources, util/, ...  # shared helpers
├── test/
│   ├── integration/          # envtest-based; singlecluster/{controller,scheduler,webhook,tas,kueuectl,...} + multikueue
│   ├── e2e/                  # kind-based; singlecluster, multikueue, tas, dra, upgrade, kueueviz, ...
│   ├── performance/          # scheduling performance tests
│   └── util/                 # shared test helpers (wrappers/builders you will use constantly)
├── keps/                     # Kueue Enhancement Proposals — the design history of every feature
├── site/                     # Hugo sources for kueue.sigs.k8s.io (docs contributions go here)
├── charts/                   # Helm chart
├── config/                   # kustomize manifests (CRDs, RBAC, webhooks) — mostly generated
├── hack/                     # dev scripts (dump_cache.sh, cherry_pick_pull.sh, debugpod, ...)
├── Makefile*                 # split into Makefile, -deps, -test, -verify fragments
├── CONTRIBUTING.md           # read it — covers `make verify` and the feature-gate doc workflow
└── AGENTS.md / CLAUDE.md     # instructions + runbooks for AI coding agents working on the repo
```

**Where do bugs usually live?** A rough router for issue triage:
- "Workload not admitted / admitted wrongly" → `pkg/scheduler/` + `pkg/cache/`
- "Wrong preemption victim / no preemption" → `pkg/scheduler/preemption/`
- "Job not suspended / workload not created / labels wrong" → `pkg/controller/jobframework/` or the specific `pkg/controller/jobs/<type>/`
- "Quota numbers wrong in status" → `pkg/controller/core/clusterqueue_controller.go` + `pkg/cache/scheduler/`
- "Rejected by webhook" → `pkg/webhooks/` (Kueue CRDs) or `pkg/controller/jobframework/base_webhook.go` (jobs)
- "Metric wrong/missing" → `pkg/metrics/`
- "MultiKueue" → `pkg/controller/admissionchecks/multikueue/`
- "TAS placement" → `pkg/cache/scheduler/tas_*.go`, `pkg/controller/tas/`


## Architecture deep dive: the life of a workload

This is the single most important thing to internalize. Trace it once with the code open.

```
 user                    webhook                jobframework             queue manager           scheduler                workload ctrl        job reconciler
  │                         │                       │                        │                       │                        │                    │
  │ kubectl create job      │                       │                        │                       │                        │                    │
  │ (queue-name label) ────▶│ defaults webhook:     │                        │                       │                        │                    │
  │                         │ suspend=true          │                        │                       │                        │                    │
  │                         │                       │ reconcile: create      │                       │                        │                    │
  │                         │                       │ Workload object ──────▶│ enqueue in            │                        │                    │
  │                         │                       │                        │ ClusterQueue heap     │                        │                    │
  │                         │                       │                        │ ──────────── heads ──▶│ scheduling cycle:      │                    │
  │                         │                       │                        │                       │ snapshot → nominate →  │                    │
  │                         │                       │                        │                       │ (preempt?) → admit     │                    │
  │                         │                       │                        │                       │ = QuotaReserved        │                    │
  │                         │                       │                        │                       │                        │ AdmissionChecks    │
  │                         │                       │                        │                       │                        │ all Ready ⇒        │
  │                         │                       │                        │                       │                        │ Admitted ─────────▶│ inject flavors'
  │                         │                       │                        │                       │                        │                    │ node affinity,
  │                         │                       │                        │                       │                        │                    │ unsuspend job
  │                         │                       │                        │                       │                        │                    │ → pods run
```

### Step by step, with file references

1. **Submission.** A user creates a `batch/Job` (or RayJob, JobSet, …) with the label `kueue.x-k8s.io/queue-name: <localqueue>` (constant `QueueLabel` in `pkg/controller/constants/constants.go`). A mutating webhook registered by the job integration (built on `pkg/controller/jobframework/base_webhook.go`) forces the job to start **suspended**. If `manageJobsWithoutQueueName` is set in the configuration (`apis/config/v1beta2/configuration_types.go`), even unlabeled jobs are managed.

2. **Workload creation.** The integration's reconciler — nearly all logic is shared in the 1800-line generic reconciler `pkg/controller/jobframework/reconciler.go` (`ReconcileGenericJob`) — creates a `Workload` object mirroring the job's pod sets (via the integration's `PodSets()` implementation). The Workload gets an `ownerReference` to the job and the label `kueue.x-k8s.io/job-uid`.

3. **Queueing.** The Workload controller (`pkg/controller/core/workload_controller.go`) and the queue **Manager** (`pkg/cache/queue/manager.go`) place the pending workload in its LocalQueue → ClusterQueue heap, ordered by the queueing strategy (priority + timestamps; `BestEffortFIFO` vs `StrictFIFO`). Inadmissible workloads are parked in `pkg/cache/queue/inadmissible_workloads.go` and retried on relevant cluster events.

4. **The scheduling cycle.** `pkg/scheduler/scheduler.go` — `schedule()` (line ~288) runs in a loop, one cycle at a time:
   1. `s.queues.Heads(ctx)` — take the head workload of every ClusterQueue (blocks while all queues are empty).
   2. `s.cache.Snapshot(ctx)` — a point-in-time copy of all quota usage from the scheduler cache (`pkg/cache/scheduler/snapshot.go`). The rest of the cycle works only on the snapshot, so the live cache can keep updating.
   3. `s.nominate(...)` — for each head, run the **flavor assigner** (`pkg/scheduler/flavorassigner/`): can this workload fit, in which flavors, with borrowing, or only by preempting? Produces `entries` (potentially admissible) and `inadmissibleEntries`.
   4. Build an ordered iterator over entries — priority order, or fair-sharing order when FairSharing is on (`fair_sharing_iterator.go`).
   5. `processEntry(...)` per entry — the admission pipeline: fits/overlap checks against the snapshot, issue preemptions if needed (`pkg/scheduler/preemption/`), pods-ready gating, then **admit**: set `QuotaReserved` + the `admission` block (flavors per pod set) on the Workload, and "assume" the usage in the cache. Only one borrowing workload per cohort per cycle.
   6. Requeue everything that didn't get admitted, with a reason recorded in the workload status (`requeueAndUpdate`).

5. **Second phase: AdmissionChecks.** If the ClusterQueue references AdmissionChecks, external controllers (ProvisioningRequest in `pkg/controller/admissionchecks/provisioning/`, MultiKueue, or third-party) now run. When every check reports `Ready`, the Workload controller flips `Admitted=True` (helpers in `pkg/workload/admissionchecks.go`). With no checks, `Admitted` follows `QuotaReserved` immediately.

6. **Unsuspend.** Back in the generic job reconciler: on `Admitted`, it calls the integration's `RunWithPodSetsInfo(...)`, which injects the node selectors/tolerations of the assigned ResourceFlavors (and TAS topology assignments, if enabled) into the job's pod templates and unsuspends it. The kube-scheduler now places the pods.

7. **Running → Finished.** The integration's `Finished()` reports completion; the reconciler sets `Finished` on the Workload; the cache releases quota; the scheduler can admit the next workloads.

8. **Evictions (the unhappy path).** At any point after admission a workload can be **evicted**: preempted by the scheduler, timed out waiting for pods to become ready (`waitForPodsReady` — **enabled by default since v0.19** with a 30-minute timeout and `blockAdmission: false`), deactivated (`.spec.active=false`), or stopped because its ClusterQueue was stopped. Eviction = suspend the job again, clear the admission, set `Evicted` and then `Requeued`, and put the workload back in the queue (logic spread across `pkg/scheduler/preemption/`, `pkg/workload/evict/`, and `workload_controller.go`).

**Key invariant to remember:** the *queue manager* holds **pending** workloads; the *scheduler cache* holds **admitted** workloads' usage. The scheduler consumes heads from the former and checks/updates the latter. Confusing these two caches is the most common newcomer mistake (both live under `pkg/cache/`, in `queue/` and `scheduler/` respectively).


## The major subsystems

### 6.1 The job framework (`pkg/controller/jobframework/`)
The abstraction that lets one generic reconciler serve ~16 job types. Each integration under `pkg/controller/jobs/<type>/` implements the `GenericJob` interface (`interface.go`):

```go
type GenericJob interface {
    Object() client.Object
    IsSuspended() bool
    Suspend()
    RunWithPodSetsInfo(ctx, c, podSetsInfo) error   // inject affinity + unsuspend
    RestorePodSetsInfo(ctx, podSetsInfo) bool       // undo on eviction
    Finished(ctx) (message string, success, finished bool)
    PodSets(ctx, c) ([]kueue.PodSet, error)         // job → workload translation
    IsActive() bool
    PodsReady(ctx, c) bool
    GVK() schema.GroupVersionKind
}
```

Optional capability interfaces (same file) add behavior: `JobWithPodLabelSelector`, interfaces for reclaimable pods, partial admission, MultiKueue adapters, etc. Integrations self-register via `pkg/controller/jobframework/integrationmanager.go`. The `pod` integration is special: it manages plain pods and pod *groups* (KEP-976) and underlies the Deployment/StatefulSet/LeaderWorkerSet integrations (one Workload per pod for serving workloads).

### 6.2 Core controllers (`pkg/controller/core/`)
One reconciler per Kueue CRD. They keep the two caches in sync with the API server and maintain status:
- `workload_controller.go` — the biggest one: condition transitions, admission-check aggregation, requeue backoff, orphan cleanup.
- `clusterqueue_controller.go` / `localqueue_controller.go` / `cohort_controller.go` — status (pending/admitted counts, flavor usage), activation state.
- `leader_aware_reconciler.go` — lets non-leader replicas serve reads for the visibility API.

### 6.3 Webhooks (`pkg/webhooks/`)
Validation/defaulting for Kueue's own CRDs (ClusterQueue, Cohort, ResourceFlavor, Workload). Job-type webhooks live with their integrations and share `jobframework/base_webhook.go`. Remember: **the webhook is the trust boundary** — reconcilers may still receive objects created before the webhook existed (see review rules in [§12](#12-review-culture)).

### 6.4 Scheduler internals worth knowing
- `flavorassigner` decides *fit mode* per pod set resource: `Fit` (no borrowing), `Fit with borrowing`, or `Preempt`. Flavor fungibility (trying the next flavor vs preempting in the current one) is configurable per ClusterQueue.
- `preemption` computes victim sets under policies (`priority`-based within CQ, `reclaimWithinCohort`, fair-sharing strategies) and issues evictions with the `Preempted` event/condition (message format documented in `cmd/experimental/skills/kueue-who-preempted/SKILL.md`).
- The whole cycle is **optimistic**: `assumeWorkload` (in `pkg/scheduler/scheduler.go`) writes the admitted usage into the live cache *before* the API-server patch; if the patch fails, the assumption is rolled back with `cache.DeleteWorkload`. The pattern mirrors kube-scheduler's assume/forget.

### 6.5 Configuration (`apis/config/v1beta2/`)
The controller manager is configured by a `Configuration` object mounted as a file (see `charts/kueue/values.yaml` or `config/components/manager/controller_manager_config.yaml`). Features like `waitForPodsReady`, `fairSharing`, `resources.transformations`, `integrations.frameworks`, and `manageJobsWithoutQueueName` live here — *not* on CRDs. When adding config, wire it: API type → validation (`pkg/config/`) → plumb to component options.
