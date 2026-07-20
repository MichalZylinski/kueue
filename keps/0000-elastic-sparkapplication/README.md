# KEP-0000: Elastic SparkApplication via Workload Slices

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Background](#background)
  - [How elastic scaling works today (batch/v1.Job and RayCluster)](#how-elastic-scaling-works-today-batchv1job-and-raycluster)
  - [How Spark elasticity works](#how-spark-elasticity-works)
  - [Key differences between RayCluster and SparkApplication](#key-differences-between-raycluster-and-sparkapplication)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
    - [Story 1 – scale-up within quota](#story-1--scale-up-within-quota)
    - [Story 2 – scale-up blocked by quota (graceful degradation)](#story-2--scale-up-blocked-by-quota-graceful-degradation)
    - [Story 3 – scale-down releases quota](#story-3--scale-down-releases-quota)
    - [Story 4 – preemption of a resized application](#story-4--preemption-of-a-resized-application)
    - [Story 5 – elastic SparkApplication in a MultiKueue configuration](#story-5--elastic-sparkapplication-in-a-multikueue-configuration)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
- [Design Details](#design-details)
  - [Phase 0 Prerequisites: Integration Hardening](#phase-0-prerequisites-integration-hardening)
    - [P1: Resource accounting fidelity](#p1-resource-accounting-fidelity)
    - [P2: restartPolicy exclusivity](#p2-restartpolicy-exclusivity)
    - [P3: batchScheduler exclusivity](#p3-batchscheduler-exclusivity)
    - [P4: Executor pod exclusion from the pod integration (verified — no fix needed)](#p4-executor-pod-exclusion-from-the-pod-integration-verified--no-fix-needed)
    - [P5: Operator deployment requirements](#p5-operator-deployment-requirements)
    - [P6: TimeToLiveSeconds interaction](#p6-timetoliveseconds-interaction)
  - [Enablement and Validation](#enablement-and-validation)
  - [Pod Scheduling Gates](#pod-scheduling-gates)
  - [Workload Construction and Initial Sizing](#workload-construction-and-initial-sizing)
  - [Observing Executor Demand](#observing-executor-demand)
  - [Reconciler Triggering](#reconciler-triggering)
  - [Workload Slice Lifecycle and Naming](#workload-slice-lifecycle-and-naming)
  - [Scale-Up Flow](#scale-up-flow)
  - [Scale-Down Flow](#scale-down-flow)
  - [Demand Decay on Pending Slices](#demand-decay-on-pending-slices)
  - [Backpressure and Churn Control](#backpressure-and-churn-control)
  - [Quota Semantics](#quota-semantics)
  - [MultiKueue Support](#multikueue-support)
  - [PodsReady Semantics](#podsready-semantics)
  - [Failure Modes](#failure-modes)
  - [Limitations and Incompatibilities](#limitations-and-incompatibilities)
- [Project Scoping and Phases](#project-scoping-and-phases)
- [Test Plan](#test-plan)
  - [Unit Tests](#unit-tests)
  - [Integration Tests](#integration-tests)
  - [E2E Tests](#e2e-tests)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

Extend Kueue's elastic workload support (KEP-77, `ElasticJobsViaWorkloadSlices`) to the
kubeflow/spark-operator `SparkApplication` integration, so that Spark applications using
[dynamic resource allocation](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation)
can scale their executor count up and down at runtime while Kueue tracks and enforces quota
via Workload Slices — without suspending or resubmitting the application.

The design deliberately differs from the RayCluster integration in one fundamental way:
Spark scaling events are **not visible in the `SparkApplication` spec**. The Spark driver
creates and deletes executor pods directly against the API server, and any mutation of the
`SparkApplication` spec causes the spark-operator to kill and resubmit the whole
application (`ApplicationStateInvalidating`). Therefore, instead of deriving PodSet counts
from the CR spec (as RayCluster does), Kueue derives the executor PodSet count from
**observed executor pod demand**, using the scheduling-gate mechanism as the admission
control point for every dynamically created executor pod. Persisting the observed demand
as an annotation on the object makes the same mechanism work in MultiKueue, where the
pods live on a worker cluster and the demand signal is synced back to the management
cluster that owns the Workload slices.

## Motivation

Kueue's `SparkApplication` integration (feature gate `SparkApplicationIntegration`,
alpha since v0.17) admits Spark applications with a static executor count. Dynamic
allocation — Spark's native elasticity mechanism, and the standard way to run
cost-efficient Spark on Kubernetes — is currently rejected by the Kueue webhook:

```go
// pkg/controller/jobs/sparkapplication/sparkapplication_webhook.go
"a kueue managed job can use dynamicAllocation only when the ElasticJobsViaWorkloadSlices
feature gate is on and the job is an elastic job"
```

This validation message already anticipates elastic support, but the elastic-job annotation
itself is rejected for `SparkApplication` because the GVK is not in
`supportedElasticJobGVKs` (`pkg/controller/jobframework/validation.go`). In practice,
dynamic allocation is unusable with Kueue today, which forces users to either
over-provision (`instances = maxExecutors`, stranding quota) or run Spark outside Kueue.

RayCluster demonstrated that Workload Slices can support in-place autoscaling of an
admitted job. Spark is the natural next framework, but its architecture requires an
adapted design rather than a copy of the Ray one.

### Goals

- Harden the existing SparkApplication integration so that Workload resource
  accounting matches the requests Spark actually places on pods, and so that
  operator-side retry/scheduling mechanisms cannot bypass Kueue
  ([Phase 0](#phase-0-prerequisites-integration-hardening)) — prerequisites that
  elastic scaling would otherwise amplify.
- Allow `SparkApplication` objects with `spec.dynamicAllocation.enabled: true` to be
  admitted and managed by Kueue when opted into elastic scaling
  (`kueue.x-k8s.io/elastic-job: "true"`).
- Track executor scale-up and scale-down in ClusterQueue quota using the existing
  Workload Slice machinery (new slice on scale-up, in-place Workload update on
  scale-down), with no application restart.
- Enforce quota on dynamically created executor pods via the
  `kueue.x-k8s.io/elastic-job` scheduling gate, so executors created beyond admitted
  quota never schedule until a replacement slice is admitted.
- Degrade gracefully when scale-up cannot be admitted: the application keeps running at
  its current size and the system converges when Spark's own pending-pod timeouts fire.
- Be MultiKueue-compatible by construction: the scaling-signal design (observed demand
  persisted as an object annotation) must work when the executor pods live on a worker
  cluster and the Workload slices are managed on the management cluster, reusing the
  existing MultiKueue elastic-slice propagation added by KEP-77.

### Non-Goals

- Scaling driven by mutation of `spec.executor.instances`. The spark-operator resubmits
  the application on any spec change, so there is no in-place semantics to preserve
  (see [Alternatives](#alternatives)).
- Vertical scaling of driver or executors.
- TAS `required`/`preferred` topology for elastic Spark jobs (inherits the KEP-77
  restriction to `unconstrained` mode).
- Changes to Apache Spark itself. (One upstream spark-operator change — adding
  `spec.managedBy` — is a dependency of the MultiKueue phase, not of the core design;
  see [MultiKueue Support](#multikueue-support).)
- Partial preemption (whole-application preemption only, as in KEP-77).

## Background

### How elastic scaling works today (batch/v1.Job and RayCluster)

The KEP-77 pipeline, as implemented:

1. **Opt-in**: `kueue.x-k8s.io/elastic-job: "true"` annotation + the
   `ElasticJobsViaWorkloadSlices` feature gate; the job's GVK must be listed in
   `supportedElasticJobGVKs` (`pkg/controller/jobframework/validation.go`). The
   annotation is immutable on update.
2. **Gating**: the integration webhook injects the `kueue.x-k8s.io/elastic-job`
   scheduling gate into all pod templates at creation
   (e.g. `raycluster_webhook.go` gates the head and every worker group template), so
   every pod of the job is born gated.
3. **Slice reconciliation**: `JobReconciler.ensureOneWorkload` calls
   `workloadslicing.EnsureWorkloadSlices` with the job's current PodSets:
   - counts equal → no-op;
   - scale-down (or slice not yet admitted) → in-place update of the Workload's
     `podSets[].count`;
   - scale-up on an admitted Workload → a new Workload slice is created, named with an
     extra generation-derived suffix (`newWorkloadName` /
     `ElasticWorkloadNameProvider`), annotated with
     `kueue.x-k8s.io/workload-slice-replacement-for` pointing at the old slice.
4. **Scheduling**: the scheduler admits the new slice accounting only for the delta,
   enforces sticky resource flavors, and marks the old slice
   `Finished/WorkloadSliceReplaced` in the same cycle (quasi-atomic aggregation).
5. **Ungating**: the `ElasticJobUngater` controller
   (`pkg/controller/elasticjobs/elastic_job_ungater.go`) watches admitted elastic
   Workloads and pods (indexed by the stable `kueue.x-k8s.io/workload-slice-name`
   annotation), and removes the scheduling gate from pods belonging to admitted slices.

For RayCluster specifically, the scaling **signal** is the KubeRay autoscaler mutating
`spec.workerGroupSpecs[].replicas` in the RayCluster CR. This bumps
`metadata.generation`, retriggers the job reconciler, and `BuildPodSets`/`UpdatePodSets`
(`pkg/controller/jobs/raycluster/common.go`) recompute the PodSet counts from the spec.
For RayJob/RayService, whose child RayCluster is a separate object, the observed replica
counts are copied onto the top-level object as the
`kueue.x-k8s.io/raycluster-podset-replica-sizes` + `kueue.x-k8s.io/raycluster-generation`
annotations (via the `JobWithCustomAnnotations` interface), which also feed the slice
naming through `GetWorkloadNameExtraPart`.

### How Spark elasticity works

A `SparkApplication` in cluster mode has exactly two pod roles: one driver pod (created
by the operator via `spark-submit`) and N executor pods. Crucially, **executor pods are
created and deleted by the Spark driver itself**, through Spark's Kubernetes scheduler
backend — not by the operator. With dynamic allocation enabled
(`spec.dynamicAllocation`):

- The initial executor target is `max(instances, initialExecutors, minExecutors)`
  (per the spark-operator API: "If .spec.executor.instances is also set, the initial
  number of executors is set to the bigger of that and this option", and Spark's
  `spark.dynamicAllocation.initialExecutors` semantics).
- The driver ramps the target up exponentially when tasks are backlogged
  (`spark.dynamicAllocation.schedulerBacklogTimeout`, default 1s) and requests executor
  pods in batches (`spark.kubernetes.allocation.batch.size`, default 5, one batch per
  `spark.kubernetes.allocation.batch.delay`, default 1s); it does not submit a new batch
  while previously created pods are still Pending.
- The driver deletes idle executors after
  `spark.dynamicAllocation.executorIdleTimeout` (default 60s; shuffle-tracking variants
  apply when `shuffleTrackingEnabled`), never going below `minExecutors`.
- Executor pods that stay Pending longer than
  `spark.kubernetes.allocation.executor.timeout` (default 600s) are deleted by the
  driver.
- **None of this is reflected in the `SparkApplication` spec.** `spec.executor.instances`
  and `metadata.generation` are unchanged throughout.

Additionally, the spark-operator's state machine moves an application whose spec was
edited into `ApplicationStateInvalidating`: the running application is killed and
resubmitted. There is no in-place spec-driven scaling to integrate with.

Executor pods carry stable labels (`sparkoperator.k8s.io/app-name`,
`spark-role: executor`), and pod customization from `spec.executor` (labels,
annotations, tolerations, template) is applied to driver/executor pods via the
operator's pod template mechanism and mutating pod webhook.

### Key differences between RayCluster and SparkApplication

| Aspect | RayCluster | SparkApplication (dynamic allocation) |
|---|---|---|
| Scaling actor | KubeRay autoscaler sidecar mutates the CR spec | Spark driver creates/deletes executor pods directly |
| Scaling signal visible to Kueue | `spec.workerGroupSpecs[].replicas` change → generation bump | **Only pod churn**; spec and generation unchanged |
| Spec mutability while running | Replicas mutable in place | Any spec change → operator kills & resubmits the app (`INVALIDATING`) |
| Pod creation path | KubeRay reads the current CR spec for every pod | Driver uses the executor pod template fixed at submission; operator webhook mutates pods at creation |
| Scale bounds | `minReplicas`/`maxReplicas` per worker group | `dynamicAllocation.minExecutors`/`maxExecutors` |
| Scale-down | Autoscaler decrements replicas, KubeRay deletes pods | Driver deletes idle executor pods |
| Worker/executor groups | Multiple named worker groups (multiple PodSets) | Exactly one executor PodSet (+ driver) |
| Ramp-up pattern | Autoscaler-paced, relatively coarse | Exponential target growth, batched pod creation, potentially bursty |

The consequence: **for Spark, the pod is the unit of truth.** Kueue must derive desired
executor count from observed executor pods and use the scheduling gate — which it
already injects into every elastic pod — as the enforcement point. This is a natural
fit: the gate makes each dynamically created executor pod an implicit admission request.

## Proposal

Add `SparkApplication` to the set of frameworks supporting
`ElasticJobsViaWorkloadSlices`, with a pod-demand-driven PodSet computation:

- The Kueue webhook injects the `kueue.x-k8s.io/elastic-job` scheduling gate into the
  driver and executor pod templates of elastic SparkApplications, so all pods —
  including executors the driver creates mid-run — are born gated.
- The `SparkApplication` job adapter computes the executor PodSet count as the observed
  number of non-terminal executor pods, clamped to
  `[initial target, maxExecutors]` bounds (details below).
- The generic elastic pipeline (EnsureWorkloadSlices, scheduler slice aggregation,
  ElasticJobUngater) is reused unchanged: scale-up produces a replacement slice whose
  admission ungates the new executor pods; scale-down updates the admitted Workload in
  place and releases quota.
- Observed counts are persisted on the object via a
  `kueue.x-k8s.io/sparkapplication-podset-replica-sizes` annotation (mirroring the
  RayCluster pattern), which also provides the extra entropy for slice naming since the
  object's generation never changes.

### User Stories

#### Story 1 – scale-up within quota

1. A user creates an elastic `SparkApplication` with
   `dynamicAllocation: {enabled: true, initialExecutors: 2, minExecutors: 1, maxExecutors: 10}`.
2. Kueue creates a Workload with PodSets `driver: 1`, `executor: 2` and admits it. The
   driver and the first two executor pods are ungated and run.
3. Task backlog grows; the driver creates 3 more executor pods. They are created gated.
4. Kueue observes 5 executor pods, creates a replacement slice with `executor: 5`,
   admits it (delta of 3 counted against the ClusterQueue), marks the old slice
   `Finished/WorkloadSliceReplaced`, and the ungater releases the 3 new pods.
5. The application runs with 5 executors; the ClusterQueue reflects driver + 5 executors.

#### Story 2 – scale-up blocked by quota (graceful degradation)

1. Same as Story 1, but the ClusterQueue has no headroom for additional executors.
2. The replacement slice stays Pending; the 3 new executor pods remain gated and never
   schedule. The application continues running with 2 executors — no disruption.
3. Either capacity frees up later (slice admits, pods ungate), or Spark's pending-pod
   timeout (`spark.kubernetes.allocation.executor.timeout`, default 600s) deletes the
   gated pods, observed demand drops back to 2, and Kueue updates the pending slice's
   counts back down (no admission needed; the pending slice converges or is finished as
   out-of-sync).

#### Story 3 – scale-down releases quota

1. The application from Story 1 finishes a stage; executors idle.
2. After `executorIdleTimeout`, the driver deletes idle executor pods (down to
   `minExecutors`).
3. Kueue observes the reduced pod count and updates the admitted Workload's executor
   PodSet count in place. The freed quota immediately becomes available to other
   workloads in the ClusterQueue.

#### Story 4 – preemption of a resized application

If Kueue needs to preempt the application, it preempts the whole current slice as a
single unit (per KEP-77's whole-job preemption semantics): the job is stopped via
`spec.suspend = true`, the operator kills the application, and it requeues at its
**initial** size when readmitted (dynamic allocation restarts from the initial target).

#### Story 5 – elastic SparkApplication in a MultiKueue configuration

1. A user creates an elastic `SparkApplication` on the management cluster with
   `spec.managedBy: kueue.x-k8s.io/multikueue` and a queue targeting a MultiKueue
   admission check.
2. Kueue admits the initial slice, nominates a worker cluster, and the MultiKueue
   adapter creates the SparkApplication (with its gated templates and elastic
   annotation intact) plus the corresponding Workload on that worker. The worker's
   Kueue admits it, unsuspends the application, and ungates the startup pods.
3. The driver on the worker scales executors up: new gated pods appear on the worker,
   the observed demand is synced back to the management cluster, a replacement slice is
   created and admitted on the manager, propagated to the worker, and the worker's
   ungater releases the new pods. Both clusters' quota views reflect the new size.
4. Scale-down on the worker flows the same way in reverse, shrinking the local and
   remote Workloads in place and releasing quota on both sides.
5. Throughout, the remote SparkApplication spec is never modified, so the operator
   never restarts the application.

### Notes/Constraints/Caveats

- **Operator pod-mutation dependency.** Kueue relies on the executor pod template
  (fixed at spark-submit time) and/or the spark-operator mutating pod webhook to
  propagate the scheduling gate and Kueue annotations onto dynamically created executor
  pods. The gate and the stable `kueue.x-k8s.io/workload-slice-name` annotation are
  injected before submission (into `spec.driver.template` / `spec.executor.template` and
  `spec.*.annotations`), so template-file propagation is sufficient; no mid-run template
  refresh is required. This mirrors the batch/v1.Job behavior, where scaled-up pods also
  carry submission-time annotations and are matched through the slice-name index.
- **Executor identity.** Kueue counts executor pods by labels
  (`sparkoperator.k8s.io/app-name=<name>`, `spark-role=executor`) in the application's
  namespace, consistent with `PodLabelSelector()` in the existing integration.
- **The driver PodSet count is always 1** and never scales.

## Design Details

### Phase 0 Prerequisites: Integration Hardening

A code audit of the existing (non-elastic) SparkApplication integration surfaced
correctness gaps that predate this KEP but become prerequisites for it: **elastic
scaling multiplies any per-executor accounting error**, and lifecycle conflicts that
are tolerable for a fixed-size app become quota-escape vectors for one that resizes
itself. These items are independent bug fixes / validations, sequenced before the
elastic work (see [Project Scoping and Phases](#project-scoping-and-phases)); none of
them is gated behind a feature gate.

#### P1: Resource accounting fidelity

The Workload's PodSet templates must carry the **same resource requests the real pods
will have**, or every downstream consumer — ClusterQueue quota, preemption math, TAS
capacity, fair sharing — operates on wrong numbers. Today
(`sparkapplication_podset.go`) three derivations diverge from what Spark actually does:

| Field | Spark / operator behavior | Kueue today | Gap |
|---|---|---|---|
| `cores` | Executor/driver pod CPU request = `coreRequest` if set, else `cores` (`spark.executor.cores`, default 1) | Only `coreRequest` mapped; `cores` ignored | Pod requests N CPUs, Workload records none |
| `memoryOverhead` / `memoryOverheadFactor` | Pod memory request = `memory` + overhead, where overhead = explicit value, else `max(factor × memory, 384Mi)`; factor defaults 0.1 (JVM) / 0.4 (non-JVM) | Only `memory` mapped | Under-counts ≥ 10% of memory per pod |
| `executor.instances` unset | Spark's Kubernetes default launches 2 executors | `ptr.Deref(instances, 0)` → PodSet count 0 (valid: Workload `count` has `Minimum=0`) | 2 unaccounted executors |

**Fix**: extend the pod-template builders to mirror Spark's derivation — CPU request
falls back `coreRequest → cores → 1`; memory request = `memory` + computed overhead
(honoring `memoryOverhead`, `memoryOverheadFactor`, and the 384Mi floor); default
executor count mirrors Spark's default of 2 when `instances` is unset. PySpark
off-heap memory (`spark.executor.pyspark.memory`) is only expressible via
`sparkConf`; deriving from arbitrary `sparkConf` keys is explicitly out of scope —
documented, with the recommendation to set `memoryOverhead` explicitly for PySpark.

**Alternatives considered**:
- *Require explicit `coreRequest`/`memoryOverhead` via webhook validation* — simple and
  exact, but breaks most existing manifests and pushes Spark's defaulting rules onto
  every user; rejected as primary (kept as a fallback validation if derivation drifts
  across Spark versions).
- *Observe the real pod requests after creation and patch the Workload* — perfectly
  accurate but too late: admission needs the numbers before any pod exists; rejected.

#### P2: restartPolicy exclusivity

`Finished()` reports the workload finished on `FAILED`, but with
`spec.restartPolicy.type: Always` or `OnFailure` the operator resubmits the
application **after** Kueue has released quota — the rerun executes unmanaged
(non-elastic pods are not gated, so nothing blocks them). Two managers cannot both own
retry.

**Fix (Alpha)**: webhook rejects `restartPolicy.type != Never` for Kueue-managed
SparkApplications on create. Retries are Kueue's job (requeueing with backoff,
`backoffLimitCount`), keeping quota accounting correct across attempts.

**Alternative (re-evaluate at Beta)**: treat operator retries as legitimate by mapping
the lifecycle — `Finished()` returns false while retry budget remains
(`FAILED` + `status.executionAttempts` vs `onFailureRetries`, and the `PENDING_RERUN`
state treated as active). More faithful to operator semantics, but duplicates the
operator's retry bookkeeping inside Kueue and holds quota through operator backoff
windows; deferred until there is user demand.

#### P3: batchScheduler exclusivity

`spec.batchScheduler` (Volcano / YuniKorn) delegates gang scheduling of the app's pods
to another scheduler, which conflicts with Kueue's admission control (double gating,
double quota management). The webhook rejects a non-empty `batchScheduler` on
Kueue-managed applications — same rationale as the existing `mode: cluster`-only
validation.

#### P4: Executor pod exclusion from the pod integration (verified — no fix needed)

This item started from a suspected gap: Spark's ownership shape is unusual among
integrations — the driver pod is owned by the SparkApplication, but **executor pods
are owned by the driver pod**, a two-hop chain — and the single-hop helper
`IsOwnerManagedByKueueForObject` cannot see past it. The concern was that, with the
plain-pod integration active (or `manageJobsWithoutQueueName` set), executor pods
might be independently suspended, gated, and double-counted.

Investigation traced the actual gating path the pod webhook uses —
`jobframework.FindAncestorJobManagedByKueue` (`pkg/controller/jobframework/reconciler.go`),
not `IsOwnerManagedByKueueForObject` — and found it already walks multi-hop ownership
chains correctly: its per-hop "known owner" check (`isKnownOwner`) matches against
**every compiled-in integration**, not just the ones enabled in this cluster's
configuration, so the intermediate driver-`Pod` hop never stops the walk even when
the plain-pod integration is disabled. Only the *terminal* hop's "is this ancestor
Kueue-managed" decision consults the enabled-integrations set. The chain
`executor Pod → driver Pod → SparkApplication` therefore resolves to the
SparkApplication correctly, exactly like the existing `Pod → ReplicaSet → Deployment`
case the framework already supports.

This was confirmed empirically, not just by reading the code: see
`TestFindAncestorJobManagedByKueueForExecutorPod` in
`pkg/controller/jobs/sparkapplication/sparkapplication_podintegration_test.go`, which
constructs the two-hop chain and asserts `FindAncestorJobManagedByKueue` resolves the
executor pod's ancestor to its SparkApplication (or to nil when the application has no
queue-name, mirroring the framework's existing unmanaged-owner semantics).

**Conclusion**: no product code change. `IsOwnerManagedByKueueForObject` remains
single-hop, but its only consumer is a diagnostic warning
(`warningForPodManagedLabel` in `pod_webhook.go`) shown when a user manually applies
`kueue.x-k8s.io/managed=true` to a pod already owned by a Kueue-managed job — for a
two-hop-owned executor pod, that warning simply doesn't fire (a missed courtesy
message, not a functional or accounting bug). Left as a documented, low-priority
follow-up rather than bundled into this KEP's scope.

#### P5: Operator deployment requirements

The integration silently depends on the spark-operator (≥ v2.4.0) watching the
application's namespace and, for pod customization, on its mutating webhook or
pod-template propagation being active. If misconfigured, Kueue admits a Workload whose
application never starts and the quota is held indefinitely. Mitigations:

- Documentation pairing the integration with `waitForPodsReady` so stuck admissions
  are evicted and requeued rather than holding quota forever (the elastic gating path
  makes this more visible, since gated pods look "stuck" by design — `PodsReady`
  semantics below account for that).
- The existing `PodsReady` implementation already detects executors that never come up.

#### P6: TimeToLiveSeconds interaction

`spec.timeToLiveSeconds` makes the operator delete the SparkApplication after
termination; Workload garbage collection then follows owner deletion. This is correct
but interacts with `objectRetentionPolicies` (the Workload history disappears when the
TTL fires first). Documentation note only; no code change.

### Enablement and Validation

Both existing feature gates are required: `SparkApplicationIntegration` (the
integration itself) and `ElasticJobsViaWorkloadSlices` (the elastic machinery). No new
feature gate is introduced.

Changes:

1. Add `sparkoperator.k8s.io/v1beta2, Kind=SparkApplication` to
   `supportedElasticJobGVKs` in `pkg/controller/jobframework/validation.go`.
2. Webhook validation (`sparkapplication_webhook.go`), for elastic SparkApplications
   (`workloadslicing.Enabled(obj)`):
   - `spec.dynamicAllocation.enabled` **must** be true. An elastic Spark application
     without dynamic allocation has no scaling mechanism (spec edits restart the app),
     so the annotation would be meaningless; requiring it keeps the elastic/non-elastic
     behavioral contract crisp. (The existing inverse validation — dynamic allocation
     requires elastic — is kept.)
   - `spec.dynamicAllocation.maxExecutors` **must** be set (> 0, and ≥ `minExecutors`
     when both set). This bounds the workload's growth and lets administrators reason
     about worst-case quota consumption; it also caps the demand-observation loop.
   - `spec.mode` must be `cluster` (existing validation).
   - The `kueue.x-k8s.io/elastic-job` scheduling gate must be present in both driver and
     executor pod templates (mirroring `validateElasticJob` in
     `raycluster_webhook.go`); the defaulter injects it, validation enforces it.
   - Reuse the generic elastic validations: annotation immutability, TAS
     `required`/`preferred` rejection (only `unconstrained` topology is supported for
     elastic jobs, per KEP-77).

### Pod Scheduling Gates

The webhook defaulter (`Default`), when the object is elastic, injects
`kueue.x-k8s.io/elastic-job` into:

- `spec.driver.template.spec.schedulingGates`
- `spec.executor.template.spec.schedulingGates`

creating the minimal `template` structure when nil (the same construction
`RunWithPodSetsInfo` already performs with `emptyDriverPodTemplateSpec` /
`emptyExecutorPodTemplateSpec`). Because the operator renders these templates into the
`spark.kubernetes.{driver,executor}.podTemplateFile` passed to spark-submit, **every**
executor pod the driver ever creates — including mid-run scale-ups — is born gated.
This is the property that makes quota enforcement work without any cooperation from
the driver.

Ungating is performed exclusively by the existing `ElasticJobUngater`; the Spark
adapter adds nothing here.

### Workload Construction and Initial Sizing

`numInitialExecutors()` in `sparkapplication_podset.go` is extended: when dynamic
allocation is enabled, the executor PodSet count for the initial Workload is

```
initialTarget = max(spec.executor.instances, dynamicAllocation.initialExecutors, dynamicAllocation.minExecutors)
```

matching the number of executor pods Spark will actually create at startup. Admitting
fewer would leave startup executors gated; admitting more would strand quota.

### Observing Executor Demand

`PodSets(ctx, client)` (which already receives a client, currently unused by Spark) is
extended for elastic applications:

```
# local mode (pods run in this cluster — single cluster, or the MultiKueue worker):
observed  = count of executor pods for this app with deletionTimestamp == nil
            and phase not in {Succeeded, Failed}
desired   = min(max(observed, floor), maxExecutors)

# delegated mode (MultiKueue management cluster; no local executor pods):
mirrored  = executor count in the kueue.x-k8s.io/sparkapplication-podset-replica-sizes
            annotation (synced from the worker cluster by the MultiKueue adapter)
desired   = min(max(mirrored, floor), maxExecutors)

floor     = initialTarget            before the driver is running
          = minExecutors             once the driver is running
```

`desired` has no artificial lower bound of 1: `minExecutors` (and thus `floor`) is
legitimately `0` for an application that scales fully to idle, and clamping to at
least 1 would waste quota exactly when elasticity is doing its job.

The two modes are distinguished by whether the job is delegated to MultiKueue
(`spec.managedBy == kueue.x-k8s.io/multikueue`). The annotation is deliberately both
the *output* of local observation and the *input* of delegated computation: it is the
single, durable carrier of the demand signal, which is what makes the design
MultiKueue-compatible without a second mechanism
(see [MultiKueue Support](#multikueue-support)). It is never an input in local mode,
so a stale annotation can never hold the count up and block scale-down.

Rationale:

- Before the driver runs (`status.appState` not yet `RUNNING`), no executor pods exist;
  the count must be the initial target so the first Workload is sized correctly.
- Once running, `observed` includes gated (Pending) pods, so newly created executors
  raise desired demand; deleted idle executors lower it. The `minExecutors` floor
  prevents quota flapping below the level Spark will always re-request.
- `maxExecutors` caps the count: pods created beyond the cap (misconfigured or rogue
  driver) are simply never accounted for nor ungated; Spark's pending-pod timeout
  eventually deletes them. This is a safety property, not an expected path.

The result feeds directly into the untouched `EnsureWorkloadSlices` logic: only
`podSets[].count` differences are produced, which is exactly the class of change the
slice path supports (any other diff falls back to the incompatible-workload path).

### Reconciler Triggering

The generic reconciler for SparkApplication currently only watches the CR — sufficient
for RayCluster (spec changes) but blind to Spark's pod churn. Using the existing
`ReconcilerSetup` hook (the same mechanism RayJob uses to watch its child RayCluster):

```go
var NewReconciler = jobframework.NewGenericReconcilerFactory(NewJob,
    func(b *builder.Builder, c client.Client) *builder.Builder {
        return b.Watches(&corev1.Pod{}, &executorPodHandler{})
    })
```

`executorPodHandler` maps pod events to the owning SparkApplication via the
`sparkoperator.k8s.io/app-name` label, filtered to `spark-role=executor`, and enqueues
with `constants.UpdatesBatchPeriod` batching (as the ElasticJobUngater's pod handler
does) so one allocation batch produces one reconcile. Pods of non-elastic applications
are not filtered at the handler — the label match alone is cheap, and the reconciler's
own `workloadslicing.Enabled` check absorbs the no-op cost for them; this avoids
needing an extra elastic-specific label or annotation on every executor pod just for
handler-side filtering.

Create and Delete events always enqueue. Update events enqueue only when the pod's
phase changes or its `deletionTimestamp` transitions from unset to set — both directly
affect `observedExecutorCount`'s output (e.g. Running → Succeeded, or the start of
graceful termination) — since only pod *existence in a non-terminal, non-deleting
state* matters for demand, other updates (resource version churn, condition flaps) are
not worth a reconcile. The controller already caches pods (the ungater and TAS watch
them), so this adds event-handler work but no new informer.

### Workload Slice Lifecycle and Naming

Slice naming for elastic jobs incorporates an "extra part" that must change on every
resize (`newWorkloadName` in `pkg/controller/jobframework/reconciler.go`). The default —
`metadata.generation` — never changes for Spark, so `SparkApplication` implements both
optional interfaces, exactly following the RayCluster/RayJob precedent:

- **`JobWithCustomAnnotations`**: `GetCustomAnnotations` returns

  ```go
  const SparkApplicationPodSetReplicaSizesAnnotation = "kueue.x-k8s.io/sparkapplication-podset-replica-sizes"
  ```

  containing the JSON-serialized `[]jobframework.PodSetReplicaSize` for the current
  observed PodSets (same schema and serialization helpers as
  `RayClusterPodsetReplicaSizesAnnotation`). The reconciler patches it onto the object
  before `EnsureWorkloadSlices` runs, making the observed size durable, debuggable
  (`kubectl get sparkapp -o yaml` shows what Kueue thinks the app's size is), and
  restart-safe.

- **`ElasticWorkloadNameProvider`**: `GetWorkloadNameExtraPart` returns
  `<generation>_<shortHash(replica-sizes-annotation)>`, so each distinct observed size
  yields a distinct, deterministic slice name — the analogue of RayCluster's
  `<generation>_<rayClusterGeneration>`.

Everything downstream — `prepareWorkloadSlice` (replacement-for and slice-name
annotations), `normalizeActiveSlices` (at most one admitted + one pending replacement),
scheduler-side slice aggregation and `Finished/WorkloadSliceReplaced`, the sticky-flavor
constraint, and garbage-collection semantics — is inherited from KEP-77 unchanged.

### Scale-Up Flow

```
driver ramps target ──▶ creates executor pods (born gated, Pending)
        │
        ▼ (pod create events, batched)
JobReconciler: PodSets() observes N' > N
        │
        ├─ patches kueue.x-k8s.io/sparkapplication-podset-replica-sizes
        ▼
EnsureWorkloadSlices: admitted slice exists & counts grew ─▶ create new slice
        (executor: N', replacement-for: old slice, sticky flavor)
        ▼
scheduler: admits delta (N' − N), finishes old slice (WorkloadSliceReplaced)
        ▼
ElasticJobUngater: ungates gated executor pods of the slice chain
        ▼
kube-scheduler places pods; driver registers executors
```

If several ramp waves land between reconciles, intermediate sizes are skipped — the new
slice reflects the latest observed count. If a pending replacement slice already exists
when demand grows again, `EnsureWorkloadSlices` updates that pending slice's counts in
place (no admission has happened yet), avoiding slice proliferation.

### Scale-Down Flow

The driver deletes idle executor pods. Pod delete events trigger reconciliation;
`PodSets()` observes the lower count; `EnsureWorkloadSlices` takes the scale-down
branch and updates the **admitted** Workload's `podSets[].count` in place. Quota is
released without creating any new slice, the job keeps running, and no pods are
touched (they are already gone). This is identical in shape to RayCluster scale-down.

A transient race — driver deleting an executor at the same moment it creates others —
resolves via level-based reconciliation: whatever count the next reconcile observes
becomes the target.

### Demand Decay on Pending Slices

When a scale-up cannot be admitted, gated pods eventually get deleted by the driver's
pending-executor timeout, and observed demand shrinks. Two cases:

- Demand shrinks back exactly to the admitted slice's count: `EnsureWorkloadSlices`
  selects the admitted slice (counts equal); the stale pending replacement is finished
  as out-of-sync by `normalizeActiveSlices`.
- Demand shrinks to a value between the admitted and requested counts: the pending
  slice (never admitted) has its counts updated in place, shrinking the request.

No new mechanism is needed; both behaviors already exist in `EnsureWorkloadSlices`.

### Backpressure and Churn Control

A naive integration could generate a Workload slice per allocation batch. Three
mechanisms bound churn:

1. **Spark-side backpressure**: the Kubernetes allocator does not create new executor
   pods while previously requested pods are Pending. Since gated pods are Pending, an
   unadmitted scale-up **pauses further pod creation** — the driver's ramp stalls at the
   first blocked batch instead of stampeding to `maxExecutors`. This is an emergent and
   desirable property of gating.
2. **Event batching**: the pod handler enqueues with `UpdatesBatchPeriod`, so one
   allocation batch coalesces into a single reconcile and (at most) one slice
   transition.
3. **Pending-slice coalescing**: successive demand increases mutate the single pending
   replacement slice rather than stacking slices (invariant: ≤ 2 active slices per job,
   enforced by `normalizeActiveSlices`).

Documentation will recommend tuning
`spark.dynamicAllocation.sustainedSchedulerBacklogTimeout`,
`executorAllocationRatio`, and `spark.kubernetes.allocation.batch.size` for
queue-friendly ramp behavior. If real-world churn still proves problematic, a
Kueue-side debounce (minimum interval between replacement slices per job) can be added
behind configuration in Beta; it is deliberately out of scope for Alpha.

### Quota Semantics

Identical to KEP-77: the replacement slice's scheduling assignment accounts only for
the executor delta; the old slice's usage is released on `WorkloadSliceReplaced`;
flavors are sticky to the originally admitted flavor, so a scale-up whose delta does
not fit the original flavor stays Pending rather than switching flavors. Whole-job
preemption treats the current slice as the single preemption unit.

### MultiKueue Support

The design is MultiKueue-compatible by construction; support is delivered as a
dedicated phase (see [Graduation Criteria](#graduation-criteria)) because two
prerequisites exist that are independent of the elastic mechanics.

#### Prerequisites

1. **A SparkApplication MultiKueue adapter** (`sparkapplication_multikueue_adapter.go`)
   does not exist today. It implements the standard `MultiKueueAdapter` contract
   (create the remote object, sync status remote→local, delete remote objects,
   `IsJobManagedByKueue`), plus `MultiKueueWatcher` for remote SparkApplication events.
   This is a prerequisite for *any* MultiKueue Spark support, elastic or not.
2. **`spec.managedBy` in the spark-operator API.** `SparkApplicationSpec` has no
   `managedBy` field, so the management-cluster operator cannot be told to skip a
   delegated application. The target design requires an upstream spark-operator change
   adding `spec.managedBy` (precedent: batch/v1 Job, JobSet, KubeRay, and Kubeflow
   Trainer all added it for MultiKueue), honored by the operator by skipping
   applications whose value is not its own default. Until it lands, an interim pattern
   is possible — keep the management-side application permanently suspended
   (`spec.suspend: true`, which the operator already respects) and sync status on
   completion, as early MultiKueue batch/v1.Job support did — but the KEP targets the
   `managedBy` protocol as the supported configuration.

#### What KEP-77 already provides

The MultiKueue workload reconciler (`pkg/controller/admissionchecks/multikueue/workload.go`)
is already slice-aware, and all of it is reused unchanged:

- Local workload slices are mirrored to the assigned worker cluster; a replacement
  slice created on the manager results in a corresponding remote slice on the worker.
- Slices finished with `WorkloadSliceReplaced`, and pending scale-up replacements
  without quota reservation, are exempted from the remote-object garbage collection
  that would otherwise tear down the running remote job.
- **Scale-down propagates in place**: when the local slice's counts shrink, the remote
  Workload's spec is updated rather than recreated.
- **Sticky cluster**: replacement slices inherit the origin slice's cluster assignment,
  the multi-cluster analogue of the sticky-flavor rule — a scale-up is admitted on the
  cluster already running the application or not at all.

#### The inverted signal path

The one genuinely new consideration: for batch/v1.Job in MultiKueue, scaling originates
on the **management** cluster (the user edits the Job spec there). For Spark, scaling
originates on the **worker** cluster, where the driver creates and deletes executor
pods. The demand signal must therefore flow worker → manager before slices — which are
managed exclusively by the manager — can react:

```
worker: driver creates gated executor pods
   │
   ▼
worker Kueue: observes demand, patches
   kueue.x-k8s.io/sparkapplication-podset-replica-sizes on the REMOTE SparkApplication
   │
   ▼ (adapter SyncJob, remote→local annotation + status sync)
manager: annotation lands on the LOCAL SparkApplication
   │
   ▼
manager JobReconciler: PodSets() in delegated mode reads the annotation
   → EnsureWorkloadSlices → new local replacement slice
   │
   ▼ (MultiKueue workload reconciler, existing KEP-77 propagation)
worker: remote replacement slice created → worker Kueue admits it
   → old remote slice replaced → worker ElasticJobUngater ungates the executor pods
```

Concretely, this requires two contained changes beyond the adapter itself:

- **Worker-side demand publication**: the job reconciler's custom-annotations patching
  (`JobWithCustomAnnotations`) currently runs only on the slice-owning path. For
  elastic jobs processed via the **prebuilt-workload** path (which is how the worker
  processes MultiKueue-delegated jobs), the reconciler additionally patches the
  observed-demand annotation onto the job object — observation only, no slice creation.
  The worker never creates slices for delegated jobs; the manager remains the single
  writer of slices, and the worker enforces admission via prebuilt workloads and
  scheduling gates.
- **Adapter annotation sync**: `SyncJob` copies the replica-sizes annotation
  remote→local alongside status. This is a metadata-only sync in the same direction as
  status, adding no new sync machinery.

Scale-down composes from existing parts: the worker publishes the reduced count, the
manager shrinks the admitted local slice in place, and the existing MultiKueue
scale-down propagation shrinks the remote Workload, releasing quota in both clusters.

#### Spark-specific invariant: the remote spec is write-once

After the adapter creates the remote SparkApplication, its **spec must never be
updated** — the operator would move the application to `INVALIDATING` and resubmit it.
This is naturally satisfied: batch/v1.Job's adapter must sync parallelism into the
remote Job spec, but Spark's scaling signal travels exclusively through annotations,
Workload objects, and pods. The only spec mutation on the worker is the worker Kueue's
own `RunWithPodSetsInfo` at unsuspend time, before the application starts. The adapter
explicitly excludes the demand annotation from any local→remote sync (it flows the
other way) and performs no remote spec updates after creation.

#### Latency budget

The MultiKueue scale-up path adds hops (worker observation → annotation sync → manager
slice → manager admission → remote slice creation → worker admission → ungate). Each
hop is event-driven, so end-to-end latency is expected in the seconds range —
comfortably inside Spark's pending-executor timeout
(`spark.kubernetes.allocation.executor.timeout`, default 600s), which remains the
backstop if any hop stalls. The e2e tests measure this latency to validate the budget.

### PodsReady Semantics

`PodsReady()` currently requires `spec.executor.instances` executors to be
Running/Completed. For elastic applications this is wrong in both directions (the
target is dynamic). For elastic applications it is redefined as: driver Running **and**
at least `max(1, minExecutors)` executors Running/Completed — i.e. the application has
reached its guaranteed floor. Scale-up beyond the floor must not re-arm
`waitForPodsReady` timeouts (consistent with elastic jobs being considered "ready" once
their admitted baseline runs).

### Failure Modes

| Failure | Behavior |
|---|---|
| Scale-up slice never admitted | Delta pods stay gated; app runs at current size; Spark deletes the pods after `spark.kubernetes.allocation.executor.timeout`; demand decays (see above). Self-healing, no operator action required. |
| Executor pods created beyond `maxExecutors` | Excess never accounted, never ungated; deleted by Spark's pending timeout. Quota is protected by construction. |
| Operator pod webhook and template propagation both fail to gate an executor pod | The pod schedules without admission — quota under-enforcement. Mitigated by validating gates in the SparkApplication templates at create/update; the pod-template file path makes this deterministic. Documented as a requirement on the spark-operator deployment. |
| Kueue controller down during scale events | Level-based recovery: on restart, `PodSets()` recomputes from live pods and the replica-sizes annotation; slices converge. Gated pods block any unaccounted scheduling in the interim. |
| Driver crash/restart (operator restart policy) | Executors are deleted; observed demand collapses to the floor; workload scales down in place. On resubmission the app re-ramps through the normal flow. |
| (MultiKueue) worker cluster unreachable during a scale event | The manager's slice state freezes at the last synced demand; the worker keeps enforcing via gates and prebuilt workloads locally. On reconnect, the annotation sync re-derives demand and slices converge; if the worker is declared lost, standard MultiKueue requeue semantics apply to the whole application. |

### Limitations and Incompatibilities

- **MultiKueue**: designed-in (see [MultiKueue Support](#multikueue-support)) but
  delivered as a later phase, gated on the SparkApplication MultiKueue adapter and the
  upstream `spec.managedBy` addition to the spark-operator API. Elastic scaling in a
  single cluster does not depend on it.
- **TAS**: only `unconstrained` topology, inherited from KEP-77
  (`ElasticJobsViaWorkloadSlicesWithTAS` applies as-is; `required`/`preferred` rejected
  at the webhook).
- **PartialAdmission**: not applicable — the Spark integration does not implement
  minimum-count PodSets, and KEP-77 forbids the combination anyway.
- **Sticky flavor**: scale-ups can pend even when another flavor has capacity (KEP-77
  behavior, documented).
- **`spec.executor.instances` edits**: remain a full application restart per
  spark-operator semantics; out of Kueue's control and documented as such.

## Project Scoping and Phases

The work is deliberately split so that every phase ships standalone value and the
elastic machinery never builds on unfixed accounting.

### Phase 0 – Integration hardening (independent bug fixes)

Deliverables: P1–P4 fixes with tests, P5–P6 documentation. No feature gate, no API
change; each item is an independent PR against the existing
`SparkApplicationIntegration` feature. **P1 and P2 are hard blockers for Phase 1**
(elastic scaling multiplies accounting error; restart-policy conflicts become quota
escapes when the app resizes itself). P3–P6 are strongly recommended but not blocking.

### Phase 1 – Single-cluster elastic scaling (Alpha)

Deliverables:

- `SparkApplication` added to `supportedElasticJobGVKs`; elastic validation set
  (dynamicAllocation coupling, `maxExecutors` required, gate presence, TAS mode).
- Scheduling-gate injection into driver/executor templates.
- Demand observation (`PodSets` local mode), executor-pod watch, initial-target
  sizing.
- `JobWithCustomAnnotations` + `ElasticWorkloadNameProvider` implementations and the
  `kueue.x-k8s.io/sparkapplication-podset-replica-sizes` annotation.
- `PodsReady` floor semantics for elastic apps.
- Unit + integration tests per the [Test Plan](#test-plan); documentation (enablement,
  operator requirements, Spark tuning guidance).

Dependencies: Phase 0 (P1, P2); `ElasticJobsViaWorkloadSlices` (Beta since v0.18) —
no changes to the slice machinery itself are anticipated.

### Phase 2 – MultiKueue (targeting Beta)

Deliverables:

- Baseline SparkApplication MultiKueue adapter (+ `MultiKueueWatcher`), useful for
  non-elastic apps on its own.
- **Upstream**: `spec.managedBy` in kubeflow/spark-operator (proposal, implementation,
  release); Kueue-side `ManagedBy`/`SetManagedBy`/`CanDefaultManagedBy`
  implementations once available.
- Worker→manager demand publication (prebuilt-path annotation patching) and adapter
  annotation sync; delegated-mode `PodSets`.
- MultiKueue integration/e2e tests, including the remote-spec write-once invariant.

Dependencies: Phase 1; upstream spark-operator release with `managedBy`. The interim
suspended-local-app pattern can unblock testing before the upstream release, but Beta
graduation of this phase requires the `managedBy` protocol.

### Phase 3 – Follow-ups (post-Beta, demand-driven)

Explicitly deferred, tracked as separate issues when demanded:

- **Kueue-side scale-up debounce** knob if churn metrics from Beta warrant it.
- **Partial-admission analog for non-elastic apps**: admitting with fewer executors
  (down to a user-declared minimum) would require operator cooperation, since
  rewriting `instances` at admission is a spec change only viable while suspended;
  feasible but unproven demand.
- **`restartPolicy` lifecycle mapping** (the P2 alternative) if users need
  operator-managed retries under Kueue.
- **TAS `required`/`preferred` for elastic Spark**, tracking KEP-77's own roadmap for
  elastic TAS modes.
- **PySpark overhead derivation** beyond the documented `memoryOverhead`
  recommendation.

## Test Plan

[x] I/we understand the owners of the involved components may require updates to
existing tests to make this code solid enough prior to committing the changes necessary
to implement this enhancement.

### Unit Tests

Phase 0:

- Resource derivation: CPU fallback chain (`coreRequest` → `cores` → default 1);
  memory overhead (explicit, factor-based, 384Mi floor, non-JVM factor); default
  executor count when `instances` is unset — asserted equal to the requests Spark
  places on real pods for each combination.
- Webhook: rejection of `restartPolicy.type != Never` and non-empty `batchScheduler`
  for Kueue-managed apps; both accepted for non-managed apps.

Phase 1:

- `sparkapplication` adapter: initial-target computation across
  `instances`/`initialExecutors`/`minExecutors` combinations; demand clamping
  (below floor, above `maxExecutors`, terminal/deleting pods excluded); replica-sizes
  annotation serialization and `GetWorkloadNameExtraPart` determinism; delegated-mode
  vs local-mode count selection.
- Webhook: gate injection into nil/non-nil templates; validation matrix
  (elastic ⇒ dynamicAllocation, `maxExecutors` required, gate presence, annotation
  immutability, TAS mode rejection).
- Pod event handler: mapping, filtering of non-executor and non-elastic pods.

### Integration Tests

Following the pattern of
`test/integration/singlecluster/controller/jobs/raycluster/raycluster_controller_test.go`
(elastic sections):

- Initial admission at the computed initial target; startup pods gated then ungated.
- Scale-up: create gated executor pods → replacement slice created and admitted → old
  slice `Finished/WorkloadSliceReplaced` → pods ungated → ClusterQueue usage reflects
  the new size.
- Scale-up without capacity: slice Pending, pods remain gated; pod deletion decays the
  pending slice; admitted usage unchanged throughout.
- Scale-down: executor pod deletion → in-place Workload count update → quota released →
  a queued workload admits into the freed capacity.
- Sticky flavor: scale-up exceeding the original flavor stays Pending with the
  `couldn't change flavor` condition.
- Preemption: whole-app suspension of a scaled-up application releases the full current
  slice usage.
- Phase 0 regression pins: Workload resource totals match Spark-derived pod requests
  for a representative spec (P1); `FindAncestorJobManagedByKueue` resolves an executor
  pod's ancestor through the driver pod to the SparkApplication, confirming the pod
  integration does not independently manage executor pods (P4, already implemented as
  `TestFindAncestorJobManagedByKueueForExecutorPod`).

### E2E Tests

A `SparkApplication` e2e (real spark-operator + a small dynamic-allocation job) covering
one full up-and-down cycle with quota assertions, mirroring the RayCluster elastic e2e.

For the MultiKueue phase (following `test/integration/multikueue/jobs_test.go` elastic
patterns):

- Delegated elastic SparkApplication: remote object created once; scale-up on the
  worker produces a manager-side replacement slice and a matching remote slice; gated
  executor pods ungate only after remote admission; quota reflected on both clusters.
- Scale-down on the worker shrinks the local and remote Workloads in place.
- Remote SparkApplication spec is byte-identical before and after multiple scale
  cycles (write-once invariant; guards against operator-triggered resubmission).
- Worker disconnection during a pending scale-up: slice recovery and demand
  re-derivation after reconnect.

## Graduation Criteria

### Alpha

- Phase 0 items P1 and P2 merged (P3–P4 merged or explicitly triaged with tests
  documenting current behavior).
- Elastic SparkApplication behind `SparkApplicationIntegration` +
  `ElasticJobsViaWorkloadSlices` (both must be enabled; no new gate).
- Gate injection, demand observation, slice creation/aggregation, in-place scale-down.
- Webhook validation set complete; integration tests for the scenarios above.
- Documented enablement, spark-operator webhook requirement, and tuning guidance.

### Beta

- E2E coverage in CI with a real spark-operator.
- Churn metrics evaluated (`replaced_workload_slices_total` labeled by framework);
  decision on a Kueue-side debounce knob.
- Re-evaluate `PodsReady` floor semantics with user feedback.
- **MultiKueue phase**: SparkApplication MultiKueue adapter merged; upstream
  spark-operator `spec.managedBy` landed and honored; worker→manager demand sync
  implemented; multikueue integration tests for elastic scale-up/scale-down across
  clusters, including verification that the remote spec is never mutated after
  creation.

### GA

- Follows KEP-77's GA criteria for frameworks adopting workload slices.

## Drawbacks

- The demand-observation loop makes Kueue's view of the job **derived from pods** rather
  than from the CR spec — a new pattern among elastic integrations, with its own failure
  modes (observation lag, dependence on pod labels and on the operator's template
  propagation).
- Reactive admission: quota is requested *after* the driver has created pods, so under
  contention Spark creates pods that may never run (bounded by Spark's own timeouts).
- Additional pod-event load on the SparkApplication reconciler in clusters with large,
  bursty Spark fleets (bounded by batching and by the `maxExecutors` validation).

## Alternatives

Alternatives local to a single decision are listed with that decision
([P1 resource fidelity](#p1-resource-accounting-fidelity),
[P2 restartPolicy](#p2-restartpolicy-exclusivity)). Whole-design alternatives:

- **Demand from `status.executorState` instead of pods** — the operator maintains a
  per-executor state map in the application status, which would avoid the pod watch.
  Rejected: the map is updated on the operator's sync interval (laggy), reflects
  executors the operator has *seen* rather than pods the driver has *requested* —
  gated pods that never start may not appear at all, which is precisely the demand
  signal the design needs — and it couples correctness to operator status-reporting
  fidelity. Pods are the ground truth the gate acts on; observing anything else
  reintroduces a translation layer.
- **MultiKueue demand observation in the adapter** — instead of the worker's Kueue
  publishing the demand annotation, the manager-side adapter could list executor pods
  on the worker via its remote client. Rejected: it widens the RBAC required of every
  MultiKueue worker kubeconfig to include pod list/watch, moves pod-churn load onto
  the manager's sync loop, and duplicates the observation logic that must exist on the
  worker anyway (for gating and ungating). The worker already watches these pods; it
  is the natural publisher.
- **Spec-driven scaling (RayCluster clone)** — treat `spec.executor.instances` as the
  scaling knob. Rejected: the spark-operator resubmits the application on any spec
  change (`ApplicationStateInvalidating`), so this is a restart, not elastic scaling;
  and Spark's own dynamic allocation would remain unusable.
- **Admit `maxExecutors` upfront** — size the Workload at the ceiling and let the driver
  scale freely underneath. Simple (no slices, works today by setting
  `instances = maxExecutors` and disabling dynamic-allocation validation), but strands
  quota exactly when elasticity matters, and reports usage the application may never
  reach. Kept as a documented workaround, rejected as the design.
- **Plain-pod integration** (`spark-submit` against Kueue's pod-group integration,
  bypassing the operator) — already documented for spark-submit users; lacks
  application-level Workload semantics, operator lifecycle management, and slice-based
  resize accounting.
- **Driver-cooperative admission** (Spark asks Kueue before requesting executors, e.g.
  via a custom `ExternalClusterManager` or operator changes) — architecturally cleaner
  but requires upstream Spark/spark-operator changes and years of ecosystem lag; the
  scheduling-gate mechanism achieves the same enforcement point with zero Spark-side
  changes.
- **A dedicated executor-pod webhook in Kueue** that gates executor pods directly
  (instead of relying on template propagation) — more robust to operator
  misconfiguration but adds a webhook on the pod fast path for all clusters; can be
  reconsidered if template propagation proves fragile in practice.
