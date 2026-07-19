---
title: "The Job Framework: how integrations really work"
linkTitle: "The Job Framework"
weight: 60
description: >
  The Job Framework: how integrations really work
type: docs
---

<!-- Written from a read of the source around the v0.19 cut. Links point at main (not a pinned commit), so files/directories stay resolvable as the code evolves; described behavior may drift in detail over time -- if something looks off, a fix or a removal is equally welcome, no need to reconcile the whole page. -->

Package: [`pkg/controller/jobframework/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/controller/jobframework) + one package per integration in [`pkg/controller/jobs/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/controller/jobs). This is the most common area for *external* contributions ("make Kueue work with framework X") and the layer with the strongest conventions.

## 6.1 The generic reconciler, step by step

`ReconcileGenericJob` ([`pkg/controller/jobframework/reconciler.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/jobframework/reconciler.go), ~1800 lines total) is the shared brain. Simplified control flow, in order:

1. **Load & filter**: fetch the job; honor `JobWithSkip`; finalize if deleting.
2. **Namespace gating**: `managedJobsNamespaceSelector` from the configuration decides whether Kueue may manage jobs in this namespace at all (the `ManagedJobsNamespaceSelectorAlwaysRespected` gate — GA and on by default since v0.19 — makes this check unconditional).
3. **Ancestor traversal**: `FindAncestorJobManagedByKueue` walks `ownerReferences` (bounded depth) so a child object of an already-managed parent (a Job inside a managed JobSet) is *not* double-managed — the parent owns the workload. Getting this wrong creates duplicate workloads; it's why prefixes/labels matter (Chapter 1.5).
4. **Workload reconciliation**: find the workload for this job (by owner + `job-uid` label); create it from `PodSets()` if missing; detect *equivalence* — if the job changed shape (pod count, requests), the old workload is invalidated. Prebuilt workloads (`kueue.x-k8s.io/prebuilt-workload-name`) skip creation.
5. **State sync, both directions**:
   - job running but workload not admitted (e.g. just evicted) → `Suspend()` the job, `RestorePodSetsInfo`;
   - workload `Admitted` and job suspended → build `podSetsInfo` from the admission's flavors (+ TAS assignments), call `RunWithPodSetsInfo` → job unsuspends;
   - job `Finished()` → set the workload's `Finished` condition.
6. **Feature hooks** along the way: WaitForPodsReady (`PodsReady()`), reclaimable pods, partial admission downsizing, MultiKueue remote delegation, elastic workload slices.

Understand this loop once and every integration package becomes a thin adapter: `<type>_controller.go` (the `GenericJob` impl), `<type>_webhook.go`, `<type>_multikueue_adapter.go`, tests.

## 6.2 The webhook side

[`base_webhook.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/jobframework/base_webhook.go) implements the shared mutating/validating admission logic: apply defaults (suspend on create when managed, LocalQueue defaulting from namespace annotation — KEP-2936), and validate (immutability of `queue-name` after QuotaReserved, TAS annotation validation via [`tas_validation.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/jobframework/tas_validation.go), etc.). Integrations register concrete webhooks in their `SetupWebhook` and are wired in [`setup.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/jobframework/setup.go).

Subtlety: **whether a job is managed can depend on config loaded at runtime**, so webhook and reconciler must agree on "managedness" (`IsManaged`-style helpers). Divergence between the two produces the classic bug "job started unsuspended and ran without quota."

## 6.3 Registration: the integration manager

[`integrationmanager.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/jobframework/integrationmanager.go) is a registry: each integration package registers itself in `init()` with callbacks (`NewJob`, `SetupIndexes`, `NewReconciler`, `SetupWebhook`, `JobType`, `AddToScheme`, MultiKueue adapter, …). The configuration's `integrations.frameworks` list selects which registered integrations actually start, and CRD presence is checked at startup (an integration whose CRD is absent is skipped with a notice). [`pkg/controller/jobs/jobs.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/jobs/jobs.go) imports all built-ins for side-effect registration — a new integration must be added there.

## 6.4 The special integrations

- **`pod`**: the universal fallback (KEP-976). Single pods or **pod groups** (`kueue.x-k8s.io/pod-group-name` + `pod-group-total-count` annotations); admission by ungating scheduling gates rather than unsuspending; finalizers to account for terminated pods. Serving integrations (`deployment`, `statefulset`, `leaderworkerset`) build on it — one workload **per pod** for Deployments (scale-friendly), per-group for LWS/STS.
- **External frameworks** (KEP-2349): a framework can integrate *without code in Kueue* — `integrations.externalFrameworks` in the configuration + the generic [`noop_controller.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/jobframework/noop_controller.go); the external controller manipulates the workload itself. Suggest this to users before writing a built-in adapter.

## 6.5 Writing a new integration — judgment notes beyond the the architecture page recipe

- `PodSets()` must be **deterministic and complete** — every pod the framework creates must be accounted for in some pod set, or quota accounting drifts.
- `RunWithPodSetsInfo` / `RestorePodSetsInfo` must be exact inverses; the restore path runs on every eviction, and asymmetries surface as "job can't be re-admitted after eviction."
- Decide early what the *unit of admission* is (whole CR? per-replica?) — it determines pod set shape, and it's the question maintainers will ask first in the KEP.
- Look at `trainjob/` or `sparkapplication/` (recent) rather than `job/` (oldest, most special-cased) as your template.
