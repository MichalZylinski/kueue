---
title: "MultiKueue: multi-cluster dispatching"
linkTitle: "MultiKueue"
weight: 80
description: >
  MultiKueue: multi-cluster dispatching
type: docs
---

<!-- Written from a read of the source around the v0.19 cut. Links point at main (not a pinned commit), so files/directories stay resolvable as the code evolves; described behavior may drift in detail over time -- if something looks off, a fix or a removal is equally welcome, no need to reconcile the whole page. -->

KEP-693 (+ many follow-ups); the #1 roadmap area for 2026. One **manager cluster** holds the queues and quota; **worker clusters** run the jobs.

## 8.1 The moving parts

APIs ([`apis/kueue/v1beta2/multikueue_types.go`](https://github.com/kubernetes-sigs/kueue/blob/main/apis/kueue/v1beta2/multikueue_types.go)):
- **`MultiKueueCluster`** — one worker: connection info via a kubeconfig (secret ref with key `kubeconfig`, or a file path) *or* a `clusterProfileRef` (ClusterProfile API, gate `MultiKueueClusterProfile`); `Active` condition reports connectivity.
- **`MultiKueueConfig`** — a named set of clusters.
- An **`AdmissionCheck`** with controller `kueue.x-k8s.io/multikueue` referencing that config — attaching it to a ClusterQueue is what makes the CQ multi-cluster. MultiKueue is "just" an admission check from the core scheduler's perspective — a beautiful example of the two-phase admission design paying off.

Controllers ([`pkg/controller/admissionchecks/multikueue/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/controller/admissionchecks/multikueue)):
- [`multikueuecluster.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/admissionchecks/multikueue/multikueuecluster.go) + [`remote_client.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/admissionchecks/multikueue/remote_client.go) — maintain a client+watch per worker (reconnect with backoff; kubeconfig changes picked up via [`fswatch.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/admissionchecks/multikueue/fswatch.go)).
- [`workload.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/admissionchecks/multikueue/workload.go) — the heart: for each manager workload whose CQ has the MultiKueue check, sync a **copy of the workload (and the job)** to worker clusters, watch remote status, and react to remote admission.
- [`admissioncheck.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/admissionchecks/multikueue/admissioncheck.go), [`clusterqueue.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/controller/admissionchecks/multikueue/clusterqueue.go), `externalframeworks/` — wiring and config validation.

## 8.2 The dispatching flow

1. Manager-side scheduler admits the workload *locally first* (quota on the manager CQ) → `QuotaReserved`; the MultiKueue admission check is now `Pending`.
2. The **dispatcher** ([`pkg/controller/workloaddispatcher/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/controller/workloaddispatcher)) nominates worker clusters: `AllAtOnce` (default — every cluster in the config) or `Incremental` (KEP-9270 — growing subsets with a configurable step/timeout), selectable via `multiKueue.dispatcherName` in the configuration; **external dispatchers** are possible for custom placement policies (this is a designed extension point).
3. The workload controller creates the workload (+ job, via the integration's **MultiKueue adapter**) on nominated workers. Each worker cluster runs its *own* Kueue and admits against its *own* local quota.
4. **First worker to admit wins**: its name lands in `status.clusterName`; copies on the other workers are deleted; the manager marks the admission check `Ready` → workload `Admitted` on the manager.
5. The job runs remotely; the adapter mirrors remote job status back to the manager-side job object so users watch their job normally. On worker failure/disconnection (heartbeat-driven `Active=False`), the workload is requeued and redispatched.

## 8.3 Adapters and the `managedBy` contract

For each integration, a MultiKueue adapter implements creating/syncing the remote job copy. Key mechanism for `batch/Job` and JobSet: the **`spec.managedBy`** field — on the manager cluster the job is managed by Kueue's MultiKueue controller (so the local job controller ignores it), while the remote copy is managed normally. The `managedBy` mechanism for `batch/Job` is unconditional since v0.19 (its `MultiKueueBatchJobWithManagedBy` gate was removed); `MultiKueueAdaptersForCustomJobs` (generic adapters for custom jobs, KEP-2349 follow-up) remains gated.

## 8.4 Active development areas (= contribution openings)

Straight from the roadmap and recent gates: orchestrated preemption across workers (KEP-8303, gate `MultiKueueOrchestratedPreemption`), re-admission on worker-side eviction (`MultiKueueRedoAdmissionOnEvictionInWorker`), waiting for full admission (`MultiKueueWaitForWorkloadAdmitted`), manager-side quota automation (KEP-9988, `MultiKueueManagerQuotaAutomation`), elastic RayJob support, long-running services, log retrieval from workers. If you want a roadmap-relevant niche as a new contributor, MultiKueue has the most open surface.

**Testing note:** [`test/integration/multikueue/`](https://github.com/kubernetes-sigs/kueue/tree/main/test/integration/multikueue) spins up *multiple envtest apiservers* (manager + workers) — the framework there is worth studying even outside MultiKueue; e2e lives in [`test/e2e/multikueue/`](https://github.com/kubernetes-sigs/kueue/tree/main/test/e2e/multikueue) with two kind clusters (`make test-multikueue-e2e`).
