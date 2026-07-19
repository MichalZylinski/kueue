---
title: "Troubleshooting decision trees"
linkTitle: "Decision trees"
weight: 5
description: >
  Flowcharts for the four most common Kueue failure investigations
type: docs
---

<!-- Written from a read of the source around the v0.19 cut. Links point at main (not a pinned commit), so files/directories stay resolvable as the code evolves; described behavior may drift in detail over time -- if something looks off, a fix or a removal is equally welcome, no need to reconcile the whole page. -->


Each leaf names the command to run or the subsystem (see the internals pages) to inspect. These orderings matter: queue health before quota, quota before checks.

### 3.1 Workload stuck Pending (no `QuotaReserved`)

```mermaid
flowchart TD
    A[Workload has no QuotaReserved condition] --> B{Workload object exists at all?}
    B -- no --> B1["Job not managed: check queue-name label,<br/>namespace selector, integration enabled<br/>in Configuration → the internals pages"]
    B -- yes --> C{LocalQueue exists & points to a CQ?}
    C -- no --> C1["kubectl get lq -n ns<br/>fix spec.clusterQueue"]
    C -- yes --> D{ClusterQueue Active=True?}
    D -- no --> D1["kubectl get cq X -o jsonpath={.status.conditions}<br/>reasons: FlavorNotFound, AdmissionCheckNotFound,<br/>Stopped → fix referenced objects"]
    D -- yes --> E{QuotaReserved message says what?}
    E -- "insufficient quota / couldn't assign flavors" --> E1["Quota math: describe cq usage,<br/>cohort borrowing limits → the internals pages"]
    E -- "insufficient unused quota, preemption possible?" --> E2["Preemption policy dials too strict,<br/>or no eligible victims → the internals pages"]
    E -- "waiting for pods ready of other workloads" --> E3["waitForPodsReady blockAdmission:<br/>find the stuck admitted workload"]
    E -- "no message / never evaluated" --> E4["Suspect missing requeue trigger or<br/>scheduler not running: check manager logs<br/>V(2) 'Scheduling cycle starts' → file a bug"]
```

### 3.2 `QuotaReserved` but never `Admitted`

```mermaid
flowchart TD
    A[QuotaReserved=True, Admitted=False] --> B{CQ has admissionChecks?}
    B -- no --> B1["Shouldn't happen — controller bug:<br/>check workload_controller logs"]
    B -- yes --> C{status.admissionChecks states?}
    C -- Pending --> C1["The check's controller isn't acting:<br/>ProvisioningRequest? autoscaler side.<br/>MultiKueue? worker connectivity —<br/>kubectl get multikueuecluster: Active cond"]
    C -- Retry/Rejected --> C2["Read the check message; Retry evicts<br/>and requeues (KEP-3258 backoff)"]
```

### 3.3 Job admitted but pods not running

```mermaid
flowchart TD
    A[Workload Admitted=True, no pods Running] --> B{Job unsuspended?}
    B -- no --> B1["Job framework didn't act: integration<br/>reconciler logs; RunWithPodSetsInfo error?<br/>→ the internals pages"]
    B -- yes --> C{Pods exist?}
    C -- no --> C1["Job controller's problem (not Kueue):<br/>kubectl describe job — quota/webhook<br/>errors from other admission controllers?"]
    C -- yes --> D{Pods SchedulingGated?}
    D -- yes --> D1["TAS: ungater waiting or stuck —<br/>check topologyAssignment in admission,<br/>tas-topology-ungater logs → the internals pages"]
    D -- no --> E["Pods Pending at kube-scheduler:<br/>injected nodeSelector from flavor may match<br/>no nodes — kubectl describe pod events"]
```

### 3.4 Unexpected eviction — "what killed my workload?"

```mermaid
flowchart TD
    A[Workload was running, now suspended/requeued] --> B["Read Evicted condition reason<br/>kubectl get wl -o jsonpath={.status.conditions}"]
    B --> C{reason}
    C -- Preempted --> C1["Parse Preempted event/condition message:<br/>UID, JobUID, paths → find preemptor via<br/>label kueue.x-k8s.io/job-uid (see the onboarding lab, kata 3)"]
    C -- PodsReadyTimeout --> C2["Pods never became Ready in time<br/>(waitForPodsReady is ON by default<br/>since v0.19, 30-min timeout):<br/>image pulls? node capacity? Check<br/>requeueState backoff count"]
    C -- Deactivated --> C3["spec.active=false — by user, by backoff<br/>limit exhaustion, or maximum execution time"]
    C -- ClusterQueueStopped --> C4["Admin set stopPolicy on CQ/LQ"]
    C -- NodeFailures --> C5["TAS node replacement failed →<br/>the TAS internals page"]
```
