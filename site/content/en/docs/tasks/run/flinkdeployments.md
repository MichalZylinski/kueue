---
title: "Run a FlinkDeployment"
linkTitle: "FlinkDeployments"
date: 2026-11-06
weight: 12
description: >
  Run a Kueue scheduled Apache Flink Deployment
---

{{< feature-state state="alpha" for_version="v0.21" >}}

This page shows how to use Kueue's scheduling, quota management, and preemption features when running [Apache Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/) `FlinkDeployment` workloads in Application mode.

This guide is for [batch users](/docs/tasks#batch-user) that have a basic understanding of Kueue. For more information, see [Kueue's overview](/docs/overview).

{{% alert title="Note" color="primary" %}}
`FlinkDeploymentIntegration` is currently an alpha feature and is disabled by default.

To enable it, activate the `FlinkDeploymentIntegration` feature gate and include `flink.apache.org/flinkdeployment` in `Configuration.integrations.frameworks`.

In Alpha, Kueue supports **Application-mode** `FlinkDeployment` resources (`spec.job` specified). Kueue rejects Kueue-managed Session clusters (`spec.job` omitted with `kueue.x-k8s.io/queue-name` set) in the validating webhook; `FlinkSessionJob` and Session cluster management are not yet supported. Unmanaged Session clusters (`spec.job` omitted without `kueue.x-k8s.io/queue-name`) remain untouched by Kueue defaulting, validation, and reconciliation even when `manageJobsWithoutQueueName` is enabled. A stale `kueue.x-k8s.io/queue-name` label on a pre-existing Session cluster is ignored by the controller and can be removed on update.
{{% /alert %}}

## Before you begin

1. Ensure you are using Kueue v0.21.0 or newer and [Apache Flink Kubernetes Operator](https://github.com/apache/flink-kubernetes-operator) v1.16.0 or newer (see the [quick-start guide](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/try-flink-kubernetes-operator/quick-start/) for operator installation). Kueue is tested against the operator's `release-1.16.1` `FlinkDeployment` CRD; it relies on `status.taskManager.replicas`, `podTemplate.spec.schedulingGates`, `kubernetes.operator.pod-template.merge-arrays-by-name`, and the `spec.jobManager.resources` / `spec.taskManager.resources` fields introduced in v1.16.0.

2. Create the `flink` ServiceAccount and RBAC role in the target namespace as described in the [Apache Flink Kubernetes Operator RBAC guide](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/deployment/helm/rbac/#running-jobs-in-other-namespaces).

3. Check [Administer cluster quotas](/docs/tasks/manage/administer_cluster_quotas) for details on setting up a `ResourceFlavor`, `ClusterQueue`, and `LocalQueue` (such as `user-queue`).

4. Enable the `FlinkDeploymentIntegration` feature gate and add `flink.apache.org/flinkdeployment` to `integrations.frameworks` in Kueue's `Configuration` (check [Change the feature gates configuration](/docs/getting-started/installation/#change-the-feature-gates-configuration) and [Install a custom-configured released version](/docs/getting-started/installation/#install-a-custom-configured-released-version)):

```yaml
apiVersion: config.kueue.x-k8s.io/v1beta2
kind: Configuration
featureGates:
  FlinkDeploymentIntegration: true
integrations:
  frameworks:
    # ... keep existing enabled frameworks (e.g., "batch/job")
    - "flink.apache.org/flinkdeployment"
```

## FlinkDeployment definition

When running [FlinkDeployments](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/custom-resource/overview/) on Kueue, consider the following aspects:

### a. Queue selection

Specify the target [LocalQueue](/docs/concepts/local_queue) in `metadata.labels`:

```yaml
metadata:
  labels:
    kueue.x-k8s.io/queue-name: user-queue
```

### b. Suspension and admission control

Kueue controls execution of an Application-mode `FlinkDeployment` via `spec.job.state`:
- On creation, Kueue's mutating webhook defaults `spec.job.state` to `suspended` so the Flink Operator does not launch pods before quota is reserved.
- Once the corresponding Kueue `Workload` is admitted, Kueue injects any assigned `ResourceFlavor` node selectors, tolerations, or Topology-Aware Scheduling gates into `spec.jobManager.podTemplate` and `spec.taskManager.podTemplate` and sets `spec.job.state` to `running`.
- While `spec.job.state` is `suspended`, valid `spec` updates are accepted and Kueue's controller reconciles the owned `Workload` to match the updated PodSets (matching `SparkApplication` and `JobSet`).
- While `spec.job.state` is `running`, the following fields and operations are immutable or unsupported:
  - `metadata.labels["kueue.x-k8s.io/queue-name"]` and `metadata.labels["kueue.x-k8s.io/max-exec-time-seconds"]`, plus adding `metadata.labels["kueue.x-k8s.io/priority-class"]` when unset (removing `metadata.labels["kueue.x-k8s.io/priority-class"]`, setting the `kueue.x-k8s.io/prebuilt-workload-name` label or annotation, and setting `metadata.annotations["kueue.x-k8s.io/elastic-job"]: "true"` are rejected in any state)
  - `spec.mode` (`native` vs. `standalone`), plus switching between Session mode (`spec.job` omitted) and Application mode (`spec.job` specified) on any update
  - `spec.image`, `spec.imagePullPolicy`, `spec.serviceAccount`, `spec.podTemplate`, `spec.job.parallelism`, and any change to `spec.<role>.replicas`, `spec.<role>.resource`, `spec.<role>.resources`, or `spec.<role>.podTemplate` that alters the derived PodSets
  - `spec.flinkConfiguration` keys that affect derived PodSet counts, default CPU, slot capacity, or array merging (`taskmanager.numberOfTaskSlots` and `kubernetes.operator.pod-template.merge-arrays-by-name`)
  - Scaling via the `/scale` subresource (`kubectl scale` or `HorizontalPodAutoscaler`): updates to `flinkdeployments/scale` use `autoscaling/v1` `Scale` payloads that bypass the `FlinkDeployment` validating webhook and trigger a `NoMatchingWorkload` suspend-and-requeue cycle, so `/scale` and `HorizontalPodAutoscaler` must not be used on Kueue-managed `FlinkDeployment` resources.
- On a managed `FlinkDeployment` transitioning `spec.job.state` from `running` to `suspended`, `metadata.labels["kueue.x-k8s.io/queue-name"]`, `metadata.labels["kueue.x-k8s.io/max-exec-time-seconds"]`, and `kueue.x-k8s.io/prebuilt-workload-name` remain immutable and `metadata.labels["kueue.x-k8s.io/priority-class"]` cannot be added when unset or removed (changing an already-set `priority-class` remains allowed, and `queue-name` and `max-exec-time-seconds` are also immutable on `suspended` to `running`), while pure `spec.job.state: suspended` patches are permitted even on pre-existing invalid specs. An unmanaged running `FlinkDeployment` can be suspended and onboarded with `kueue.x-k8s.io/queue-name` (and an optional `kueue.x-k8s.io/priority-class`) in a single update. When `deletionTimestamp` is set, `spec` and immutable Kueue labels cannot be modified while finalizer updates remain allowed.

{{% alert title="Warning" color="warning" %}}
Do not set `spec.job.state: suspended` manually on an admitted `FlinkDeployment`. Kueue re-sets it to `running` while the `Workload` stays admitted, so the Flink Operator sees a suspend immediately followed by a resume (including a savepoint under `upgradeMode: savepoint`) and no quota is released. To stop a job in place and release its quota (for example to take a savepoint before a planned change), deactivate the `Workload` (or run `kueuectl stop workload <workload-name>`); Kueue then sets `spec.job.state: suspended` and preserves `upgradeMode`:

```shell
kubectl patch workload <workload-name> --type=merge -p '{"spec":{"active":false}}'
```

Set `spec.active` back to `true` (or run `kueuectl resume workload <workload-name>`) to requeue the job. Patching `Workload` `spec.active` requires `kueue-workload-editor-role` or [batch administrator](/docs/tasks/manage/rbac/) permissions because the default `kueue-batch-user-role` grants read-only access to `Workload` objects; batch users without `Workload` write access can release quota by deleting the `FlinkDeployment`.
{{% /alert %}}

### c. PodSet composition and resource requests

Kueue models each `FlinkDeployment` as two PodSets:
- `job-manager`: Represents the JobManager coordination pods (`spec.jobManager.replicas`, defaulting to `1`).
- `task-manager`: Represents the TaskManager worker pods. When `spec.taskManager.replicas` is set, Kueue uses that count; when omitted, `spec.job.parallelism` is required (`>= 1`) and Kueue derives the TaskManager count as `ceil(parallelism / taskSlots)` from `spec.job.parallelism` and `flinkConfiguration["taskmanager.numberOfTaskSlots"]` (default `1`). The Flink Operator sets `parallelism.default` to `spec.taskManager.replicas * taskSlots` when `spec.taskManager.replicas` is set (ignoring `spec.job.parallelism`) and to `spec.job.parallelism` otherwise. In `native` mode this count is an estimate based on `parallelism.default`, not a cap: Flink's `ActiveResourceManager` sizes TaskManagers from the job graph's slot demand, so JARs that set per-operator parallelism above `parallelism.default`, use multiple slot-sharing groups, or request fine-grained resource profiles start more TaskManager pods than Kueue admitted. For strict accounting, use `spec.mode: standalone` (which creates a fixed TaskManager `Deployment` of `spec.taskManager.replicas` pods, or `ceil(spec.job.parallelism / taskSlots)` pods when `spec.taskManager.replicas` is omitted); to stay within admitted quota in `native` mode, use a single slot-sharing group without fine-grained resource profiles and keep every operator's parallelism at or below `parallelism.default`.

For each component, Kueue merges the root `spec.podTemplate` with the component's `podTemplate` (honoring `flinkConfiguration["kubernetes.operator.pod-template.merge-arrays-by-name"]`, which defaults to `false`, when every element of both arrays carries a non-empty `name`, and including sidecar and init containers) and computes requests for `"flink-main-container"`. Every container, init container, and ephemeral container in the merged `podTemplate` must have a unique DNS-1123 label `name` within its list, every resolved container and init container must specify a non-whitespace `image`, any configured `spec.imagePullPolicy` or container `imagePullPolicy` must be `Always`, `Never`, or `IfNotPresent`, and merged `podTemplate` `labels`, `annotations`, `nodeSelector`, `tolerations`, and Topology-Aware Scheduling annotations are validated by Kueue (general `PodSpec` schema validation for volumes, ports, probes, and affinity is left to Kubernetes and the Apache Flink Kubernetes Operator). When both `spec.podTemplate` and `spec.<role>.podTemplate` specify non-empty `spec.tolerations` (or `spec.schedulingGates` under positional array merging), the component list must include the root `spec.podTemplate` list as a prefix. Non-whitespace top-level `spec.image` and `spec.imagePullPolicy` take precedence over `"flink-main-container"` values in `podTemplate` across all modes and roles; when `spec.image` or `spec.imagePullPolicy` is omitted or whitespace-only, `job-manager` (in both modes) and `native`-mode `task-manager` preserve `"flink-main-container"` `image` and `imagePullPolicy` from `podTemplate`, whereas `standalone`-mode `task-manager` ignores `podTemplate` `"flink-main-container"` `image` and `imagePullPolicy` (`spec.image` is required when `spec.mode` is `standalone`, and `standalone`-mode `task-manager` uses `IfNotPresent` when `spec.imagePullPolicy` is omitted). Non-empty `spec.serviceAccount` sets `serviceAccountName` across all modes and roles (overriding `podTemplate`); when `spec.serviceAccount` is omitted, `job-manager` (in both modes) and `native`-mode `task-manager` fall back from `podTemplate.spec.serviceAccount` to `podTemplate.spec.serviceAccountName`, whereas `standalone`-mode `task-manager` uses `"default"`:
- **`spec.jobManager.resource` / `spec.taskManager.resource`**: `memory` accepts plain integer bytes, Apache Flink `MemorySize` units (for example `"2048m"`, `"2g"`, or `"2gb"`, interpreted as `BinarySI` multiples of `1024`), and integer Kubernetes `Quantity` strings (for example `"2Gi"`, `"512Mi"`, or `"2gib"`). Fractional values such as `"1.5Gi"` are rejected because the Flink Operator cannot parse them; use integer units such as `"1536Mi"` instead. `ephemeralStorage` (when set) must be a positive Kubernetes `Quantity` without whitespace. Every Kueue-managed `FlinkDeployment` must explicitly configure memory on both `spec.jobManager` and `spec.taskManager` via `spec.<role>.resource.memory` or `spec.<role>.resources` (memory set only on `"flink-main-container"` in `podTemplate` is not used by the Flink Operator for process sizing and is rejected). When `cpu` is omitted on both `resource` and `resources`, Kueue defaults `"flink-main-container"` CPU to match Flink's pod decorators: `1` core for `job-manager` (in both `native` and `standalone` modes), and `taskmanager.numberOfTaskSlots` cores (default `1`) for `task-manager` in `native` mode. When `spec.mode` is `standalone`, `spec.taskManager` must explicitly configure CPU via `spec.taskManager.resource.cpu` or `spec.taskManager.resources`.
- **`spec.jobManager.resources` / `spec.taskManager.resources`** (Flink Operator v1.16.0+): Standard Kubernetes `corev1.ResourceRequirements` `requests` and `limits` without whitespace (for example `2Gi`, `500m`, `nvidia.com/gpu`). Because `resource` and `resources` cannot both be set on the same component, `resources` must include `memory` (at least `1Ki`, in `requests` or `limits`), whereas `cpu` is optional (except on `spec.taskManager` when `spec.mode` is `standalone`) and must be positive (`> 0`) when set. `memory` must use a positive integer `BinarySI` suffix (`Ki`, `Mi`, `Gi`, `Ti`, `Pi`, up to `2147483647Mi`): fractional values such as `"1.5Gi"` fail Flink's `MemorySize` parser, and plain integer bytes, `"2048m"`, `"2G"`, `"2M"`, or `"2k"` are rejected because Go `resource.Quantity` and Flink's `MemorySize` parser canonicalize or interpret them differently (use `spec.<role>.resource.memory` if you need plain integer bytes or Flink `MemorySize` units). Mirroring the Flink Operator, `cpu` and `memory` from `resources` replace both the requests and limits on `"flink-main-container"`: an omitted request is filled from the limit, an omitted limit is set equal to the request, and any `cpu` or `memory` inherited from `podTemplate` is discarded. Because the Flink Kubernetes Operator ignores `claims` on component `resources`, configure Dynamic Resource Allocation container `resources.claims` and pod `spec.resourceClaims` in `spec.podTemplate` or `spec.<role>.podTemplate`.
- **`spec.flinkConfiguration` restrictions**: Kueue's validating webhook rejects keys that bypass PodSet sizing, placement, or lifecycle accounting (listed in the table below), as well as whitespace-padded keys and duplicate flat/nested key collisions. Validated boolean keys (`job.autoscaler.enabled`, `kubernetes.operator.job.autoscaler.enabled`, `kubernetes.operator.job.restart.failed`, `kubernetes.operator.deployment.rollback.enabled`, and `kubernetes.operator.pod-template.merge-arrays-by-name`) must use `"true"` or `"false"` without surrounding whitespace, and `taskmanager.numberOfTaskSlots` must be a positive integer without whitespace when set (empty or `null` values are rejected). Internal memory split ratios such as `taskmanager.memory.managed.fraction` remain allowed when component memory is configured.

  | Rejected keys | Reason | Configure instead |
  | --- | --- | --- |
  | `job.autoscaler.enabled: "true"`, `kubernetes.operator.job.autoscaler.enabled: "true"` | Dynamic Flink autoscaling is not supported for Kueue-managed deployments. | Fixed `spec.taskManager.replicas` or `spec.job.parallelism` |
  | `jobmanager.memory.*` and `taskmanager.memory.*` ending in `.size`, `.min`, or `.max` (plus deprecated `taskmanager.network.memory.{min,max}`); `kubernetes.{jobmanager,taskmanager}.cpu` and `.cpu.amount`; `taskmanager.cpu.cores`; `kubernetes.{jobmanager,taskmanager}.replicas`; `kubernetes.internal.taskmanager.replicas`; `kubernetes.{jobmanager,taskmanager}.{cpu,memory}.{limit,request}-factor` | Bypasses component resource and replica fields used to compute Kueue PodSets. | `spec.jobManager` and `spec.taskManager` (`replicas`, `resource`, or `resources`) |
  | `slotmanager.redundant-taskmanager-num`, `slotmanager.number-of-slots.min`, `slotmanager.min-total-resource.cpu`, `slotmanager.min-total-resource.memory` | Allocates extra Native-mode TaskManager pods beyond job slot demand. | `spec.taskManager.replicas` or `spec.job.parallelism` |
  | `external-resources`, `external-resource.*` | Requests extended resources such as GPUs outside PodSet container specs. | `spec.jobManager.resources`, `spec.taskManager.resources`, or `podTemplate` |
  | `kubernetes.{jobmanager,taskmanager}.{node-selector,tolerations,annotations,labels}` (and subkeys) | Mutates pod placement or metadata after `podTemplate` rendering. | `spec.podTemplate`, `spec.jobManager.podTemplate`, or `spec.taskManager.podTemplate` |
  | `kubernetes.service-account`, `kubernetes.{jobmanager,taskmanager}.service-account`, `kubernetes.container.image.ref`, `kubernetes.container.image`, `kubernetes.container.image.pull-policy`, `kubernetes.container.image.pull-secrets`, `kubernetes.hostnetwork.enabled` | Mutates pod service account, container image, pull policy, pull secrets, or host networking in Flink pod decorators after `podTemplate` rendering. | `spec.serviceAccount`, `spec.image`, `spec.imagePullPolicy`, or `podTemplate` |
  | `kubernetes.pod-template-file`, `kubernetes.pod-template-file.*` | Operator-local pod template files are invisible to Kueue and overwritten once component `podTemplate` specs are materialized on admission. | `spec.podTemplate`, `spec.jobManager.podTemplate`, or `spec.taskManager.podTemplate` |
  | `kubernetes.operator.job.restart.failed: "true"` | Resubmits a `FAILED` job after Kueue has already released its quota. | Kueue requeueing and backoff (for example `waitForPodsReady`) |
  | `kubernetes.operator.deployment.rollback.enabled: "true"` | Rolls back a timed-out upgrade in memory to `status.reconciliationStatus.lastStableSpec`, launching pods with stale placement or parallelism without updating `spec`. | Kueue eviction and requeueing (for example `waitForPodsReady`) |
  | `pipeline.jobvertex-parallelism-overrides` (and subkeys), `execution.batch.adaptive.auto-parallelism.{max-parallelism,min-parallelism,default-source-parallelism}`, and deprecated `jobmanager.adaptive-batch-scheduler.{max-parallelism,min-parallelism,default-source-parallelism}` (unless `spec.mode` is `standalone` and `spec.taskManager.replicas` is set) | Per-vertex parallelism overrides and `AdaptiveBatchScheduler` auto-parallelism bounds rescale TaskManager pod demand in `native` mode and change slot demand without updating `spec.job.parallelism`. | Set `spec.mode: standalone` and `spec.taskManager.replicas` explicitly |

```yaml
spec:
  jobManager:
    resource:
      cpu: 1.0
      memory: "2048m"
  taskManager:
    replicas: 2
    resource:
      cpu: 1.0
      memory: "2048m"
```

### d. Stateful preemption and upgradeMode

When a higher-priority workload preempts or evicts a running `FlinkDeployment`, Kueue sets `spec.job.state: suspended` and **preserves your configured `spec.job.upgradeMode`** (`stateless`, `last-state`, or `savepoint`) without overwriting it:
- While the Flink Operator is taking a savepoint or tearing down pods, Kueue holds the evicted Workload's quota while `status.jobManagerDeploymentStatus` indicates an active deployment (`READY`, `DEPLOYING`, `DEPLOYED_NOT_READY`, or `ERROR`), `status.taskManager.replicas` is greater than `0`, or `status.jobStatus.state` is non-terminal (`INITIALIZING`, `CREATED`, `RUNNING`, `FAILING`, `CANCELLING`, `RESTARTING`, or `RECONCILING`). Quota is released once `status.jobManagerDeploymentStatus` is `MISSING` (or unset), `status.taskManager.replicas` is `0` (or `status.taskManager` is omitted), and `status.jobStatus.state` is terminal or empty (`FINISHED`, `FAILED`, `CANCELED`, `SUSPENDED`, or unset), regardless of `status.lifecycleState`, so a deployment the operator leaves parked in `UPGRADING` without pods does not hold quota. In `native` mode (where TaskManager pods are owned by the JobManager `Deployment`), Kueue also releases an evicted Workload's quota when a deployed or rolled-back resource enters `status.lifecycleState: FAILED` with `status.jobManagerDeploymentStatus: MISSING`, even if `status.taskManager.replicas` or `status.jobStatus.state: RECONCILING` remains stale after an unrecoverable JobManager or HA failure.
- To use stateful preemption with `upgradeMode: savepoint` or `upgradeMode: last-state`, configure durable shared storage (such as object storage or a `ReadWriteMany` PersistentVolumeClaim) in `spec.flinkConfiguration` as described in the [Flink Operator job management guide](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/docs/managing/job-management/):

```yaml
spec:
  flinkConfiguration:
    state.checkpoints.dir: s3://my-flink-bucket/checkpoints
    state.savepoints.dir: s3://my-flink-bucket/savepoints
    execution.checkpointing.interval: 60s
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 4
    upgradeMode: savepoint
    state: running
```

### e. Workload readiness and completion

- **Readiness (`PodsReady`)**: Kueue marks the `PodsReady` condition `True` on the `Workload` when `status.lifecycleState` is `STABLE`, `status.jobManagerDeploymentStatus` is `READY`, and `status.jobStatus.state` is `RUNNING`. Because Apache Flink transitions `JobStatus` to `RUNNING` as soon as the JobManager submits the `ExecutionGraph` while TaskManager pods may still be registering task slots (up to Flink's `slot.request.timeout`), requiring `status.lifecycleState: STABLE` waits for the Flink Operator's deployment stability verification before Kueue marks `PodsReady` `True`. When [All-or-nothing with ready Pods (`waitForPodsReady`)](/docs/tasks/manage/setup_wait_for_pods_ready/) is enabled, size the `waitForPodsReady` timeout to cover JobManager startup, TaskManager slot allocation, and Flink Operator stability checks.
- **Completion (`Finished`) vs. continuous streaming**: Bounded batch jobs that complete with `status.jobStatus.state: FINISHED` mark the Kueue `Workload` `Finished` (`Succeeded`) and release quota. Deployments whose job reaches `status.jobStatus.state` `FAILED` or `CANCELED`, or whose post-deployment `status.lifecycleState` becomes `FAILED` with no active pods remaining (such as `native` mode with `status.jobManagerDeploymentStatus: MISSING` after unrecoverable JobManager or HA metadata loss while `status.jobStatus.state` remains `RECONCILING`), mark the `Workload` `Finished` (`Failed`); post-deployment `status.lifecycleState: FAILED` continues holding quota while `status.jobManagerDeploymentStatus` is `ERROR` or when `spec.mode` is `standalone` with active TaskManager pods. Kueue honors these terminal states only once the Flink Operator reports the current spec as deployed (`status.reconciliationStatus.state: DEPLOYED` or `ROLLED_BACK`). The operator also writes `FINISHED` or `FAILED` while suspending, before each deployment attempt (`status.reconciliationStatus.state: UPGRADING`, also shown as `status.lifecycleState: UPGRADING` after every un-suspend and in-place upgrade such as a `spec.restartNonce` bump), and while a never-deployed resource is rejected by operator validation (`status.lifecycleState: FAILED` with `status.reconciliationStatus.state` unset or `UPGRADING`); none of those states finish the `Workload`, because the operator can still deploy the job. Continuous streaming jobs stay `RUNNING` indefinitely and retain their admitted ClusterQueue quota until the `Workload` is deactivated (`spec.active: false`, see section b), preempted, or the `FlinkDeployment` is deleted.
- **Completed and failed jobs**: Once Kueue marks a `Workload` `Finished` (or a deployed `FlinkDeployment` reaches a terminal `FINISHED`, `FAILED`, or `CANCELED` job state), Kueue finalizes the `Workload` and stops reconciling the `FlinkDeployment` while it remains finished (in addition to rejecting `kubernetes.operator.job.restart.failed: "true"` on all Kueue-managed deployments and enforcing running-state PodSet and label immutability while `spec.job.state` is `running`). Do not suspend and mutate a completed `FlinkDeployment` in place to rerun it; configure `kubernetes.operator.jm-deployment.shutdown-ttl` or delete the `FlinkDeployment` to tear down retained pods, and submit a new `FlinkDeployment` when you want Kueue to schedule a new run.
- **JobManager retention after completion (`kubernetes.operator.jm-deployment.shutdown-ttl`)**: After a terminal job state the Flink Operator keeps the JobManager `Deployment` (and, in `standalone` mode, the TaskManager `Deployment`) running until `kubernetes.operator.jm-deployment.shutdown-ttl` (default `1d`) elapses past `status.jobStatus.updateTime`, so that the REST API stays reachable for inspection. Kueue releases quota when the job finishes, so those pods run outside ClusterQueue accounting for the TTL. For Kueue-managed batch jobs set a short TTL in `spec.flinkConfiguration`, for example `kubernetes.operator.jm-deployment.shutdown-ttl: 1m`, or set it operator-wide in the Flink Operator configuration.

### f. Limitation: interaction with the deployment and pod integrations

The Apache Flink Kubernetes Operator creates the JobManager `Deployment` (and, in `standalone` mode, the TaskManager `Deployment`) with an OwnerReference to the `FlinkDeployment` that has `controller: false`. Kueue resolves the parent of a child object only through its controller OwnerReference, so in Alpha it cannot recognize those Deployments and their Pods as children of a `FlinkDeployment`. When the `deployment` or `pod` integration is active in the same namespace as Flink resources (both are enabled by default, and `deployment`, `statefulset`, and `leaderworkerset.x-k8s.io/leaderworkerset` also implicitly enable `pod` and its mutating webhook), Kueue's Deployment and Pod webhooks treat those children as standalone jobs:

- With `manageJobsWithoutQueueName: true` (and no `default` `LocalQueue`), the child Deployment and its Pods are gated behind a second `Workload` without a queue, which never gets admitted and deadlocks the `FlinkDeployment`.
- With a `default` `LocalQueue` in the namespace, the child Deployment and Pods receive `kueue.x-k8s.io/queue-name: default`, double-charging quota already reserved by the `FlinkDeployment` Workload or deadlocking admission.
- Children of unmanaged Session clusters (`spec.job` omitted without `kueue.x-k8s.io/queue-name`) are intercepted the same way, even though the `FlinkDeployment` itself is untouched.

Use one of the following mitigations until a later release adds non-controller OwnerReference matching for Flink children:

- Keep `manageJobsWithoutQueueName: false` (labeling `FlinkDeployment` resources explicitly with `kueue.x-k8s.io/queue-name`) **and** do not create a `LocalQueue` named `default` in namespaces running Flink.
- Remove `pod`, `deployment`, `statefulset`, and `leaderworkerset.x-k8s.io/leaderworkerset` from `integrations.frameworks` when none of those pod-based integrations are needed (leaving `statefulset` or `leaderworkerset.x-k8s.io/leaderworkerset` enabled in `integrations.frameworks` keeps the `pod` mutating webhook active even when `pod` and `deployment` are removed).

`managedJobsNamespaceSelector` gates all Kueue reconciliation (and, with the Helm chart, all Kueue webhooks) in a namespace, so Flink namespaces must match it for the FlinkDeployment integration to work; excluding them is not a mitigation. See [Opt-in namespace management](/docs/tasks/manage/enforce_job_management/opt_in_namespace_management/).

## Run a sample FlinkDeployment

Here is a sample Application-mode `FlinkDeployment`:

{{< include "examples/jobs/sample-flinkdeployment-streaming.yaml" "yaml" >}}

1. Create the `FlinkDeployment`:

```shell
kubectl create -f sample-flinkdeployment-streaming.yaml
```

2. Inspect the `FlinkDeployment` and the generated Kueue `Workload`:

```shell
kubectl get flinkdeployments
```

The output is similar to:

```text
NAME                  JOB STATUS   LIFECYCLE STATE
flink-streaming-job   RUNNING      STABLE
```

Check the `Workload` status:

```shell
kubectl get workloads
```

The output is similar to:

```text
NAME                                              QUEUE        RESERVED IN     ADMITTED   FINISHED   AGE
flinkdeployment-flink-streaming-job-7f6b9         user-queue   cluster-queue   True                  30s
```

3. Stop and delete the continuous streaming `FlinkDeployment` when you no longer need it:

```shell
kubectl delete -f sample-flinkdeployment-streaming.yaml
```
