---
title: "Run a FlinkDeployment"
linkTitle: "FlinkDeployments"
date: 2026-11-06
weight: 12
description: >
  Run a Kueue scheduled Apache Flink Deployment
---

{{< feature-state state="alpha" for_version="v0.21" >}}

This page shows how to leverage Kueue's scheduling, quota management, and preemption capabilities when running [Apache Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/) FlinkDeployments.

This guide is for [batch users](/docs/tasks#batch-user) that have a basic understanding of Kueue. For more information, see [Kueue's overview](/docs/overview).

{{% alert title="Note" color="primary" %}}
`FlinkDeploymentIntegration` is currently an alpha feature and is disabled by default.

To enable it, the `FlinkDeploymentIntegration` feature gate must be activated, and `flink.apache.org/flinkdeployment` must be included in `Configuration.integrations.frameworks`.
{{% /alert %}}

## Before you begin

1. Ensure you are using Kueue v0.21.0 or newer and Apache Flink Kubernetes Operator v1.8.0 or newer.

2. Check [Administer cluster quotas](/docs/tasks/manage/administer_cluster_quotas) for details on the initial Kueue setup.

3. Enable the `FlinkDeploymentIntegration` feature gate. Check the [installation](/docs/installation/#change-the-feature-gates-configuration) guide for details on feature gate configuration.

4. Enable FlinkDeployment integration in Kueue. You can [modify Kueue configurations from installed releases](/docs/installation#install-a-custom-configured-released-version) to include `flink.apache.org/flinkdeployment` as an allowed workload framework:

```yaml
apiVersion: config.kueue.x-k8s.io/v1beta1
kind: Configuration
integrations:
  frameworks:
    - "flink.apache.org/flinkdeployment"
```

## FlinkDeployment definition

When running [FlinkDeployments](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/overview/) on Kueue, take into consideration the following aspects:

### Queue selection

The target [local queue](/docs/concepts/local_queue) should be specified in the `metadata.labels` section of the FlinkDeployment configuration:

```yaml
metadata:
  labels:
    kueue.x-k8s.io/queue-name: user-queue
```

### Suspend control

Kueue manages the execution state of the Flink cluster via `spec.job.state` (for Application mode) or cluster-level lifecycle management (for Session mode):
- When a FlinkDeployment is submitted, Kueue defaults `spec.job.state` to `suspended`.
- When admitted by Kueue, Kueue un-suspends the deployment by patching `spec.job.state` to `running`.
- If preempted or evicted, Kueue requests suspension with `upgradeMode: savepoint` so the Flink Operator takes a stateful savepoint before releasing pods.

### Resource requests and PodSet composition

Kueue represents each FlinkDeployment as two distinct PodSets:
- `jobmanager`: Represents the JobManager coordination pod (count: 1).
- `taskmanager`: Represents the TaskManager worker pods (count: `spec.taskManager.replicas`, default 1).

Kueue calculates resource requests by combining explicit resource requests in `spec.jobManager.resource` and `spec.taskManager.resource` with container templates in `spec.jobManager.podTemplate`, `spec.taskManager.podTemplate`, or the root `spec.podTemplate`.

```yaml
spec:
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1.0
  taskManager:
    replicas: 2
    resource:
      memory: "2048m"
      cpu: 1.0
```

### Workload readiness

Kueue tracks workload readiness through the `PodsReady` condition on the Workload object. A FlinkDeployment is considered ready when:
- `status.jobManagerDeploymentStatus` is `READY`.
- `status.jobStatus.state` is `RUNNING` (for Application mode).

When `waitForPodsReady` is configured on the ClusterQueue, Kueue waits for the Flink cluster to reach this state before considering admission complete.

### Stateful preemption and savepoints

When a higher-priority workload preempts an active Flink streaming deployment, Kueue invokes its custom stop handler. The controller patches:
- `spec.job.state: suspended`
- `spec.job.upgradeMode: savepoint`

The Apache Flink Kubernetes Operator takes a stateful savepoint to durable storage (configured via Flink state backend configuration) before tearing down JobManager and TaskManager pods. When Kueue un-suspends the workload, Flink automatically restores pipeline state from the latest savepoint.

## Example: Streaming Application Deployment

In Application mode, the Flink application jar is submitted directly as part of the FlinkDeployment specification. The cluster terminates when the application completes (for bounded batch jobs) or runs continuously (for unbounded streaming jobs).

{{< include "examples/jobs/sample-flinkdeployment-streaming.yaml" "yaml" >}}

## Example: Session Cluster Deployment

In Session mode (`spec.job` omitted), the FlinkDeployment creates a long-running Flink session cluster that accepts ad-hoc Flink jobs submitted via the Flink CLI or REST API.

{{< include "examples/jobs/sample-flinkdeployment-session.yaml" "yaml" >}}
