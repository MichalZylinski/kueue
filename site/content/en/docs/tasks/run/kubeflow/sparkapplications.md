---
title: "Run a SparkApplication"
date: 2025-11-17
weight: 7
description: >
  Run a Kueue scheduled SparkApplication
---

{{< feature-state state="alpha" for_version="v0.17" >}}

This page shows how to leverage Kueue's scheduling and resource management capabilities when running [Spark Operator](https://github.com/kubeflow/spark-operator) SparkApplication.

This guide is for [batch users](/docs/tasks#batch-user) that have a basic understanding of Kueue. For more information, see [Kueue's overview](/docs/overview).

{{% alert title="Note" color="primary" %}}
`SparkApplicationIntegration` is currently an alpha feature and is disabled by default.

To enable it, the `SparkApplicationIntegration` feature gate needs to be activated, and `sparkoperator.k8s.io/sparkapplication` must be added as an allowed workload.

{{% /alert %}}

## Before you begin

Enable SparkApplication integration in Kueue. You can [modify Kueue configurations from installed releases](/docs/installation#install-a-custom-configured-released-version) to include `sparkoperator.k8s.io/sparkapplication` as an allowed workload.

Enable the `SparkApplicationIntegration` feature gate. Check the [installation](/docs/installation/#change-the-feature-gates-configuration) guide for details on feature gate configuration.

Check [administer cluster quotas](/docs/tasks/manage/administer_cluster_quotas) for details on the initial cluster setup.

Check [the Spark Operator installation guide](https://www.kubeflow.org/docs/components/spark-operator/getting-started/#installation).

{{% alert title="Note" color="primary" %}}
In order to use SparkApplication integration, you must install [Spark Operator](https://github.com/kubeflow/spark-operator) [v2.4.0](https://github.com/kubeflow/spark-operator/releases/tag/v2.4.0) or above.

Please also remember belows:
- You will have to activate namespaces that you will deploy SparkApplication as described in [the official installation docs](https://www.kubeflow.org/docs/components/spark-operator/getting-started/#about-spark-job-namespaces).
- You will have to create spark serviceaccount and attach a proper role beforehand. Please refer to [Spark Operator's Getting Started Guide](https://www.kubeflow.org/docs/components/spark-operator/getting-started/#about-the-service-account-for-driver-pods) for details.
{{% /alert %}}

{{% alert title="Note" color="primary" %}}
In order to use SparkApplication, prior to v0.8.1, you need to restart Kueue after the installation.
You can do it by running: `kubectl delete pods -l control-plane=controller-manager -n kueue-system`.
{{% /alert %}}

## Spark Operator definition


### a. Queue selection

The target [local queue](/docs/concepts/local_queue) should be specified in the `metadata.labels` section of the SparkApplication configuration.

```yaml
metadata:
  labels:
    kueue.x-k8s.io/queue-name: user-queue
```

{{% alert title="Note" color="primary" %}}
By default, SparkApplication integration does not support [Spark's dynamic resource allocation](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation). If you set `spec.dynamicAllocation.enabled=true` on a SparkApplication that is not opted into [elastic scaling](#elastic-scaling-with-dynamic-allocation), Kueue rejects it in the webhook.
{{% /alert %}}

### b. Optionally set Suspend field in SparkOperation

```yaml
spec:
  suspend: true
```

By default, Kueue will set `suspend` to true via webhook and unsuspend it when the SparkApplication is admitted.

## Elastic scaling with dynamic allocation

{{< feature-state state="alpha" for_version="v0.19" >}}

Kueue can track and enforce quota for a SparkApplication that uses Spark's own
[dynamic resource allocation](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation),
scaling the number of executors up and down at runtime without suspending or resubmitting the
application. This builds on [Elastic Workloads](/docs/concepts/elastic_workload), Kueue's general
mechanism for in-place workload resizing.

### Enabling elastic scaling

Elastic scaling requires both the `ElasticJobsViaWorkloadSlices` feature gate (see the
[installation guide](/docs/installation/#change-the-feature-gates-configuration)) and an explicit
opt-in on the SparkApplication itself:

```yaml
metadata:
  annotations:
    kueue.x-k8s.io/elastic-job: "true"
```

An elastic SparkApplication must also:

- Set `spec.dynamicAllocation.enabled: true` and `spec.dynamicAllocation.maxExecutors` (required — it
  bounds how much quota the application can eventually claim as it scales).
- Set `spec.restartPolicy.type: Never` (or leave `restartPolicy` unset). Kueue owns retries for
  managed jobs; an operator-driven restart (`Always` or `OnFailure`) would resubmit the application
  after Kueue has already released its quota, leaving the retry unmanaged.
- Not set `spec.batchScheduler`. Delegating gang scheduling to another scheduler conflicts with
  Kueue's own admission control over the same pods.

The webhook validates all of the above and rejects a create/update that violates them.

### How it works

Unlike frameworks whose autoscaler mutates the resource spec directly (for example
[RayCluster](/docs/tasks/run/rayclusters)), a Spark driver creates and deletes executor
pods on its own, without ever touching the SparkApplication spec. Kueue therefore derives the
executor count from **observed executor pod demand** rather than from the spec:

- Every driver and executor pod is created with the `kueue.x-k8s.io/elastic-job` scheduling gate,
  so a newly created executor pod cannot start running until Kueue admits the additional quota it
  represents.
- When the driver ramps up and creates more executor pods, Kueue creates a new Workload slice
  representing the larger executor count. Once that slice is admitted, the new pods are ungated and
  the previous slice is marked finished.
- When the driver scales down and deletes idle executor pods, Kueue updates the admitted Workload in
  place and releases the corresponding quota immediately — no new slice is needed.
- If a scale-up cannot be admitted (the queue has no spare capacity), the new executor pods simply
  stay gated and the application keeps running at its current size. Spark's own
  `spark.kubernetes.allocation.executor.timeout` (default 600s) eventually deletes pods that were
  never scheduled, and the request shrinks back down on its own.

### Tuning for queue-friendly scaling

Because Kueue only admits a scale-up after it observes the driver's newly created (gated) executor
pods, consider tuning Spark's own allocation pacing so that ramp-ups arrive in reasonably sized,
infrequent batches rather than a rapid burst — see
`spark.dynamicAllocation.schedulerBacklogTimeout`, `spark.dynamicAllocation.executorAllocationRatio`,
and `spark.kubernetes.allocation.batch.size` in the
[Spark configuration reference](https://spark.apache.org/docs/latest/configuration.html).

### Limitations

- MultiKueue is not yet supported for elastic SparkApplications.
- Only `unconstrained` [Topology Aware Scheduling](/docs/concepts/topology_aware_scheduling) mode is
  supported; an elastic SparkApplication with a required or preferred topology annotation on the
  driver or executor is rejected.
- Editing `spec.executor.instances` remains a full application restart (the spark-operator resubmits
  the application on any spec change) — it is not part of elastic scaling.

### Sample elastic SparkApplication

{{< include "examples/jobs/sample-sparkapplication-elastic.yaml" "yaml" >}}

### Watching it scale

After applying the sample above, the initial Workload is admitted with `driver: 1` and
`executor: 2` (the `initialExecutors` value) in `spec.podSets`:

```sh
kubectl get workload -l kueue.x-k8s.io/job-uid=$(kubectl get sparkapplication spark-pi-elastic -o jsonpath='{.metadata.uid}') \
  -o jsonpath='{.items[0].spec.podSets[*].count}'
```

As the driver ramps up executors under task backlog, a replacement Workload slice is created and
admitted with the larger executor count, and the ClusterQueue's reported usage grows to match:

```sh
kubectl get clusterqueue <your-cluster-queue> -o jsonpath='{.status.flavorsUsage[0].resources[?(@.name=="cpu")].total}'
```

As executors go idle and Spark deletes them, the admitted Workload's executor count — and the
ClusterQueue's reported usage — shrink back down in place, without a new slice or any disruption to
the running driver. This exact sequence (admit → scale up → scale down, with ClusterQueue usage
asserted at each step) is covered by an automated integration test in
`test/integration/singlecluster/controller/jobs/sparkapplication/sparkapplication_elastic_test.go`.

## Sample SparkApplication

{{< include "examples/jobs/sample-sparkapplication.yaml" "yaml" >}}
