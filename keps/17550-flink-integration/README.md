# KEP-17550: Apache Flink FlinkDeployment Integration

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
    - [Story: Dedicated Quotas for Production Streaming Pipelines](#story-dedicated-quotas-for-production-streaming-pipelines)
    - [Story: Stateful Preemption via Flink Savepoints](#story-stateful-preemption-via-flink-savepoints)
  - [Risks and Mitigations](#risks-and-mitigations)
    - [Risk: Operator Dependency and CRD Availability](#risk-operator-dependency-and-crd-availability)
    - [Risk: Savepoint Preemption Latency](#risk-savepoint-preemption-latency)
- [Design Details](#design-details)
  - [API and Framework Configuration](#api-and-framework-configuration)
  - [PodSet Composition](#podset-composition)
  - [Suspension and Admission Protocol](#suspension-and-admission-protocol)
  - [Workload Readiness](#workload-readiness)
  - [Test Plan](#test-plan)
    - [Unit Tests](#unit-tests)
    - [Integration Tests](#integration-tests)
    - [E2E Validation](#e2e-validation)
  - [Graduation Criteria](#graduation-criteria)
    - [Alpha (v0.21)](#alpha-v021)
    - [Beta (v0.22)](#beta-v022)
    - [Stable / GA (v0.24)](#stable--ga-v024)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

This proposal outlines the integration of Apache Flink workloads into Kueue using the official Apache Flink Kubernetes Operator (`flinkdeployments.flink.apache.org/v1beta1`). The integration enables multi-tenant batch and streaming clusters to schedule Flink application and session deployments alongside other distributed computing engines (such as Spark, Ray, and JobSet) under unified Kueue ClusterQueues, ResourceFlavors, cohorts, and priorities.

## Motivation

Apache Flink is widely adopted across enterprise organizations for mission-critical, large-scale stream processing and real-time stateful analytics. While traditional batch workloads terminate upon completion, streaming Flink pipelines run continuously (often 24/7) with dedicated JobManager and TaskManager allocations.

Running unmanaged Flink clusters on Kubernetes leads to resource fragmentation, lack of fair sharing across multi-tenant teams, and cluster over-subscription. Integrating Flink with Kueue allows platform operators to:
- Enforce strict resource quotas and lending across organizational units.
- Prioritize streaming and batch pipelines using Kueue WorkloadPriorityClass.
- Coordinate all-or-nothing scheduling across JobManager and TaskManager pods.
- Execute graceful, state-preserving preemption by triggering Flink savepoints before releasing resources.

### Goals

- Support `FlinkDeployment` CRD (`flink.apache.org/v1beta1`) as a first-class workload framework in Kueue.
- Define a two-tier PodSet mapping: a `jobmanager` PodSet (1 replica) and a `taskmanager` PodSet (`replicas` count).
- Support suspension and admission via the declarative `spec.job.state` field (`suspended` vs `running`).
- Provide graceful preemption and eviction by taking a Flink savepoint (`spec.job.upgradeMode: savepoint`) before revoking quota.
- Implement workload readiness tracking (`PodsReady`) based on Flink JobManager deployment readiness and Flink job running state.
- Support both Flink Application mode and Flink Session Cluster mode.

### Non-Goals

- Managing `FlinkSessionJob` objects directly in Phase 1 (session jobs run inside pre-allocated Flink Session clusters).
- Dynamic autoscaling of TaskManagers within an active Kueue admission slice (Kueue reserves the peak configured replica count).
- Managing Flink external storage volumes or checkpoint / savepoint storage buckets directly.

## Proposal

We propose introducing a built-in Kueue controller and webhook package for Apache Flink deployments: `pkg/controller/jobs/flinkdeployment`. The controller implements Kueue's standard `GenericJob` interface, registering under the feature gate `FlinkDeploymentIntegration`.

### User Stories

#### Story: Dedicated Quotas for Production Streaming Pipelines
Platform administrators run continuous Flink streaming pipelines that process event streams. Administrators want to guarantee that mission-critical pipelines receive dedicated quota in a primary ResourceFlavor, while non-production or lower-priority Flink jobs queue safely until capacity is freed.

#### Story: Stateful Preemption via Flink Savepoints
During cluster resource pressure, a higher-priority workload preempts a lower-priority Flink streaming deployment. Instead of terminating task pods abruptly, Kueue requests suspension with `upgradeMode: savepoint`. The Flink Operator captures the pipeline state to durable storage before releasing the pods and quota. When resources become available again, Kueue un-suspends the deployment, and Flink resumes seamlessly from the saved checkpoint.

### Risks and Mitigations

#### Risk: Operator Dependency and CRD Availability
The Apache Flink Operator is an external third-party controller. If its CRD is not registered in the cluster, Kueue could fail during informer registration.
*Mitigation*: The Flink integration controller is gated by the `FlinkDeploymentIntegration` feature gate and only enabled when explicitly listed in Kueue `Configuration.integrations.frameworks`. When the CRD is absent, Kueue issues an informational log and safely ignores the uninstalled resource.

#### Risk: Savepoint Preemption Latency
Generating a savepoint on large streaming states (hundreds of gigabytes) can take several minutes. If Kueue marks the workload immediately stopped, underlying pods might be deleted before the savepoint writes complete.
*Mitigation*: The reconciler delegates stopping to `JobWithCustomStop`, setting `upgradeMode: savepoint` and `state: suspended` on the Flink CR. Kueue honors workload eviction grace periods, allowing the operator sufficient time to complete the savepoint.

## Design Details

### API and Framework Configuration

The Flink integration is guarded by the `FlinkDeploymentIntegration` feature gate:

```go
FlinkDeploymentIntegration featuregate.Feature = "FlinkDeploymentIntegration"
```

To activate the controller, administrators add `flink.apache.org/flinkdeployment` to the Kueue manager configuration:

```yaml
apiVersion: config.kueue.x-k8s.io/v1beta1
kind: Configuration
integrations:
  frameworks:
    - "flink.apache.org/flinkdeployment"
```

### PodSet Composition

Kueue represents each `FlinkDeployment` as two distinct PodSets:
1. `jobmanager`: Represents the coordination node.
   - Count: 1
   - PodSpec: Extracted from `spec.jobManager.podTemplate` merged with `spec.jobManager.resource` requests (CPU and memory).
2. `taskmanager`: Represents distributed execution workers.
   - Count: `spec.taskManager.replicas` (defaults to 1 if omitted).
   - PodSpec: Extracted from `spec.taskManager.podTemplate` merged with `spec.taskManager.resource` requests.

If `spec.podTemplate` is defined at the deployment root, it serves as the base template, overridden by specific JobManager or TaskManager pod templates according to Flink Operator merge semantics.

### Suspension and Admission Protocol

1. Creation: The mutating admission webhook intercepts newly submitted `FlinkDeployment` resources with a `kueue.x-k8s.io/queue-name` label.
2. In Application mode (`spec.job != nil`), the webhook defaults `spec.job.state` to `suspended`.
3. In Session mode (`spec.job == nil`), the webhook ensures the deployment is held in an initial suspended state until admitted.
4. Admission: When the Kueue scheduler admits the associated Workload, the controller patches `spec.job.state` to `running`, signaling the Flink Operator to spawn JobManager and TaskManager pods.
5. Eviction and Preemption: When a Workload is evicted or preempted, Kueue patches `spec.job.state` to `suspended` with `spec.job.upgradeMode: savepoint`.

### Workload Readiness

Kueue tracks workload readiness through the `JobWithPodsReady` interface. A Flink deployment is considered ready when:
- `status.jobManagerDeploymentStatus == "READY"`
- `status.jobStatus.state == "RUNNING"` (for Application mode)

### Test Plan

#### Unit Tests
Unit tests in `pkg/controller/jobs/flinkdeployment/` verify:
- PodSet extraction across Application and Session modes.
- Mutating webhook validation, queue defaulting, and suspension enforcement.
- Custom stop logic and savepoint upgrade mode patching.
- Target test coverage: >= 85%.

#### Integration Tests
Single-cluster integration tests in `test/integration/singlecluster/controller/jobs/flinkdeployment/` verify:
- Complete Workload lifecycle: queueing, admission, un-suspension, and completion.
- Priority preemption and savepoint patching.
- PodsReady condition transitions under simulated status updates.

#### E2E Validation
- Validation on Kubernetes / GKE clusters running the official Apache Flink Kubernetes Operator.
- Verification of actual streaming job submission, Workload admission, pod scaling, and savepoint generation on suspension.

### Graduation Criteria

#### Alpha (v0.21)
- Feature gate `FlinkDeploymentIntegration` introduced (disabled by default).
- Support for `FlinkDeployment` v1beta1 Application and Session modes.
- Complete unit and envtest integration test coverage.
- Task documentation and example manifests published.

#### Beta (v0.22)
- Feature gate enabled by default.
- Integration test coverage for TAS and ResourceFlavor fallback.
- Production readiness review (PRR) metrics: tracking admitted and preempted Flink deployments.

#### Stable / GA (v0.24)
- General availability with no breaking API changes.
- Conformance testing across multiple Flink Kubernetes Operator versions (v1.8+).

## Implementation History

- 2026-11-06: Initial KEP-17550 drafted and prototype implementation verified.

## Drawbacks

- Introduces additional informer cache overhead for the `FlinkDeployment` CRD when enabled.
- Requires operator administrators to ensure checkpoint storage is accessible across potential preemption restarts.

## Alternatives

- Plain Pod integration: Submitting raw pods directly bypasses the Flink Kubernetes Operator lifecycle, losing automated restart, high-availability, and savepoint coordination.
- Custom External Workload: Using Kueue's generic external workload adapter requires users to maintain separate synchronization scripts and does not provide native `upgradeMode: savepoint` integration.
