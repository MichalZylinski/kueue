---
title: "Setup Lifespan-Aware Scheduling"
linkTitle: "Setup Lifespan-Aware Scheduling"
date: 2026-09-12
weight: 6
description: >
  Run jobs on time-bounded capacity by configuring a lifespan source, a lifespan policy and a fallback flavor.
---

This page shows how to set up
[Lifespan-Aware Scheduling (LAS)](/docs/concepts/lifespan_aware_scheduling), so
that Workloads are only placed on time-bounded nodes that will outlive them, and
fall back to durable capacity when they will not.

The intended audience for this page are [batch administrators](/docs/tasks#batch-administrator),
with a final step showing what a [batch user](/docs/tasks#batch-user) has to do to
submit a Workload once the cluster is configured.

## Before you begin

Make sure the following conditions are met:

- A Kubernetes cluster is running, and the kubectl command-line tool is installed.
- [Kueue is installed](/docs/installation) in version v0.21 or newer.
- The `LifespanAwareScheduling` feature gate is enabled. It is alpha and disabled
  by default; see the
  [Installation guide](/docs/installation/#change-the-feature-gates-configuration).
- Your time-bounded nodes carry some node metadata from which their remaining
  operational time can be derived, and are distinguishable from your durable nodes
  by a node label. If they are not, see
  [Configure Lifespan Sources](/docs/tasks/manage/configure_lifespan_sources/)
  first.

Throughout this guide, `example.com/leased: "true"` marks the time-bounded nodes
and `example.com/max-run-duration-seconds` holds the lease length published by the
platform. Replace both with whatever your provider actually uses.

## Step 1: Apply the setup

The setup below creates every object at once. The individual pieces are explained
in the steps that follow.

{{< include "examples/lifespan/lifespan-setup.yaml" "yaml" >}}

```bash
kubectl apply -f https://kueue.sigs.k8s.io/examples/lifespan/lifespan-setup.yaml
```

## Step 2: Understand the lifespan source

The `NodeLifespanConfig` object tells Kueue how to turn platform-specific node
metadata into the neutral node label
`kueue.x-k8s.io/node-remaining-time-seconds`:

```yaml
apiVersion: kueue.x-k8s.io/v1alpha1
kind: NodeLifespanConfig
metadata:
  name: "default"
spec:
  sources:
  - name: leased-capacity
    appliesTo:
      matchLabels:
        example.com/leased: "true"
    mode: RelativeToNodeCreation
    from:
      label: example.com/max-run-duration-seconds
    format: DurationSeconds
```

Verify that the nodes are being labelled:

```bash
kubectl get nodes -L example.com/leased,kueue.x-k8s.io/node-remaining-time-seconds
```

The output is similar to:

```text
NAME      STATUS   ROLES    AGE   VERSION   LEASED   NODE-REMAINING-TIME-SECONDS
node-1    Ready    <none>   3h    v1.34.0   true     75600
node-2    Ready    <none>   21h   v1.34.0   true     10800
node-3    Ready    <none>   14d   v1.34.0   false
```

`node-3` is a durable node outside the lifespan-managed flavor, so it is expected
to have no value. An **empty value on a node that is inside a lifespan-managed
flavor** means that node will never be selected — see
[Troubleshooting Lifespan-Aware Scheduling](/docs/tasks/troubleshooting/troubleshooting_lifespan/).

## Step 3: Understand the policy and the admission check

The `LifespanPolicy` holds the admission-side tunables, and the `AdmissionCheck`
binds them to the LAS controller:

```yaml
apiVersion: kueue.x-k8s.io/v1alpha1
kind: LifespanPolicy
metadata:
  name: "default-lifespan-policy"
spec:
  safetyBuffer: 30m
  minimumWindow: 0s
  fallbackPolicy: AllowWithDefault
  defaultDuration: 2h
  expiringTaintThreshold: 15m
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: AdmissionCheck
metadata:
  name: "lifespan-aware-scheduling"
spec:
  controllerName: kueue.x-k8s.io/lifespan-aware-scheduling
  parameters:
    apiGroup: kueue.x-k8s.io
    kind: LifespanPolicy
    name: "default-lifespan-policy"
```

With these values, a Workload estimated at 4 hours needs
`4h + 30m + 300s = 4h35m` of remaining time on a node, and a node must have
*strictly more* than that to be selected.

Confirm the check is active before continuing:

```bash
kubectl get admissioncheck lifespan-aware-scheduling -o jsonpath='{.status.conditions[?(@.type=="Active")]}' | jq
```

## Step 4: Understand the queues and the fallback flavor

Two flavors are defined: `leased-gpu` for the time-bounded nodes and
`on-demand-gpu` for durable ones. The AdmissionCheck is attached to the
time-bounded flavor only:

```yaml
  admissionChecksStrategy:
    admissionChecks:
    - name: "lifespan-aware-scheduling"
      onFlavors: ["leased-gpu"]
```

{{% alert title="Note" color="primary" %}}
A flavor becomes lifespan-managed purely by being listed in `onFlavors`; there is
nothing on the ResourceFlavor object itself that says so. Keep the flavor's
`nodeLabels` and the `appliesTo` selectors of your lifespan sources in sync, or
you will end up with nodes in a managed flavor that no source ever labels — and
those nodes are permanently ineligible.
{{% /alert %}}

The order of flavors in `spec.resourceGroups[*].flavors` determines the fallback
order: `leased-gpu` is attempted first, and a `Retry` from the lifespan check
sends the Workload to `on-demand-gpu`.

## Step 5: Submit a Workload

As a batch user, annotate the Job with the expected duration of a single execution
attempt:

{{< include "examples/lifespan/sample-job.yaml" "yaml" >}}

```bash
kubectl create -f https://kueue.sigs.k8s.io/examples/lifespan/sample-job.yaml
```

If your platform team provides a mutating admission webhook that infers durations,
the annotation is injected for you and users submit ordinary Jobs. Workloads
without the annotation fall back to `defaultDuration` under the
`AllowWithDefault` policy used in this setup.

## Step 6: Observe the decision

Inspect the admission check state on the Workload:

```bash
WORKLOAD=$(kubectl get workload -o name --sort-by='.metadata.creationTimestamp' | tail -n 1)
kubectl describe $WORKLOAD
```

When the runway is sufficient, the output contains the injected affinity and a
`Ready` state:

```text
Status:
  Admission Checks:
    Last Transition Time:  2026-09-12T09:14:02Z
    Message:               Admitted to flavor leased-gpu with required lifespan 16500s <= available 75600s.
    Name:                  lifespan-aware-scheduling
    Reason:                PlacementVerified
    Pod Set Updates:
      Name:  main
      Node Affinity:
        Required During Scheduling Ignored During Execution:
          Node Selector Terms:
            Match Expressions:
              Key:       kueue.x-k8s.io/node-remaining-time-seconds
              Operator:  Gt
              Values:    16500
    State:                 Ready
```

When it is not, the check reports `Retry` and Kueue moves on to the next flavor:

```text
    Message:  Flavor leased-gpu has insufficient remaining lifespan (10800s available < 16500s required). Attempting multi-flavor fallback.
    Reason:   FlavorLifespanInsufficient
    State:    Retry
```

You can confirm where the Workload ended up:

```bash
kubectl get $WORKLOAD -o jsonpath='{.status.admission.podSetAssignments[0].flavors}'
```

## Step 7: Watch capacity age out

Nothing needs to be done as nodes age; the behavior is worth observing once,
though, because it is the point of the feature:

- As remaining time falls, long Workloads stop being admitted to `leased-gpu` and
  are redirected to `on-demand-gpu`, while short ones keep packing onto the
  aging nodes.
- Below `expiringTaintThreshold`, Kueue taints the node with
  `kueue.x-k8s.io/expiring-node:NoSchedule` and nothing new binds to it.
- Running Workloads are never evicted by LAS; the node quiesces as they finish.

```bash
kubectl get nodes -L kueue.x-k8s.io/node-remaining-time-seconds
kubectl get events --field-selector reason=NodeMissingLifespanLabel
```

## What's next

- Read the [Lifespan-Aware Scheduling](/docs/concepts/lifespan_aware_scheduling) concept page.
- Describe more capacity classes in [Configure Lifespan Sources](/docs/tasks/manage/configure_lifespan_sources/).
- Diagnose problems with [Troubleshooting Lifespan-Aware Scheduling](/docs/tasks/troubleshooting/troubleshooting_lifespan/).
- Learn how [Admission Checks](/docs/concepts/admission_check/) interact with [flavor fungibility](/docs/concepts/cluster_queue/#flavorfungibility).
