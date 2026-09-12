---
title: "Troubleshooting Lifespan-Aware Scheduling"
linkTitle: "Troubleshooting Lifespan-Aware Scheduling"
date: 2026-09-12
weight: 7
description: >
  Diagnose Workloads that are not admitted onto time-bounded capacity
---

This document helps you troubleshoot
[Lifespan-Aware Scheduling (LAS)](/docs/concepts/lifespan_aware_scheduling).

LAS is **fail-closed**: when the information it needs is missing, it refuses to
place Workloads rather than placing them on nodes that might disappear mid-run.
The practical consequence is that a broken setup usually presents as *"nothing is
ever admitted to this flavor"* rather than as an obvious error. Most of this page
is about telling that case apart from a correct refusal.

## Before you begin

Make sure the following conditions are met:

- The `LifespanAwareScheduling` feature gate is enabled.
- Your ClusterQueue and LocalQueue are otherwise healthy. See
  [Troubleshooting Queues](/docs/tasks/troubleshooting/troubleshooting_queues/).
- The Workload has reserved quota. LAS runs *after* quota reservation, so a
  Workload without the `QuotaReserved` condition is not blocked by LAS. See
  [Troubleshooting Jobs](/docs/tasks/troubleshooting/troubleshooting_jobs/#identifying-the-workload-for-your-job)
  to identify the Workload for your Job.

## What is the state of the lifespan admission check?

```bash
kubectl describe workload WORKLOAD_NAME
```

| State | Reason | Meaning |
| --- | --- | --- |
| `Ready` | `PlacementVerified` | Enough nodes in the flavor have the required runway; node affinity was injected. |
| `Retry` | `FlavorLifespanInsufficient` | Not enough nodes in the flavor have the required runway. Kueue releases the quota reservation and tries the next flavor. |
| `Rejected` | `MissingDurationAnnotation` | The Workload has no duration annotation and the policy uses `fallbackPolicy: StrictReject`. |
| `Pending` | — | The check has not run yet. If it stays `Pending`, verify the AdmissionCheck is `Active` and that its `controllerName` is `kueue.x-k8s.io/lifespan-aware-scheduling`. |

## My Workload keeps falling back, or never gets admitted

A `FlavorLifespanInsufficient` message states both sides of the comparison:

```text
Flavor leased-gpu has insufficient remaining lifespan (10800s available < 16500s required). Attempting multi-flavor fallback.
```

Work through the following, in order.

### a. Check the required runway, not just the estimate

The required runway is not the annotated duration. It is:

```text
T_required = max(estimated-duration + safetyBuffer + 300s, minimumWindow)
```

With the defaults (`safetyBuffer: 30m`, `minimumWindow: 0s`), a 20-minute Job
requires `20m + 30m + 300s = 55m` of node runway — already noticeably more than
the annotated duration.

If short Workloads are rejected from capacity that visibly has time left, check
whether someone has set a non-zero `minimumWindow` on the `LifespanPolicy`:

```bash
kubectl get lifespanpolicy POLICY_NAME -o jsonpath='{.spec.minimumWindow}'
```

A floor applies to every Workload on the flavor regardless of how short it is, so
`minimumWindow: 1h` makes a 20-minute Job demand a full hour of runway and strands
the remainder of every aging node. That defeats one of the main reasons to run the
feature. Unless you have a concrete reason to believe short placements are never
worth making, return it to `0s` and raise `safetyBuffer` instead, which is easier
to reason about.

### b. Remember that `Gt` is strict

The injected affinity uses `operator: Gt`, which is **strictly** greater than. A
node with exactly `16500` remaining does not satisfy a requirement of `16500`.
Pools therefore have to be sized with headroom above the longest expected job plus
its buffer; matching them exactly produces a flavor that looks like it should work
and never does.

### c. Check gang feasibility

A Workload requesting `K` nodes needs `K` nodes that each satisfy the runway. If
only `K-1` qualify, the check reports `Retry` even though some capacity is clearly
available. Compare the required node count with:

```bash
kubectl get nodes -L kueue.x-k8s.io/node-remaining-time-seconds
```

### d. Check that the nodes are actually labelled

See the next section. Unlabeled nodes never count toward the qualifying total.

## Nodes in a lifespan-managed flavor have no label

This is the single most common misconfiguration, and it is silent unless you look
for it.

A node whose `kueue.x-k8s.io/node-remaining-time-seconds` label is missing, empty,
negative, non-integer, or carries a unit suffix (`3600s`) cannot match a `Gt`
expression, so `kube-scheduler` excludes it and Kueue does not count it. The
flavor behaves as if it had no capacity at all.

Kueue surfaces this in two ways.

**A Prometheus gauge**, broken down by flavor:

```text
kueue_lifespan_unlabeled_nodes{flavor="leased-gpu"} 8
```

A persistently non-zero value means a broken or absent lifespan source. See
[Setup Prometheus](/docs/tasks/manage/observability/setup_prometheus) if metrics
are not being scraped yet.

**A warning event** on the affected nodes:

```bash
kubectl get events --field-selector reason=NodeMissingLifespanLabel
```

```text
Warning   NodeMissingLifespanLabel   node/node-7   Node node-7 in lifespan-managed flavor leased-gpu has no remaining-time label and is ineligible for placement. Verify the lifespan source is running.
```

To fix it, check the following:

1. **Does a source match these nodes?** Compare the `appliesTo` selector of each
   source in your `NodeLifespanConfig` with the actual node labels, and with the
   `nodeLabels` of the ResourceFlavor. A flavor is lifespan-managed because it is
   listed in `admissionChecksStrategy.admissionChecks[].onFlavors`, which is
   unrelated to source selectors — it is easy for the two sets to drift apart.
2. **Does the node carry the key the source reads?** If the platform omits the key
   on some nodes because an implicit default applies, set `defaultWhenAbsent` on
   the source so that the default is materialised. Do **not** set it if the absent
   key means the nodes are genuinely unbounded — see item 4.
3. **Is the value parseable in the configured `format`?** A present-but-unparseable
   value is an error, not an absent value: the reconcile logs it and stops, and
   `defaultWhenAbsent` does not apply. Check the Kueue controller logs for the
   offending node and key, and fix the value or the `format`.
4. **Are you mixing bounded and unbounded nodes in one flavor?** That is not
   supported. Model the unbounded nodes as a separate ResourceFlavor with no
   lifespan AdmissionCheck attached. Do not paper over the mix with
   `defaultWhenAbsent`; that invents a deadline for nodes that do not have one and
   refuses long Workloads from capacity that would have run them. See
   [Defaults for absent keys](/docs/tasks/manage/configure_lifespan_sources/#defaults-for-absent-keys).

## Scale-from-zero does not trigger

If your lifespan-managed flavor is backed by an autoscaled node pool, and
Workloads stay pending without the pool ever growing, the injected affinity is
almost certainly the cause.

The autoscaler decides whether to scale up by simulating a node built from the
pool's node template. If that template does not carry
`kueue.x-k8s.io/node-remaining-time-seconds`, the simulated node fails the `Gt`
expression, the autoscaler concludes that adding a node would not help the pending
Pods, and it does not scale up. Nothing logs an error; the pool simply stays at
zero.

Make sure your node templates reproduce both the labels your source's `appliesTo`
selects on and a representative lifespan value. See
[Autoscaled node pools](/docs/tasks/manage/configure_lifespan_sources/#autoscaled-node-pools).

## My Job was killed even though it was admitted

LAS guarantees placement eligibility *at admission time*, given the estimate it was
handed. It does not enforce that a Workload stops running at any point. A Job that
runs significantly longer than its `kueue.x-k8s.io/estimated-duration` will reach
the node's hard boundary and be reclaimed by the platform.

Check whether the estimate was realistic:

```bash
kubectl get workload WORKLOAD_NAME -o jsonpath='{.metadata.annotations.kueue\.x-k8s\.io/estimated-duration}'
```

If estimates are systematically optimistic, raise `safetyBuffer` or fix the
estimator. If you additionally want a hard stop, combine LAS with
[`maximumExecutionTimeSeconds`](/docs/concepts/workload/#maximum-execution-time),
which bounds actual runtime — LAS only bounds placement.

## A node stopped accepting Pods before its lifespan ran out

Below `LifespanPolicy.spec.expiringTaintThreshold` (default `15m`), Kueue taints
the node with `kueue.x-k8s.io/expiring-node:NoSchedule`:

```bash
kubectl get node NODE_NAME -o jsonpath='{.spec.taints}'
```

This is expected. The taint stops anything new from binding while the node is torn
down; already-running Pods are not evicted by it.

## The lifespan values look stale

Lifespan labels are debounced into 300-second buckets (60 seconds below 10 minutes
remaining), so a label can trail reality by up to that amount. That staleness is
already accounted for by the fixed 300-second term in the required runway, so it is
not a correctness problem, and label values are not expected to tick down every
second.

Values that are stale by considerably more than that indicate the producer has
stopped. Check the Kueue controller logs, and confirm that
`kueue_lifespan_unlabeled_nodes` is zero for the flavor.

If none of the above steps resolves your problem, contact us at the
[Slack `wg-batch` channel](https://kubernetes.slack.com/archives/C032ZE66A2X)
