---
title: "Lifespan-Aware Scheduling"
linkTitle: "Lifespan-Aware Scheduling"
date: 2026-09-12
weight: 6
description: >
  Allows scheduling of Workloads based on how much operational time the nodes of a flavor have left.
---

{{< feature-state state="alpha" for_version="v0.21" >}}
{{% alert title="Note" color="primary" %}}
`LifespanAwareScheduling` is currently an alpha feature and is disabled by default.

You can enable it by editing the `LifespanAwareScheduling` feature gate. Refer to the
[Installation guide](/docs/installation/#change-the-feature-gates-configuration)
for instructions on configuring feature gates.
{{% /alert %}}

A growing share of accelerator capacity is offered on terms that trade permanence
for price or availability: the node exists now, it is healthy now, and it will be
taken away at a time that is already known. Leased capacity tiers that cap the
maximum run duration, nodes scheduled for a maintenance evacuation, and node
max-age policies all share the same property — a **finite remaining lifespan that
is knowable in advance**.

The Kubernetes scheduler is temporally blind to this property. It evaluates
capacity (CPU, memory, accelerators) at a single instant, without asking whether
a candidate node will still exist when the work placed on it is supposed to
finish. A six-hour fine-tuning Job lands on a node with 45 minutes of lease left,
the infrastructure reclaims the machine mid-run, and uncheckpointed progress is
lost. Because nodes are reused across sequential batch runs, their remaining time
decreases monotonically, so the probability of this failure grows with every
reuse.

Lifespan-Aware Scheduling (LAS) makes remaining node lifespan a first-class
scheduling dimension, in the same way that
[Topology-Aware Scheduling](/docs/concepts/topology_aware_scheduling) does for the
physical placement of nodes. Kueue admits a Workload onto time-bounded capacity
only when the nodes of that flavor will outlive the Workload's estimated
execution time, and falls back to a durable flavor when they will not.

## Remaining node lifespan

LAS represents remaining operational time with a single node label:

| Key | Kind | Value |
| --- | --- | --- |
| `kueue.x-k8s.io/node-remaining-time-seconds` | Node label | Whole seconds of remaining operational life, as a non-negative base-10 integer with no unit suffix, sign, or zero-padding |

This label is the only node metadata LAS reads when deciding placement. Everything
that is specific to a platform — leases, maintenance plans, instance lifecycles,
reservation systems — is translated into this one number before Kueue sees it.

The label is produced by a **lifespan source**. Kueue ships one configurable,
generic source in-tree: you describe, in a `NodeLifespanConfig` object, which
nodes a source applies to and where on those nodes the underlying bound can be
found. No provider-specific code ships in Kueue, and no out-of-tree module is
required; the cloud specifics live entirely in your YAML. See
[Configure Lifespan Sources](/docs/tasks/manage/configure_lifespan_sources/) for
the full field reference.

`NodeLifespanConfig` is optional. Kueue writes the label only for nodes matched by
a configured source, so if your platform already publishes the neutral label
itself, you can run with the producer disabled and define no sources at all.

When several sources apply to the same node, the effective lifespan is the
**minimum** across them: a node that is both under a lease and inside a
maintenance horizon really only has until the earlier of the two.

{{% alert title="Note" color="primary" %}}
Values are recomputed centrally, against the Kueue controller's clock, and are
written with a 300-second debounce so that a large fleet does not generate one
node write per second. The staleness this introduces is paid for explicitly in the
runway calculation below.
{{% /alert %}}

## Estimated Workload duration

The other half of the comparison is how long the Workload expects to run. LAS
reads it from an annotation:

| Key | Kind | Value |
| --- | --- | --- |
| `kueue.x-k8s.io/estimated-duration` | Job / Workload annotation | Expected duration of a *single* execution attempt, as a Go duration string, for example `"4h"` |

The annotation describes one attempt, not a cumulative budget across retries. It
is deliberately independent of
[`maximumExecutionTimeSeconds`](/docs/concepts/workload/#maximum-execution-time),
which accumulates across eviction and restart cycles and therefore shrinks exactly
when a restarted Workload needs a *full* runway again.

Kueue does not produce this estimate. Users can set the annotation by hand, or a
platform team can inject it with a mutating admission webhook that infers a
duration from the Job specification. Workloads that carry no annotation are
handled according to the `fallbackPolicy` of the `LifespanPolicy`:
`AllowWithDefault` substitutes `defaultDuration`, while `StrictReject` rejects the
Workload on lifespan-managed flavors.

## Required runway

For a Workload with estimated duration `T_est`, Kueue computes the runway a node
must still have:

```text
T_required = T_est + safetyBuffer + 300s
T_required = max(T_required, minimumWindow)
```

| Term | Source | Why it is there |
| --- | --- | --- |
| `T_est` | `kueue.x-k8s.io/estimated-duration`, or `defaultDuration` | Expected duration of one execution attempt. |
| `safetyBuffer` | `LifespanPolicy.spec.safetyBuffer` (default `30m`) | Absorbs image pull, initialization latency, runtime variance, and a graceful checkpoint drain before the node is reclaimed. It must exceed whatever pre-termination notice the platform gives; where the platform gives none, it must cover the full checkpoint-and-drain cost. |
| `300s` | Fixed | Debounce staleness margin. Because labels are only refreshed every 300 seconds, a node's true remaining time can be up to 300 seconds lower than the value stamped on it. Charging this margin once, centrally, guarantees that a Pod binding at the very end of a debounce interval still has the runway it was promised. |
| `minimumWindow` | `LifespanPolicy.spec.minimumWindow` (default `0s`) | An optional floor below which placement onto time-bounded capacity is considered not worthwhile. It defaults to `0s`, meaning no floor. A non-zero floor opposes one of the main reasons to run this feature — packing short jobs onto capacity that is nearly used up — because it demands an hour of runway for a twenty minute job and strands the rest. Set it only if you have a concrete reason to believe short placements are never worth making, and prefer raising `safetyBuffer`, which is easier to reason about. |

A node is eligible when its remaining time is **strictly greater** than
`T_required`.

## How a Workload is admitted

LAS is implemented as an [AdmissionCheck](/docs/concepts/admission_check/), so it
runs after quota has been reserved and before the Workload is admitted. A flavor
is *lifespan-managed* when the LAS AdmissionCheck is attached to it through
`ClusterQueue.spec.admissionChecksStrategy.admissionChecks[].onFlavors`.

| Stage | What happens |
| --- | --- |
| 1. Quota reservation | The Kueue scheduler picks the first flavor in the ClusterQueue that fits, and the Workload gets the `QuotaReserved` condition. |
| 2. Flavor classification | If the assigned flavor is not lifespan-managed, the check passes through immediately and nothing is injected. |
| 3. Runway evaluation | Kueue computes `T_required` and counts how many nodes of the flavor have strictly more remaining time than that. A gang of `K` Pods needs `K` qualifying nodes; admitting it when only `K-1` qualify would guarantee the failure the feature exists to prevent. |
| 4a. Enough nodes qualify | The check reports `Ready` with reason `PlacementVerified` and emits a `PodSetUpdate` injecting the node affinity shown below. |
| 4b. Too few nodes qualify | The check reports `Retry` with reason `FlavorLifespanInsufficient`, which releases the quota reservation and lets Kueue try the next flavor. |
| 5. Admission | Once every AdmissionCheck is `Ready`, Kueue unsuspends the Job, and `kube-scheduler` places the Pods within the injected affinity. |

The injected requirement is an ordinary node affinity term:

```yaml
key: "kueue.x-k8s.io/node-remaining-time-seconds"
operator: "Gt"
values: ["18300"]
```

Kueue merges this requirement into the `matchExpressions` of **every**
`nodeSelectorTerm` under
`requiredDuringSchedulingIgnoredDuringExecution`. Terms are OR-ed against each
other, so appending a new term instead would let Pods bind to any node matching a
pre-existing term and silently bypass the constraint entirely.

{{% alert title="Note" color="primary" %}}
`Gt` is *strictly* greater than. A Workload whose required runway is exactly equal
to a node's remaining time does **not** match. This is intentional — equality
leaves zero margin — but it means pools have to be sized with headroom above the
longest expected job plus its buffer.
{{% /alert %}}

## Multi-flavor fallback

The `Retry` state is what makes LAS useful rather than merely restrictive. A
ClusterQueue that lists a time-bounded flavor first and a durable flavor second
gets the following behavior for free:

- Short Workloads pack onto the time-bounded nodes, including aging ones, for as
  long as those nodes have runway left.
- Long Workloads that cannot fit inside the remaining lease are redirected to the
  durable flavor instead of being placed and then killed mid-run.
- Nodes approaching their boundary quiesce naturally as running work completes,
  rather than needing a disruptive forced drain.

Flavor order in `spec.resourceGroups[*].flavors` determines what is tried first.
If no fallback flavor exists, a Workload that cannot fit anywhere stays pending;
with `StrictFIFO` it also blocks the Workloads behind it, so a fallback flavor is
strongly recommended on lifespan-managed queues.

## Nodes without a lifespan label

A node in a lifespan-managed flavor that has no
`kueue.x-k8s.io/node-remaining-time-seconds` label, or whose label cannot be
parsed as a non-negative integer, is **ineligible**. This is fail-closed *by
construction*: a node that lacks the label cannot match a `Gt` expression, so
`kube-scheduler` excludes it without Kueue doing anything extra, and the
feasibility count applies the same rule.

This is deliberate. A platform may impose a default maximum lifetime without
recording it anywhere on the node; if silence were read as "unbounded", Kueue would
cheerfully place a week-long Job on a machine that expires tomorrow — the exact
failure the feature exists to prevent, now with a false sense of safety attached.

Because the failure mode of a broken or absent source is therefore "nothing is
ever schedulable on this flavor" rather than something visibly broken, Kueue makes
the condition loud:

- the `kueue_lifespan_unlabeled_nodes` gauge counts unlabeled or unparseable nodes
  per flavor, and
- a `NodeMissingLifespanLabel` warning event is emitted for the affected nodes.

If you deliberately run a mix of bounded and unbounded nodes, model the unbounded
ones as a **separate ResourceFlavor** without the AdmissionCheck attached. Mixing
both populations inside one flavor is not supported: they need different placement
logic, and conflating them makes fallback accounting meaningless.

See [Troubleshooting Lifespan-Aware Scheduling](/docs/tasks/troubleshooting/troubleshooting_lifespan/)
for how to diagnose this.

## Expiring nodes

Independently of admission, Kueue taints a node with
`kueue.x-k8s.io/expiring-node:NoSchedule` once its remaining time drops below
`LifespanPolicy.spec.expiringTaintThreshold` (default `15m`). Node affinity governs
*admission-time* eligibility for Kueue-managed Workloads; the taint governs
everything else that might try to land on the node while it is being torn down.

## Limitations

- **LAS is inert without a lifespan source.** If nothing publishes the label, no
  node in a lifespan-managed flavor is ever eligible. This is the intended
  fail-closed behavior, but it does mean the feature has a prerequisite that is not
  obvious from the ClusterQueue configuration alone.
- **Placement quality is bounded by estimate quality.** A systematically optimistic
  estimate still produces mid-run terminations; the safety buffer only absorbs
  ordinary variance. LAS never makes the situation *worse* than not using it, but
  it cannot make a bad estimate good.
- **Scale-from-zero requires label-carrying node templates.** If your autoscaler's
  node templates do not reproduce the lifespan label, the injected `Gt` affinity
  will not match the simulated node and scale-up will not be triggered. See the
  [autoscaler caveat](/docs/tasks/troubleshooting/troubleshooting_lifespan/#scale-from-zero-does-not-trigger).
- **Uniform, flavor-level expiry is out of scope for alpha.** Capacity where every
  node expires at the same instant — a reservation with a fixed end time, for
  example — has no per-node variance to discriminate on. It is better modelled by a
  single deadline on the ResourceFlavor, which LAS does not yet provide.
- **Kueue tracks nodes.** As with TAS, watching nodes increases the memory
  footprint of the Kueue controller.

## What's next?

- Follow the [Setup Lifespan-Aware Scheduling](/docs/tasks/manage/setup_lifespan_aware_scheduling/) guide.
- Learn how to describe your capacity in [Configure Lifespan Sources](/docs/tasks/manage/configure_lifespan_sources/).
- Read about [Admission Checks](/docs/concepts/admission_check/) and [Resource Flavors](/docs/concepts/resource_flavor/).
- Diagnose problems with [Troubleshooting Lifespan-Aware Scheduling](/docs/tasks/troubleshooting/troubleshooting_lifespan/).
