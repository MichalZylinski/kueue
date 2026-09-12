---
title: "Configure Lifespan Sources"
linkTitle: "Configure Lifespan Sources"
date: 2026-09-12
weight: 7
description: >
  Describe how the remaining operational time of your nodes is derived, using the generic lifespan source shipped with Kueue.
---

This page is the reference for the lifespan sources used by
[Lifespan-Aware Scheduling (LAS)](/docs/concepts/lifespan_aware_scheduling). A
lifespan source answers one question for every node — *how many seconds does this
node have left?* — and publishes the answer in the node label
`kueue.x-k8s.io/node-remaining-time-seconds`.

Kueue ships a single generic, configurable source in-tree. There are no
provider-specific adapters, and no out-of-tree module is required: everything that
is specific to your platform is expressed as data in a `NodeLifespanConfig`
object.

The intended audience for this page are [batch administrators](/docs/tasks#batch-administrator).
For the end-to-end setup, see
[Setup Lifespan-Aware Scheduling](/docs/tasks/manage/setup_lifespan_aware_scheduling/).

## The NodeLifespanConfig object

`NodeLifespanConfig` is a cluster-scoped object holding a list of sources:

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
    defaultWhenAbsent: "24h"
```

## Source fields

| Field | Required | Description |
| --- | --- | --- |
| `name` | yes | Identifier for the source. Must be unique within the config. It appears in diagnostics, so name it after the capacity class it describes (`leased-capacity`, `planned-maintenance`), not after the label it reads. |
| `appliesTo` | yes | A standard [`metav1.LabelSelector`](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors) selecting the nodes this source describes. Nodes that do not match are untouched by this source. |
| `mode` | yes | How the value found at `from` is interpreted: `Absolute` or `RelativeToNodeCreation`. See [Modes](#modes). |
| `from` | yes | Where the value lives on the node. Exactly one of `label` or `annotation`, whose value is the key to read. |
| `format` | yes | How the value is parsed: `RFC3339`, `UnixEpochSeconds`, `DurationSeconds`, or `GoDuration`. See [Formats](#formats). |
| `defaultWhenAbsent` | no | Lifespan to assume when a node matches `appliesTo` but the key named by `from` is **missing**. It is a duration (for example `"168h"`), independent of the source's `format`, and is only valid with `mode: RelativeToNodeCreation`. When unset, a matching node with no value gets no lifespan from this source. See [Defaults for absent keys](#defaults-for-absent-keys). |

### Modes

| Mode | The value means | Remaining time is |
| --- | --- | --- |
| `Absolute` | A point in time at which the node stops being usable — an evacuation deadline, a reservation end, a scheduled reclamation. | `value - now` |
| `RelativeToNodeCreation` | A length of time the node is allowed to live, counted from `.metadata.creationTimestamp`. | `(node.creationTimestamp + value) - now` |

Use `RelativeToNodeCreation` for capacity whose bound is expressed as "this node
may run for at most N hours", and `Absolute` for capacity whose bound is expressed
as "this node goes away at time T". Results are clamped at `0`; a node past its
boundary reports `0` rather than a negative value.

### Formats

| Format | Example value | Meaning |
| --- | --- | --- |
| `RFC3339` | `2026-09-12T18:00:00Z` | A timestamp, as defined by [RFC 3339](https://www.rfc-editor.org/rfc/rfc3339). |
| `UnixEpochSeconds` | `1789329600` | Seconds since the Unix epoch, as a base-10 integer. |
| `DurationSeconds` | `86400` | A number of whole seconds, as a base-10 integer. |
| `GoDuration` | `24h30m` | A [Go duration string](https://pkg.go.dev/time#ParseDuration). |

### Valid mode and format combinations

A format either describes an instant or a length of time, and must match the mode:

| | `RFC3339` | `UnixEpochSeconds` | `DurationSeconds` | `GoDuration` |
| --- | :---: | :---: | :---: | :---: |
| `Absolute` | ✅ | ✅ | ❌ | ❌ |
| `RelativeToNodeCreation` | ❌ | ❌ | ✅ | ✅ |

Invalid combinations are rejected when the `NodeLifespanConfig` is applied.

{{% alert title="Note" color="primary" %}}
Node labels cannot contain `:` or `+`, so an `RFC3339` timestamp can only be read
from an **annotation**. Use `UnixEpochSeconds` when the platform publishes the
bound as a label. Kueue does not rewrite one into the other for you.

A source combining `from.label` with `format: RFC3339` is rejected at controller
startup, with a message pointing you at `UnixEpochSeconds` or at reading the value
from an annotation instead. You find out immediately, rather than ending up with a
source that quietly matches nothing.
{{% /alert %}}

## Multiple sources on one node

Several sources may match the same node. The effective lifespan is the
**minimum** across all matching sources: a node that is both under a lease and
inside a maintenance horizon has only until the earlier of the two.

Sources whose `appliesTo` does not match a node contribute nothing — they do not
make the node unbounded, and they do not make it ineligible. A node in a
lifespan-managed flavor that matches *no* source ends up without a lifespan label
and is therefore ineligible for placement; see
[Nodes without a lifespan label](/docs/concepts/lifespan_aware_scheduling/#nodes-without-a-lifespan-label).

## Defaults for absent keys

Some platforms apply a bound that they never record on the node: the capacity has
a maximum lifetime, but nothing on the object says so. `defaultWhenAbsent` exists
to materialise that implicit bound, so that the node is scheduled against the
limit that really applies to it:

```yaml
  - name: leased-capacity
    appliesTo:
      matchLabels:
        example.com/leased: "true"
    mode: RelativeToNodeCreation
    from:
      label: example.com/max-run-duration-seconds
    format: DurationSeconds
    defaultWhenAbsent: "24h"   # the platform's implicit maximum lifetime
```

The field is a duration, independent of the source's `format`, and is only valid
with `mode: RelativeToNodeCreation`: an absolute instant has nothing to anchor a
default to.

It applies **only when the key is missing**. A key that is present but cannot be
parsed in the configured `format` is an error, not an absent value: the reconcile
logs it and stops, rather than substituting the default or retrying in a loop. A
typo in a node label therefore surfaces as a loud, diagnosable failure instead of
quietly becoming a made-up deadline.

{{% alert title="Warning" color="warning" %}}
**Do not use `defaultWhenAbsent` for nodes that are genuinely unbounded.** This is
the most likely misconfiguration in the whole feature.

Some platforms ship two variants of the same capacity product that are
distinguished *only* by the presence of the bound: on one variant an absent key
means "the implicit default applies", and on the other it means "there is no bound
at all". A default configured for the first variant invents a deadline for the
second, and long Workloads are then refused from capacity that would have run them
perfectly well.

Put genuinely unbounded nodes in a **separate ResourceFlavor with no lifespan
AdmissionCheck attached**. They then need no lifespan label, no source, and no
default, and Workloads of any length can use them.
{{% /alert %}}


## Examples

The examples below are vendor-neutral illustrations. Substitute the label keys
your own provider publishes.

### Example: a cloud provider that leases capacity for a fixed duration

A provider offers discounted nodes that are reclaimed a fixed number of seconds
after the node is created, and publishes that limit as a node label:

```yaml
# Capacity leased for a fixed duration from node creation
- name: leased-capacity
  appliesTo:
    matchLabels:
      example.com/leased: "true"
  mode: RelativeToNodeCreation
  from:
    label: example.com/max-run-duration-seconds
  format: DurationSeconds
```

For a node created at `09:00` carrying
`example.com/max-run-duration-seconds: "86400"`, evaluated at `15:00` the same
day, Kueue derives `86400 - 21600 = 64800` and publishes
`kueue.x-k8s.io/node-remaining-time-seconds: "64800"`.

### Example: a cloud provider that schedules a maintenance evacuation

A provider announces host maintenance by stamping an absolute evacuation time on
the affected nodes:

```yaml
# Nodes with a scheduled maintenance/evacuation deadline
- name: planned-maintenance
  appliesTo:
    matchExpressions:
    - key: example.com/scheduled-maintenance-time
      operator: Exists
  mode: Absolute
  from:
    label: example.com/scheduled-maintenance-time
  format: UnixEpochSeconds
```

Note that `appliesTo` selects on the *presence* of the key, so the source only
covers nodes that actually have maintenance scheduled. Nodes without it are
unaffected — which is why a second source is normally needed to describe those
nodes' ordinary bound.

With both sources configured, a leased node with 64,800 seconds of lease left that
is also scheduled for maintenance in two hours reports `7200`: the minimum wins.

## Write frequency

Kueue recomputes lifespans against its own clock, never against node-local time,
and debounces writes into discrete 300-second buckets so that a node object is
only patched when it crosses a bucket boundary. Below 600 seconds remaining, the
bucket narrows to 60 seconds for a more precise boundary defence.

That debounce is why the admission side adds a fixed 300-second margin to every
required runway; see
[Required runway](/docs/concepts/lifespan_aware_scheduling/#required-runway). You
do not need to account for it in your source configuration.

## Autoscaled node pools

If your nodes are provisioned on demand, the node templates the autoscaler uses to
simulate scale-up must also carry whatever `appliesTo` selects on **and** the
resulting `kueue.x-k8s.io/node-remaining-time-seconds` value. A simulated node
without that label cannot match the `Gt` affinity Kueue injects, so the autoscaler
concludes that scaling up would not help and does not scale up at all. This is a
common and quiet failure mode; see
[Scale-from-zero does not trigger](/docs/tasks/troubleshooting/troubleshooting_lifespan/#scale-from-zero-does-not-trigger).

## Using your own producer instead

The node label is a plain contract, so you can bypass `NodeLifespanConfig`
entirely and have your own controller write
`kueue.x-k8s.io/node-remaining-time-seconds` directly. This is appropriate when
the bound cannot be derived from node metadata alone — for example when it has to
be fetched from an external inventory or reservation system.

Such a producer must:

- publish a **non-negative base-10 integer** of whole seconds, with no unit
  suffix, sign, or zero-padding, since the value is compared by
  `kube-scheduler` as a `Gt` node-affinity expression;
- compute against a trusted central clock, not node-local time;
- resolve platform defaults rather than omitting the label — omission means
  "genuinely unknown", and unknown nodes are never selected;
- publish `0` when the lifespan is exhausted, rather than a negative value or no
  label at all;
- debounce its writes to no finer than 300 seconds, to avoid overwhelming etcd;
  and
- publish the minimum when several bounds apply to the same node.

### Who owns the label

Kueue writes `kueue.x-k8s.io/node-remaining-time-seconds` **only for nodes matched
by a configured source**. It never touches the label on any other node, so:

- If your platform publishes the neutral label directly, run with the producer
  disabled and define no sources at all. `NodeLifespanConfig` is optional, and the
  admission side works the same either way.
- If both are active, configured sources win for the nodes they match: Kueue
  overwrites whatever an external producer wrote there.
- Avoid the overlap in the first place by scoping `appliesTo` so that each node is
  labelled by exactly one of the two.

## What's next

- Return to [Setup Lifespan-Aware Scheduling](/docs/tasks/manage/setup_lifespan_aware_scheduling/).
- Read the [Lifespan-Aware Scheduling](/docs/concepts/lifespan_aware_scheduling) concept page.
- Diagnose problems with [Troubleshooting Lifespan-Aware Scheduling](/docs/tasks/troubleshooting/troubleshooting_lifespan/).
