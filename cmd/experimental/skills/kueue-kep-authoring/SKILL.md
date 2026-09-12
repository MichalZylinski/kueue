---
name: kueue-kep-authoring
description: "Authoring, reviewing, and validating Kubernetes Kueue Enhancement Proposals (KEPs) following upstream SIG-level standards, API conventions, and PR review guardrails."
---

# Kueue KEP Authoring & Review Guide

This guide establishes the standard engineering practices, structural requirements, API design principles, and review guardrails for writing and reviewing **Kubernetes Enhancement Proposals (KEPs)** in the Kueue project (`kubernetes-sigs/kueue`).

# KEP Lifecycle and Directory Structure

Every Kueue enhancement proposal resides in `keps/<NNNN>-<short-descriptive-name>/`:

```
keps/NNNN-feature-name/
├── kep.yaml        # Machine-readable metadata, feature gate, and milestone tracking
└── README.md       # Complete architectural proposal, API specs, test plan, and alternatives
```

## `kep.yaml` Specification

```yaml
title: Short Descriptive Title (Upper Camel or Title Case)
kep-number: NNNN
authors:
  - "@author-github-handle"
status: provisional | implementable | implemented | deferred | rejected
creation-date: yyyy-mm-dd
reviewers:
  - "@mimowo"
  - "@alculquicondor"
  - "@tenzen-y"
  - "@kshalot"
approvers:
  - "@mimowo"
  - "@alculquicondor"
see-also:
  - "/keps/1432-admission-check-per-resource-flavor"
  - "/keps/10076-quota-release-strategy"

# Target maturity stage in the current development cycle
stage: alpha | beta | stable

# Milestone tracking
latest-milestone: "v0.21"
milestone:
  alpha: "v0.21"
  beta: "v0.22"
  stable: "v0.24"

# Feature gate definition
feature-gates:
  - name: FeatureNameInUpperCamelCase
disable-supported: true

# PRR metrics required at beta release
metrics:
  - kueue_feature_action_total
  - kueue_feature_latency_seconds
```

## Table of Contents and Formatting Tools

The Table of Contents in `README.md` must be wrapped in:
```markdown
<!-- toc -->
- [Summary](#summary)
...
<!-- /toc -->
```
Generate or update the TOC using the repo tool:
```bash
hack/tools/mdtoc/generate.sh keps/NNNN-feature-name/README.md
```

# Core Upstream Review Guardrails and Invariants

Upstream Kueue maintainers scrutinize proposals across critical architectural dimensions. Address each proactively in the proposal text.

## Guardrail: Scheduling vs. Admission Boundaries
* **Kueue's Domain:** Queueing, quota reservation, resource flavor selection, topology allocation, admission checks, and pod un-suspension.
* **kube-scheduler's Domain:** Binding individual pods to specific nodes, filtering, scoring, and pod-level preemption.
* **Invariant:** Kueue must never attempt to act as a second kube-scheduler by hard-binding pods to nodes directly. If a feature directs workloads to specific nodes (e.g. TAS or time-bounded placement), it must do so via standard Kubernetes constructs: `NodeSelector`, `NodeAffinity`, or `Tolerations` injected into `PodSetUpdate`.

## Guardrail: In-Memory Scheduling Performance and Complexity
* The Kueue scheduling cycle runs in-memory and sequentially or concurrently across ClusterQueues.
* **Invariant:** The scheduling loop and admission checks must never execute network I/O, synchronous etcd reads, or blocking RPCs.
* **Complexity Budget:** Workload evaluation across flavors must be $O(F)$ where $F$ is the number of flavors, or $O(N)$ where $N$ is the number of topology domains/nodes in memory.
* **Latency Budget:** In-memory admission check evaluation must complete in $< 1$ millisecond.

## Guardrail: Control Plane and Etcd Protection (Write Amplification and Debounce Defense)
* **The 1,000-Node / 100,000-Pod Rule:** If a controller updates node objects or workload objects on high-frequency timers (e.g. ticking down remaining lifespan or progress), 1,000 nodes generate 1,000 writes/sec, collapsing etcd.
* **Invariant:** All background status or label updates must implement adaptive debouncing (e.g., updating only when crossing discrete intervals like 5 minutes / 300s, or when crossing safety thresholds).
* **Debounce Staleness Defense:** When node labels or statuses are debounced, the in-memory cache may lag physical reality by up to the debounce period ($\Delta_{\text{debounce}}$). Any admission formula calculating remaining runways must explicitly incorporate $\Delta_{\text{debounce}}$ as a safety margin ($T_{\text{required}} = T_{\text{est}} + T_{\text{buffer}} + \Delta_{\text{debounce}}$) to eliminate race conditions on aging resources.
* Real-time dynamic values should be computed in-memory from static timestamps (e.g., `Node.CreationTimestamp + lease - Now()`) rather than polling high-frequency etcd state.

## Guardrail: API Design, Types, and Kubebuilder Markers
* **Duration Types:** For durations, windows, timeouts, and buffers in CRD configurations, prefer `*metav1.Duration` (e.g., `30m`, `1h`, `15m`) over raw integer seconds (`*int64`). `metav1.Duration` provides self-documenting syntax, standard Go duration parsing, and eliminates unit ambiguity.
* **Buffer Simplification:** Avoid compound buffer anti-patterns (such as combining a percentage buffer and a fixed second buffer). Compounding percentage buffers excessively penalizes long-running jobs without operational justification. Provide a single, well-defined duration buffer (`spec.safetyBuffer: *metav1.Duration`).
* **Pointer Usage:** All optional scalar and struct fields must use pointers (`*int32`, `*int64`, `*bool`, `*metav1.Duration`) to distinguish unset (`nil`) from zero/false.
* **Validation Markers:**
  - `// +optional` or `// +required`
  - `// +kubebuilder:validation:Minimum=0`
  - `// +kubebuilder:validation:Enum=Allow;Reject;AllowWithDefault`
  - `// +kubebuilder:default=...`
  - CEL validations: `// +kubebuilder:validation:XValidation:rule="...",message="..."`
* **Configuration API Placement:** Global controller options belong in `apis/config/v1beta2` (`Configuration` ConfigMap), queue-level options in `ClusterQueue`, and workload-specific overrides in `Workload`.

## Guardrail: Cumulative Lifecycle Budgets vs. Per-Attempt Continuous Runway
* Distinguish between overall lifecycle budgets and single-attempt physical runway requirements.
* `workload.spec.maximumExecutionTimeSeconds` (KEP-3125) acts as an overall cumulative budget across the entire lifecycle of a workload. It tracks past execution across evictions and retries via `status.accumulatedPastExecutionTimeSeconds` and does not reset on restart.
* If a retried job with 15 minutes of remaining cumulative budget is placed on an ephemeral node with a 20-minute lifespan, rerunning the job from scratch or an earlier checkpoint will cause it to be terminated by the node's lease expiry.
* KEPs addressing node placement or ephemeral execution must use dedicated per-attempt duration constructs (e.g. `kueue.x-k8s.io/estimated-duration`) and keep them decoupled from cumulative lifecycle safeguards.

## Guardrail: NodeAffinity Injection and PodSpec Mutation (OR vs. AND Evaluation Pitfall)
* Kubernetes evaluates `NodeSelectorTerms` inside `RequiredDuringSchedulingIgnoredDuringExecution` with logical `OR` semantics.
* **The Pitfall:** If an admission check or controller appends a new `NodeSelectorTerm` to an existing `NodeAffinity`, the pod will schedule if it matches *either* the existing term *or* the new term, completely bypassing the new constraint.
* **Invariant:** When enforcing node constraints, the reconciler must inject the requirement into the `MatchExpressions` slice across **all** existing `NodeSelectorTerms`. This guarantees strict logical `AND` enforcement across all scheduling options.

## Guardrail: AdmissionCheck State Semantics and Multi-Flavor Fungibility
* Admission checks interact directly with Kueue's flavor evaluation loop.
* `CheckStatePending` indicates the check is waiting on an external event or processing; it does *not* trigger multi-flavor fallback.
* `CheckStateRetry` signals that the current candidate flavor cannot accommodate the workload (e.g., reason `FlavorLifespanInsufficient`). Kueue's scheduler intercepts `Retry` and immediately evaluates alternative flavors within the ClusterQueue (such as falling back from spot/ephemeral to on-demand).
* KEP authors must define the exact state transitions (`Pending` $\to$ `Retry` $\to$ `Ready` or `Rejected`) and the conditions under which multi-flavor fallback occurs.

## Guardrail: Multi-Node Gang Feasibility for Distributed Workloads
* For multi-pod or multi-node workloads (e.g. gang scheduling with PyTorch, MPI, or JobSet), verifying that a single node satisfies constraints is insufficient.
* **Invariant:** An admission check validating node constraints must verify that at least $K$ qualifying nodes exist in the candidate flavor/topology domain (where $K$ matches the required replica count) before setting `Ready=True`. Otherwise, partially schedulable gang workloads will cause head-of-line blocking or scheduling timeouts.

## Guardrail: Vendor Neutrality and In-Tree vs. Out-of-Tree Boundaries
* In-tree Kueue components must remain strictly cloud-agnostic and vendor-neutral.
* In-tree controllers must only consume well-known community labels and annotations (prefixed with `kueue.x-k8s.io/`).
* Cloud-provider or vendor-specific labels (e.g., `cloud.google.com/...`, `eks.amazonaws.com/...`) must never be hardcoded in core Kueue reconcilers.
* **Never name a cloud-provider capacity product in a KEP.** Not in the Summary, not in a User Story, not in an example, not as a parenthetical "(e.g. ...)". This includes product names, marketing names, internal codenames, SKU families, and provider version numbers. Express the underlying property abstractly — "capacity leased for a bounded duration", "nodes with a scheduled evacuation deadline" — and use `example.com/*` for every illustrative label key. Provider-specific detail belongs in an internal PRD, never in the upstream artifact.

### Prefer a configurable generic adapter over provider adapters or out-of-tree modules

When a feature needs to read a platform-specific signal, there are three candidate designs. Rank them in this order:

1. **One in-tree generic adapter, configured by YAML** (strongly preferred). Ship a single code path parameterised over *where* the value lives (label vs. annotation, which key), *what it means* (absolute instant vs. relative to some baseline), and *how it is encoded* (RFC3339, epoch seconds, duration seconds, Go duration). Provider specificity then lives entirely in operator configuration. Nothing vendor-named is compiled into the binary, there is nothing to ship or version separately, and every user exercises the same, well-tested path.
2. **Out-of-tree provider modules.** Acceptable, but weaker: it creates version skew, a second release process, and code paths that upstream CI never runs.
3. **In-tree provider-specific adapters.** Reject. This is the design the vendor-neutrality rule exists to prevent.

Design notes for option 1, learned the hard way:
* Enumerate the real signals you intend to support *before* fixing the configuration schema, and check whether they actually share a shape. Two signals from the same provider frequently differ on every axis at once — one relative-to-node-creation and duration-encoded, the other an absolute epoch instant. If your schema only supports one shape, it is not generic, it is one provider's adapter wearing a costume.
* Treat "can this configuration express signal X?" as a **scope-enforcement mechanism**, not a limitation. A signal that lives behind a provider API call rather than in node metadata is structurally unreachable from declarative config, and that is the correct outcome: the boundary is enforced by the mechanism instead of by reviewer vigilance.
* Validate the whole configuration once at construction/startup, not per-reconcile, and reject illegal mode/format combinations explicitly. Kubernetes label values cannot contain `:` or `+`, so an RFC3339 timestamp can never be a label value — a combination like that must fail loudly at startup rather than silently matching no node forever.
* Reduce multiple applicable sources with an explicit, documented rule (typically minimum), and state it in the KEP.

### Verify platform signals end-to-end, never from the stamping code alone

Before a KEP asserts that a provider label is present or absent, verify it against the full production path. Reading the function that emits the label is **not sufficient evidence**: upstream defaulting logic may mutate the underlying object before the emitting code reads it, so a label that appears conditional in the stamping function can in fact be universally present in practice (or vice versa). Unit tests of the emitting function are especially misleading here, because they populate the input struct directly and therefore assert behaviour that does not hold end-to-end.

Related, and more important for design: **an absent signal usually has more than one meaning.** The same missing label can mean "this node is bounded but the platform did not tell us" and "this node is genuinely unbounded" on two variants of the same product. Design for that ambiguity explicitly:
* Model "unknown" distinctly from zero in the API (e.g. a pointer type), never conflate them.
* Do not make a defaulting escape hatch load-bearing for correctness. Defaulting an unbounded node to a fabricated deadline silently refuses long workloads from capacity that would have run them fine.
* Steer the two cases into separate ResourceFlavors instead, and say so in the KEP.

## Guardrail: Requeuing, Inadmissibility, and Head-of-Line Blocking
* A proposal must explicitly document how failed checks or unsatisfied constraints interact with queue policies:
  - `StrictFIFO`: Does an unschedulable workload block subsequent workloads? If so, what is the bypass or backoff mechanism?
  - `BestEffortFIFO`: Can subsequent workloads be evaluated while this workload is backoff-requeued?
* **Requeue Triggers:** Enumerate the exact Kubernetes events that wake up an inadmissible workload (e.g., new node ready, node label updated, quota released, admission check transitioned).

## Guardrail: Preemption, Cohorts, and Fair Sharing Co-existence
* **Preemption Invariant:** Kueue will never preempt an existing workload to place an incoming workload if the incoming workload cannot satisfy its admission checks or placement horizon on the freed resources.
* **Borrowing Invariant:** Borrowing from cohorts must respect flavor ordering and priority boundaries.
* **Status Updates:** Preempted workloads must record precise reasons in `workload.status.conditions` (e.g. `PreemptedByFairSharing`, `EvictedDueToNodeShutdown`).

## Guardrail: Metrics Cardinality and Feature Gating
* Metrics must follow Prometheus conventions and existing Kueue metric naming (`kueue_<subsystem>_<name>_<unit>`).
* **No Unbounded Labels:** Never include pod names, workload names, or raw timestamps in Prometheus labels. Acceptable labels: `cluster_queue`, `flavor`, `reason`, `status`.
* **Feature Gating:** When a feature gate is disabled, corresponding metrics must not be registered or must remain inactive.

## Guardrail: Rollback Safety and Feature Gate Lifecycle
* When a feature gate is disabled (`disable-supported: true`), the controller must handle existing resources gracefully:
  - Admitted workloads remain admitted and complete normally.
  - Injected affinities or labels remain valid without crashing reconcilers.
  - New workloads bypass the gated logic cleanly.

# Required KEP Document Sections

A complete Kueue KEP must contain the following sections in order:

## Summary
* 2–3 paragraphs written for a broad Kubernetes audience.
* Clearly explain what the feature does, who benefits, and the primary mechanism.

## Motivation
* Context on real production problems.
* Why existing Kubernetes or Kueue mechanisms are insufficient.
* **Goals:** 3–5 crisp, measurable technical objectives.
* **Non-Goals:** Explicit boundaries to prevent scope creep.

## Proposal
* High-level architectural concept and component interaction.
* **User Stories:** Real-world scenarios describing user actions and system outcomes.
* **Notes / Constraints / Caveats:** Known limitations and operational assumptions.
* **Risks and Mitigations:** Security, scalability, etcd load, upgrade safety.

## Design Details
* **APIs:** Exact Go struct definitions with kubebuilder markers, duration types, and comments.
* **YAML Examples:** Clear administrator and user manifests.
* **Controllers & Algorithms:** Step-by-step reconciliation flows and mathematical formulas.
* **Queueing & Scheduling Interactions:** Requeueing, preemption, cohorts, and multi-flavor behavior.
* **Observability & Metrics:** Prometheus metrics, events, conditions.
* **Test Plan:**
  - *Prerequisite updates*
  - *Unit tests:* Specific packages and table-driven scenarios.
  - *Integration tests:* Envtest suites covering multi-flavor, race condition, and error paths.
  - *E2E tests:* Kind cluster automation.
* **Graduation Criteria:** Explicit milestones for Alpha $\to$ Beta $\to$ Stable.

## Implementation History
* Date-stamped changelog of PRs, design reviews, and milestone transitions.

## Drawbacks & Alternatives Considered
* Genuine architectural alternatives analyzed with objective pros and cons.

# Verification and PR Review Checklist

Before submitting a KEP PR to `kubernetes-sigs/kueue`:
- [ ] `kep.yaml` is valid YAML and includes all required fields.
- [ ] TOC is updated using `hack/tools/mdtoc/generate.sh`.
- [ ] All new Go structs adhere to Go style guidelines and compile.
- [ ] Duration fields use `*metav1.Duration` instead of raw integers where appropriate.
- [ ] Kubebuilder markers and CEL validation rules are verified.
- [ ] Write amplification is analyzed and debounced for large cluster scales.
- [ ] NodeAffinity injections modify `MatchExpressions` across all existing `NodeSelectorTerms`.
- [ ] Multi-flavor fallback paths use `CheckStateRetry` rather than `CheckStatePending`.
- [ ] Vendor neutrality is strictly preserved (no provider-specific hardcoded labels).
- [ ] Grep the KEP for every cloud product name, marketing name, internal codename, and provider version number you know of, including in examples and parentheticals — expect zero hits. Illustrative label keys use `example.com/*`.
- [ ] Any platform-signal integration uses a single configurable generic adapter, not per-provider code; the config schema was checked against at least two real signals with genuinely different shapes.
- [ ] Every claim about a provider label being present or absent was verified against the full production path, not just the function that emits it; the two meanings of an absent signal are distinguished in the API and the text.
- [ ] No auto-close keywords (`Fixes #123`) or `#` mentions in commit messages.
- [ ] Test plan covers unit, integration, and e2e levels with negative test cases.
