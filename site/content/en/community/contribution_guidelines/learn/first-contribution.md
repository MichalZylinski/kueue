---
title: "Your first contribution"
linkTitle: "First contribution"
weight: 20
description: >
  The PR workflow, recipes for common change types, and what reviewers look for
type: docs
---

<!-- Written from a read of the source around the v0.19 cut. Links point at main (not a pinned commit), so files/directories stay resolvable as the code evolves; described behavior may drift in detail over time -- if something looks off, a fix or a removal is equally welcome, no need to reconcile the whole page. -->

## Your first contribution, step by step

### 10.0 One-time setup
1. **Sign the CNCF CLA** — PRs are blocked until you do: [git.k8s.io/community/CLA.md](https://git.k8s.io/community/CLA.md).
2. Join **Kubernetes Slack** ([slack.k8s.io](https://slack.k8s.io)) → channels `#wg-batch` (Kueue's home) and `#sig-scheduling`.
3. Skim the [Kubernetes Contributor Guide](https://k8s.dev/guide) — Kueue follows all standard k8s-project mechanics (prow, OWNERS, LGTM/approve).

### 10.1 Find something to work on
- Issues labeled [`good first issue`](https://github.com/kubernetes-sigs/kueue/issues?q=is%3Aopen+label%3A%22good+first+issue%22) and `help wanted`.
- Failing/flaky tests (`kind/flake`).
- Docs gaps — [`site/content/en/docs/`](https://github.com/kubernetes-sigs/kueue/tree/main/site/content/en/docs) PRs are genuinely valued and a low-risk way to learn the concepts deeply.
- Comment on the issue and ask to be assigned (`/assign`). If the approach isn't obvious, propose it in the issue *before* writing code — this saves everyone review cycles.

### 10.2 The PR loop
```bash
git checkout -b my-fix upstream/main
# ... code + tests ...
make verify && make test
# targeted integration run for your area (see §8.2)
git push origin my-fix
```
Open the PR **using the template** in [`.github/PULL_REQUEST_TEMPLATE.md`](https://github.com/kubernetes-sigs/kueue/blob/main/.github/PULL_REQUEST_TEMPLATE.md). Fill in:
- **What type of PR** (`/kind bug`, `/kind feature`, …)
- **What it does / which issue it fixes** (`Fixes #1234`)
- **Does this PR introduce a user-facing change?** — the release-note block. Write it about *user-observable behavior*, not implementation ("Fixed a bug where X could Y", prefixed by area like `MultiKueue:` / `TAS:` / `Helm:`). A full style guide with templates lives in [`cmd/experimental/skills/kueue-release-notes/SKILL.md`](https://github.com/kubernetes-sigs/kueue/blob/main/cmd/experimental/skills/kueue-release-notes/SKILL.md).

### 10.3 Prow: the bot commands you'll see
| Command | Who | Effect |
|---|---|---|
| `/ok-to-test` | org member | allow CI to run on your first PRs |
| `/retest` | anyone | re-run failed jobs (check it's a flake first!) |
| `/assign @user` | anyone | request review/assignment |
| `/lgtm` | reviewer | approval step 1 |
| `/approve` | OWNERS-file approver | approval step 2 → merge queue |
| `/hold`, `/hold cancel` | anyone | block/unblock merge |
| `/cherry-pick release-0.19` | maintainers | backport to a release branch (see also [`hack/cherry_pick_pull.sh`](https://github.com/kubernetes-sigs/kueue/blob/main/hack/cherry_pick_pull.sh)) |

A PR merges when it has **both** `lgtm` and `approve` labels and CI is green. Reviewers are volunteers across time zones — a few days' latency is normal; a polite ping on Slack after ~a week is fine.

### 10.4 If you use AI tools
Kueue follows the [Kubernetes AI policy](https://www.kubernetes.dev/docs/guide/pull-requests/#ai-guidance): **disclose AI usage in the PR description** (and in issues/comments), and **never add AI co-author trailers** or `assisted-by` markers to commits. You are fully responsible for understanding and defending every line you submit.


## Recipes for common change types

### Recipe A: add a field to a Kueue API
1. Add the field in `apis/kueue/v1beta2/<type>_types.go` with a **complete godoc comment** — for enum-like fields, list *every* valid value and its semantics; never document values that aren't implemented yet.
2. Add validation: kubebuilder markers on the field + webhook logic in [`pkg/webhooks/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/webhooks) if cross-field.
3. `make generate-code manifests`.
4. Plumb the behavior; guard with a feature gate if it changes behavior (Recipe B).
5. Tests: webhook validation unit tests + integration test in [`test/integration/singlecluster/webhook/`](https://github.com/kubernetes-sigs/kueue/tree/main/test/integration/singlecluster/webhook) and the behavioral suite.
6. Docs: update the relevant page under [`site/content/en/docs/`](https://github.com/kubernetes-sigs/kueue/tree/main/site/content/en/docs).

### Recipe B: add a feature gate
1. Declare it in [`pkg/features/kube_features.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/features/kube_features.go) (with the standard comment block: owner, kep link, alpha/beta since).
2. Add the `featuregate.FeatureSpec` entry in the same file's defaults map.
3. Check it with `features.Enabled(features.MyGate)` — put the check **inside** the guarded function, not at every call site, and make sure *both* gate states are correct and tested (an untested gate-off path is a latent bug — explicit review rule).
4. `make generate-featuregates`.
5. Integration tests typically run with the gate enabled via `features.SetFeatureGateDuringTest`.

### Recipe C: add a new job integration
This is a big one — read KEP-369 ("job interface") and mimic a recent integration (e.g. [`pkg/controller/jobs/trainjob/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/controller/jobs/trainjob)):
1. New package `pkg/controller/jobs/<framework>/` implementing `GenericJob` (+ optional interfaces you need).
2. Register in the integration manager; add the framework to `apis/config/*/configuration_types.go` integration list.
3. Webhook (embed the base webhook), RBAC markers, indexes.
4. MultiKueue adapter if applicable.
5. Integration tests + e2e (extended suite, with a `feature:<name>` ginkgo label); dependency CRDs wired into [`Makefile-test.mk`](https://github.com/kubernetes-sigs/kueue/blob/main/Makefile-test.mk)/`hack` e2e setup.
6. Docs page under [`site/content/en/docs/tasks/run/`](https://github.com/kubernetes-sigs/kueue/tree/main/site/content/en/docs/tasks/run).
External frameworks can also integrate *without* touching Kueue's repo via the external-frameworks mechanism (KEP-2349) — worth suggesting to users when a built-in integration isn't warranted.

### Recipe D: add a metric
1. Define it in [`pkg/metrics/metrics.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/metrics/metrics.go) following existing naming (`kueue_` prefix).
2. **Match the label set of existing metrics of the same family** — e.g. ClusterQueue metrics deliberately have no `cohort` label (semantics ambiguity, issue #7539). Don't add novel labels casually.
3. Beware unbounded cardinality: never use workload/pod names as labels.
4. Gate it consistently with similar metrics (e.g. `EnableClusterQueueResources` config, or a feature gate like `LocalQueueMetrics`/`CustomMetricLabels`).
5. Docs: [`site/content/en/docs/reference/metrics.md`](https://github.com/kubernetes-sigs/kueue/blob/main/site/content/en/docs/reference/metrics.md).

### Recipe E: fix a scheduling bug
1. Reproduce as an integration test in [`test/integration/singlecluster/scheduler/`](https://github.com/kubernetes-sigs/kueue/tree/main/test/integration/singlecluster/scheduler) first (or a unit test in [`pkg/scheduler/`](https://github.com/kubernetes-sigs/kueue/tree/main/pkg/scheduler)) — red before green.
2. Fix, keeping the snapshot/live-cache distinction in mind: cycle logic must only read the snapshot.
3. Run the whole scheduler suite plus preemption/fair-sharing unit tests — these subsystems interact.


## Code review culture: what maintainers look for

Kueue has an unusually explicit review playbook — the maintainers encoded their standards as review "skills" under [`cmd/experimental/skills/reviewer/`](https://github.com/kubernetes-sigs/kueue/tree/main/cmd/experimental/skills/reviewer). Reading that directory *is* reading the maintainers' minds. Highlights you should apply to your own PR before submitting:

**Correctness & compatibility**
- **Never delete backwards-compatibility code** (finalizer cleanup, annotation migration) without thinking through a rolling upgrade — objects created by the previous version must still reconcile to a terminal state. This is a hard blocker.
- Feature-gated code: both gate states must be correct and tested.
- Reconcilers must nil-check optional fields even if the webhook validates them (old objects predate the webhook).
- Fix races at the **mutation site** (clone there) rather than adding snapshot/mirror fields at the read site.

**Style & structure**
- Table-driven tests, always.
- Log verbosity: `V(2)` is for coarse lifecycle events; anything per-reconcile-cycle goes to `V(4)`+. Never silence `ctx` (`_ = ctx`); use structured logging via the context logger.
- Don't export identifiers with no callers outside the package.
- Push repeated guards into the callee; encapsulate fields/calls that must change together.
- Smallest diff that solves the stated problem — no drive-by refactors bundled with fixes (scope creep is flagged).
- New tests for a distinctly-named feature go in `<package>_<feature>_test.go` when they'd add 150+ lines to an already-large file (precedent: [`scheduler_tas_test.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/scheduler/scheduler_tas_test.go)).

**Comments**
- Comments explain *why*, not *what*; an inaccurate comment is worse than none.
- Compatibility shims **require** a removal marker naming the version and tracking issue.
- Terminology must match semantics — e.g. don't call something `Preempted` unless it is actually priority-based preemption; introduce a new reason constant instead.

**Security instincts** (Kueue is a privileged controller in the admission path)
- Validate everything user-settable at the trust boundary; cap loops/allocations sized by user data.
- A panic in the scheduler cache or a webhook wedges cluster-wide scheduling — treat nil-safety as high severity.
- No credentials in logs/status/annotations; no unpinned images; RBAC with minimum verbs.


## Suggested 30-day learning path

- **Week 1** — Run Kueue on kind (§7). Do the getting-started examples. Read all concept docs. Trace the life of a workload (§5) with the code open. 
- **Week 2** — Run the unit + one integration suite locally. Pick a `good first issue` (docs, kueuectl, or KueueViz are the friendliest entry points). Sign the CLA, join Slack, submit your first PR.
- **Week 3** — Read [`pkg/scheduler/scheduler.go`](https://github.com/kubernetes-sigs/kueue/blob/main/pkg/scheduler/scheduler.go) end-to-end plus `flavorassigner`. Read the KEP for one feature you find interesting. Attend a wg-batch meeting.
- **Week 4** — Take a `help wanted` bug in one subsystem (§4 router). Write the failing integration test first. Review two other open PRs using §12 as your checklist.

Welcome aboard — the queue is FIFO, but questions in `#wg-batch` get admitted immediately. 🎉
