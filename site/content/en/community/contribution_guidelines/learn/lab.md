---
title: "Onboarding lab: the katas"
linkTitle: "Onboarding lab"
weight: 30
description: >
  Eight hands-on exercises with verifiable outcomes, from first admission to a red-then-green integration test
type: docs
---

<!-- Verified against main@2b494fec3 (the v0.19 cut). Re-verify each minor release. -->

Eight hands-on exercises with a goal, steps, and a concrete pass condition each. Expected total time: one focused day, or a kata per evening for a week. Prerequisites: a clone of kueue, Go (version from `go.mod`), Docker, `kind`, `kubectl`; on macOS also `brew install gnu-sed`. Katas 0-5 need no code changes; katas 6-7 are code katas.

## Kata 0 — Boot the lab

**Goal:** a kind cluster running Kueue **you built yourself**.

```bash
cd kueue
kind create cluster --name kueue-lab
make install                   # CRDs into the cluster
make kind-image-build          # build the manager image locally (~2 min warm)
TAG=$(git describe --tags --dirty --always)
kind load docker-image us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:$TAG --name kueue-lab
make deploy                    # deploy manifests referencing exactly that $TAG
kubectl -n kueue-system patch deploy kueue-controller-manager --type json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'
kubectl -n kueue-system wait --for=condition=Ready pod -l control-plane=controller-manager --timeout=300s
```

> ⚠️ **Why not just `make deploy`?** The deploy manifests bake in an image tag derived from `git describe` (e.g. `kueue:v0.19.0-devel-284-gxxxxxxx-dirty`) — a tag that exists in **no registry**, so the pod lands in `ImagePullBackOff`. You must build and `kind load` that exact tag yourself. And because the base manifest sets `imagePullPolicy: Always`, the kubelet would still try the registry — the patch flips it to `IfNotPresent`, exactly what the project's own e2e scripts do (`hack/testing/e2e-common.sh`).

**Verify (pass condition):**
```bash
kubectl get crds | grep -c kueue.x-k8s.io       # expect 11+
kubectl -n kueue-system logs deploy/kueue-controller-manager | grep -im2 "starting"
```

**What you learned:** the deployment surface — CRDs from `config/crd/` (generated!), the image/tag plumbing every e2e run uses, manager in `kueue-system`, and where its logs live.


## Kata 1 — First admission, observed closely

**Goal:** watch the full life of a workload from the architecture page happen in real time.

```bash
kubectl apply -f examples/admin/single-clusterqueue-setup.yaml   # flavor + CQ (9 cpu) + LQ
kubectl create -f examples/jobs/sample-job.yaml                  # 3 pods × 1 cpu, sleeps 60s
kubectl get workloads -n default -w
```

Now inspect while it runs:

```bash
WL=$(kubectl get workloads -n default -o name | head -1)
kubectl get $WL -o yaml
```

**Verify:** in the workload YAML find, and be able to explain, all four of:
1. `status.conditions` — `QuotaReserved` and `Admitted` both `True` (why did they flip together here? — no AdmissionChecks configured);
2. `status.admission.podSetAssignments[0].flavors.cpu: default-flavor` — the scheduler's output;
3. `metadata.ownerReferences[0]` → your Job, and the `kueue.x-k8s.io/job-uid` label;
4. on the Job itself: `spec.suspend` went `true → false` (check `kubectl get job -o yaml`; the mutating webhook suspended it, the reconciler unsuspended it after admission).

**What you learned:** the object contract between webhook, job framework, scheduler, and workload controller — the Workload and job-framework internals pages, live.


## Kata 2 — Starve the queue, then feed it

**Goal:** see the pending side (queue manager) and event-driven requeue with your own eyes.

The ClusterQueue has 9 CPUs. Fill it past capacity:

```bash
for i in 1 2 3 4; do kubectl create -f examples/jobs/sample-job.yaml; done
kubectl get workloads -n default        # some Admitted, at least one not
```

Inspect the starved workload and the queue:

```bash
kubectl get workloads -n default -o wide
# pick a non-admitted one:
kubectl get workload <pending-wl> -n default -o jsonpath='{.status.conditions[?(@.type=="QuotaReserved")].message}'; echo
kubectl describe clusterqueue cluster-queue   # pendingWorkloads count, flavor usage
```

The message tells you *why* it doesn't fit (insufficient quota for cpu in flavor default-flavor, …). That string was written by the scheduler's `requeueAndUpdate` path from the flavor assigner's reasons.

Now free capacity and watch the parked workload get admitted **without any resubmission**:

```bash
kubectl delete job <one-admitted-job> -n default
kubectl get workloads -n default -w     # the pending one flips to Admitted within seconds
```

**Verify:** the previously pending workload becomes `Admitted` after the deletion, and you can articulate the chain that made it happen: quota release → cache update → requeue trigger moves it out of the inadmissible set → `Broadcast()` wakes the scheduler (the internals pages–2.4).

**Bonus:** repeat with the ClusterQueue's `queueingStrategy` set to `StrictFIFO` and submit one job that can *never* fit (e.g. `cpu: "50"`). Observe it now blocks everything behind it — the BestEffortFIFO/StrictFIFO trade-off from the internals pages


## Kata 3 — Make a preemption happen, then investigate it like an SRE

**Goal:** trigger a real preemption and trace the victim→preemptor evidence chain.

Set up priorities and an aggressive ClusterQueue (apply this manifest):

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata:
  name: low
value: 100

apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata:
  name: high
value: 1000
```

Then patch the ClusterQueue to allow preemption within itself:

```bash
kubectl patch clusterqueue cluster-queue --type merge -p \
  '{"spec":{"preemption":{"withinClusterQueue":"LowerPriority"}}}'
```

Fill the quota with **low**-priority work, then submit one **high**-priority job. Derive the manifests from the sample job — extend the sleep to 600s so the low jobs *hold* the quota instead of draining mid-kata:

```bash
mkdir -p /tmp/kueue-lab
sed -e 's/sleep 60/sleep 600/' \
    -e 's|kueue.x-k8s.io/queue-name: user-queue|&\n    kueue.x-k8s.io/priority-class: low|' \
    examples/jobs/sample-job.yaml > /tmp/kueue-lab/low-job.yaml
sed 's/priority-class: low/priority-class: high/' /tmp/kueue-lab/low-job.yaml > /tmp/kueue-lab/high-job.yaml

for i in 1 2 3; do kubectl create -f /tmp/kueue-lab/low-job.yaml; done   # 3×3 cpu = quota full
kubectl get workloads -n default -o custom-columns=NAME:.metadata.name,PRIO:.spec.priority,ADMITTED:'.status.conditions[?(@.type=="Admitted")].status'
kubectl create -f /tmp/kueue-lab/high-job.yaml                            # the trigger
```

Within seconds one low workload shows `Admitted=False, Evicted=True` while the priority-1000 workload admits.

**Verify:**
```bash
kubectl get events -n default --field-selector reason=Preempted \
  -o jsonpath='{.items[0].message}'; echo
```
You should see the canonical message: `Preempted to accommodate a workload (UID: …, JobUID: …) due to prioritization in the ClusterQueue; preemptor path: …; preemptee path: …; preemptor effective priority: 1000 …`. Parse it: find the preemptor workload via the `JobUID`:

```bash
kubectl get workloads -n default -l kueue.x-k8s.io/job-uid=<JobUID>
```

Also check the victim's `Evicted` and `Requeued` conditions — eviction's two phases (the internals pages).

**What you learned:** preemption policies (the internals pages), and the exact forensic workflow of the `kueue-who-preempted` runbook — the most common production question Kueue admins ask.


## Kata 4 — Read the scheduler's mind

**Goal:** map log lines to the six phases of `schedule()` you read about in the architecture page.

All the phase lines below are emitted at `V(2)`, which is the **default verbosity of the deployed manager** (its args already contain `--zap-log-level=2`) — no config change needed. Submit a job, then:

```bash
kubectl create -f examples/jobs/sample-job.yaml
kubectl -n kueue-system logs deploy/kueue-controller-manager --since=2m \
  | grep -E "Scheduling cycle starts|Obtained heads|Snapshot taken|Nomination done|Attempting to schedule|assumed in the cache|Workload processing done"
```

> ⚠️ The two admission lines — `"Workload assumed in the cache"` and `"Workload successfully admitted and assigned flavors"` — only appear when a workload is **actually admitted** in that cycle. If your quota is still full from Kata 3, you'll see the cycle-phase lines but no admission; free some quota (delete a low job) and watch them appear.

**Verify:** you can point at the log line for each phase and name the function that emitted it (all in `pkg/scheduler/scheduler.go`): `Heads()` blocking → `Snapshot()` → `nominate()` → per-entry `processEntry` → `assumeWorkload` → requeue. Note the `schedulingCycle` counter incrementing per cycle.

**Bonus:** for per-workload flavor-assignment detail, raise verbosity by *replacing* the existing level arg (don't append a second one):
```bash
kubectl -n kueue-system patch deploy kueue-controller-manager --type json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/args/1","value":"--zap-log-level=4"}]'
```
Then read one full admission and its `assignments` field — it's `Admission.PodSetAssignments`, the same block you saw in Kata 1's workload status.


## Kata 5 — Break it on purpose: the inactive ClusterQueue (and the finalizer that stops you)

**Goal:** learn how Kueue reports misconfiguration — and meet the `resource-in-use` finalizer on the way.

First, try the obvious thing:

```bash
kubectl delete resourceflavor default-flavor
kubectl get resourceflavor default-flavor -o jsonpath='{.metadata.deletionTimestamp} {.metadata.finalizers}'
```

> 🔍 **Surprise (verified):** the delete "succeeds" but the flavor only goes `Terminating` — it carries the `kueue.x-k8s.io/resource-in-use` finalizer, which the ResourceFlavor controller holds **as long as any ClusterQueue references the flavor** (see `ClusterQueuesUsingFlavor` in `pkg/controller/core/resourceflavor_controller.go`). Kueue is protecting you from exactly the breakage this kata wants to demonstrate. The CQ meanwhile stays `Active=True`.

So break it the way it happens in real life — a **reference to a flavor that doesn't exist** (typo, wrong apply order):

```bash
kubectl patch clusterqueue cluster-queue --type json \
  -p '[{"op":"replace","path":"/spec/resourceGroups/0/flavors/0/name","value":"typo-flavor"}]'
kubectl create -f examples/jobs/sample-job.yaml   # submit into the broken world
```

**Verify:**
```bash
kubectl get clusterqueue cluster-queue -o jsonpath='{.status.conditions[?(@.type=="Active")]}' | jq .
# → status False, reason FlavorNotFound, "references missing ResourceFlavor(s): typo-flavor"
kubectl get workloads -n default -o jsonpath='{.items[0].status.conditions[?(@.type=="QuotaReserved")].message}'
# → "ClusterQueue cluster-queue is inactive"
kubectl get resourceflavor    # the old flavor's deletion has now completed — nothing references it
```

Note the workload's message points at **queue health**, not quota. Now repair:

```bash
kubectl apply -f examples/admin/single-clusterqueue-setup.yaml   # recreates flavor, fixes CQ ref
```
…and confirm the parked workload admits by itself within seconds (same requeue machinery as Kata 2 — CQ/flavor events are among the triggers). Re-apply your Kata 3 preemption patch if you continue on.

**What you learned:** CQ activation state lives in the scheduler cache (`pkg/cache/scheduler/clusterqueue.go` — the `FlavorNotFound` reason), the in-use finalizer's real semantics, and the debugging order "queue health before quota" — the spine of the troubleshooting trees in the architecture pageV.


## Kata 6 — Code kata: ship a feature gate end-to-end

**Goal:** run the full change workflow (code → gate → codegen → verify) on a throwaway change. This rehearses Recipe B from the architecture page with real tooling friction.

1. In `pkg/features/kube_features.go`, declare a gate `SchedulerCycleGreeting` (copy the comment/format of a neighboring alpha gate, including the `versionedSpecs` default entry — Alpha, default off).
2. In `pkg/scheduler/scheduler.go`, inside `schedule()`, add:
   ```go
   if features.Enabled(features.SchedulerCycleGreeting) {
       log.V(2).Info("Hello from my first Kueue change")
   }
   ```
3. Regenerate and check:
   ```bash
   make generate-featuregates
   git status   # test/compatibility_lifecycle/reference/versioned_feature_list.yaml
                # and site/data/featuregates/... changed — commit-worthy generated files
   make test    # unit tests still green
   ```
4. Run it in the cluster — rebuild, reload, enable the gate (the same loop as Kata 0; the image is also tagged `:main`, which is convenient here):
   ```bash
   make kind-image-build
   kind load docker-image us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:main --name kueue-lab
   kubectl -n kueue-system set image deploy/kueue-controller-manager \
     manager=us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:main
   kubectl -n kueue-system patch deploy kueue-controller-manager --type json \
     -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--feature-gates=SchedulerCycleGreeting=true"}]'
   kubectl -n kueue-system rollout status deploy/kueue-controller-manager
   ```
   Submit a job and grep the logs for your greeting. Then turn the gate off by resetting the args wholesale (safer than removing by index — verified the hard way):
   ```bash
   kubectl -n kueue-system patch deploy kueue-controller-manager --type json \
     -p '[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":["--config=/controller_manager_config.yaml","--zap-log-level=2"]}]'
   ```
   Wait for the rollout **and** pod readiness before submitting the next job (the webhook briefly refuses connections mid-restart), then confirm the greeting is gone while `"Scheduling cycle starts"` lines keep flowing — **you have now tested both gate states**, which is exactly what reviewers will ask about.

**Verify:** greeting appears only when the gate is on; the gate shows up in `test/compatibility_lifecycle/reference/versioned_feature_list.yaml` after codegen.

Then `git checkout . && git clean -fd` — this kata ships nothing. 😉


## Kata 7 — Test kata: write a failing integration test first

**Goal:** learn the envtest framework and `test/util` builders — the muscle you'll use in every real PR.

1. Open `test/integration/singlecluster/scheduler/` and skim one existing spec to absorb the pattern — two helper packages work together:
   - **object builders** in `pkg/util/testing/v1beta2` (imported as `utiltestingapi`): `utiltestingapi.MakeClusterQueue("cq").ResourceGroup(...).Obj()`, `MakeResourceFlavor`, `MakeLocalQueue`, `MakeWorkload`…
   - **assertion/lifecycle helpers** in `test/util` (imported as `util`): `util.MustCreate`, `util.ExpectWorkloadsToBeAdmitted`, `util.ExpectWorkloadsToBePending`, `util.DeleteNamespace`…
2. Write a new file (e.g. `kata_test.go`, `package scheduler`) with a focused spec asserting something **currently false**: create a flavor + CQ with 5 CPU nominal quota + LQ in a fresh namespace, then a 6-CPU workload, then `util.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, wl)`. Clean up in `AfterEach` (`util.DeleteNamespace` + `util.ExpectObjectToBeDeleted` for CQ and flavor). Compile-check cheaply with `go vet ./test/integration/singlecluster/scheduler/` before invoking the heavy runner.
3. Run just your spec:
   ```bash
   GINKGO_ARGS='--focus="your Describe text"' \
     INTEGRATION_TARGET=./test/integration/singlecluster/scheduler/... \
     make test-integration
   ```
   First run downloads envtest binaries and dependency CRDs; the focused run itself takes ~1–2 min (suite `BeforeSuite` starts the whole manager even for one spec — you'll see `Ran 1 of 90 Specs … 89 Skipped`).
4. Read the failure output carefully — this is what a genuine red test looks like: `[FAIL] … [It] admits a workload that exceeds the nominal quota` with your file:line, after the `Eventually` timeout. Learning to read this output *is* the kata.
5. Flip the assertion to the true behavior — `util.ExpectWorkloadsToBePending(ctx, k8sClient, wl)` — and make it green.

**Verify:** you produced one red run and one green run of your own spec, and you can explain what the envtest environment does and doesn't simulate (real apiserver, no kubelet — pods never run; that's why the framework asserts on conditions, not pod states).


## Graduation

Delete the lab cluster (`kind delete cluster --name kueue-lab`), then:

1. Pick a [`good first issue`](https://github.com/kubernetes-sigs/kueue/issues?q=is%3Aopen+label%3A%22good+first+issue%22).
2. Reproduce it as Kata-7-style failing test where possible.
3. Fix it, run `make verify`, open the PR per the architecture page — and mention in `#wg-batch` that you've completed the katas. 

Every kata above corresponds to a subsystem chapter in the internals section — when an issue stumps you, the chapter for that kata is where to reread.
