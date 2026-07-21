/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"testing"

	sparkappv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/featuregate"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingv1beta2 "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

// TestNumInitialExecutors asserts that the initial executor PodSet count
// matches the number of executor pods Spark will actually create at startup,
// per P1 of KEP-0000 (elastic SparkApplication).
func TestNumInitialExecutors(t *testing.T) {
	base := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns")

	testCases := map[string]struct {
		sparkApp *sparkappv1beta2.SparkApplication
		want     int32
	}{
		"static mode, instances unset defaults to Spark's static default of 2": {
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := base.Clone().Obj()
				app.Spec.Executor.Instances = nil
				return app
			}(),
			want: 2,
		},
		"static mode, instances explicit": {
			sparkApp: base.Clone().ExecutorInstances(5).Obj(),
			want:     5,
		},
		"dynamic allocation enabled, nothing set ramps from zero": {
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := base.Clone().
					DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true}).
					Obj()
				app.Spec.Executor.Instances = nil
				return app
			}(),
			want: 0,
		},
		"dynamic allocation enabled, only instances set": {
			sparkApp: base.Clone().
				ExecutorInstances(3).
				DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true}).
				Obj(),
			want: 3,
		},
		"dynamic allocation enabled, initialExecutors and minExecutors set, no instances": {
			sparkApp: base.Clone().
				DynamicAllocation(&sparkappv1beta2.DynamicAllocation{
					Enabled:          true,
					InitialExecutors: ptr.To[int32](4),
					MinExecutors:     ptr.To[int32](2),
				}).
				Obj(),
			want: 4,
		},
		"dynamic allocation enabled, minExecutors is the largest value": {
			sparkApp: base.Clone().
				ExecutorInstances(2).
				DynamicAllocation(&sparkappv1beta2.DynamicAllocation{
					Enabled:          true,
					InitialExecutors: ptr.To[int32](1),
					MinExecutors:     ptr.To[int32](5),
				}).
				Obj(),
			want: 5,
		},
		"dynamic allocation disabled ignores initialExecutors/minExecutors": {
			sparkApp: base.Clone().
				DynamicAllocation(&sparkappv1beta2.DynamicAllocation{
					Enabled:          false,
					InitialExecutors: ptr.To[int32](7),
					MinExecutors:     ptr.To[int32](9),
				}).
				Obj(),
			want: 1, // builder default Instances=1
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			j := (*SparkApplication)(tc.sparkApp)
			if got := j.numInitialExecutors(); got != tc.want {
				t.Errorf("numInitialExecutors() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestComputeMemoryOverhead asserts overhead is computed the way Spark
// computes it for the real pod: explicit override, else factor * memory
// (JVM 0.1 / non-JVM 0.4 default, or an explicit factor), floored at 384Mi.
func TestComputeMemoryOverhead(t *testing.T) {
	testCases := map[string]struct {
		memory           string
		explicitOverhead *string
		factorOverride   *string
		appType          sparkappv1beta2.SparkApplicationType
		want             string
		wantErr          bool
	}{
		"explicit overhead wins regardless of memory or type": {
			memory:           "8Gi",
			explicitOverhead: ptr.To("256Mi"),
			appType:          sparkappv1beta2.SparkApplicationTypePython,
			want:             "256Mi",
		},
		"JVM default factor below floor uses the 384Mi floor": {
			memory:  "512Mi",
			appType: sparkappv1beta2.SparkApplicationTypeScala,
			want:    "384Mi",
		},
		"JVM default factor above floor": {
			// 8000Mi chosen (rather than 8Gi) so 10% lands on a whole byte count.
			memory:  "8000Mi",
			appType: sparkappv1beta2.SparkApplicationTypeJava,
			want:    "800Mi", // 10% of 8000Mi = 800Mi
		},
		"non-JVM (Python) default factor is 0.4": {
			// 1000Mi chosen (rather than 1Gi) so 40% lands on a whole byte count.
			memory:  "1000Mi",
			appType: sparkappv1beta2.SparkApplicationTypePython,
			want:    "400Mi", // 40% of 1000Mi = 400Mi
		},
		"non-JVM (R) default factor is 0.4": {
			memory:  "1000Mi",
			appType: sparkappv1beta2.SparkApplicationTypeR,
			want:    "400Mi",
		},
		"explicit factor override": {
			memory:         "10Gi",
			factorOverride: ptr.To("0.2"),
			appType:        sparkappv1beta2.SparkApplicationTypeScala,
			want:           "2Gi",
		},
		"invalid factor override errors": {
			memory:         "1Gi",
			factorOverride: ptr.To("not-a-float"),
			appType:        sparkappv1beta2.SparkApplicationTypeScala,
			wantErr:        true,
		},
		"invalid explicit overhead errors": {
			memory:           "1Gi",
			explicitOverhead: ptr.To("not-a-quantity"),
			appType:          sparkappv1beta2.SparkApplicationTypeScala,
			wantErr:          true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			memQuantity := resource.MustParse(tc.memory)
			got, err := computeMemoryOverhead(memQuantity, tc.explicitOverhead, tc.factorOverride, tc.appType)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("computeMemoryOverhead() returned no error, want one")
				}
				return
			}
			if err != nil {
				t.Fatalf("computeMemoryOverhead() returned error: %v", err)
			}
			want := resource.MustParse(tc.want)
			if got.Cmp(want) != 0 {
				t.Errorf("computeMemoryOverhead() = %s, want %s", got.String(), want.String())
			}
		})
	}
}

// TestAddCPURequestsFallback asserts the CPU request fallback chain:
// coreRequest -> cores -> Spark's default of 1 core.
func TestAddCPURequestsFallback(t *testing.T) {
	base := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns")

	testCases := map[string]struct {
		sparkApp *sparkappv1beta2.SparkApplication
		want     string
	}{
		"coreRequest set is used verbatim": {
			sparkApp: base.Clone().ExecutorCoreRequest("250m").Obj(),
			want:     "250m",
		},
		"coreRequest unset falls back to cores": {
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := base.Clone().Obj()
				app.Spec.Executor.CoreRequest = nil
				app.Spec.Executor.Cores = ptr.To[int32](4)
				return app
			}(),
			want: "4",
		},
		"neither set falls back to Spark's default of 1 core": {
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := base.Clone().Obj()
				app.Spec.Executor.CoreRequest = nil
				return app
			}(),
			want: "1",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			j := (*SparkApplication)(tc.sparkApp)
			tmpl, err := j.buildExecutorPodTemplateSpec()
			if err != nil {
				t.Fatalf("buildExecutorPodTemplateSpec() returned error: %v", err)
			}
			// addObjectMeta (the last mutateSparkPod option) replaces the pod's
			// labels with those derived from the app spec, so the synthetic
			// spark-role label used internally by findContainer is gone by the
			// time the template is returned. The single default container is
			// always at index 0.
			got := tmpl.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]
			want := resource.MustParse(tc.want)
			if got.Cmp(want) != 0 {
				t.Errorf("executor CPU request = %s, want %s", got.String(), want.String())
			}
		})
	}
}

func makeExecutorPod(app *sparkappv1beta2.SparkApplication, name string, phase corev1.PodPhase, deleting bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: app.Namespace,
			Labels: map[string]string{
				sparkcommon.LabelSparkAppName: app.Name,
				sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
	if deleting {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		pod.Finalizers = []string{"kueue.x-k8s.io/test-finalizer"}
	}
	return pod
}

// TestNumExecutors asserts the executor PodSet count computation: unchanged
// (numInitialExecutors) for non-elastic applications, and demand-observed
// (floored at the initial target or minExecutors, capped at maxExecutors) for
// elastic ones.
func TestNumExecutors(t *testing.T) {
	base := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns")

	elasticBase := func() *sparkapplicationtesting.SparkApplicationWrapper {
		return base.Clone().
			Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
			ExecutorInstances(1)
	}

	testCases := map[string]struct {
		sparkApp     *sparkappv1beta2.SparkApplication
		pods         []*corev1.Pod
		nilClient    bool
		featureGates map[featuregate.Feature]bool
		want         int32
	}{
		"non-elastic: returns numInitialExecutors regardless of live pods": {
			sparkApp: base.Clone().ExecutorInstances(3).Obj(),
			pods: func() []*corev1.Pod {
				app := base.Clone().ExecutorInstances(3).Obj()
				return []*corev1.Pod{makeExecutorPod(app, "exec-1", corev1.PodRunning, false)}
			}(),
			want: 3,
		},
		"elastic, driver not running: floors at the initial target": {
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			sparkApp: elasticBase().
				DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](1), MaxExecutors: ptr.To[int32](10)}).
				Obj(),
			want: 1, // numInitialExecutors(): instances=1
		},
		"elastic, driver running, no pods observed: floors at minExecutors": {
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := elasticBase().
					DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](2), MaxExecutors: ptr.To[int32](10)}).
					Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				return app
			}(),
			want: 2,
		},
		"elastic, driver running, observed demand above floor": {
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := elasticBase().
					DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](1), MaxExecutors: ptr.To[int32](10)}).
					Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				return app
			}(),
			pods: func() []*corev1.Pod {
				app := elasticBase().Obj()
				return []*corev1.Pod{
					makeExecutorPod(app, "exec-1", corev1.PodRunning, false),
					makeExecutorPod(app, "exec-2", corev1.PodRunning, false),
					makeExecutorPod(app, "exec-3", corev1.PodPending, false), // gated, still counted as demand
				}
			}(),
			want: 3,
		},
		"elastic, driver running, observed demand clamped at maxExecutors": {
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := elasticBase().
					DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](1), MaxExecutors: ptr.To[int32](2)}).
					Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				return app
			}(),
			pods: func() []*corev1.Pod {
				app := elasticBase().Obj()
				return []*corev1.Pod{
					makeExecutorPod(app, "exec-1", corev1.PodRunning, false),
					makeExecutorPod(app, "exec-2", corev1.PodRunning, false),
					makeExecutorPod(app, "exec-3", corev1.PodRunning, false),
				}
			}(),
			want: 2,
		},
		"elastic, driver running, terminal and deleting pods are not counted as demand": {
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := elasticBase().
					DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](2), MaxExecutors: ptr.To[int32](10)}).
					Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				return app
			}(),
			pods: func() []*corev1.Pod {
				app := elasticBase().Obj()
				return []*corev1.Pod{
					makeExecutorPod(app, "exec-succeeded", corev1.PodSucceeded, false),
					makeExecutorPod(app, "exec-failed", corev1.PodFailed, false),
					makeExecutorPod(app, "exec-deleting", corev1.PodRunning, true),
				}
			}(),
			want: 2, // floor (minExecutors), since no live demand observed
		},
		"elastic, nil client falls back to the floor without listing": {
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := elasticBase().
					DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](4), MaxExecutors: ptr.To[int32](10)}).
					Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				return app
			}(),
			nilClient: true,
			want:      4,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.featureGates)
			ctx, _ := utiltesting.ContextWithLog(t)

			j := (*SparkApplication)(tc.sparkApp)

			var c client.Client
			if !tc.nilClient {
				objs := make([]client.Object, len(tc.pods))
				for i, p := range tc.pods {
					objs[i] = p
				}
				c = utiltesting.NewClientBuilder(sparkappv1beta2.AddToScheme).
					WithIndex(&kueue.Workload{}, indexer.OwnerReferenceIndexKey(gvk), indexer.WorkloadOwnerIndexFunc(gvk)).
					WithObjects(objs...).
					Build()
			}

			got, err := j.numExecutors(ctx, c)
			if err != nil {
				t.Fatalf("numExecutors() returned error: %v", err)
			}
			if got != tc.want {
				t.Errorf("numExecutors() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestNumExecutorsMultiKueueDelegated covers numExecutors' delegated-mode
// branch: when this SparkApplication is the management-cluster copy of a
// MultiKueue-admitted application, demand must come from the mirrored
// SparkApplicationPodSetReplicaSizesAnnotation (synced back by the
// SparkApplication MultiKueue adapter), never from local pod observation,
// since no executor pods ever run on the management cluster.
func TestNumExecutorsMultiKueueDelegated(t *testing.T) {
	base := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns")
	app := base.Clone().
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		ExecutorInstances(1).
		DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](1), MaxExecutors: ptr.To[int32](10)}).
		AppState(sparkappv1beta2.ApplicationStateRunning)

	multiKueueAC := utiltestingv1beta2.MakeAdmissionCheck("multikueue-ac").ControllerName(kueue.MultiKueueControllerName).Obj()
	otherAC := utiltestingv1beta2.MakeAdmissionCheck("other-ac").ControllerName("other-controller").Obj()

	testCases := map[string]struct {
		sparkApp        *sparkappv1beta2.SparkApplication
		admissionChecks []*kueue.AdmissionCheck
		workloads       []*kueue.Workload
		pods            []*corev1.Pod
		featureGates    map[featuregate.Feature]bool
		want            int32
	}{
		"delegated: mirrors the replica-sizes annotation instead of observed pods": {
			sparkApp: app.Clone().
				Annotation(SparkApplicationPodSetReplicaSizesAnnotation, `[{"name":"executor","count":6}]`).
				Obj(),
			admissionChecks: []*kueue.AdmissionCheck{multiKueueAC},
			workloads: []*kueue.Workload{
				utiltestingv1beta2.MakeWorkload("sparkapp-wl", "ns").
					OwnerReference(gvk, "sparkapp", "uid1").
					AdmissionCheck(kueue.AdmissionCheckState{Name: "multikueue-ac"}).
					Obj(),
			},
			pods: []*corev1.Pod{
				makeExecutorPod(app.Clone().Obj(), "exec-1", corev1.PodRunning, false),
				makeExecutorPod(app.Clone().Obj(), "exec-2", corev1.PodRunning, false),
			},
			want: 6,
		},
		"delegated, no mirrored annotation yet: floors at minExecutors": {
			sparkApp:        app.Clone().Obj(),
			admissionChecks: []*kueue.AdmissionCheck{multiKueueAC},
			workloads: []*kueue.Workload{
				utiltestingv1beta2.MakeWorkload("sparkapp-wl", "ns").
					OwnerReference(gvk, "sparkapp", "uid1").
					AdmissionCheck(kueue.AdmissionCheckState{Name: "multikueue-ac"}).
					Obj(),
			},
			pods: []*corev1.Pod{
				makeExecutorPod(app.Clone().Obj(), "exec-1", corev1.PodRunning, false),
			},
			want: 1,
		},
		"not delegated: admission check is not a MultiKueue controller, observes pods as usual": {
			sparkApp:        app.Clone().Obj(),
			admissionChecks: []*kueue.AdmissionCheck{otherAC},
			workloads: []*kueue.Workload{
				utiltestingv1beta2.MakeWorkload("sparkapp-wl", "ns").
					OwnerReference(gvk, "sparkapp", "uid1").
					AdmissionCheck(kueue.AdmissionCheckState{Name: "other-ac"}).
					Obj(),
			},
			pods: []*corev1.Pod{
				makeExecutorPod(app.Clone().Obj(), "exec-1", corev1.PodRunning, false),
				makeExecutorPod(app.Clone().Obj(), "exec-2", corev1.PodRunning, false),
			},
			want: 2,
		},
		"MultiKueue feature gate disabled: never treated as delegated, even with a matching admission check": {
			featureGates: map[featuregate.Feature]bool{features.MultiKueue: false},
			sparkApp: app.Clone().
				Annotation(SparkApplicationPodSetReplicaSizesAnnotation, `[{"name":"executor","count":6}]`).
				Obj(),
			admissionChecks: []*kueue.AdmissionCheck{multiKueueAC},
			workloads: []*kueue.Workload{
				utiltestingv1beta2.MakeWorkload("sparkapp-wl", "ns").
					OwnerReference(gvk, "sparkapp", "uid1").
					AdmissionCheck(kueue.AdmissionCheckState{Name: "multikueue-ac"}).
					Obj(),
			},
			pods: []*corev1.Pod{
				makeExecutorPod(app.Clone().Obj(), "exec-1", corev1.PodRunning, false),
			},
			want: 1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.featureGates)
			ctx, _ := utiltesting.ContextWithLog(t)

			j := (*SparkApplication)(tc.sparkApp)

			var objs []client.Object
			for _, p := range tc.pods {
				objs = append(objs, p)
			}
			for _, ac := range tc.admissionChecks {
				objs = append(objs, ac)
			}
			for _, wl := range tc.workloads {
				objs = append(objs, wl)
			}
			c := utiltesting.NewClientBuilder(sparkappv1beta2.AddToScheme).
				WithIndex(&kueue.Workload{}, indexer.OwnerReferenceIndexKey(gvk), indexer.WorkloadOwnerIndexFunc(gvk)).
				WithObjects(objs...).
				Build()

			got, err := j.numExecutors(ctx, c)
			if err != nil {
				t.Fatalf("numExecutors() returned error: %v", err)
			}
			if got != tc.want {
				t.Errorf("numExecutors() = %d, want %d", got, tc.want)
			}
		})
	}
}
