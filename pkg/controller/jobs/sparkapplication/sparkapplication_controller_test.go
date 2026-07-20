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
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	sparkappv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/featuregate"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var (
	sparkAppCmpOpts = cmp.Options{
		cmpopts.EquateEmpty(),
		cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion"),
	}
	workloadCmpOpts = cmp.Options{
		cmpopts.EquateEmpty(),
		cmpopts.SortSlices(func(a, b metav1.Condition) bool { return a.Type < b.Type }),
		cmpopts.IgnoreFields(kueue.Workload{}, "TypeMeta"),
		cmpopts.IgnoreFields(metav1.ObjectMeta{}, "Name", "Labels", "ResourceVersion", "OwnerReferences", "Finalizers"),
		cmpopts.IgnoreFields(kueue.WorkloadSpec{}, "Priority"),
		cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime"),
		cmpopts.IgnoreFields(kueue.PodSet{}, "Template"),
	}
)

func TestPodSets(t *testing.T) {
	toleration := corev1.Toleration{
		Key:      "t1k",
		Operator: corev1.TolerationOpEqual,
		Value:    "t1v",
		Effect:   corev1.TaintEffectNoExecute,
	}
	nodeSelector := map[string]string{"test": "test"}
	testSparkApp := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns")

	testCases := map[string]struct {
		sparkApp     *sparkappv1beta2.SparkApplication
		featureGates map[featuregate.Feature]bool
		want         []kueue.PodSet
	}{
		"base": {
			sparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorInstances(3).Obj(),
			want: []kueue.PodSet{
				*utiltestingapi.MakePodSet("driver", 1).PodSpec(corev1.PodSpec{
					NodeSelector:   maps.Clone(nodeSelector),
					Tolerations:    []corev1.Toleration{*toleration.DeepCopy()},
					InitContainers: []corev1.Container{},
					Containers: []corev1.Container{
						{
							Name: sparkcommon.SparkDriverContainerName,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("896Mi"),
								},
							},
						},
					},
				}).Obj(),
				*utiltestingapi.MakePodSet("executor", 3).PodSpec(corev1.PodSpec{
					NodeSelector:   maps.Clone(nodeSelector),
					Tolerations:    []corev1.Toleration{*toleration.DeepCopy()},
					InitContainers: []corev1.Container{},
					Containers: []corev1.Container{
						{
							Name: sparkcommon.Spark3DefaultExecutorContainerName,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("896Mi"),
								},
							},
						},
					},
				}).Obj(),
			},
		},
		"with TopologyAwareScheduling": {
			featureGates: map[featuregate.Feature]bool{features.TopologyAwareScheduling: true},
			sparkApp: testSparkApp.Clone().Queue("local-queue").
				DriverAnnotation(
					kueue.PodSetRequiredTopologyAnnotation, "cloud.com/block",
				).
				ExecutorAnnotation(
					kueue.PodSetRequiredTopologyAnnotation, "cloud.com/block",
				).Obj(),
			want: []kueue.PodSet{
				*utiltestingapi.MakePodSet("driver", 1).
					RequiredTopologyRequest("cloud.com/block").
					Annotations(map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.com/block",
					}).
					PodSpec(corev1.PodSpec{
						NodeSelector:   map[string]string{},
						Tolerations:    []corev1.Toleration{},
						InitContainers: []corev1.Container{},
						Containers: []corev1.Container{
							{
								Name: sparkcommon.SparkDriverContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("896Mi"),
									},
								},
							},
						},
					}).
					Obj(),
				*utiltestingapi.MakePodSet("executor", 1).
					RequiredTopologyRequest("cloud.com/block").
					Annotations(map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.com/block",
					}).
					PodSpec(corev1.PodSpec{
						NodeSelector:   map[string]string{},
						Tolerations:    []corev1.Toleration{},
						InitContainers: []corev1.Container{},
						Containers: []corev1.Container{
							{
								Name: sparkcommon.Spark3DefaultExecutorContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("896Mi"),
									},
								},
							},
						},
					}).Obj(),
			},
		},
		"with TopologyAwareScheduling annotation but TopologyAwareScheduling feature gate disabled": {
			featureGates: map[featuregate.Feature]bool{features.TopologyAwareScheduling: false},
			sparkApp: testSparkApp.Clone().Queue("local-queue").
				DriverAnnotation(
					kueue.PodSetRequiredTopologyAnnotation, "cloud.com/block",
				).
				ExecutorAnnotation(
					kueue.PodSetRequiredTopologyAnnotation, "cloud.com/block",
				).Obj(),
			want: []kueue.PodSet{
				*utiltestingapi.MakePodSet("driver", 1).
					Annotations(map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.com/block",
					}).
					PodSpec(corev1.PodSpec{
						NodeSelector:   map[string]string{},
						Tolerations:    []corev1.Toleration{},
						InitContainers: []corev1.Container{},
						Containers: []corev1.Container{
							{
								Name: sparkcommon.SparkDriverContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("896Mi"),
									},
								},
							},
						},
					}).
					Obj(),
				*utiltestingapi.MakePodSet("executor", 1).
					Annotations(map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.com/block",
					}).
					PodSpec(corev1.PodSpec{
						NodeSelector:   map[string]string{},
						Tolerations:    []corev1.Toleration{},
						InitContainers: []corev1.Container{},
						Containers: []corev1.Container{
							{
								Name: sparkcommon.Spark3DefaultExecutorContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("896Mi"),
									},
								},
							},
						},
					}).Obj(),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.featureGates)

			ctx, _ := utiltesting.ContextWithLog(t)

			kSparkApp := (*SparkApplication)(tc.sparkApp)
			got, err := kSparkApp.PodSets(ctx, nil)

			if err != nil {
				t.Fatalf("PodSets() returned error: %v", err)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("PodSets() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRunWithPodsetsInfo(t *testing.T) {
	toleration := corev1.Toleration{
		Key:      "t1k",
		Operator: corev1.TolerationOpEqual,
		Value:    "t1v",
		Effect:   corev1.TaintEffectNoExecute,
	}
	toleration2 := corev1.Toleration{
		Key:      "t2k",
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	}
	nodeSelector := map[string]string{"disktype": "ssd"}
	nodeSelector2 := map[string]string{"disktype": "hdd"}
	schedulingGate := corev1.PodSchedulingGate{Name: "test-scheduling-gate-1"}
	schedulingGate2 := corev1.PodSchedulingGate{Name: "test-scheduling-gate-2"}

	testSparkApp := sparkapplicationtesting.MakeSparkApplication("test-sparkapp", "ns")

	cases := map[string]struct {
		sparkApp     *sparkappv1beta2.SparkApplication
		podsetsInfo  []podset.PodSetInfo
		wantSparkApp *sparkappv1beta2.SparkApplication
		wantErr      bool
	}{
		"should add to the SparkApplication specified in the PodSet info": {
			sparkApp: testSparkApp.DeepCopy(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
				{
					Name:            "executor",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
			},
			wantSparkApp: testSparkApp.Clone().
				Suspend(false).
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				Obj(),
			wantErr: false,
		},
		"should raise error when PodSet info config conflicts to the SparkApplication": {
			sparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector2)).
				DriverTolerations([]corev1.Toleration{*toleration2.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate2.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorTolerations([]corev1.Toleration{*toleration2.DeepCopy()}).
				ExecutorNodeSelector(maps.Clone(nodeSelector2)).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate2.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				Obj(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
				{
					Name:            "executor",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
			},
			wantErr: true,
		},
		"should apply different node selectors per role when global nodeSelector is set": {
			sparkApp: testSparkApp.Clone().
				NodeSelector(map[string]string{"zone": "us-east"}).
				Obj(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:         "driver",
					NodeSelector: map[string]string{"zone": "us-east", "node-type": "cpu"},
				},
				{
					Name:         "executor",
					NodeSelector: map[string]string{"zone": "us-east", "node-type": "gpu"},
				},
			},
			wantSparkApp: testSparkApp.Clone().
				Suspend(false).
				DriverNodeSelector(map[string]string{"zone": "us-east", "node-type": "cpu"}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(map[string]string{"zone": "us-east", "node-type": "gpu"}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				Obj(),
			wantErr: false,
		},
		"should raise error if the wrong number of PodSet infos is provided": {
			sparkApp: testSparkApp.DeepCopy(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
			},
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)

			kSparkApp := (*SparkApplication)(tc.sparkApp)
			err := kSparkApp.RunWithPodSetsInfo(ctx, nil, tc.podsetsInfo)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected RunWithPodSetsInfo() to fail")
					return
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected RunWithPodSetsInfo() error: %v", err)
				return
			}
			if diff := cmp.Diff(tc.wantSparkApp, tc.sparkApp, sparkAppCmpOpts); diff != "" {
				t.Errorf("RunWithPodSetsInfo() mismatch (-want,+got):\n%s", diff)
				return
			}
		})
	}
}

func TestRestorePodSetsInfo(t *testing.T) {
	toleration := corev1.Toleration{
		Key:      "t1k",
		Operator: corev1.TolerationOpEqual,
		Value:    "t1v",
		Effect:   corev1.TaintEffectNoExecute,
	}
	nodeSelector := map[string]string{"disktype": "ssd"}
	schedulingGate := corev1.PodSchedulingGate{Name: "test-scheduling-gate-1"}
	testSparkApp := sparkapplicationtesting.MakeSparkApplication("test-sparkapp", "ns")

	cases := map[string]struct {
		sparkApp     *sparkappv1beta2.SparkApplication
		podsetsInfo  []podset.PodSetInfo
		wantSparkApp *sparkappv1beta2.SparkApplication
		wantChanged  bool
	}{
		"should restore PodSet info to the SparkApplication": {
			sparkApp: testSparkApp.DeepCopy(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
				{
					Name:            "executor",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
					Count:           3,
				},
			},
			wantSparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				ExecutorInstances(3).
				Obj(),
			wantChanged: true,
		},
		"should not modify the SparkApplication when already restored": {
			sparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				ExecutorInstances(3).
				Obj(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
				{
					Name:            "executor",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
					Count:           3,
				},
			},
			wantSparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				ExecutorInstances(3).
				Obj(),
			wantChanged: false,
		},
		"should restore per-role node selectors when global nodeSelector is set": {
			sparkApp: testSparkApp.Clone().
				NodeSelector(map[string]string{"zone": "us-east"}).
				Obj(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:         "driver",
					NodeSelector: map[string]string{"zone": "us-east", "node-type": "cpu"},
				},
				{
					Name:         "executor",
					NodeSelector: map[string]string{"zone": "us-east", "node-type": "gpu"},
					Count:        3,
				},
			},
			wantSparkApp: testSparkApp.Clone().
				DriverNodeSelector(map[string]string{"zone": "us-east", "node-type": "cpu"}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(map[string]string{"zone": "us-east", "node-type": "gpu"}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				ExecutorInstances(3).
				Obj(),
			wantChanged: true,
		},
		"should not modify the SparkApplication  if the wrong number of PodSet infos is provided": {
			sparkApp: testSparkApp.DeepCopy(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
			},
			wantSparkApp: testSparkApp.DeepCopy(),
			wantChanged:  false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			kSparkApp := (*SparkApplication)(tc.sparkApp)
			changed := kSparkApp.RestorePodSetsInfo(t.Context(), tc.podsetsInfo)
			if diff := cmp.Diff(tc.wantChanged, changed); diff != "" {
				t.Errorf("changed mismatch (-want,+got):\n%s", diff)
				return
			}
			if diff := cmp.Diff(tc.wantSparkApp, tc.sparkApp, sparkAppCmpOpts); diff != "" {
				t.Errorf("RunWithPodSetsInfo() mismatch (-want,+got):\n%s", diff)
				return
			}
		})
	}
}

func TestReconciler(t *testing.T) {
	now := time.Now().Truncate(time.Second)

	testNamespace := utiltesting.MakeNamespaceWrapper("ns").Label(corev1.LabelMetadataName, "ns").Obj()
	const testSparkAppUID types.UID = "test-sparkapp-uid"
	testSparkApp := sparkapplicationtesting.MakeSparkApplication("test-sparkapp", testNamespace.Name)

	// Helpers for the PodsReady cases: build a SparkApplication whose status
	// simulates "driver Running and N executors in the given state". The
	// framework calls PodsReady() during Reconcile and we assert the resulting
	// Workload condition.
	withUID := func(s *sparkappv1beta2.SparkApplication) *sparkappv1beta2.SparkApplication {
		s.UID = testSparkAppUID
		return s
	}
	withExecutorsRunning := func(s *sparkappv1beta2.SparkApplication, n int32) *sparkappv1beta2.SparkApplication {
		s.Spec.Executor.Instances = new(n)
		s.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
		states := make(map[string]sparkappv1beta2.ExecutorState, n)
		for i := int32(1); i <= n; i++ {
			states[fmt.Sprintf("%s-exec-%d", s.Name, i)] = sparkappv1beta2.ExecutorStateRunning
		}
		s.Status.ExecutorState = states
		return s
	}
	withDriverRunningOnly := func(s *sparkappv1beta2.SparkApplication, expected int32) *sparkappv1beta2.SparkApplication {
		s.Spec.Executor.Instances = new(expected)
		s.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
		// No ExecutorState entries — simulates the Spark Operator state where
		// AppState flips to Running as soon as the driver starts even though
		// executors haven't reported ready yet (the bug this fix addresses).
		return s
	}

	// Build a workload whose PodSets are derived from the same SparkApplication
	// the framework will Reconcile, so EquivalentToWorkload returns true and
	// the workload is treated as "matching" rather than recreated.
	makeAdmittedWorkload := func(s *sparkappv1beta2.SparkApplication) *utiltestingapi.WorkloadWrapper {
		t.Helper()
		podSets, err := (*SparkApplication)(s).PodSets(t.Context(), nil)
		if err != nil {
			t.Fatalf("PodSets returned error during test setup: %v", err)
		}
		psas := make([]kueue.PodSetAssignment, 0, len(podSets))
		for i := range podSets {
			psas = append(psas, utiltestingapi.MakePodSetAssignment(podSets[i].Name).Count(podSets[i].Count).Obj())
		}
		return utiltestingapi.MakeWorkload("wl", testNamespace.Name).
			Finalizers(kueue.ResourceInUseFinalizerName).
			ControllerReference(gvk, s.Name, string(s.UID)).
			PodSets(podSets...).
			ReserveQuotaAt(
				utiltestingapi.MakeAdmission("cq").PodSets(psas...).Obj(),
				now,
			).
			AdmittedAt(true, now)
	}

	baseWaitForPodsReadyConf := &configapi.WaitForPodsReady{}

	// SparkApp variants used by the PodsReady cases.
	sparkAppDriverRunningOnly := withDriverRunningOnly(withUID(testSparkApp.DeepCopy()), 2)
	sparkAppAllExecutorsReady := withExecutorsRunning(withUID(testSparkApp.DeepCopy()), 2)

	cases := map[string]struct {
		reconcilerOptions []jobframework.Option
		sparkApp          *sparkappv1beta2.SparkApplication
		workloads         []kueue.Workload
		wantWorkloads     []kueue.Workload
	}{
		"workload is created with the corresponding podsets": {
			reconcilerOptions: []jobframework.Option{
				jobframework.WithManageJobsWithoutQueueName(true),
				jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
			},
			sparkApp: testSparkApp.DeepCopy(),
			wantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload(testSparkApp.Name, testSparkApp.Namespace).
					PodSets(
						*utiltestingapi.MakePodSet("driver", 1).Obj(),
						*utiltestingapi.MakePodSet("executor", 1).Obj(),
					).
					Obj(),
			},
		},
		"PodsReady stays False/WaitForStart while AppState=Running but no executors reported ready": {
			reconcilerOptions: []jobframework.Option{
				jobframework.WithManageJobsWithoutQueueName(true),
				jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
				jobframework.WithWaitForPodsReady(baseWaitForPodsReadyConf),
			},
			sparkApp: sparkAppDriverRunningOnly,
			workloads: []kueue.Workload{
				*makeAdmittedWorkload(sparkAppDriverRunningOnly).Obj(),
			},
			wantWorkloads: []kueue.Workload{
				*makeAdmittedWorkload(sparkAppDriverRunningOnly).
					Condition(metav1.Condition{
						Type:    kueue.WorkloadPodsReady,
						Status:  metav1.ConditionFalse,
						Reason:  kueue.WorkloadWaitForStart,
						Message: "Not all pods are ready or succeeded",
					}).
					Obj(),
			},
		},
		"PodsReady becomes True/Started after AppState=Running and all executors reach Running": {
			reconcilerOptions: []jobframework.Option{
				jobframework.WithManageJobsWithoutQueueName(true),
				jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
				jobframework.WithWaitForPodsReady(baseWaitForPodsReadyConf),
			},
			sparkApp: sparkAppAllExecutorsReady,
			workloads: []kueue.Workload{
				*makeAdmittedWorkload(sparkAppAllExecutorsReady).Obj(),
			},
			wantWorkloads: []kueue.Workload{
				*makeAdmittedWorkload(sparkAppAllExecutorsReady).
					Condition(metav1.Condition{
						Type:    kueue.WorkloadPodsReady,
						Status:  metav1.ConditionTrue,
						Reason:  kueue.WorkloadStarted,
						Message: "All pods reached readiness and the workload is running",
					}).
					Obj(),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)

			clientBuilder := utiltesting.NewClientBuilder(sparkappv1beta2.AddToScheme).
				WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: utiltesting.TreatSSAAsStrategicMerge})
			kClient := clientBuilder.
				WithObjects(tc.sparkApp, testNamespace).
				WithStatusSubresource(&kueue.Workload{}).
				Build()
			// Pre-existing workloads must be created via the client (not WithObjects)
			// so that the status subresource is initialized correctly.
			for i := range tc.workloads {
				if err := kClient.Create(ctx, &tc.workloads[i]); err != nil {
					t.Fatalf("Could not create pre-existing workload: %v", err)
				}
			}
			indexer := utiltesting.AsIndexer(clientBuilder)
			if err := SetupIndexes(ctx, indexer); err != nil {
				t.Fatalf("Could not setup indexes: %v", err)
			}
			recorder := &utiltesting.EventRecorder{}
			reconciler, err := NewReconciler(ctx, kClient, indexer, recorder,
				append(tc.reconcilerOptions,
					jobframework.WithCache(schdcache.New(kClient)),
					jobframework.WithClock(testingclock.NewFakeClock(now)),
				)...)
			if err != nil {
				t.Errorf("Error creating the reconciler: %v", err)
			}

			sparkAppKey := client.ObjectKeyFromObject(tc.sparkApp)
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: sparkAppKey,
			})
			if err != nil {
				t.Errorf("Reconcile returned error: %v", err)
			}

			var gotWorkloads kueue.WorkloadList
			if err := kClient.List(ctx, &gotWorkloads); err != nil {
				t.Fatalf("Could not get Workloads after reconcile: %v", err)
			}
			if diff := cmp.Diff(tc.wantWorkloads, gotWorkloads.Items, workloadCmpOpts...); diff != "" {
				t.Errorf("Workloads after reconcile (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestGetCustomAnnotations asserts that the replica-sizes annotation is only
// produced for elastic applications, and that it accurately serializes the
// given PodSet counts.
func TestGetCustomAnnotations(t *testing.T) {
	elasticApp := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns").
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		Obj()
	nonElasticApp := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns").Obj()

	podSets := []kueue.PodSet{
		*utiltestingapi.MakePodSet(driverPodSetName, 1).Obj(),
		*utiltestingapi.MakePodSet(executorPodSetName, 5).Obj(),
	}

	testCases := map[string]struct {
		sparkApp     *sparkappv1beta2.SparkApplication
		featureGates map[featuregate.Feature]bool
		want         map[string]string
	}{
		"elastic application returns the replica-sizes annotation": {
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			sparkApp:     elasticApp,
			want: map[string]string{
				SparkApplicationPodSetReplicaSizesAnnotation: `[{"name":"driver","count":1},{"name":"executor","count":5}]`,
			},
		},
		"non-elastic application returns nil": {
			sparkApp: nonElasticApp,
			want:     nil,
		},
		"elastic annotation without the feature gate returns nil": {
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: false},
			sparkApp:     elasticApp,
			want:         nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.featureGates)
			ctx, _ := utiltesting.ContextWithLog(t)

			j := (*SparkApplication)(tc.sparkApp)
			got, err := j.GetCustomAnnotations(ctx, nil, podSets)
			if err != nil {
				t.Fatalf("GetCustomAnnotations() returned error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("GetCustomAnnotations() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestGetWorkloadNameExtraPart asserts that distinct observed sizes produce
// distinct extra parts, and that the same size is deterministic.
func TestGetWorkloadNameExtraPart(t *testing.T) {
	base := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns").Obj()

	withReplicaSizes := func(sizes string) *SparkApplication {
		app := base.DeepCopy()
		if sizes != "" {
			app.Annotations = map[string]string{SparkApplicationPodSetReplicaSizesAnnotation: sizes}
		}
		return (*SparkApplication)(app)
	}

	noAnnotation := withReplicaSizes("")
	fiveExecutors := withReplicaSizes(`[{"name":"driver","count":1},{"name":"executor","count":5}]`)
	fiveExecutorsAgain := withReplicaSizes(`[{"name":"driver","count":1},{"name":"executor","count":5}]`)
	tenExecutors := withReplicaSizes(`[{"name":"driver","count":1},{"name":"executor","count":10}]`)

	if got := noAnnotation.GetWorkloadNameExtraPart(); got == "" {
		t.Errorf("GetWorkloadNameExtraPart() with no annotation = %q, want non-empty (should at least contain the generation)", got)
	}
	if got, want := fiveExecutors.GetWorkloadNameExtraPart(), fiveExecutorsAgain.GetWorkloadNameExtraPart(); got != want {
		t.Errorf("GetWorkloadNameExtraPart() is not deterministic for the same replica sizes: %q != %q", got, want)
	}
	if got, other := fiveExecutors.GetWorkloadNameExtraPart(), tenExecutors.GetWorkloadNameExtraPart(); got == other {
		t.Errorf("GetWorkloadNameExtraPart() returned the same value %q for different replica sizes", got)
	}
	if got, other := noAnnotation.GetWorkloadNameExtraPart(), fiveExecutors.GetWorkloadNameExtraPart(); got == other {
		t.Errorf("GetWorkloadNameExtraPart() returned the same value %q with and without a replica-sizes annotation", got)
	}
}

// TestPodsReady asserts readiness semantics: non-elastic applications must
// reach spec.executor.instances Running/Completed executors; elastic
// applications only need to reach their guaranteed floor
// (max(1, minExecutors)), since their target size is not fixed.
func TestPodsReady(t *testing.T) {
	base := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns")

	withExecutorStates := func(app *sparkappv1beta2.SparkApplication, states ...sparkappv1beta2.ExecutorState) *sparkappv1beta2.SparkApplication {
		app.Status.ExecutorState = make(map[string]sparkappv1beta2.ExecutorState, len(states))
		for i, st := range states {
			app.Status.ExecutorState[fmt.Sprintf("exec-%d", i)] = st
		}
		return app
	}

	testCases := map[string]struct {
		sparkApp *sparkappv1beta2.SparkApplication
		want     bool
	}{
		"driver not running: not ready": {
			sparkApp: base.Clone().ExecutorInstances(2).Obj(),
			want:     false,
		},
		"non-elastic: fewer running executors than instances": {
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := base.Clone().ExecutorInstances(2).Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				return withExecutorStates(app, sparkappv1beta2.ExecutorStateRunning)
			}(),
			want: false,
		},
		"non-elastic: instances met": {
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := base.Clone().ExecutorInstances(2).Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				return withExecutorStates(app, sparkappv1beta2.ExecutorStateRunning, sparkappv1beta2.ExecutorStateCompleted)
			}(),
			want: true,
		},
		"elastic: minExecutors 0 floors at 1, not met with zero executors": {
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := base.Clone().
					Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
					DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](0)}).
					Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				return app
			}(),
			want: false,
		},
		"elastic: minExecutors 0 floors at 1, met with one running executor": {
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := base.Clone().
					Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
					DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](0)}).
					Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				return withExecutorStates(app, sparkappv1beta2.ExecutorStateRunning)
			}(),
			want: true,
		},
		"elastic: scale-up beyond minExecutors does not re-arm readiness": {
			sparkApp: func() *sparkappv1beta2.SparkApplication {
				app := base.Clone().
					Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
					DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](1)}).
					Obj()
				app.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
				// Only 1 of the (hypothetical) 5 scaled-up executors is ready,
				// but the floor of 1 is already satisfied.
				return withExecutorStates(app, sparkappv1beta2.ExecutorStateRunning)
			}(),
			want: true,
		},
	}

	features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true})

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			j := (*SparkApplication)(tc.sparkApp)
			ctx, _ := utiltesting.ContextWithLog(t)
			if got := j.PodsReady(ctx, nil); got != tc.want {
				t.Errorf("PodsReady() = %v, want %v", got, tc.want)
			}
		})
	}
}
