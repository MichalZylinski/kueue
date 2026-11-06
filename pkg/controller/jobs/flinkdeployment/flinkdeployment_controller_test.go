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

package flinkdeployment

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

var (
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
	testCases := map[string]struct {
		flinkDep *flinkv1beta1.FlinkDeployment
		want     []kueue.PodSet
	}{
		"default replicas and resources": {
			flinkDep: &flinkv1beta1.FlinkDeployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-flink",
					Namespace: "default",
				},
				Spec: flinkv1beta1.FlinkDeploymentSpec{
					Image: "flink:1.18",
					JobManager: &flinkv1beta1.JobManagerSpec{
						Replicas: ptr.To[int32](1),
						Resource: &flinkv1beta1.Resource{
							CPU:    ptr.To(1.0),
							Memory: "2048m",
						},
					},
					TaskManager: &flinkv1beta1.TaskManagerSpec{
						Replicas: ptr.To[int32](3),
						Resource: &flinkv1beta1.Resource{
							CPU:    ptr.To(2.0),
							Memory: "4096m",
						},
					},
					Job: &flinkv1beta1.JobSpec{
						State:       flinkv1beta1.JobStateRunning,
						UpgradeMode: flinkv1beta1.UpgradeModeSavepoint,
					},
				},
			},
			want: []kueue.PodSet{
				*utiltestingapi.MakePodSet(jobManagerPodSetName, 1).Obj(),
				*utiltestingapi.MakePodSet(taskManagerPodSetName, 3).Obj(),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			job := (*FlinkDeployment)(tc.flinkDep)
			podSets, err := job.PodSets(context.Background(), nil)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if len(podSets) != len(tc.want) {
				t.Fatalf("Got %d podsets, want %d", len(podSets), len(tc.want))
			}
			for i := range podSets {
				if podSets[i].Name != tc.want[i].Name {
					t.Errorf("PodSet %d name = %s, want %s", i, podSets[i].Name, tc.want[i].Name)
				}
				if podSets[i].Count != tc.want[i].Count {
					t.Errorf("PodSet %d count = %d, want %d", i, podSets[i].Count, tc.want[i].Count)
				}
			}
		})
	}
}

func TestRunWithPodSetsInfo(t *testing.T) {
	flinkDep := &flinkv1beta1.FlinkDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-flink",
			Namespace: "default",
		},
		Spec: flinkv1beta1.FlinkDeploymentSpec{
			Image: "flink:1.18",
			Job: &flinkv1beta1.JobSpec{
				State: flinkv1beta1.JobStateSuspended,
			},
			JobManager: &flinkv1beta1.JobManagerSpec{
				Replicas: ptr.To[int32](1),
			},
			TaskManager: &flinkv1beta1.TaskManagerSpec{
				Replicas: ptr.To[int32](2),
			},
		},
	}

	job := (*FlinkDeployment)(flinkDep)
	podSetsInfo := []podset.PodSetInfo{
		{
			NodeSelector: map[string]string{"topology.kubernetes.io/zone": "zone-a"},
			Tolerations: []corev1.Toleration{
				{Key: "spot", Operator: corev1.TolerationOpExists},
			},
		},
		{
			NodeSelector: map[string]string{"topology.kubernetes.io/zone": "zone-a"},
			Tolerations: []corev1.Toleration{
				{Key: "spot", Operator: corev1.TolerationOpExists},
			},
		},
	}

	err := job.RunWithPodSetsInfo(context.Background(), nil, podSetsInfo)
	if err != nil {
		t.Fatalf("RunWithPodSetsInfo failed: %v", err)
	}

	if job.Spec.Job.State != flinkv1beta1.JobStateRunning {
		t.Errorf("Expected job state to be running, got %s", job.Spec.Job.State)
	}

	if job.Spec.JobManager.PodTemplate.Spec.NodeSelector["topology.kubernetes.io/zone"] != "zone-a" {
		t.Errorf("JobManager node selector not injected properly")
	}

	if job.Spec.TaskManager.PodTemplate.Spec.NodeSelector["topology.kubernetes.io/zone"] != "zone-a" {
		t.Errorf("TaskManager node selector not injected properly")
	}
}

func TestFinishedAndPodsReady(t *testing.T) {
	flinkDep := &flinkv1beta1.FlinkDeployment{
		Status: flinkv1beta1.FlinkDeploymentStatus{
			JobManagerDeploymentStatus: flinkv1beta1.JobManagerDeploymentStatusReady,
			JobStatus: flinkv1beta1.JobStatus{
				State: "RUNNING",
			},
		},
	}

	job := (*FlinkDeployment)(flinkDep)
	if !job.PodsReady(context.Background(), nil) {
		t.Errorf("Expected PodsReady to be true for READY and RUNNING status")
	}

	_, _, finished := job.Finished(context.Background())
	if finished {
		t.Errorf("Expected running streaming job to not be finished")
	}

	flinkDep.Status.JobStatus.State = "FINISHED"
	_, success, finished := job.Finished(context.Background())
	if !finished || !success {
		t.Errorf("Expected finished=true, success=true for FINISHED job")
	}

	flinkDep.Status.JobStatus.State = "FAILED"
	_, success, finished = job.Finished(context.Background())
	if !finished || success {
		t.Errorf("Expected finished=true, success=false for FAILED job")
	}
}
