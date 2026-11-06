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
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	clientutil "sigs.k8s.io/kueue/pkg/util/client"
)

var (
	gvk = flinkv1beta1.SchemeGroupVersion.WithKind("FlinkDeployment")
)

const (
	FrameworkName          = "flink.apache.org/flinkdeployment"
	jobManagerPodSetName   = "jobmanager"
	taskManagerPodSetName  = "taskmanager"
	jobManagerPodSetIndex  = 0
	taskManagerPodSetIndex = 1
	podSetsCount           = 2
)

func RegisterIntegration(m *jobframework.IntegrationManager) error {
	return m.RegisterIntegration(FrameworkName, jobframework.IntegrationCallbacks{
		SetupIndexes:          SetupIndexes,
		NewJob:                NewJob,
		NewReconciler:         NewReconciler,
		SetupWebhook:          SetupWebhook,
		JobType:               &flinkv1beta1.FlinkDeployment{},
		AddToScheme:           flinkv1beta1.AddToScheme,
		CanSupportIntegration: CanSupportIntegration,
	})
}

// +kubebuilder:rbac:groups=flink.apache.org,resources=flinkdeployments,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=flink.apache.org,resources=flinkdeployments/status,verbs=get

func NewJob() jobframework.GenericJob {
	return &FlinkDeployment{}
}

var NewReconciler = jobframework.NewGenericReconcilerFactory(NewJob)

type FlinkDeployment flinkv1beta1.FlinkDeployment

var _ jobframework.GenericJob = (*FlinkDeployment)(nil)
var _ jobframework.JobWithPodLabelSelector = (*FlinkDeployment)(nil)
var _ jobframework.JobWithCustomStop = (*FlinkDeployment)(nil)

func (j *FlinkDeployment) Object() client.Object {
	return (*flinkv1beta1.FlinkDeployment)(j)
}

func (j *FlinkDeployment) IsSuspended() bool {
	return j.Spec.Job == nil || j.Spec.Job.State == flinkv1beta1.JobStateSuspended
}

func (j *FlinkDeployment) IsActive() bool {
	return j.Status.JobStatus.State == "RUNNING" ||
		j.Status.JobManagerDeploymentStatus == flinkv1beta1.JobManagerDeploymentStatusReady ||
		j.Status.JobManagerDeploymentStatus == flinkv1beta1.JobManagerDeploymentStatusDeploying
}

func (j *FlinkDeployment) Suspend() {
	if j.Spec.Job == nil {
		j.Spec.Job = &flinkv1beta1.JobSpec{}
	}
	j.Spec.Job.State = flinkv1beta1.JobStateSuspended
}

func (j *FlinkDeployment) GVK() schema.GroupVersionKind {
	return gvk
}

func (j *FlinkDeployment) PodLabelSelector() string {
	return fmt.Sprintf("app=%s", j.Name)
}

func (j *FlinkDeployment) PodSets(ctx context.Context, c client.Client) ([]kueue.PodSet, error) {
	podSets := make([]kueue.PodSet, podSetsCount)

	setTopologyRequestToPodSetIfEnabled := func(podSet *kueue.PodSet, podTemplateSpec *corev1.PodTemplateSpec) error {
		if features.Enabled(features.TopologyAwareScheduling) {
			topologyRequest, err := jobframework.NewPodSetTopologyRequest(
				&podTemplateSpec.ObjectMeta).Build()
			if err != nil {
				return err
			}
			podSet.TopologyRequest = topologyRequest
		}
		return nil
	}

	// JobManager
	jmPodTemplateSpec, err := j.buildJobManagerPodTemplateSpec()
	if err != nil {
		return nil, err
	}
	podSets[jobManagerPodSetIndex] = kueue.PodSet{
		Name:     jobManagerPodSetName,
		Template: *jmPodTemplateSpec,
		Count:    j.numJobManagers(),
	}
	if err := setTopologyRequestToPodSetIfEnabled(&podSets[jobManagerPodSetIndex], jmPodTemplateSpec); err != nil {
		return nil, err
	}

	// TaskManager
	tmPodTemplateSpec, err := j.buildTaskManagerPodTemplateSpec()
	if err != nil {
		return nil, err
	}
	podSets[taskManagerPodSetIndex] = kueue.PodSet{
		Name:     taskManagerPodSetName,
		Template: *tmPodTemplateSpec,
		Count:    j.numTaskManagers(),
	}
	if err := setTopologyRequestToPodSetIfEnabled(&podSets[taskManagerPodSetIndex], tmPodTemplateSpec); err != nil {
		return nil, err
	}

	return podSets, nil
}

func (j *FlinkDeployment) RunWithPodSetsInfo(ctx context.Context, _ client.Client, podSetsInfo []podset.PodSetInfo) error {
	if len(podSetsInfo) != podSetsCount {
		return podset.BadPodSetsInfoLenError(podSetsCount, len(podSetsInfo))
	}

	if j.Spec.Job == nil {
		j.Spec.Job = &flinkv1beta1.JobSpec{}
	}
	j.Spec.Job.State = flinkv1beta1.JobStateRunning

	mutatePodSetInfoFor := func(isJobManager bool) error {
		var info podset.PodSetInfo
		var targetPodTemplate **corev1.PodTemplateSpec

		if isJobManager {
			info = podSetsInfo[jobManagerPodSetIndex]
			if j.Spec.JobManager == nil {
				j.Spec.JobManager = &flinkv1beta1.JobManagerSpec{}
			}
			targetPodTemplate = &j.Spec.JobManager.PodTemplate
		} else {
			info = podSetsInfo[taskManagerPodSetIndex]
			if j.Spec.TaskManager == nil {
				j.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{}
			}
			targetPodTemplate = &j.Spec.TaskManager.PodTemplate
		}

		if *targetPodTemplate == nil {
			*targetPodTemplate = &corev1.PodTemplateSpec{}
		}

		targetInfo := &podset.PodSetInfo{
			Annotations:  (*targetPodTemplate).Annotations,
			Labels:       (*targetPodTemplate).Labels,
			NodeSelector: (*targetPodTemplate).Spec.NodeSelector,
			Tolerations:  (*targetPodTemplate).Spec.Tolerations,
		}
		if err := targetInfo.Merge(info); err != nil {
			return err
		}
		(*targetPodTemplate).Annotations = targetInfo.Annotations
		(*targetPodTemplate).Labels = targetInfo.Labels
		(*targetPodTemplate).Spec.NodeSelector = targetInfo.NodeSelector
		(*targetPodTemplate).Spec.Tolerations = targetInfo.Tolerations
		return nil
	}

	if err := mutatePodSetInfoFor(true); err != nil {
		return err
	}
	if err := mutatePodSetInfoFor(false); err != nil {
		return err
	}

	return nil
}

func (j *FlinkDeployment) RestorePodSetsInfo(ctx context.Context, podSetsInfo []podset.PodSetInfo) bool {
	if len(podSetsInfo) != podSetsCount {
		ctrl.LoggerFrom(ctx).V(2).Info(
			"Skipping pod set info restore because the pod set count does not match the admitted workload",
			"expectedCount", podSetsCount,
			"gotCount", len(podSetsInfo),
		)
		return false
	}

	changed := false

	restoreFor := func(isJobManager bool) {
		var info podset.PodSetInfo
		var targetPodTemplate *corev1.PodTemplateSpec

		if isJobManager {
			info = podSetsInfo[jobManagerPodSetIndex]
			if j.Spec.JobManager != nil {
				targetPodTemplate = j.Spec.JobManager.PodTemplate
			}
		} else {
			info = podSetsInfo[taskManagerPodSetIndex]
			if j.Spec.TaskManager != nil {
				targetPodTemplate = j.Spec.TaskManager.PodTemplate
			}
		}

		if targetPodTemplate == nil {
			return
		}

		if !maps.Equal(targetPodTemplate.Annotations, info.Annotations) {
			targetPodTemplate.Annotations = maps.Clone(info.Annotations)
			changed = true
		}
		if !maps.Equal(targetPodTemplate.Labels, info.Labels) {
			targetPodTemplate.Labels = maps.Clone(info.Labels)
			changed = true
		}
		if !maps.Equal(targetPodTemplate.Spec.NodeSelector, info.NodeSelector) {
			targetPodTemplate.Spec.NodeSelector = maps.Clone(info.NodeSelector)
			changed = true
		}
		if !slices.Equal(targetPodTemplate.Spec.Tolerations, info.Tolerations) {
			targetPodTemplate.Spec.Tolerations = slices.Clone(info.Tolerations)
			changed = true
		}
	}

	restoreFor(true)
	restoreFor(false)

	return changed
}

func (j *FlinkDeployment) Finished(ctx context.Context) (message string, success, finished bool) {
	switch j.Status.JobStatus.State {
	case "FINISHED":
		return "Flink application completed", true, true
	case "FAILED":
		return "Flink application failed", false, true
	case "CANCELED":
		return "Flink application canceled", true, true
	default:
		return "", false, false
	}
}

func (j *FlinkDeployment) PodsReady(ctx context.Context, _ client.Client, _ ...jobframework.PodsReadyOption) bool {
	return j.Status.JobManagerDeploymentStatus == flinkv1beta1.JobManagerDeploymentStatusReady &&
		j.Status.JobStatus.State == "RUNNING"
}

func (j *FlinkDeployment) Stop(ctx context.Context, c client.Client, podSetsInfo []podset.PodSetInfo, stopReason jobframework.StopReason, eventMsg string) (bool, error) {
	if j.IsSuspended() {
		return false, nil
	}
	return true, clientutil.Patch(ctx, c, j.Object(), func() (bool, error) {
		j.Suspend()
		if j.Spec.Job != nil {
			j.Spec.Job.UpgradeMode = flinkv1beta1.UpgradeModeSavepoint
		}
		if len(podSetsInfo) > 0 {
			j.RestorePodSetsInfo(ctx, podSetsInfo)
		}
		return true, nil
	})
}

func SetupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	return jobframework.SetupWorkloadOwnerIndex(ctx, indexer, gvk)
}

func GetWorkloadNameForFlinkDeployment(name string, uid types.UID) string {
	return jobframework.GetWorkloadNameForOwnerWithGVK(name, uid, gvk)
}

func CanSupportIntegration(opts ...jobframework.Option) (bool, error) {
	if !features.Enabled(features.FlinkDeploymentIntegration) {
		return false, fmt.Errorf("%s integration is alpha feature. please enable %s featuregate", FrameworkName, features.FlinkDeploymentIntegration)
	}
	return true, nil
}

func fromObject(o runtime.Object) *FlinkDeployment {
	return (*FlinkDeployment)(o.(*flinkv1beta1.FlinkDeployment))
}
