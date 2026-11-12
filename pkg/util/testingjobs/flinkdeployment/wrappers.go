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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/kueue/pkg/controller/constants"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
)

// FlinkDeploymentWrapper wraps a FlinkDeployment for fluent construction in unit and integration tests.
type FlinkDeploymentWrapper struct {
	flinkv1beta1.FlinkDeployment
}

// MakeFlinkDeployment creates a minimal Application-mode FlinkDeployment test fixture.
func MakeFlinkDeployment(name, ns string) *FlinkDeploymentWrapper {
	return &FlinkDeploymentWrapper{
		Name:      name,
		Namespace: ns,
		Spec: flinkv1beta1.FlinkDeploymentSpec{
			Image:        "flink:1.20",
			FlinkVersion: flinkv1beta1.FlinkVersionV1_20,
			JobManager: &flinkv1beta1.JobManagerSpec{
				Resource: &flinkv1beta1.Resource{
					CPU:    new(1.0),
					Memory: "1024m",
				},
			},
			TaskManager: &flinkv1beta1.TaskManagerSpec{
				Resource: &flinkv1beta1.Resource{
					CPU:    new(1.0),
					Memory: "2048m",
				},
			},
			Job: &flinkv1beta1.JobSpec{
				JarURI:      "local:///opt/flink/examples/streaming/StateMachineExample.jar",
				Parallelism: new(int64(1)),
				State:       flinkv1beta1.JobStateSuspended,
				UpgradeMode: flinkv1beta1.UpgradeModeStateless,
			},
		},
	}
}

// Obj returns the underlying *flinkv1beta1.FlinkDeployment pointer.
func (w *FlinkDeploymentWrapper) Obj() *flinkv1beta1.FlinkDeployment {
	return &w.FlinkDeployment
}

// Clone returns a deep copy of the wrapper.
func (w *FlinkDeploymentWrapper) Clone() *FlinkDeploymentWrapper {
	return &FlinkDeploymentWrapper{
		FlinkDeployment: *w.DeepCopy(),
	}
}

// UID sets metadata.uid.
func (w *FlinkDeploymentWrapper) UID(uid string) *FlinkDeploymentWrapper {
	w.ObjectMeta.UID = types.UID(uid)
	return w
}

// Queue sets the kueue.x-k8s.io/queue-name label.
func (w *FlinkDeploymentWrapper) Queue(queue string) *FlinkDeploymentWrapper {
	return w.Label(constants.QueueLabel, queue)
}

// WorkloadPriorityClass sets the kueue.x-k8s.io/priority-class label.
func (w *FlinkDeploymentWrapper) WorkloadPriorityClass(wpc string) *FlinkDeploymentWrapper {
	return w.Label(constants.WorkloadPriorityClassLabel, wpc)
}

// Label sets a metadata label on the FlinkDeployment.
func (w *FlinkDeploymentWrapper) Label(k, v string) *FlinkDeploymentWrapper {
	if w.Labels == nil {
		w.Labels = make(map[string]string)
	}
	w.Labels[k] = v
	return w
}

// Annotation sets a metadata annotation on the FlinkDeployment.
func (w *FlinkDeploymentWrapper) Annotation(k, v string) *FlinkDeploymentWrapper {
	if w.Annotations == nil {
		w.Annotations = make(map[string]string)
	}
	w.Annotations[k] = v
	return w
}

// Image sets spec.image.
func (w *FlinkDeploymentWrapper) Image(image string) *FlinkDeploymentWrapper {
	w.Spec.Image = image
	return w
}

// ServiceAccount sets spec.serviceAccount.
func (w *FlinkDeploymentWrapper) ServiceAccount(sa string) *FlinkDeploymentWrapper {
	w.Spec.ServiceAccount = sa
	return w
}

// Suspend sets spec.job.state to suspended or running.
func (w *FlinkDeploymentWrapper) Suspend(s bool) *FlinkDeploymentWrapper {
	if w.Spec.Job == nil {
		w.Spec.Job = &flinkv1beta1.JobSpec{}
	}
	if s {
		w.Spec.Job.State = flinkv1beta1.JobStateSuspended
	} else {
		w.Spec.Job.State = flinkv1beta1.JobStateRunning
	}
	return w
}

// SessionCluster clears spec.job to construct a Session-mode FlinkDeployment.
func (w *FlinkDeploymentWrapper) SessionCluster() *FlinkDeploymentWrapper {
	w.Spec.Job = nil
	return w
}

// UpgradeMode sets spec.job.upgradeMode.
func (w *FlinkDeploymentWrapper) UpgradeMode(mode flinkv1beta1.UpgradeMode) *FlinkDeploymentWrapper {
	if w.Spec.Job == nil {
		w.Spec.Job = &flinkv1beta1.JobSpec{}
	}
	w.Spec.Job.UpgradeMode = mode
	return w
}

// Parallelism sets spec.job.parallelism.
func (w *FlinkDeploymentWrapper) Parallelism(p int64) *FlinkDeploymentWrapper {
	if w.Spec.Job == nil {
		w.Spec.Job = &flinkv1beta1.JobSpec{}
	}
	w.Spec.Job.Parallelism = new(p)
	return w
}

// FlinkConfig sets a key-value pair in spec.flinkConfiguration.
func (w *FlinkDeploymentWrapper) FlinkConfig(k, v string) *FlinkDeploymentWrapper {
	w.Spec.FlinkConfiguration.Set(k, v)
	return w
}

// FlinkConfiguration replaces spec.flinkConfiguration with a cloned map.
func (w *FlinkDeploymentWrapper) FlinkConfiguration(cfg map[string]string) *FlinkDeploymentWrapper {
	w.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(cfg)
	return w
}

// Mode sets spec.mode (native or standalone).
func (w *FlinkDeploymentWrapper) Mode(mode flinkv1beta1.KubernetesDeploymentMode) *FlinkDeploymentWrapper {
	w.Spec.Mode = mode
	return w
}

// ImagePullPolicy sets spec.imagePullPolicy.
func (w *FlinkDeploymentWrapper) ImagePullPolicy(policy corev1.PullPolicy) *FlinkDeploymentWrapper {
	w.Spec.ImagePullPolicy = policy
	return w
}

// PodTemplate sets the shared root spec.podTemplate.
func (w *FlinkDeploymentWrapper) PodTemplate(pt *corev1.PodTemplateSpec) *FlinkDeploymentWrapper {
	w.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(pt)
	return w
}

// JobManagerReplicas sets spec.jobManager.replicas.
func (w *FlinkDeploymentWrapper) JobManagerReplicas(r int64) *FlinkDeploymentWrapper {
	if w.Spec.JobManager == nil {
		w.Spec.JobManager = &flinkv1beta1.JobManagerSpec{}
	}
	w.Spec.JobManager.Replicas = new(r)
	return w
}

// JobManagerResource sets spec.jobManager.resource and clears spec.jobManager.resources.
func (w *FlinkDeploymentWrapper) JobManagerResource(cpu *float64, memory, ephemeralStorage string) *FlinkDeploymentWrapper {
	if w.Spec.JobManager == nil {
		w.Spec.JobManager = &flinkv1beta1.JobManagerSpec{}
	}
	w.Spec.JobManager.Resources = nil
	w.Spec.JobManager.Resource = &flinkv1beta1.Resource{
		CPU:              cpu,
		Memory:           memory,
		EphemeralStorage: ephemeralStorage,
	}
	return w
}

// JobManagerResources sets spec.jobManager.resources (Kubernetes ResourceRequirements) and clears spec.jobManager.resource.
func (w *FlinkDeploymentWrapper) JobManagerResources(reqs corev1.ResourceRequirements) *FlinkDeploymentWrapper {
	if w.Spec.JobManager == nil {
		w.Spec.JobManager = &flinkv1beta1.JobManagerSpec{}
	}
	w.Spec.JobManager.Resource = nil
	w.Spec.JobManager.Resources = flinkv1beta1.ResourceRequirementsFromCore(&reqs)
	return w
}

// JobManagerRawResources sets spec.jobManager.resources with raw-preserving Quantity literals and clears spec.jobManager.resource.
func (w *FlinkDeploymentWrapper) JobManagerRawResources(reqs flinkv1beta1.ResourceRequirements) *FlinkDeploymentWrapper {
	if w.Spec.JobManager == nil {
		w.Spec.JobManager = &flinkv1beta1.JobManagerSpec{}
	}
	w.Spec.JobManager.Resource = nil
	w.Spec.JobManager.Resources = reqs.DeepCopy()
	return w
}

// JobManagerPodTemplate sets spec.jobManager.podTemplate.
func (w *FlinkDeploymentWrapper) JobManagerPodTemplate(pt *corev1.PodTemplateSpec) *FlinkDeploymentWrapper {
	if w.Spec.JobManager == nil {
		w.Spec.JobManager = &flinkv1beta1.JobManagerSpec{}
	}
	w.Spec.JobManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(pt)
	return w
}

// TaskManagerReplicas sets spec.taskManager.replicas.
func (w *FlinkDeploymentWrapper) TaskManagerReplicas(r int64) *FlinkDeploymentWrapper {
	if w.Spec.TaskManager == nil {
		w.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{}
	}
	w.Spec.TaskManager.Replicas = new(r)
	return w
}

// TaskManagerResource sets spec.taskManager.resource and clears spec.taskManager.resources.
func (w *FlinkDeploymentWrapper) TaskManagerResource(cpu *float64, memory, ephemeralStorage string) *FlinkDeploymentWrapper {
	if w.Spec.TaskManager == nil {
		w.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{}
	}
	w.Spec.TaskManager.Resources = nil
	w.Spec.TaskManager.Resource = &flinkv1beta1.Resource{
		CPU:              cpu,
		Memory:           memory,
		EphemeralStorage: ephemeralStorage,
	}
	return w
}

// TaskManagerResources sets spec.taskManager.resources (Kubernetes ResourceRequirements) and clears spec.taskManager.resource.
func (w *FlinkDeploymentWrapper) TaskManagerResources(reqs corev1.ResourceRequirements) *FlinkDeploymentWrapper {
	if w.Spec.TaskManager == nil {
		w.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{}
	}
	w.Spec.TaskManager.Resource = nil
	w.Spec.TaskManager.Resources = flinkv1beta1.ResourceRequirementsFromCore(&reqs)
	return w
}

// TaskManagerRawResources sets spec.taskManager.resources with raw-preserving Quantity literals and clears spec.taskManager.resource.
func (w *FlinkDeploymentWrapper) TaskManagerRawResources(reqs flinkv1beta1.ResourceRequirements) *FlinkDeploymentWrapper {
	if w.Spec.TaskManager == nil {
		w.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{}
	}
	w.Spec.TaskManager.Resource = nil
	w.Spec.TaskManager.Resources = reqs.DeepCopy()
	return w
}

// TaskManagerPodTemplate sets spec.taskManager.podTemplate.
func (w *FlinkDeploymentWrapper) TaskManagerPodTemplate(pt *corev1.PodTemplateSpec) *FlinkDeploymentWrapper {
	if w.Spec.TaskManager == nil {
		w.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{}
	}
	w.Spec.TaskManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(pt)
	return w
}

// JobManagerDeploymentStatus sets status.jobManagerDeploymentStatus.
func (w *FlinkDeploymentWrapper) JobManagerDeploymentStatus(s flinkv1beta1.JobManagerDeploymentStatus) *FlinkDeploymentWrapper {
	w.Status.JobManagerDeploymentStatus = s
	return w
}

// LifecycleState sets status.lifecycleState.
func (w *FlinkDeploymentWrapper) LifecycleState(s flinkv1beta1.LifecycleState) *FlinkDeploymentWrapper {
	w.Status.LifecycleState = s
	return w
}

// JobStatusState sets status.jobStatus.state.
func (w *FlinkDeploymentWrapper) JobStatusState(s flinkv1beta1.JobStatusState) *FlinkDeploymentWrapper {
	w.Status.JobStatus.State = s
	return w
}

// StatusError sets status.error.
func (w *FlinkDeploymentWrapper) StatusError(errMsg string) *FlinkDeploymentWrapper {
	w.Status.Error = errMsg
	return w
}

// ReconciliationState sets status.reconciliationStatus.state.
func (w *FlinkDeploymentWrapper) ReconciliationState(s flinkv1beta1.ReconciliationState) *FlinkDeploymentWrapper {
	w.Status.ReconciliationStatus.State = s
	return w
}

// TaskManagerStatusReplicas sets status.taskManager.replicas.
func (w *FlinkDeploymentWrapper) TaskManagerStatusReplicas(replicas int64) *FlinkDeploymentWrapper {
	if w.Status.TaskManager == nil {
		w.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{}
	}
	w.Status.TaskManager.Replicas = replicas
	return w
}
