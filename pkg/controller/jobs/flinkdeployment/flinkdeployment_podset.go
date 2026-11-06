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
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	defaultJobManagerContainerName  = "flink-job-manager"
	defaultTaskManagerContainerName = "flink-task-manager"
)

func (j *FlinkDeployment) numJobManagers() int32 {
	if j.Spec.JobManager != nil && j.Spec.JobManager.Replicas != nil {
		return *j.Spec.JobManager.Replicas
	}
	return 1
}

func (j *FlinkDeployment) numTaskManagers() int32 {
	if j.Spec.TaskManager != nil && j.Spec.TaskManager.Replicas != nil {
		return *j.Spec.TaskManager.Replicas
	}
	return 1
}

func (j *FlinkDeployment) buildJobManagerPodTemplateSpec() (*corev1.PodTemplateSpec, error) {
	var podTemplate *corev1.PodTemplateSpec
	if j.Spec.JobManager != nil && j.Spec.JobManager.PodTemplate != nil {
		podTemplate = j.Spec.JobManager.PodTemplate.DeepCopy()
	} else if j.Spec.PodTemplate != nil {
		podTemplate = j.Spec.PodTemplate.DeepCopy()
	} else {
		podTemplate = &corev1.PodTemplateSpec{}
	}

	if podTemplate.Labels == nil {
		podTemplate.Labels = make(map[string]string)
	}
	podTemplate.Labels["app"] = j.Name
	podTemplate.Labels["component"] = "jobmanager"

	// Ensure at least one container exists
	if len(podTemplate.Spec.Containers) == 0 {
		podTemplate.Spec.Containers = []corev1.Container{{
			Name:  defaultJobManagerContainerName,
			Image: j.Spec.Image,
		}}
	}

	if j.Spec.JobManager != nil && j.Spec.JobManager.Resource != nil {
		resList := corev1.ResourceList{}
		if j.Spec.JobManager.Resource.CPU != nil {
			resList[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(*j.Spec.JobManager.Resource.CPU*1000), resource.DecimalSI)
		}
		if j.Spec.JobManager.Resource.Memory != "" {
			if q, err := resource.ParseQuantity(j.Spec.JobManager.Resource.Memory); err == nil {
				resList[corev1.ResourceMemory] = q
			}
		}
		if len(resList) > 0 {
			if podTemplate.Spec.Containers[0].Resources.Requests == nil {
				podTemplate.Spec.Containers[0].Resources.Requests = corev1.ResourceList{}
			}
			for k, v := range resList {
				podTemplate.Spec.Containers[0].Resources.Requests[k] = v
			}
		}
	}

	return podTemplate, nil
}

func (j *FlinkDeployment) buildTaskManagerPodTemplateSpec() (*corev1.PodTemplateSpec, error) {
	var podTemplate *corev1.PodTemplateSpec
	if j.Spec.TaskManager != nil && j.Spec.TaskManager.PodTemplate != nil {
		podTemplate = j.Spec.TaskManager.PodTemplate.DeepCopy()
	} else if j.Spec.PodTemplate != nil {
		podTemplate = j.Spec.PodTemplate.DeepCopy()
	} else {
		podTemplate = &corev1.PodTemplateSpec{}
	}

	if podTemplate.Labels == nil {
		podTemplate.Labels = make(map[string]string)
	}
	podTemplate.Labels["app"] = j.Name
	podTemplate.Labels["component"] = "taskmanager"

	// Ensure at least one container exists
	if len(podTemplate.Spec.Containers) == 0 {
		podTemplate.Spec.Containers = []corev1.Container{{
			Name:  defaultTaskManagerContainerName,
			Image: j.Spec.Image,
		}}
	}

	if j.Spec.TaskManager != nil && j.Spec.TaskManager.Resource != nil {
		resList := corev1.ResourceList{}
		if j.Spec.TaskManager.Resource.CPU != nil {
			resList[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(*j.Spec.TaskManager.Resource.CPU*1000), resource.DecimalSI)
		}
		if j.Spec.TaskManager.Resource.Memory != "" {
			if q, err := resource.ParseQuantity(j.Spec.TaskManager.Resource.Memory); err == nil {
				resList[corev1.ResourceMemory] = q
			}
		}
		if len(resList) > 0 {
			if podTemplate.Spec.Containers[0].Resources.Requests == nil {
				podTemplate.Spec.Containers[0].Resources.Requests = corev1.ResourceList{}
			}
			for k, v := range resList {
				podTemplate.Spec.Containers[0].Resources.Requests[k] = v
			}
		}
	}

	return podTemplate, nil
}
