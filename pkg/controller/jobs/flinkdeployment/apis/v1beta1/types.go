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

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type JobState string

const (
	JobStateRunning   JobState = "running"
	JobStateSuspended JobState = "suspended"
)

type UpgradeMode string

const (
	UpgradeModeStateless UpgradeMode = "stateless"
	UpgradeModeSavepoint UpgradeMode = "savepoint"
	UpgradeModeLastState UpgradeMode = "last-state"
)

type JobManagerDeploymentStatus string

const (
	JobManagerDeploymentStatusReady            JobManagerDeploymentStatus = "READY"
	JobManagerDeploymentStatusDeployedNotReady JobManagerDeploymentStatus = "DEPLOYED_NOT_READY"
	JobManagerDeploymentStatusDeploying        JobManagerDeploymentStatus = "DEPLOYING"
	JobManagerDeploymentStatusError            JobManagerDeploymentStatus = "ERROR"
	JobManagerDeploymentStatusMissing          JobManagerDeploymentStatus = "MISSING"
)

type LifecycleState string

const (
	LifecycleStateCreated   LifecycleState = "CREATED"
	LifecycleStateDeployed  LifecycleState = "DEPLOYED"
	LifecycleStateFailed    LifecycleState = "FAILED"
	LifecycleStateSuspended LifecycleState = "SUSPENDED"
	LifecycleStateUpgrading LifecycleState = "UPGRADING"
)

// Resource defines CPU and Memory requests/limits.
type Resource struct {
	CPU    *float64 `json:"cpu,omitempty"`
	Memory string   `json:"memory,omitempty"`
}

// JobManagerSpec defines the JobManager attributes.
type JobManagerSpec struct {
	Replicas    *int32                  `json:"replicas,omitempty"`
	Resource    *Resource               `json:"resource,omitempty"`
	PodTemplate *corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
}

// TaskManagerSpec defines the TaskManager attributes.
type TaskManagerSpec struct {
	Replicas    *int32                  `json:"replicas,omitempty"`
	Resource    *Resource               `json:"resource,omitempty"`
	PodTemplate *corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
}

// JobSpec defines the Flink application job specification.
type JobSpec struct {
	JarURI      string      `json:"jarURI,omitempty"`
	Parallelism *int32      `json:"parallelism,omitempty"`
	EntryClass  string      `json:"entryClass,omitempty"`
	Args        []string    `json:"args,omitempty"`
	State       JobState    `json:"state,omitempty"`
	UpgradeMode UpgradeMode `json:"upgradeMode,omitempty"`
}

// FlinkDeploymentSpec defines the desired state of FlinkDeployment.
type FlinkDeploymentSpec struct {
	Image              string                  `json:"image,omitempty"`
	ImagePullPolicy    corev1.PullPolicy       `json:"imagePullPolicy,omitempty"`
	ServiceAccount     string                  `json:"serviceAccount,omitempty"`
	FlinkVersion       string                  `json:"flinkVersion,omitempty"`
	FlinkConfiguration map[string]string       `json:"flinkConfiguration,omitempty"`
	JobManager         *JobManagerSpec         `json:"jobManager,omitempty"`
	TaskManager        *TaskManagerSpec        `json:"taskManager,omitempty"`
	Job                *JobSpec                `json:"job,omitempty"`
	PodTemplate        *corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
	Mode               string                  `json:"mode,omitempty"`
}

// JobStatus contains execution state of the Flink application.
type JobStatus struct {
	JobID      string `json:"jobId,omitempty"`
	JobName    string `json:"jobName,omitempty"`
	State      string `json:"state,omitempty"`
	StartTime  string `json:"startTime,omitempty"`
	UpdateTime string `json:"updateTime,omitempty"`
}

// FlinkDeploymentStatus defines the observed state of FlinkDeployment.
type FlinkDeploymentStatus struct {
	LifecycleState             LifecycleState             `json:"lifecycleState,omitempty"`
	JobManagerDeploymentStatus JobManagerDeploymentStatus `json:"jobManagerDeploymentStatus,omitempty"`
	JobStatus                  JobStatus                  `json:"jobStatus,omitempty"`
	Conditions                 []metav1.Condition         `json:"conditions,omitempty"`
	ObservedGeneration         int64                      `json:"observedGeneration,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=flinkdep

type FlinkDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   FlinkDeploymentSpec   `json:"spec,omitempty"`
	Status FlinkDeploymentStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

type FlinkDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []FlinkDeployment `json:"items"`
}
