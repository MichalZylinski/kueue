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

// Package v1beta1 defines the flink.apache.org/v1beta1 FlinkDeployment API schema.
//
// +k8s:deepcopy-gen=package
// +groupName=flink.apache.org
package v1beta1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

var (
	SchemeGroupVersion = schema.GroupVersion{Group: "flink.apache.org", Version: "v1beta1"}
	SchemeBuilder      = runtime.NewSchemeBuilder(func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(SchemeGroupVersion, &FlinkDeployment{}, &FlinkDeploymentList{})
		metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
		return nil
	})
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	utilruntime.Must(apiequality.Semantic.AddFunc(func(a, b FlinkConfiguration) bool { return a.Equal(b) }))
	utilruntime.Must(apiequality.Semantic.AddFunc(func(a, b PodTemplateSpec) bool { return a.Equal(b) }))
	utilruntime.Must(apiequality.Semantic.AddFunc(func(a, b Quantity) bool { return a.Equal(b) }))
}

type (
	FlinkVersion               string
	KubernetesDeploymentMode   string
	JobState                   string
	UpgradeMode                string
	LifecycleState             string
	JobManagerDeploymentStatus string
	JobStatusState             string
	ReconciliationState        string
)

const (
	FlinkVersionV1_20                          FlinkVersion               = "v1_20"
	KubernetesDeploymentModeNative             KubernetesDeploymentMode   = "native"
	KubernetesDeploymentModeStandalone         KubernetesDeploymentMode   = "standalone"
	JobStateRunning                            JobState                   = "running"
	JobStateSuspended                          JobState                   = "suspended"
	UpgradeModeStateless                       UpgradeMode                = "stateless"
	UpgradeModeLastState                       UpgradeMode                = "last-state"
	UpgradeModeSavepoint                       UpgradeMode                = "savepoint"
	LifecycleStateCreated                      LifecycleState             = "CREATED"
	LifecycleStateSuspended                    LifecycleState             = "SUSPENDED"
	LifecycleStateUpgrading                    LifecycleState             = "UPGRADING"
	LifecycleStateDeployed                     LifecycleState             = "DEPLOYED"
	LifecycleStateStable                       LifecycleState             = "STABLE"
	LifecycleStateRollingBack                  LifecycleState             = "ROLLING_BACK"
	LifecycleStateRolledBack                   LifecycleState             = "ROLLED_BACK"
	LifecycleStateFailed                       LifecycleState             = "FAILED"
	JobManagerDeploymentStatusReady            JobManagerDeploymentStatus = "READY"
	JobManagerDeploymentStatusDeployedNotReady JobManagerDeploymentStatus = "DEPLOYED_NOT_READY"
	JobManagerDeploymentStatusDeploying        JobManagerDeploymentStatus = "DEPLOYING"
	JobManagerDeploymentStatusMissing          JobManagerDeploymentStatus = "MISSING"
	JobManagerDeploymentStatusError            JobManagerDeploymentStatus = "ERROR"
	JobStatusStateInitializing                 JobStatusState             = "INITIALIZING"
	JobStatusStateCreated                      JobStatusState             = "CREATED"
	JobStatusStateRunning                      JobStatusState             = "RUNNING"
	JobStatusStateFailing                      JobStatusState             = "FAILING"
	JobStatusStateFailed                       JobStatusState             = "FAILED"
	JobStatusStateCancelling                   JobStatusState             = "CANCELLING"
	JobStatusStateCanceled                     JobStatusState             = "CANCELED"
	JobStatusStateFinished                     JobStatusState             = "FINISHED"
	JobStatusStateRestarting                   JobStatusState             = "RESTARTING"
	JobStatusStateSuspended                    JobStatusState             = "SUSPENDED"
	JobStatusStateReconciling                  JobStatusState             = "RECONCILING"
	ReconciliationStateDeployed                ReconciliationState        = "DEPLOYED"
	ReconciliationStateUpgrading               ReconciliationState        = "UPGRADING"
	ReconciliationStateRollingBack             ReconciliationState        = "ROLLING_BACK"
	ReconciliationStateRolledBack              ReconciliationState        = "ROLLED_BACK"
)

// FlinkConfiguration retains raw JSON bytes for spec.flinkConfiguration so mutating webhook patches
// preserve nested maps/lists verbatim and NoopWebhook/informers never fail on unmanaged resources.
type FlinkConfiguration struct {
	raw       json.RawMessage
	decodeErr error
}

func NewFlinkConfiguration(entries map[string]string) FlinkConfiguration {
	if len(entries) == 0 {
		return FlinkConfiguration{}
	}
	raw, err := json.Marshal(entries)
	return FlinkConfiguration{raw: raw, decodeErr: err}
}

func (c FlinkConfiguration) IsZero() bool { return len(c.raw) == 0 && c.decodeErr == nil }
func (c *FlinkConfiguration) DeepCopyInto(out *FlinkConfiguration) {
	*out = FlinkConfiguration{raw: bytes.Clone(c.raw), decodeErr: c.decodeErr}
}

func (c FlinkConfiguration) Equal(other FlinkConfiguration) bool {
	if bytes.Equal(c.raw, other.raw) && (c.decodeErr == nil) == (other.decodeErr == nil) {
		return true
	}
	if c.decodeErr != nil || other.decodeErr != nil {
		return false
	}
	m1, err1 := c.flatMap(true)
	m2, err2 := other.flatMap(true)
	if err1 == nil || err2 == nil {
		return err1 == nil && err2 == nil && maps.Equal(m1, m2)
	}
	raw1, e1 := DecodeJSONMapUseNumber(c.raw)
	raw2, e2 := DecodeJSONMapUseNumber(other.raw)
	b1, m1Err := json.Marshal(raw1)
	b2, m2Err := json.Marshal(raw2)
	return e1 == nil && e2 == nil && m1Err == nil && m2Err == nil && bytes.Equal(b1, b2)
}

func (c FlinkConfiguration) MarshalJSON() ([]byte, error) {
	if len(c.raw) == 0 {
		return []byte("null"), nil
	}
	return bytes.Clone(c.raw), nil
}

func (c *FlinkConfiguration) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*c = FlinkConfiguration{}
		return nil
	}
	var obj map[string]json.RawMessage
	*c = FlinkConfiguration{raw: bytes.Clone(trimmed), decodeErr: json.Unmarshal(trimmed, &obj)}
	return nil
}

func (c FlinkConfiguration) FlatMap() (map[string]string, error) { return c.flatMap(false) }

func (c FlinkConfiguration) flatMap(canonical bool) (map[string]string, error) {
	if c.decodeErr != nil || len(c.raw) == 0 {
		return nil, c.decodeErr
	}
	rawMap, err := DecodeJSONMapUseNumber(c.raw)
	if err != nil || len(rawMap) == 0 {
		return nil, err
	}
	out := make(map[string]string, len(rawMap))
	return out, flattenFlinkConfig("", rawMap, out, make(map[string]struct{}, len(rawMap)), canonical)
}

func formatFlinkScalar(val any, canonical bool) (string, bool) {
	switch typed := val.(type) {
	case nil:
		if canonical {
			return "N", true
		}
		return "null", true
	case string, json.Number, bool:
		s := fmt.Sprint(typed)
		if canonical {
			return "S:" + s, true
		}
		return s, true
	default:
		return "", false
	}
}

func flattenFlinkConfig(prefix string, in map[string]any, out map[string]string, seen map[string]struct{}, canonical bool) error {
	claim := func(key string) error {
		if _, dup := seen[key]; dup {
			return fmt.Errorf("duplicate flattened flinkConfiguration key %q", key)
		}
		seen[key] = struct{}{}
		return nil
	}
	for _, k := range slices.Sorted(maps.Keys(in)) {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("flinkConfiguration key %q must not be empty", k)
		}
		if k != strings.TrimSpace(k) || slices.ContainsFunc(strings.Split(k, "."), func(s string) bool { return s != strings.TrimSpace(s) }) {
			return fmt.Errorf("flinkConfiguration key %q must not contain leading or trailing whitespace", k)
		}
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if s, ok := formatFlinkScalar(in[k], canonical); ok {
			if err := claim(key); err != nil {
				return err
			}
			out[key] = s
			continue
		}
		switch typed := in[k].(type) {
		case map[string]any:
			if len(typed) == 0 {
				if err := claim(key); err != nil {
					return err
				}
				if canonical {
					out[key] = "M"
				}
			} else if err := flattenFlinkConfig(key, typed, out, seen, canonical); err != nil {
				return err
			}
		case []any:
			if err := claim(key); err != nil {
				return err
			}
			if len(typed) == 0 {
				if canonical {
					out[key] = "L:[]"
				}
				continue
			}
			parts := make([]string, len(typed))
			for i, elem := range typed {
				s, isScalar := formatFlinkScalar(elem, canonical)
				switch elem.(type) {
				case map[string]any, []any:
					if canonical {
						b, _ := json.Marshal(elem)
						s = "J:" + string(b)
					}
				default:
					if !isScalar {
						return fmt.Errorf("element %d of %q: unsupported slice element type %T", i, key, elem)
					}
				}
				parts[i] = s
			}
			out[key] = strings.Join(parts, ";")
			if canonical {
				b, _ := json.Marshal(parts)
				out[key] = "L:" + string(b)
			}
		default:
			return fmt.Errorf("unsupported value type %T for flinkConfiguration key %q", in[k], key)
		}
	}
	return nil
}

type Resource struct {
	CPU              *float64 `json:"cpu,omitempty"`
	Memory           string   `json:"memory,omitempty"`
	EphemeralStorage string   `json:"ephemeralStorage,omitempty"`
}

// PodTemplateSpec models Flink Operator's @SchemaFrom(type = Pod.class) podTemplate while retaining
// raw JSON bytes so partial component overrides survive mutating webhook patches and overlay updates.
type PodTemplateSpec struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`
	Spec              corev1.PodSpec    `json:"spec,omitempty,omitzero"`
	Status            *corev1.PodStatus `json:"status,omitempty"`
	raw               json.RawMessage
	decodeErr         error
}

func (in *PodTemplateSpec) DecodeError() error {
	if in == nil {
		return nil
	}
	return in.decodeErr
}

func (in *PodTemplateSpec) ToCorePodTemplateSpec() *corev1.PodTemplateSpec {
	if in == nil {
		return nil
	}
	return &corev1.PodTemplateSpec{ObjectMeta: *in.ObjectMeta.DeepCopy(), Spec: *in.Spec.DeepCopy()}
}

func (in *PodTemplateSpec) DeepCopyInto(out *PodTemplateSpec) {
	*out = *in
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	if in.Status != nil {
		out.Status = in.Status.DeepCopy()
	}
	out.raw = bytes.Clone(in.raw)
}

func (in PodTemplateSpec) Equal(other PodTemplateSpec) bool {
	if in.decodeErr != nil || other.decodeErr != nil {
		return (in.decodeErr == nil) == (other.decodeErr == nil) && bytes.Equal(in.raw, other.raw)
	}
	return in.TypeMeta == other.TypeMeta && apiequality.Semantic.DeepEqual(in.ObjectMeta, other.ObjectMeta) &&
		apiequality.Semantic.DeepEqual(in.Spec, other.Spec) && apiequality.Semantic.DeepEqual(in.Status, other.Status)
}

func (in *PodTemplateSpec) UnmarshalJSON(data []byte) error {
	type alias PodTemplateSpec
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*in = PodTemplateSpec{}
		return nil
	}
	var decoded alias
	err := json.Unmarshal(trimmed, &decoded)
	if err != nil {
		decoded = alias{}
	}
	*in = PodTemplateSpec(decoded)
	in.raw, in.decodeErr = bytes.Clone(trimmed), err
	return nil
}

func (in PodTemplateSpec) MarshalJSON() ([]byte, error) {
	type alias PodTemplateSpec
	if in.decodeErr != nil && len(in.raw) > 0 {
		return bytes.Clone(in.raw), nil
	}
	if len(in.raw) > 0 {
		var orig alias
		if json.Unmarshal(in.raw, &orig) == nil {
			origPT := PodTemplateSpec(orig)
			if in.Equal(origPT) {
				return bytes.Clone(in.raw), nil
			}
			origMeta, curMeta := origPT.ObjectMeta.DeepCopy(), in.ObjectMeta.DeepCopy()
			origMeta.Labels, origMeta.Annotations, curMeta.Labels, curMeta.Annotations = nil, nil, nil, nil
			origSpec, curSpec := origPT.Spec.DeepCopy(), in.Spec.DeepCopy()
			origSpec.NodeSelector, origSpec.Tolerations, origSpec.SchedulingGates, curSpec.NodeSelector, curSpec.Tolerations, curSpec.SchedulingGates = nil, nil, nil, nil, nil, nil
			if in.TypeMeta == origPT.TypeMeta && apiequality.Semantic.DeepEqual(origMeta, curMeta) &&
				apiequality.Semantic.DeepEqual(origSpec, curSpec) && apiequality.Semantic.DeepEqual(origPT.Status, in.Status) {
				ptMap, err := DecodeJSONMapUseNumber(in.raw)
				if err != nil {
					return nil, err
				}
				setSub := func(changed bool, sec, key string, empty bool, val any) {
					if sub, _ := ptMap[sec].(map[string]any); changed && empty {
						delete(sub, key)
						if len(sub) == 0 {
							delete(ptMap, sec)
						}
					} else if changed {
						if sub == nil {
							sub = map[string]any{}
							ptMap[sec] = sub
						}
						sub[key] = val
					}
				}
				setSub(!maps.Equal(origPT.Labels, in.Labels), "metadata", "labels", len(in.Labels) == 0, in.Labels)
				setSub(!maps.Equal(origPT.Annotations, in.Annotations), "metadata", "annotations", len(in.Annotations) == 0, in.Annotations)
				setSub(!maps.Equal(origPT.Spec.NodeSelector, in.Spec.NodeSelector), "spec", "nodeSelector", len(in.Spec.NodeSelector) == 0, in.Spec.NodeSelector)
				setSub(!apiequality.Semantic.DeepEqual(origPT.Spec.Tolerations, in.Spec.Tolerations), "spec", "tolerations", len(in.Spec.Tolerations) == 0, in.Spec.Tolerations)
				setSub(!slices.Equal(origPT.Spec.SchedulingGates, in.Spec.SchedulingGates), "spec", "schedulingGates", len(in.Spec.SchedulingGates) == 0, in.Spec.SchedulingGates)
				return json.Marshal(ptMap)
			}
		}
	}
	raw, err := json.Marshal(alias(in))
	if err != nil || !bytes.Contains(raw, []byte(":null")) {
		return raw, err
	}
	ptMap, err := DecodeJSONMapUseNumber(raw)
	if err != nil {
		return nil, err
	}
	for _, key := range []string{"spec", "status"} {
		if sub, ok := ptMap[key].(map[string]any); ok {
			stripNullEntries(sub)
			if len(sub) == 0 {
				delete(ptMap, key)
			}
		}
	}
	return json.Marshal(ptMap)
}

func stripNullEntries(v any) {
	switch typed := v.(type) {
	case map[string]any:
		maps.DeleteFunc(typed, func(_ string, elem any) bool {
			stripNullEntries(elem)
			return elem == nil
		})
	case []any:
		for _, elem := range typed {
			stripNullEntries(elem)
		}
	}
}

// Quantity wraps resource.Quantity while retaining raw JSON bytes and deferring ParseQuantity errors
// so unmanaged FlinkDeployments with Flink MemorySize literals (such as "2g") never fail ListAndWatch.
type Quantity struct {
	quantity resource.Quantity
	raw      json.RawMessage
	parseErr error
}

func (q Quantity) ParseError() error             { return q.parseErr }
func (q Quantity) AsQuantity() resource.Quantity { return q.quantity.DeepCopy() }

func (q Quantity) RawLiteral() string {
	if len(q.raw) > 0 {
		var s string
		if q.raw[0] == '"' && json.Unmarshal(q.raw, &s) == nil {
			return s
		}
		return string(q.raw)
	}
	return q.quantity.String()
}

func (q Quantity) Equal(other Quantity) bool {
	if q.parseErr != nil || other.parseErr != nil {
		return (q.parseErr == nil) == (other.parseErr == nil) && bytes.Equal(q.raw, other.raw)
	}
	return q.quantity.Equal(other.quantity)
}

func (q *Quantity) DeepCopyInto(out *Quantity) {
	*out = Quantity{quantity: q.quantity.DeepCopy(), raw: bytes.Clone(q.raw), parseErr: q.parseErr}
}

func (q Quantity) MarshalJSON() ([]byte, error) {
	if len(q.raw) > 0 {
		return bytes.Clone(q.raw), nil
	}
	return q.quantity.MarshalJSON()
}

func (q *Quantity) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*q = Quantity{}
		return nil
	}
	var lit string
	var num json.Number
	err := json.Unmarshal(trimmed, &lit)
	if trimmed[0] != '"' {
		err = json.Unmarshal(trimmed, &num)
		lit = num.String()
	}
	if err != nil {
		return err
	}
	parsed, pErr := resource.ParseQuantity(strings.TrimSpace(lit))
	*q = Quantity{quantity: parsed, raw: bytes.Clone(trimmed), parseErr: pErr}
	return nil
}

type ResourceList map[corev1.ResourceName]Quantity

func (rl ResourceList) ToCore() corev1.ResourceList {
	if rl == nil {
		return nil
	}
	out := make(corev1.ResourceList, len(rl))
	for k, v := range rl {
		out[k] = v.AsQuantity()
	}
	return out
}

type ResourceRequirements struct {
	Limits   ResourceList           `json:"limits,omitempty"`
	Requests ResourceList           `json:"requests,omitempty"`
	Claims   []corev1.ResourceClaim `json:"claims,omitempty"`
}

func (rr *ResourceRequirements) ToCore() *corev1.ResourceRequirements {
	if rr == nil {
		return nil
	}
	return &corev1.ResourceRequirements{Limits: rr.Limits.ToCore(), Requests: rr.Requests.ToCore(), Claims: slices.Clone(rr.Claims)}
}

type ComponentPodSpec struct {
	Resource    *Resource             `json:"resource,omitempty"`
	Resources   *ResourceRequirements `json:"resources,omitempty"`
	Replicas    *int64                `json:"replicas,omitempty"`
	PodTemplate *PodTemplateSpec      `json:"podTemplate,omitempty"`
}

type JobManagerSpec struct {
	ComponentPodSpec `json:",inline"`
}
type TaskManagerSpec struct {
	ComponentPodSpec `json:",inline"`
}

type IngressSpec struct {
	Template    string                    `json:"template,omitempty"`
	ClassName   string                    `json:"className,omitempty"`
	Annotations map[string]string         `json:"annotations,omitempty"`
	Labels      map[string]string         `json:"labels,omitempty"`
	TLS         []networkingv1.IngressTLS `json:"tls,omitempty"`
}

type JobSpec struct {
	JarURI                 string      `json:"jarURI,omitempty"`
	Parallelism            *int64      `json:"parallelism,omitempty"`
	EntryClass             string      `json:"entryClass,omitempty"`
	Args                   []string    `json:"args,omitempty"`
	State                  JobState    `json:"state,omitempty"`
	SavepointTriggerNonce  *int64      `json:"savepointTriggerNonce,omitempty"`
	InitialSavepointPath   string      `json:"initialSavepointPath,omitempty"`
	CheckpointTriggerNonce *int64      `json:"checkpointTriggerNonce,omitempty"`
	UpgradeMode            UpgradeMode `json:"upgradeMode,omitempty"`
	AllowNonRestoredState  *bool       `json:"allowNonRestoredState,omitempty"`
	SavepointRedeployNonce *int64      `json:"savepointRedeployNonce,omitempty"`
	AutoscalerResetNonce   *int64      `json:"autoscalerResetNonce,omitempty"`
}

type FlinkDeploymentSpec struct {
	Image              string                   `json:"image,omitempty"`
	ImagePullPolicy    corev1.PullPolicy        `json:"imagePullPolicy,omitempty"`
	ServiceAccount     string                   `json:"serviceAccount,omitempty"`
	FlinkVersion       FlinkVersion             `json:"flinkVersion,omitempty"`
	Ingress            *IngressSpec             `json:"ingress,omitempty"`
	PodTemplate        *PodTemplateSpec         `json:"podTemplate,omitempty"`
	JobManager         *JobManagerSpec          `json:"jobManager,omitempty"`
	TaskManager        *TaskManagerSpec         `json:"taskManager,omitempty"`
	LogConfiguration   map[string]string        `json:"logConfiguration,omitempty"`
	FlinkConfiguration FlinkConfiguration       `json:"flinkConfiguration,omitempty,omitzero"`
	Job                *JobSpec                 `json:"job,omitempty"`
	RestartNonce       *int64                   `json:"restartNonce,omitempty"`
	Mode               KubernetesDeploymentMode `json:"mode,omitempty"`
}

func DecodeJSONMapUseNumber(raw []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	return m, dec.Decode(&m)
}

type Savepoint struct {
	TimeStamp    int64  `json:"timeStamp,omitempty"`
	Location     string `json:"location,omitempty"`
	TriggerType  string `json:"triggerType,omitempty"`
	TriggerNonce *int64 `json:"triggerNonce,omitempty"`
	FormatType   string `json:"formatType,omitempty"`
}

type SavepointInfo struct {
	LastSavepoint                  *Savepoint  `json:"lastSavepoint,omitempty"`
	TriggerID                      string      `json:"triggerId,omitempty"`
	TriggerTimestamp               *int64      `json:"triggerTimestamp,omitempty"`
	TriggerType                    string      `json:"triggerType,omitempty"`
	FormatType                     string      `json:"formatType,omitempty"`
	SavepointHistory               []Savepoint `json:"savepointHistory,omitempty"`
	LastPeriodicSavepointTimestamp int64       `json:"lastPeriodicSavepointTimestamp,omitempty"`
}

type Checkpoint struct {
	TimeStamp    int64  `json:"timeStamp,omitempty"`
	TriggerType  string `json:"triggerType,omitempty"`
	TriggerNonce *int64 `json:"triggerNonce,omitempty"`
	FormatType   string `json:"formatType,omitempty"`
}

type CheckpointInfo struct {
	LastCheckpoint                  *Checkpoint `json:"lastCheckpoint,omitempty"`
	TriggerID                       string      `json:"triggerId,omitempty"`
	TriggerTimestamp                *int64      `json:"triggerTimestamp,omitempty"`
	TriggerType                     string      `json:"triggerType,omitempty"`
	FormatType                      string      `json:"formatType,omitempty"`
	LastPeriodicCheckpointTimestamp int64       `json:"lastPeriodicCheckpointTimestamp,omitempty"`
}

type JobStatus struct {
	JobName              string          `json:"jobName,omitempty"`
	JobID                string          `json:"jobId,omitempty"`
	State                JobStatusState  `json:"state,omitempty"`
	StartTime            string          `json:"startTime,omitempty"`
	UpdateTime           string          `json:"updateTime,omitempty"`
	UpgradeSavepointPath string          `json:"upgradeSavepointPath,omitempty"`
	SavepointInfo        *SavepointInfo  `json:"savepointInfo,omitempty"`
	CheckpointInfo       *CheckpointInfo `json:"checkpointInfo,omitempty"`
}

type ReconciliationStatus struct {
	ReconciliationTimestamp int64               `json:"reconciliationTimestamp,omitempty"`
	LastReconciledSpec      string              `json:"lastReconciledSpec,omitempty"`
	LastStableSpec          string              `json:"lastStableSpec,omitempty"`
	State                   ReconciliationState `json:"state,omitempty"`
}

type TaskManagerStatusInfo struct {
	LabelSelector string `json:"labelSelector,omitempty"`
	Replicas      int64  `json:"replicas,omitempty"`
}

type DeploymentCondition struct {
	Type               string `json:"type,omitempty"`
	Status             string `json:"status,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
}

type FlinkDeploymentStatus struct {
	LifecycleState             LifecycleState             `json:"lifecycleState,omitempty"`
	Error                      string                     `json:"error,omitempty"`
	JobStatus                  JobStatus                  `json:"jobStatus,omitempty,omitzero"`
	JobManagerDeploymentStatus JobManagerDeploymentStatus `json:"jobManagerDeploymentStatus,omitempty"`
	ReconciliationStatus       ReconciliationStatus       `json:"reconciliationStatus,omitempty,omitzero"`
	TaskManager                *TaskManagerStatusInfo     `json:"taskManager,omitempty"`
	ClusterInfo                map[string]string          `json:"clusterInfo,omitempty"`
	Conditions                 []DeploymentCondition      `json:"conditions,omitempty"`
	ObservedGeneration         int64                      `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type FlinkDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              FlinkDeploymentSpec   `json:"spec,omitempty"`
	Status            FlinkDeploymentStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type FlinkDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FlinkDeployment `json:"items"`
}
