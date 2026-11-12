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
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FlinkVersion represents the supported Apache Flink major/minor version enum in FlinkDeployment.
type FlinkVersion string

const (
	// FlinkVersionV1_13 targets Apache Flink 1.13.
	FlinkVersionV1_13 FlinkVersion = "v1_13"
	// FlinkVersionV1_14 targets Apache Flink 1.14.
	FlinkVersionV1_14 FlinkVersion = "v1_14"
	// FlinkVersionV1_15 targets Apache Flink 1.15.
	FlinkVersionV1_15 FlinkVersion = "v1_15"
	// FlinkVersionV1_16 targets Apache Flink 1.16.
	FlinkVersionV1_16 FlinkVersion = "v1_16"
	// FlinkVersionV1_17 targets Apache Flink 1.17.
	FlinkVersionV1_17 FlinkVersion = "v1_17"
	// FlinkVersionV1_18 targets Apache Flink 1.18.
	FlinkVersionV1_18 FlinkVersion = "v1_18"
	// FlinkVersionV1_19 targets Apache Flink 1.19.
	FlinkVersionV1_19 FlinkVersion = "v1_19"
	// FlinkVersionV1_20 targets Apache Flink 1.20.
	FlinkVersionV1_20 FlinkVersion = "v1_20"
	// FlinkVersionV2_0 targets Apache Flink 2.0.
	FlinkVersionV2_0 FlinkVersion = "v2_0"
	// FlinkVersionV2_1 targets Apache Flink 2.1.
	FlinkVersionV2_1 FlinkVersion = "v2_1"
	// FlinkVersionV2_2 targets Apache Flink 2.2.
	FlinkVersionV2_2 FlinkVersion = "v2_2"
	// FlinkVersionV2_3 targets Apache Flink 2.3.
	FlinkVersionV2_3 FlinkVersion = "v2_3"
	// FlinkVersionV2_4 targets Apache Flink 2.4.
	FlinkVersionV2_4 FlinkVersion = "v2_4"
)

// KubernetesDeploymentMode specifies whether the Flink cluster runs in native or standalone Kubernetes mode.
type KubernetesDeploymentMode string

const (
	// KubernetesDeploymentModeNative uses Flink's native Kubernetes resource manager.
	KubernetesDeploymentModeNative KubernetesDeploymentMode = "native"
	// KubernetesDeploymentModeStandalone uses standalone JobManager and TaskManager Kubernetes Deployments.
	KubernetesDeploymentModeStandalone KubernetesDeploymentMode = "standalone"
)

// JobState defines the desired execution state of a Flink Application job.
type JobState string

const (
	// JobStateRunning indicates that the Flink job should be deployed and running.
	JobStateRunning JobState = "running"
	// JobStateSuspended indicates that the Flink job should be suspended and its cluster pods torn down.
	JobStateSuspended JobState = "suspended"
)

// UpgradeMode defines the state transfer mechanism used by the Flink Kubernetes Operator during suspension and upgrades.
type UpgradeMode string

const (
	// UpgradeModeStateless stops and starts the job with empty state unless initialSavepointPath is set.
	UpgradeModeStateless UpgradeMode = "stateless"
	// UpgradeModeLastState restores the job from the latest retained checkpoint or HA metadata.
	UpgradeModeLastState UpgradeMode = "last-state"
	// UpgradeModeSavepoint takes a canonical savepoint on suspension and restores from it on resumption.
	UpgradeModeSavepoint UpgradeMode = "savepoint"
)

// LifecycleState represents the high-level lifecycle state of a FlinkDeployment resource.
type LifecycleState string

const (
	// LifecycleStateCreated indicates the resource was created in Kubernetes but not yet handled by the operator.
	LifecycleStateCreated LifecycleState = "CREATED"
	// LifecycleStateSuspended indicates the resource (and underlying cluster) has been suspended.
	LifecycleStateSuspended LifecycleState = "SUSPENDED"
	// LifecycleStateUpgrading indicates the resource is currently being upgraded or suspended with a savepoint.
	LifecycleStateUpgrading LifecycleState = "UPGRADING"
	// LifecycleStateDeployed indicates the resource has been deployed, though it may not yet be considered stable.
	LifecycleStateDeployed LifecycleState = "DEPLOYED"
	// LifecycleStateStable indicates the deployed resource has passed stability checks.
	LifecycleStateStable LifecycleState = "STABLE"
	// LifecycleStateRollingBack indicates the operator is rolling back to the last stable spec.
	LifecycleStateRollingBack LifecycleState = "ROLLING_BACK"
	// LifecycleStateRolledBack indicates the resource is running with a rolled-back stable spec.
	LifecycleStateRolledBack LifecycleState = "ROLLED_BACK"
	// LifecycleStateFailed indicates the deployment entered a terminal failure state.
	LifecycleStateFailed LifecycleState = "FAILED"
	// LifecycleStateDeleting indicates the resource is undergoing foreground/finalizer deletion.
	LifecycleStateDeleting LifecycleState = "DELETING"
	// LifecycleStateDeleted indicates cleanup completed during deletion.
	LifecycleStateDeleted LifecycleState = "DELETED"
)

// JobManagerDeploymentStatus describes the readiness of the Flink JobManager Kubernetes Deployment.
type JobManagerDeploymentStatus string

const (
	// JobManagerDeploymentStatusReady indicates the JobManager is running and ready to serve REST calls.
	JobManagerDeploymentStatusReady JobManagerDeploymentStatus = "READY"
	// JobManagerDeploymentStatusDeployedNotReady indicates the JobManager is deployed but not yet ready.
	JobManagerDeploymentStatusDeployedNotReady JobManagerDeploymentStatus = "DEPLOYED_NOT_READY"
	// JobManagerDeploymentStatusDeploying indicates the JobManager Pod is being scheduled or starting up.
	JobManagerDeploymentStatusDeploying JobManagerDeploymentStatus = "DEPLOYING"
	// JobManagerDeploymentStatusMissing indicates no JobManager Deployment is currently present.
	JobManagerDeploymentStatusMissing JobManagerDeploymentStatus = "MISSING"
	// JobManagerDeploymentStatusError indicates the JobManager Deployment is in an unrecoverable error state.
	JobManagerDeploymentStatusError JobManagerDeploymentStatus = "ERROR"
)

// JobStatusState represents Apache Flink's internal org.apache.flink.api.common.JobStatus state reported in status.jobStatus.state.
type JobStatusState string

const (
	// JobStatusStateInitializing indicates the Flink job has been received by the Dispatcher and is initializing.
	JobStatusStateInitializing JobStatusState = "INITIALIZING"
	// JobStatusStateCreated indicates the Flink job graph is created and awaiting task scheduling.
	JobStatusStateCreated JobStatusState = "CREATED"
	// JobStatusStateRunning indicates tasks are scheduled or running.
	JobStatusStateRunning JobStatusState = "RUNNING"
	// JobStatusStateFailing indicates the job failed and is cleaning up tasks before failing or restarting.
	JobStatusStateFailing JobStatusState = "FAILING"
	// JobStatusStateFailed indicates the Flink job reached a terminal failed state.
	JobStatusStateFailed JobStatusState = "FAILED"
	// JobStatusStateCancelling indicates the Flink job is currently being cancelled.
	JobStatusStateCancelling JobStatusState = "CANCELLING"
	// JobStatusStateCanceled indicates the Flink job was cancelled without error.
	JobStatusStateCanceled JobStatusState = "CANCELED"
	// JobStatusStateFinished indicates all tasks of a bounded Flink job completed successfully.
	JobStatusStateFinished JobStatusState = "FINISHED"
	// JobStatusStateRestarting indicates the Flink job is restarting after a task failure.
	JobStatusStateRestarting JobStatusState = "RESTARTING"
	// JobStatusStateSuspended indicates the Flink job has been suspended.
	JobStatusStateSuspended JobStatusState = "SUSPENDED"
	// JobStatusStateReconciling indicates the Flink job status is being reconciled by the operator after reconnect.
	JobStatusStateReconciling JobStatusState = "RECONCILING"
)

// ReconciliationState describes the state of the last spec reconciliation in status.reconciliationStatus.state.
type ReconciliationState string

const (
	// ReconciliationStateDeployed indicates lastReconciledSpec is currently deployed.
	ReconciliationStateDeployed ReconciliationState = "DEPLOYED"
	// ReconciliationStateUpgrading indicates the spec is currently being upgraded or suspended asynchronously.
	ReconciliationStateUpgrading ReconciliationState = "UPGRADING"
	// ReconciliationStateRollingBack indicates the spec is rolling back to lastStableSpec.
	ReconciliationStateRollingBack ReconciliationState = "ROLLING_BACK"
	// ReconciliationStateRolledBack indicates the spec rolled back to lastStableSpec.
	ReconciliationStateRolledBack ReconciliationState = "ROLLED_BACK"
)

// FlinkConfiguration represents Flink configuration properties under spec.flinkConfiguration.
// In the upstream CRD this field uses x-kubernetes-preserve-unknown-fields: true and accepts
// flat string maps as well as numeric, boolean, list, or nested YAML/JSON maps.
// Retaining raw JSON bytes preserves nested or list-of-map structures verbatim during
// mutating webhook patches unless mutated via Go setters. Structural JSON decode errors (such as
// non-object JSON on unmanaged resources) are deferred to FlatMap() so NoopWebhook and informer
// ListAndWatch never fail JSON unmarshaling.
type FlinkConfiguration struct {
	raw       json.RawMessage
	entries   map[string]string
	decodeErr error
}

// NewFlinkConfiguration constructs a FlinkConfiguration from a flat key-value string map.
func NewFlinkConfiguration(entries map[string]string) FlinkConfiguration {
	if len(entries) == 0 {
		return FlinkConfiguration{}
	}
	return FlinkConfiguration{
		entries: maps.Clone(entries),
	}
}

// IsZero reports whether the FlinkConfiguration contains neither raw JSON nor Go map entries.
func (c FlinkConfiguration) IsZero() bool {
	return len(c.raw) == 0 && len(c.entries) == 0 && c.decodeErr == nil
}

// Set sets a key-value pair on the FlinkConfiguration. When raw JSON contains structured or
// non-lossless entries (such as JSON null values, list-of-maps, nested arrays, or entries that
// cannot be flattened losslessly), Set updates key in the decoded JSON object (removing any
// duplicate nested or flat leaf entries for key and lifting any nested sub-map at key) and
// re-marshals raw so sibling entries survive intact; otherwise it materializes a flat Go map and
// clears raw.
func (c *FlinkConfiguration) Set(key, val string) {
	if len(c.raw) > 0 && c.decodeErr == nil {
		if rawMap, err := DecodeJSONMapUseNumber(c.raw); err == nil {
			var flat map[string]string
			useRawMap := hasStructuredFlinkConfigEntries(rawMap)
			if !useRawMap && c.entries == nil {
				var flatErr error
				flat, flatErr = c.FlatMap()
				if flatErr != nil {
					useRawMap = true
				}
			}
			if useRawMap {
				deleteConflictingFlinkConfigPaths("", rawMap, key)
				if subMap, isMap := rawMap[key].(map[string]any); isMap && len(subMap) > 0 {
					delete(rawMap, key)
					liftFlinkConfigSubMap(rawMap, key, subMap)
				}
				rawMap[key] = val
				if encoded, marshalErr := json.Marshal(rawMap); marshalErr == nil {
					c.raw = encoded
					c.entries = nil
					c.decodeErr = nil
					return
				}
			} else if c.entries == nil && flat != nil {
				c.entries = flat
			}
		}
	}
	c.raw = nil
	c.decodeErr = nil
	if c.entries == nil {
		c.entries = make(map[string]string)
	}
	c.entries[key] = val
}

// DeepCopyInto copies the receiver into out, cloning underlying byte slices and maps.
func (c *FlinkConfiguration) DeepCopyInto(out *FlinkConfiguration) {
	*out = FlinkConfiguration{
		decodeErr: c.decodeErr,
	}
	if len(c.raw) > 0 {
		out.raw = bytes.Clone(c.raw)
	}
	if c.entries != nil {
		out.entries = maps.Clone(c.entries)
	}
}

// Equal reports whether two FlinkConfiguration values represent equivalent configuration maps,
// preserving flat-versus-nested scalar equivalence while distinguishing structured YAML lists,
// semicolon-containing list elements, JSON null values, and empty list/map values.
func (c FlinkConfiguration) Equal(other FlinkConfiguration) bool {
	if c.IsZero() && other.IsZero() {
		return true
	}
	if c.decodeErr != nil || other.decodeErr != nil {
		return (c.decodeErr == nil) == (other.decodeErr == nil) &&
			bytes.Equal(c.raw, other.raw) &&
			maps.Equal(c.entries, other.entries)
	}
	m1, err1 := c.canonicalMap()
	m2, err2 := other.canonicalMap()
	if err1 != nil || err2 != nil {
		return (err1 == nil) == (err2 == nil) &&
			bytes.Equal(c.raw, other.raw) &&
			maps.Equal(c.entries, other.entries)
	}
	return reflect.DeepEqual(m1, m2)
}

// MarshalJSON returns the original raw JSON bytes verbatim when unmarshaled from JSON and not
// mutated via Go setters; otherwise it marshals the Go string map.
func (c FlinkConfiguration) MarshalJSON() ([]byte, error) {
	if len(c.raw) > 0 {
		return bytes.Clone(c.raw), nil
	}
	if len(c.entries) == 0 {
		return []byte("null"), nil
	}
	return json.Marshal(c.entries)
}

// UnmarshalJSON retains raw JSON bytes verbatim so mutating webhook JSONPatches never flatten or
// duplicate nested keys, deferring any non-object decode error to FlatMap().
func (c *FlinkConfiguration) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*c = FlinkConfiguration{}
		return nil
	}
	var obj map[string]json.RawMessage
	err := json.Unmarshal(trimmed, &obj)
	*c = FlinkConfiguration{
		raw:       bytes.Clone(trimmed),
		decodeErr: err,
	}
	return nil
}

// FlatMap returns a deterministic dot-flattened string map representation of the FlinkConfiguration,
// rejecting non-object JSON, whitespace-padded keys or segments, and duplicate flat/nested key
// collisions (for example both `parallelism: {default: 4}` and `parallelism.default: "1"`).
func (c FlinkConfiguration) FlatMap() (map[string]string, error) {
	if c.decodeErr != nil {
		return nil, c.decodeErr
	}
	if len(c.raw) == 0 {
		if len(c.entries) == 0 {
			return nil, nil
		}
		for _, k := range slices.Sorted(maps.Keys(c.entries)) {
			if err := validateFlinkConfigKey(k); err != nil {
				return nil, err
			}
		}
		return maps.Clone(c.entries), nil
	}
	rawMap, err := DecodeJSONMapUseNumber(c.raw)
	if err != nil {
		return nil, err
	}
	if len(rawMap) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(rawMap))
	seen := make(map[string]struct{}, len(rawMap))
	if err := flattenFlinkConfig("", rawMap, out, seen); err != nil {
		return nil, err
	}
	return out, nil
}

type (
	canonicalEmptyFlinkMap   struct{}
	canonicalEmptyFlinkSlice struct{}
	canonicalNullFlinkValue  struct{}
)

func hasStructuredFlinkConfigEntries(m map[string]any) bool {
	for _, v := range m {
		switch typed := v.(type) {
		case nil:
			return true
		case map[string]any:
			if len(typed) == 0 || hasStructuredFlinkConfigEntries(typed) {
				return true
			}
		case []any:
			return true
		}
	}
	return false
}

func deleteConflictingFlinkConfigPaths(prefix string, m map[string]any, targetKey string) {
	for k, v := range m {
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}
		switch {
		case fullKey == targetKey:
			if subMap, isMap := v.(map[string]any); !isMap || len(subMap) == 0 {
				delete(m, k)
			}
		case strings.HasPrefix(targetKey, fullKey+"."):
			if subMap, isMap := v.(map[string]any); isMap && len(subMap) > 0 {
				deleteConflictingFlinkConfigPaths(fullKey, subMap, targetKey)
				if len(subMap) == 0 {
					delete(m, k)
				}
			}
		}
	}
}

func liftFlinkConfigSubMap(rootMap map[string]any, prefix string, subMap map[string]any) {
	for _, k := range slices.Sorted(maps.Keys(subMap)) {
		v := subMap[k]
		liftedKey := prefix + "." + k
		if nested, isMap := v.(map[string]any); isMap && len(nested) > 0 {
			if _, exists := rootMap[liftedKey]; exists {
				liftFlinkConfigSubMap(rootMap, liftedKey, nested)
				continue
			}
			rootMap[liftedKey] = nested
			continue
		}
		if existingMap, isMap := rootMap[liftedKey].(map[string]any); isMap && len(existingMap) > 0 {
			delete(rootMap, liftedKey)
			liftFlinkConfigSubMap(rootMap, liftedKey, existingMap)
		}
		rootMap[liftedKey] = v
	}
}

func (c FlinkConfiguration) canonicalMap() (map[string]any, error) {
	if c.decodeErr != nil {
		return nil, c.decodeErr
	}
	if len(c.raw) == 0 {
		if len(c.entries) == 0 {
			return nil, nil
		}
		out := make(map[string]any, len(c.entries))
		for _, k := range slices.Sorted(maps.Keys(c.entries)) {
			if err := validateFlinkConfigKey(k); err != nil {
				return nil, err
			}
			out[k] = c.entries[k]
		}
		return out, nil
	}
	rawMap, err := DecodeJSONMapUseNumber(c.raw)
	if err != nil {
		return nil, err
	}
	if len(rawMap) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(rawMap))
	if err := canonicalizeFlinkConfigMap("", rawMap, out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func canonicalizeFlinkConfigMap(prefix string, in map[string]any, out map[string]any) error {
	for _, k := range slices.Sorted(maps.Keys(in)) {
		if err := validateFlinkConfigKey(k); err != nil {
			return err
		}
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}
		if err := canonicalizeFlinkConfigEntry(fullKey, in[k], out); err != nil {
			return err
		}
	}
	return nil
}

func writeCanonicalFlinkConfigEntry(key string, val any, out map[string]any) error {
	if _, exists := out[key]; exists {
		return fmt.Errorf("duplicate flattened flinkConfiguration key %q", key)
	}
	out[key] = val
	return nil
}

func isLosslessFlinkConfigSlice(items []any) bool {
	if len(items) == 0 {
		return false
	}
	for _, elem := range items {
		switch typed := elem.(type) {
		case json.Number, bool:
		case string:
			if typed == "" || strings.Contains(typed, ";") {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func canonicalizeStructuredFlinkValue(val any) (any, error) {
	switch typed := val.(type) {
	case nil, string, json.Number, bool:
		return typed, nil
	case []any:
		out := make([]any, len(typed))
		for i, elem := range typed {
			canonElem, err := canonicalizeStructuredFlinkValue(elem)
			if err != nil {
				return nil, err
			}
			out[i] = canonElem
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, elem := range typed {
			canonElem, err := canonicalizeStructuredFlinkValue(elem)
			if err != nil {
				return nil, err
			}
			out[k] = canonElem
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported structured value type %T", val)
	}
}

func canonicalizeFlinkConfigEntry(key string, val any, out map[string]any) error {
	switch typed := val.(type) {
	case nil:
		return writeCanonicalFlinkConfigEntry(key, canonicalNullFlinkValue{}, out)
	case string:
		return writeCanonicalFlinkConfigEntry(key, typed, out)
	case json.Number:
		return writeCanonicalFlinkConfigEntry(key, typed.String(), out)
	case bool:
		return writeCanonicalFlinkConfigEntry(key, strconv.FormatBool(typed), out)
	case map[string]any:
		if len(typed) == 0 {
			return writeCanonicalFlinkConfigEntry(key, canonicalEmptyFlinkMap{}, out)
		}
		return canonicalizeFlinkConfigMap(key, typed, out)
	case []any:
		if len(typed) == 0 {
			return writeCanonicalFlinkConfigEntry(key, canonicalEmptyFlinkSlice{}, out)
		}
		if isLosslessFlinkConfigSlice(typed) {
			parts := make([]string, 0, len(typed))
			for i, elem := range typed {
				formatted, err := formatFlinkConfigSliceElement(elem)
				if err != nil {
					return fmt.Errorf("element %d of %q: %w", i, key, err)
				}
				parts = append(parts, formatted)
			}
			return writeCanonicalFlinkConfigEntry(key, strings.Join(parts, ";"), out)
		}
		structured, err := canonicalizeStructuredFlinkValue(typed)
		if err != nil {
			return fmt.Errorf("key %q: %w", key, err)
		}
		return writeCanonicalFlinkConfigEntry(key, structured, out)
	default:
		return fmt.Errorf("unsupported value type %T for flinkConfiguration key %q", val, key)
	}
}

func validateFlinkConfigKey(k string) error {
	if strings.TrimSpace(k) == "" {
		return fmt.Errorf("flinkConfiguration key %q must not be empty", k)
	}
	if k != strings.TrimSpace(k) {
		return fmt.Errorf("flinkConfiguration key %q must not contain leading or trailing whitespace", k)
	}
	for seg := range strings.SplitSeq(k, ".") {
		if seg != strings.TrimSpace(seg) {
			return fmt.Errorf("flinkConfiguration key %q must not contain leading or trailing whitespace in segment %q", k, seg)
		}
	}
	return nil
}

func flattenFlinkConfig(prefix string, in map[string]any, out map[string]string, seen map[string]struct{}) error {
	for _, k := range slices.Sorted(maps.Keys(in)) {
		if err := validateFlinkConfigKey(k); err != nil {
			return err
		}
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}
		if err := flattenFlinkConfigValue(fullKey, in[k], out, seen); err != nil {
			return err
		}
	}
	return nil
}

func recordFlatFlinkConfigLeafKey(key string, seen map[string]struct{}) error {
	if _, exists := seen[key]; exists {
		return fmt.Errorf("duplicate flattened flinkConfiguration key %q", key)
	}
	seen[key] = struct{}{}
	return nil
}

func writeFlatFlinkConfigEntry(key, val string, out map[string]string, seen map[string]struct{}) error {
	if err := recordFlatFlinkConfigLeafKey(key, seen); err != nil {
		return err
	}
	out[key] = val
	return nil
}

func flattenFlinkConfigValue(key string, val any, out map[string]string, seen map[string]struct{}) error {
	switch typed := val.(type) {
	case nil:
		return writeFlatFlinkConfigEntry(key, "null", out, seen)
	case string:
		return writeFlatFlinkConfigEntry(key, typed, out, seen)
	case json.Number:
		return writeFlatFlinkConfigEntry(key, typed.String(), out, seen)
	case bool:
		return writeFlatFlinkConfigEntry(key, strconv.FormatBool(typed), out, seen)
	case map[string]any:
		if len(typed) == 0 {
			return recordFlatFlinkConfigLeafKey(key, seen)
		}
		return flattenFlinkConfig(key, typed, out, seen)
	case []any:
		if len(typed) == 0 {
			return recordFlatFlinkConfigLeafKey(key, seen)
		}
		parts := make([]string, 0, len(typed))
		for i, elem := range typed {
			formatted, err := formatFlinkConfigSliceElement(elem)
			if err != nil {
				return fmt.Errorf("element %d of %q: %w", i, key, err)
			}
			parts = append(parts, formatted)
		}
		return writeFlatFlinkConfigEntry(key, strings.Join(parts, ";"), out, seen)
	default:
		return fmt.Errorf("unsupported value type %T for flinkConfiguration key %q", val, key)
	}
}

func formatFlinkConfigSliceElement(val any) (string, error) {
	switch typed := val.(type) {
	case nil:
		return "null", nil
	case string:
		return typed, nil
	case json.Number:
		return typed.String(), nil
	case bool:
		return strconv.FormatBool(typed), nil
	case map[string]any, []any:
		return "", nil
	default:
		return "", fmt.Errorf("unsupported slice element type %T", val)
	}
}

// Resource defines Flink component compute resources using Flink's CPU float and MemorySize/Quantity string format.
type Resource struct {
	// CPU is the amount of CPU allocated to each Pod (e.g. 1.0 or 0.5).
	CPU *float64 `json:"cpu,omitempty"`
	// Memory is the amount of total process memory allocated to each Pod in Flink MemorySize or Kubernetes Quantity format (e.g. "2048m", "2g", or "2Gi").
	Memory string `json:"memory,omitempty"`
	// EphemeralStorage is the amount of ephemeral storage allocated to each Pod in Kubernetes Quantity format (e.g. "2Gi").
	EphemeralStorage string `json:"ephemeralStorage,omitempty"`
}

// PodTemplateSpec models Apache Flink Kubernetes Operator's @SchemaFrom(type = Pod.class)
// podTemplate schema, which accepts optional Pod TypeMeta (apiVersion, kind) and status
// fields alongside standard PodTemplateSpec metadata and spec.
// Retaining raw JSON bytes preserves partial component podTemplate overrides verbatim during
// mutating webhook patches and metadata/placement merges without injecting Go zero values.
// Decode errors (such as Flink-only quantity strings on unmanaged resources) are deferred to
// DecodeError() so NoopWebhook and informer ListAndWatch never fail JSON unmarshaling.
type PodTemplateSpec struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`
	Spec              corev1.PodSpec    `json:"spec,omitempty,omitzero"`
	Status            *corev1.PodStatus `json:"status,omitempty"`

	raw       json.RawMessage
	decodeErr error
}

type podTemplateSpecJSON struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`
	Spec              corev1.PodSpec    `json:"spec,omitempty,omitzero"`
	Status            *corev1.PodStatus `json:"status,omitempty"`
}

// DecodeError returns any deferred JSON decode error recorded during UnmarshalJSON.
func (in *PodTemplateSpec) DecodeError() error {
	if in == nil {
		return nil
	}
	return in.decodeErr
}

// FromCorePodTemplateSpec converts a *corev1.PodTemplateSpec into a *PodTemplateSpec.
func FromCorePodTemplateSpec(pt *corev1.PodTemplateSpec) *PodTemplateSpec {
	if pt == nil {
		return nil
	}
	copied := pt.DeepCopy()
	return &PodTemplateSpec{
		ObjectMeta: copied.ObjectMeta,
		Spec:       copied.Spec,
	}
}

// ToCorePodTemplateSpec extracts metadata and spec into a standalone *corev1.PodTemplateSpec.
func (in *PodTemplateSpec) ToCorePodTemplateSpec() *corev1.PodTemplateSpec {
	if in == nil {
		return nil
	}
	return &corev1.PodTemplateSpec{
		ObjectMeta: *in.ObjectMeta.DeepCopy(),
		Spec:       *in.Spec.DeepCopy(),
	}
}

// DeepCopyInto copies the receiver into out, cloning all nested fields and any retained raw JSON bytes.
func (in *PodTemplateSpec) DeepCopyInto(out *PodTemplateSpec) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	if in.Status != nil {
		in, out := &in.Status, &out.Status
		*out = new(corev1.PodStatus)
		(*in).DeepCopyInto(*out)
	}
	if len(in.raw) > 0 {
		out.raw = bytes.Clone(in.raw)
	} else {
		out.raw = nil
	}
	out.decodeErr = in.decodeErr
}

// Equal reports whether two PodTemplateSpec values are semantically equal across their exported
// fields (or carry equivalent raw bytes when either operand failed PodTemplateSpec decoding).
func (in PodTemplateSpec) Equal(other PodTemplateSpec) bool {
	if in.decodeErr != nil || other.decodeErr != nil {
		return (in.decodeErr == nil) == (other.decodeErr == nil) && bytes.Equal(in.raw, other.raw)
	}
	return in.TypeMeta == other.TypeMeta &&
		apiequality.Semantic.DeepEqual(in.ObjectMeta, other.ObjectMeta) &&
		apiequality.Semantic.DeepEqual(in.Spec, other.Spec) &&
		apiequality.Semantic.DeepEqual(in.Status, other.Status)
}

// HasUnmodifiedRawSpec reports whether in was decoded from JSON and no non-placement PodSpec
// fields (everything in Spec outside NodeSelector, Tolerations, and SchedulingGates) have been
// mutated in Go since decoding.
func (in *PodTemplateSpec) HasUnmodifiedRawSpec() bool {
	if in == nil || in.decodeErr != nil || len(in.raw) == 0 {
		return false
	}
	var orig podTemplateSpecJSON
	if err := json.Unmarshal(in.raw, &orig); err != nil {
		return false
	}
	return apiequality.Semantic.DeepEqual(PodSpecWithoutOverlay(&orig.Spec), PodSpecWithoutOverlay(&in.Spec))
}

// RawSpecTolerations returns the per-element raw JSON maps for spec.tolerations when in is
// backed by unmodified raw PodSpec JSON, or nil when in was constructed in Go without raw JSON.
func (in *PodTemplateSpec) RawSpecTolerations() []map[string]any {
	if !in.HasUnmodifiedRawSpec() {
		return nil
	}
	encoded, err := in.MarshalJSON()
	if err != nil {
		return nil
	}
	ptMap, err := DecodeJSONMapUseNumber(encoded)
	if err != nil {
		return nil
	}
	specMap, ok := ptMap["spec"].(map[string]any)
	if !ok {
		return nil
	}
	rawSlice, ok := specMap["tolerations"].([]any)
	if !ok || len(rawSlice) == 0 {
		return nil
	}
	out := make([]map[string]any, len(rawSlice))
	for i, elem := range rawSlice {
		out[i], _ = elem.(map[string]any)
	}
	return out
}

// UnmarshalJSON decodes a PodTemplateSpec while retaining a copy of the raw JSON bytes so
// unmutated templates round-trip verbatim and metadata/placement overlays patch only modified
// paths without materializing Go zero values into partial PodSpec overrides. Structural decode
// errors are deferred to DecodeError().
func (in *PodTemplateSpec) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*in = PodTemplateSpec{}
		return nil
	}
	var decoded podTemplateSpecJSON
	decodeErr := json.Unmarshal(trimmed, &decoded)
	if decodeErr != nil {
		decoded = podTemplateSpecJSON{}
	}
	*in = PodTemplateSpec{
		TypeMeta:   decoded.TypeMeta,
		ObjectMeta: decoded.ObjectMeta,
		Spec:       decoded.Spec,
		Status:     decoded.Status,
		raw:        bytes.Clone(trimmed),
		decodeErr:  decodeErr,
	}
	return nil
}

// MarshalJSON returns retained raw JSON bytes verbatim when unmutated since UnmarshalJSON,
// patches only modified metadata/placement subtrees onto the raw JSON map when Kueue injects or
// restores PodSet overlays, and otherwise omits JSON `null` entries inside spec and status.
// Go's corev1 and metav1 types omit `,omitempty` (or `omitzero`) on a number of PodSpec and
// PodStatus fields (such as `spec.containers`, `spec.volumes[].projected.sources`,
// `spec.volumes[].scaleIO.secretRef`, probe `grpc.service`, ephemeral PVC `dataSource.apiGroup`,
// `nodeSelectorTerms`, and zero `metav1.Time` values in `status`), whereas the Fabric8-generated
// flinkdeployments.flink.apache.org CRD defines them as optional non-nullable fields, so a
// mutating webhook patch carrying `"value": null` for any of them fails CRD schema validation.
func (in PodTemplateSpec) MarshalJSON() ([]byte, error) {
	if in.decodeErr != nil && len(in.raw) > 0 {
		return bytes.Clone(in.raw), nil
	}
	if len(in.raw) > 0 {
		var orig podTemplateSpecJSON
		if err := json.Unmarshal(in.raw, &orig); err == nil {
			origSpec := PodTemplateSpec{
				TypeMeta:   orig.TypeMeta,
				ObjectMeta: orig.ObjectMeta,
				Spec:       orig.Spec,
				Status:     orig.Status,
			}
			if in.Equal(origSpec) {
				return bytes.Clone(in.raw), nil
			}
			if apiequality.Semantic.DeepEqual(PodSpecWithoutOverlay(&orig.Spec), PodSpecWithoutOverlay(&in.Spec)) &&
				apiequality.Semantic.DeepEqual(orig.Status, in.Status) {
				return in.marshalOverlayOntoRaw(&orig)
			}
		}
	}
	return marshalPodTemplateSpecClean(podTemplateSpecJSON{
		TypeMeta:   in.TypeMeta,
		ObjectMeta: in.ObjectMeta,
		Spec:       in.Spec,
		Status:     in.Status,
	})
}

func marshalPodTemplateSpecClean(value podTemplateSpecJSON) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if !bytes.Contains(raw, []byte(`"spec":`)) && !bytes.Contains(raw, []byte(":null")) {
		return raw, nil
	}
	ptMap, err := DecodeJSONMapUseNumber(raw)
	if err != nil {
		return nil, err
	}
	if specMap, ok := ptMap["spec"].(map[string]any); ok {
		stripNullEntries(specMap)
		PruneEmptyPodSpecFields(specMap)
		if len(specMap) == 0 {
			delete(ptMap, "spec")
		}
	}
	if statusMap, ok := ptMap["status"].(map[string]any); ok {
		stripNullEntries(statusMap)
		if len(statusMap) == 0 {
			delete(ptMap, "status")
		}
	}
	return json.Marshal(ptMap)
}

var podSpecStructFieldCache sync.Map // map[reflect.Type]map[string]reflect.StructField

func derefType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func jsonFieldHasOmit(tag string) bool {
	for _, opt := range strings.Split(tag, ",")[1:] {
		if opt == "omitempty" || opt == "omitzero" {
			return true
		}
	}
	return false
}

func structJSONFields(t reflect.Type) map[string]reflect.StructField {
	t = derefType(t)
	if cached, ok := podSpecStructFieldCache.Load(t); ok {
		return cached.(map[string]reflect.StructField)
	}
	fields := make(map[string]reflect.StructField, t.NumField())
	collectStructJSONFields(t, fields)
	actual, _ := podSpecStructFieldCache.LoadOrStore(t, fields)
	return actual.(map[string]reflect.StructField)
}

func collectStructJSONFields(t reflect.Type, dst map[string]reflect.StructField) {
	t = derefType(t)
	if t.Kind() != reflect.Struct {
		return
	}
	for sf := range t.Fields() {
		if !sf.IsExported() {
			continue
		}
		tag := sf.Tag.Get("json")
		if tag == "-" {
			continue
		}
		parts := strings.Split(tag, ",")
		name := parts[0]
		isInline := slices.Contains(parts[1:], "inline")
		if isInline || (sf.Anonymous && name == "") {
			collectStructJSONFields(sf.Type, dst)
			continue
		}
		if name == "" {
			name = sf.Name
		}
		dst[name] = sf
	}
}

// PruneEmptyPodSpecFields strips non-pointer zero-valued corev1.PodSpec struct fields that lack
// omitempty in Go (such as omitted Container.Name "", ContainerPort.ContainerPort 0,
// ResourceFieldSelector.Divisor "0", probe port 0, PreferredSchedulingTerm.Weight 0, and
// TopologySpreadConstraint zero fields) from a UseNumber-decoded spec map so Go-constructed
// partial PodTemplateSpec overrides match Fabric8's @JsonInclude(NON_EMPTY) / NON_NULL semantics.
func PruneEmptyPodSpecFields(specMap map[string]any) {
	pruneFabric8EmptyStructFields(specMap, reflect.TypeFor[corev1.PodSpec]())
}

func isPreservedEmptyStringField(t reflect.Type, fieldName string, m map[string]any) bool {
	if fieldName == "value" && (t == reflect.TypeFor[corev1.HTTPHeader]() || t == reflect.TypeFor[corev1.Sysctl]()) {
		name, _ := m["name"].(string)
		return name != ""
	}
	return false
}

func pruneFabric8EmptyStructFields(m map[string]any, t reflect.Type) {
	t = derefType(t)
	if t.Kind() != reflect.Struct {
		return
	}
	fields := structJSONFields(t)
	for k, v := range m {
		sf, ok := fields[k]
		if !ok {
			continue
		}
		ft := derefType(sf.Type)
		switch typed := v.(type) {
		case string:
			if typed == "" && sf.Type.Kind() != reflect.Pointer && ft.Kind() == reflect.String &&
				!jsonFieldHasOmit(sf.Tag.Get("json")) && !isPreservedEmptyStringField(t, k, m) {
				delete(m, k)
			} else if typed == "0" && sf.Type == reflect.TypeFor[resource.Quantity]() {
				delete(m, k)
			}
		case json.Number:
			if typed.String() == "0" && sf.Type.Kind() != reflect.Pointer && !jsonFieldHasOmit(sf.Tag.Get("json")) &&
				t != reflect.TypeFor[corev1.ISCSIVolumeSource]() &&
				t != reflect.TypeFor[corev1.SleepAction]() {
				delete(m, k)
			}
		case []any:
			if len(typed) == 0 {
				delete(m, k)
				continue
			}
			if ft.Kind() == reflect.Slice {
				elemType := derefType(ft.Elem())
				if elemType.Kind() == reflect.Struct {
					for _, elem := range typed {
						if elemMap, isMap := elem.(map[string]any); isMap {
							pruneFabric8EmptyStructFields(elemMap, elemType)
						}
					}
				}
			}
		case map[string]any:
			switch ft.Kind() {
			case reflect.Map:
				if len(typed) == 0 {
					delete(m, k)
				}
			case reflect.Struct:
				pruneFabric8EmptyStructFields(typed, ft)
				if sf.Type.Kind() == reflect.Struct && len(typed) == 0 {
					delete(m, k)
				}
			}
		}
	}
}

func (in PodTemplateSpec) marshalOverlayOntoRaw(orig *podTemplateSpecJSON) ([]byte, error) {
	ptMap, err := DecodeJSONMapUseNumber(in.raw)
	if err != nil {
		return nil, err
	}
	if ptMap == nil {
		ptMap = make(map[string]any)
	}

	if in.APIVersion != orig.APIVersion {
		if in.APIVersion == "" {
			delete(ptMap, "apiVersion")
		} else {
			ptMap["apiVersion"] = in.APIVersion
		}
	}
	if in.Kind != orig.Kind {
		if in.Kind == "" {
			delete(ptMap, "kind")
		} else {
			ptMap["kind"] = in.Kind
		}
	}

	switch {
	case apiequality.Semantic.DeepEqual(
		objectMetaWithoutLabelsAnnotations(&orig.ObjectMeta),
		objectMetaWithoutLabelsAnnotations(&in.ObjectMeta),
	):
		metaMap, _ := ptMap["metadata"].(map[string]any)
		metaChanged := false
		if !apiequality.Semantic.DeepEqual(orig.Labels, in.Labels) {
			metaChanged = true
			if len(in.Labels) == 0 {
				delete(metaMap, "labels")
			} else {
				if metaMap == nil {
					metaMap = make(map[string]any)
					ptMap["metadata"] = metaMap
				}
				metaMap["labels"] = stringMapToAnyMap(in.Labels)
			}
		}
		if !apiequality.Semantic.DeepEqual(orig.Annotations, in.Annotations) {
			metaChanged = true
			if len(in.Annotations) == 0 {
				delete(metaMap, "annotations")
			} else {
				if metaMap == nil {
					metaMap = make(map[string]any)
					ptMap["metadata"] = metaMap
				}
				metaMap["annotations"] = stringMapToAnyMap(in.Annotations)
			}
		}
		if metaChanged && len(metaMap) == 0 {
			delete(ptMap, "metadata")
		}
	case apiequality.Semantic.DeepEqual(in.ObjectMeta, metav1.ObjectMeta{}):
		delete(ptMap, "metadata")
	default:
		metaRaw, err := json.Marshal(in.ObjectMeta)
		if err != nil {
			return nil, err
		}
		metaMap, err := DecodeJSONMapUseNumber(metaRaw)
		if err != nil {
			return nil, err
		}
		ptMap["metadata"] = metaMap
	}

	specMap, _ := ptMap["spec"].(map[string]any)
	specChanged := false
	if !apiequality.Semantic.DeepEqual(orig.Spec.NodeSelector, in.Spec.NodeSelector) {
		specChanged = true
		if len(in.Spec.NodeSelector) == 0 {
			delete(specMap, "nodeSelector")
		} else {
			if specMap == nil {
				specMap = make(map[string]any)
				ptMap["spec"] = specMap
			}
			specMap["nodeSelector"] = stringMapToAnyMap(in.Spec.NodeSelector)
		}
	}
	if !apiequality.Semantic.DeepEqual(orig.Spec.Tolerations, in.Spec.Tolerations) {
		specChanged = true
		if len(in.Spec.Tolerations) == 0 {
			delete(specMap, "tolerations")
		} else {
			tolSlice, err := marshalCleanSliceSubtree(in.Spec.Tolerations)
			if err != nil {
				return nil, err
			}
			if origTolSlice, ok := specMap["tolerations"].([]any); ok {
				for i := 0; i < len(orig.Spec.Tolerations) && i < len(origTolSlice) && i < len(tolSlice); i++ {
					if apiequality.Semantic.DeepEqual(orig.Spec.Tolerations[i], in.Spec.Tolerations[i]) {
						tolSlice[i] = origTolSlice[i]
					}
				}
			}
			if specMap == nil {
				specMap = make(map[string]any)
				ptMap["spec"] = specMap
			}
			specMap["tolerations"] = tolSlice
		}
	}
	if !apiequality.Semantic.DeepEqual(orig.Spec.SchedulingGates, in.Spec.SchedulingGates) {
		specChanged = true
		if len(in.Spec.SchedulingGates) == 0 {
			delete(specMap, "schedulingGates")
		} else {
			gateSlice, err := marshalCleanSliceSubtree(in.Spec.SchedulingGates)
			if err != nil {
				return nil, err
			}
			if specMap == nil {
				specMap = make(map[string]any)
				ptMap["spec"] = specMap
			}
			specMap["schedulingGates"] = gateSlice
		}
	}
	if specChanged && len(specMap) == 0 {
		delete(ptMap, "spec")
	}

	return json.Marshal(ptMap)
}

// PodSpecWithoutOverlay returns a deep copy of spec with Kueue PodSet overlay fields
// (NodeSelector, Tolerations, and SchedulingGates) cleared.
func PodSpecWithoutOverlay(spec *corev1.PodSpec) *corev1.PodSpec {
	cloned := spec.DeepCopy()
	cloned.NodeSelector = nil
	cloned.Tolerations = nil
	cloned.SchedulingGates = nil
	return cloned
}

func objectMetaWithoutLabelsAnnotations(meta *metav1.ObjectMeta) *metav1.ObjectMeta {
	cloned := meta.DeepCopy()
	cloned.Labels = nil
	cloned.Annotations = nil
	return cloned
}

func stringMapToAnyMap(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func marshalCleanSliceSubtree(val any) ([]any, error) {
	raw, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var arr []any
	if err := dec.Decode(&arr); err != nil {
		return nil, err
	}
	stripNullEntriesInSlice(arr)
	return arr, nil
}

// stripNullEntries recursively deletes map entries whose value is nil from m and from any
// nested objects or arrays of objects it contains.
func stripNullEntries(m map[string]any) {
	for k, v := range m {
		switch typed := v.(type) {
		case nil:
			delete(m, k)
		case map[string]any:
			stripNullEntries(typed)
		case []any:
			stripNullEntriesInSlice(typed)
		}
	}
}

func stripNullEntriesInSlice(items []any) {
	for _, item := range items {
		switch typed := item.(type) {
		case map[string]any:
			stripNullEntries(typed)
		case []any:
			stripNullEntriesInSlice(typed)
		}
	}
}

// Quantity wraps k8s.io/apimachinery/pkg/api/resource.Quantity while preserving the raw JSON
// token and literal string when decoded from JSON. Preserving raw literals lets Kueue's
// webhook reject fractional BinarySI memory strings (such as "1.5Gi") before canonicalization,
// accept plain integer byte counts (such as 1000000000 or "1024000"), and emit user quantities
// verbatim during mutating webhook defaulting and merge patches without rewriting 1000000000
// to "1G" in etcd. UnmarshalJSON defers Kubernetes resource.ParseQuantity errors to ParseError()
// so valid Flink MemorySize literals (such as "2g" or "2GiB") on unmanaged resources or when
// FlinkDeploymentIntegration is disabled do not fail webhook decoding or informer LIST sync.
type Quantity struct {
	quantity   resource.Quantity
	raw        json.RawMessage
	literal    string
	hasLiteral bool
	parseErr   error
}

// ParseQuantity parses str into a Quantity while preserving str as its raw literal.
func ParseQuantity(str string) (Quantity, error) {
	parsed, err := resource.ParseQuantity(strings.TrimSpace(str))
	if err != nil {
		return Quantity{}, err
	}
	return Quantity{
		quantity:   parsed,
		literal:    str,
		hasLiteral: true,
	}, nil
}

// MustParseQuantity parses str into a Quantity or panics if parsing fails.
func MustParseQuantity(str string) Quantity {
	q, err := ParseQuantity(str)
	if err != nil {
		panic(err)
	}
	return q
}

// QuantityFromResource wraps an existing resource.Quantity without a raw JSON literal.
func QuantityFromResource(q resource.Quantity) Quantity {
	return Quantity{
		quantity: q.DeepCopy(),
	}
}

// RawLiteral returns the uncanonicalized string or number literal when available, falling back
// to the underlying resource.Quantity canonical string when constructed via QuantityFromResource.
func (q Quantity) RawLiteral() string {
	if q.hasLiteral {
		return q.literal
	}
	return q.quantity.String()
}

// ParseError returns any deferred Kubernetes resource.ParseQuantity error recorded when
// decoding a JSON string or number token in UnmarshalJSON.
func (q Quantity) ParseError() error {
	return q.parseErr
}

// AsQuantity returns a deep copy of the underlying Kubernetes resource.Quantity.
func (q Quantity) AsQuantity() resource.Quantity {
	return q.quantity.DeepCopy()
}

// String returns the canonical Kubernetes Quantity string representation.
func (q Quantity) String() string {
	return q.quantity.String()
}

// Sign returns 0 if the quantity is zero, -1 if negative, or 1 if positive.
func (q Quantity) Sign() int {
	return q.quantity.Sign()
}

// Cmp compares q to other using Kubernetes resource.Quantity semantics.
func (q Quantity) Cmp(other Quantity) int {
	return q.quantity.Cmp(other.quantity)
}

// Equal reports whether q and other represent the same Kubernetes resource quantity value, or
// the same raw literal when either operand failed Kubernetes resource.ParseQuantity decoding.
func (q Quantity) Equal(other Quantity) bool {
	if q.parseErr != nil || other.parseErr != nil {
		return (q.parseErr == nil) == (other.parseErr == nil) &&
			q.hasLiteral == other.hasLiteral &&
			q.literal == other.literal &&
			bytes.Equal(q.raw, other.raw)
	}
	return q.quantity.Equal(other.quantity)
}

// DeepCopyInto copies the receiver into out, cloning any retained raw JSON bytes.
func (q *Quantity) DeepCopyInto(out *Quantity) {
	*out = Quantity{
		quantity:   q.quantity.DeepCopy(),
		literal:    q.literal,
		hasLiteral: q.hasLiteral,
		parseErr:   q.parseErr,
	}
	if len(q.raw) > 0 {
		out.raw = bytes.Clone(q.raw)
	}
}

// MarshalJSON emits the original JSON token verbatim when present so webhook patches never
// rewrite user quantities in etcd, falling back to literal or resource.Quantity marshaling
// when constructed in Go.
func (q Quantity) MarshalJSON() ([]byte, error) {
	if len(q.raw) > 0 {
		return bytes.Clone(q.raw), nil
	}
	if q.hasLiteral {
		return json.Marshal(q.literal)
	}
	return q.quantity.MarshalJSON()
}

// UnmarshalJSON decodes a JSON string or number into a Quantity while preserving the raw JSON
// token, uncanonicalized literal, and any deferred Kubernetes resource.ParseQuantity error.
func (q *Quantity) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*q = Quantity{}
		return nil
	}

	if trimmed[0] == '"' {
		var lit string
		if err := json.Unmarshal(trimmed, &lit); err != nil {
			return err
		}
		parsed, err := resource.ParseQuantity(strings.TrimSpace(lit))
		*q = Quantity{
			quantity:   parsed,
			raw:        bytes.Clone(trimmed),
			literal:    lit,
			hasLiteral: true,
			parseErr:   err,
		}
		return nil
	}

	var num json.Number
	if err := json.Unmarshal(trimmed, &num); err != nil {
		return err
	}
	lit := num.String()
	parsed, err := resource.ParseQuantity(lit)
	*q = Quantity{
		quantity:   parsed,
		raw:        bytes.Clone(trimmed),
		literal:    lit,
		hasLiteral: true,
		parseErr:   err,
	}
	return nil
}

// ResourceList maps Kubernetes resource names to raw-preserving Quantity values.
type ResourceList map[corev1.ResourceName]Quantity

// ResourceListFromCore converts a corev1.ResourceList into a ResourceList.
func ResourceListFromCore(in corev1.ResourceList) ResourceList {
	if in == nil {
		return nil
	}
	out := make(ResourceList, len(in))
	for k, v := range in {
		out[k] = QuantityFromResource(v)
	}
	return out
}

// ToCore converts a ResourceList into a corev1.ResourceList.
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

// ResourceRequirements mirrors corev1.ResourceRequirements using raw-preserving Quantity values.
type ResourceRequirements struct {
	// Limits describes the maximum amount of compute resources allowed.
	Limits ResourceList `json:"limits,omitempty"`
	// Requests describes the minimum amount of compute resources required.
	Requests ResourceList `json:"requests,omitempty"`
	// Claims lists the names of resources from pod.spec.resourceClaims required by the container.
	Claims []corev1.ResourceClaim `json:"claims,omitempty"`
}

// ResourceRequirementsFromCore converts a *corev1.ResourceRequirements into a *ResourceRequirements.
func ResourceRequirementsFromCore(in *corev1.ResourceRequirements) *ResourceRequirements {
	if in == nil {
		return nil
	}
	return &ResourceRequirements{
		Limits:   ResourceListFromCore(in.Limits),
		Requests: ResourceListFromCore(in.Requests),
		Claims:   slices.Clone(in.Claims),
	}
}

// ToCore converts a *ResourceRequirements into a *corev1.ResourceRequirements.
func (rr *ResourceRequirements) ToCore() *corev1.ResourceRequirements {
	if rr == nil {
		return nil
	}
	return &corev1.ResourceRequirements{
		Limits:   rr.Limits.ToCore(),
		Requests: rr.Requests.ToCore(),
		Claims:   slices.Clone(rr.Claims),
	}
}

// ComponentPodSpec holds the shared Pod template, replica count, and resource settings for JobManager and TaskManager roles.
type ComponentPodSpec struct {
	// Resource specifies Flink CPU, Memory, and EphemeralStorage values for the main Flink container.
	Resource *Resource `json:"resource,omitempty"`
	// Resources specifies standard Kubernetes ResourceRequirements (requests and limits) for the main Flink container.
	Resources *ResourceRequirements `json:"resources,omitempty"`
	// Replicas specifies the number of replicas for the component.
	Replicas *int64 `json:"replicas,omitempty"`
	// PodTemplate specifies role-specific PodTemplateSpec overrides merged on top of spec.podTemplate.
	PodTemplate *PodTemplateSpec `json:"podTemplate,omitempty"`
}

// JobManagerSpec defines the configuration of the Flink JobManager pods.
type JobManagerSpec struct {
	ComponentPodSpec `json:",inline"`
}

// TaskManagerSpec defines the configuration of the Flink TaskManager pods.
type TaskManagerSpec struct {
	ComponentPodSpec `json:",inline"`
}

// IngressSpec defines optional Ingress routing configuration for the Flink web UI / REST endpoint.
type IngressSpec struct {
	// Template is the ingress host/path template string.
	Template string `json:"template,omitempty"`
	// ClassName is the IngressClass name.
	ClassName string `json:"className,omitempty"`
	// Annotations are custom annotations added to the Ingress resource.
	Annotations map[string]string `json:"annotations,omitempty"`
	// Labels are custom labels added to the Ingress resource.
	Labels map[string]string `json:"labels,omitempty"`
	// TLS configures TLS termination on the Ingress resource.
	TLS []networkingv1.IngressTLS `json:"tls,omitempty"`
}

// JobSpec defines an Application-mode Flink job executed by the FlinkDeployment.
type JobSpec struct {
	// JarURI is the URI of the job JAR within the container or remote filesystem.
	JarURI string `json:"jarURI,omitempty"`
	// Parallelism is the job-level parallelism.
	Parallelism *int64 `json:"parallelism,omitempty"`
	// EntryClass is the fully qualified main class name of the Flink job.
	EntryClass string `json:"entryClass,omitempty"`
	// Args is the array of command-line arguments passed to the job main method.
	Args []string `json:"args,omitempty"`
	// State is the desired running or suspended state of the job.
	State JobState `json:"state,omitempty"`
	// SavepointTriggerNonce triggers a manual savepoint when incremented.
	SavepointTriggerNonce *int64 `json:"savepointTriggerNonce,omitempty"`
	// InitialSavepointPath specifies a savepoint path used on initial deployment or savepoint redeploy.
	InitialSavepointPath string `json:"initialSavepointPath,omitempty"`
	// CheckpointTriggerNonce triggers a manual checkpoint when incremented.
	CheckpointTriggerNonce *int64 `json:"checkpointTriggerNonce,omitempty"`
	// UpgradeMode controls how job state is preserved across upgrades and suspensions.
	UpgradeMode UpgradeMode `json:"upgradeMode,omitempty"`
	// AllowNonRestoredState permits restoring from a savepoint containing state for removed operators.
	AllowNonRestoredState *bool `json:"allowNonRestoredState,omitempty"`
	// SavepointRedeployNonce triggers a full redeploy from initialSavepointPath when incremented.
	SavepointRedeployNonce *int64 `json:"savepointRedeployNonce,omitempty"`
	// AutoscalerResetNonce resets autoscaler state when incremented.
	AutoscalerResetNonce *int64 `json:"autoscalerResetNonce,omitempty"`
}

// FlinkDeploymentSpec defines the desired state of a FlinkDeployment.
type FlinkDeploymentSpec struct {
	// Image is the Flink container image used for JobManager and TaskManager pods.
	Image string `json:"image,omitempty"`
	// ImagePullPolicy is the container image pull policy for Flink pods.
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// ServiceAccount is the Kubernetes ServiceAccount name used by Flink pods.
	ServiceAccount string `json:"serviceAccount,omitempty"`
	// FlinkVersion is the target Apache Flink version enum.
	FlinkVersion FlinkVersion `json:"flinkVersion,omitempty"`
	// Ingress configures optional Ingress exposure for the JobManager REST/UI service.
	Ingress *IngressSpec `json:"ingress,omitempty"`
	// PodTemplate defines the base PodTemplateSpec shared by JobManager and TaskManager pods.
	PodTemplate *PodTemplateSpec `json:"podTemplate,omitempty"`
	// JobManager configures the JobManager replicas, resources, and podTemplate overrides.
	JobManager *JobManagerSpec `json:"jobManager,omitempty"`
	// TaskManager configures the TaskManager replicas, resources, and podTemplate overrides.
	TaskManager *TaskManagerSpec `json:"taskManager,omitempty"`
	// LogConfiguration provides custom log4j/logback configuration file contents by filename.
	LogConfiguration map[string]string `json:"logConfiguration,omitempty"`
	// FlinkConfiguration holds Flink configuration key-value overrides (flink-conf.yaml / config.yaml).
	FlinkConfiguration FlinkConfiguration `json:"flinkConfiguration,omitempty,omitzero"`
	// Job defines the Application-mode Flink job specification; nil denotes an unmanaged Session cluster.
	Job *JobSpec `json:"job,omitempty"`
	// RestartNonce triggers a cluster restart when changed.
	RestartNonce *int64 `json:"restartNonce,omitempty"`
	// Mode selects native or standalone Kubernetes deployment mode.
	Mode KubernetesDeploymentMode `json:"mode,omitempty"`
}

// DecodeJSONMapUseNumber decodes a JSON object into map[string]any using json.Decoder.UseNumber
// so 64-bit integer tokens (such as *int64 nonces > 2^53) round-trip without float64 rounding.
func DecodeJSONMapUseNumber(raw []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// SnapshotTriggerType identifies what initiated a Flink savepoint or checkpoint.
type SnapshotTriggerType string

const (
	// SnapshotTriggerTypeManual indicates a user-requested manual trigger.
	SnapshotTriggerTypeManual SnapshotTriggerType = "MANUAL"
	// SnapshotTriggerTypePeriodic indicates an operator-scheduled periodic trigger.
	SnapshotTriggerTypePeriodic SnapshotTriggerType = "PERIODIC"
	// SnapshotTriggerTypeUpgrade indicates a snapshot taken during suspension or spec upgrade.
	SnapshotTriggerTypeUpgrade SnapshotTriggerType = "UPGRADE"
	// SnapshotTriggerTypeUnknown indicates an unknown snapshot trigger source.
	SnapshotTriggerTypeUnknown SnapshotTriggerType = "UNKNOWN"
)

// SavepointFormatType describes the binary format of a Flink savepoint.
type SavepointFormatType string

const (
	// SavepointFormatTypeCanonical is Flink's cross-backend canonical savepoint format.
	SavepointFormatTypeCanonical SavepointFormatType = "CANONICAL"
	// SavepointFormatTypeNative is the state-backend-native savepoint format.
	SavepointFormatTypeNative SavepointFormatType = "NATIVE"
	// SavepointFormatTypeUnknown indicates an unspecified savepoint format.
	SavepointFormatTypeUnknown SavepointFormatType = "UNKNOWN"
)

// CheckpointFormatType describes the format of a Flink checkpoint.
type CheckpointFormatType string

const (
	// CheckpointFormatTypeFull is a full checkpoint.
	CheckpointFormatTypeFull CheckpointFormatType = "FULL"
	// CheckpointFormatTypeIncremental is an incremental checkpoint.
	CheckpointFormatTypeIncremental CheckpointFormatType = "INCREMENTAL"
	// CheckpointFormatTypeUnknown indicates an unspecified checkpoint format.
	CheckpointFormatTypeUnknown CheckpointFormatType = "UNKNOWN"
)

// Savepoint records metadata for a completed Flink savepoint.
type Savepoint struct {
	// TimeStamp is the epoch-millisecond timestamp when the savepoint completed.
	TimeStamp int64 `json:"timeStamp,omitempty"`
	// Location is the external storage URI of the savepoint.
	Location string `json:"location,omitempty"`
	// TriggerType identifies what triggered the savepoint.
	TriggerType SnapshotTriggerType `json:"triggerType,omitempty"`
	// TriggerNonce is the nonce value that triggered a manual savepoint, if applicable.
	TriggerNonce *int64 `json:"triggerNonce,omitempty"`
	// FormatType is the format of the savepoint.
	FormatType SavepointFormatType `json:"formatType,omitempty"`
}

// SavepointInfo tracks the latest and historical savepoints in status.jobStatus.savepointInfo.
type SavepointInfo struct {
	// LastSavepoint is the most recently completed savepoint.
	LastSavepoint *Savepoint `json:"lastSavepoint,omitempty"`
	// TriggerID is the Flink REST trigger ID of an in-progress savepoint operation.
	TriggerID string `json:"triggerId,omitempty"`
	// TriggerTimestamp is the epoch-millisecond timestamp of the active savepoint trigger.
	TriggerTimestamp *int64 `json:"triggerTimestamp,omitempty"`
	// TriggerType is the trigger type of the active savepoint operation.
	TriggerType SnapshotTriggerType `json:"triggerType,omitempty"`
	// FormatType is the format type of the active savepoint operation.
	FormatType SavepointFormatType `json:"formatType,omitempty"`
	// SavepointHistory lists retained historical savepoints.
	SavepointHistory []Savepoint `json:"savepointHistory,omitempty"`
	// LastPeriodicSavepointTimestamp records when the last periodic savepoint ran.
	LastPeriodicSavepointTimestamp int64 `json:"lastPeriodicSavepointTimestamp,omitempty"`
}

// Checkpoint records metadata for a completed Flink checkpoint.
type Checkpoint struct {
	// TimeStamp is the epoch-millisecond timestamp when the checkpoint completed.
	TimeStamp int64 `json:"timeStamp,omitempty"`
	// TriggerType identifies what triggered the checkpoint.
	TriggerType SnapshotTriggerType `json:"triggerType,omitempty"`
	// TriggerNonce is the nonce value that triggered a manual checkpoint, if applicable.
	TriggerNonce *int64 `json:"triggerNonce,omitempty"`
	// FormatType is the format of the checkpoint.
	FormatType CheckpointFormatType `json:"formatType,omitempty"`
}

// CheckpointInfo tracks checkpoint status in status.jobStatus.checkpointInfo.
type CheckpointInfo struct {
	// LastCheckpoint is the most recently completed checkpoint.
	LastCheckpoint *Checkpoint `json:"lastCheckpoint,omitempty"`
	// TriggerID is the Flink REST trigger ID of an in-progress checkpoint operation.
	TriggerID string `json:"triggerId,omitempty"`
	// TriggerTimestamp is the epoch-millisecond timestamp of the active checkpoint trigger.
	TriggerTimestamp *int64 `json:"triggerTimestamp,omitempty"`
	// TriggerType is the trigger type of the active checkpoint operation.
	TriggerType SnapshotTriggerType `json:"triggerType,omitempty"`
	// FormatType is the format type of the active checkpoint operation.
	FormatType CheckpointFormatType `json:"formatType,omitempty"`
	// LastPeriodicCheckpointTimestamp records when the last periodic checkpoint ran.
	LastPeriodicCheckpointTimestamp int64 `json:"lastPeriodicCheckpointTimestamp,omitempty"`
}

// JobStatus reports the observed status of the Flink Application job.
type JobStatus struct {
	// JobName is the name of the Flink job.
	JobName string `json:"jobName,omitempty"`
	// JobID is the hex Flink JobID string.
	JobID string `json:"jobId,omitempty"`
	// State is the observed Flink JobStatus state (e.g. RUNNING, FINISHED, FAILED, CANCELED, SUSPENDED).
	State JobStatusState `json:"state,omitempty"`
	// StartTime is the epoch-millisecond start time reported as a string by the Flink operator.
	StartTime string `json:"startTime,omitempty"`
	// UpdateTime is the epoch-millisecond status update time reported as a string by the Flink operator.
	UpdateTime string `json:"updateTime,omitempty"`
	// UpgradeSavepointPath records the savepoint or checkpoint location used for the next restore.
	UpgradeSavepointPath string `json:"upgradeSavepointPath,omitempty"`
	// SavepointInfo holds in-progress and completed savepoint metadata.
	SavepointInfo *SavepointInfo `json:"savepointInfo,omitempty"`
	// CheckpointInfo holds in-progress and completed checkpoint metadata.
	CheckpointInfo *CheckpointInfo `json:"checkpointInfo,omitempty"`
}

// ReconciliationStatus tracks the last reconciled and last stable specs serialized by the Flink operator.
type ReconciliationStatus struct {
	// ReconciliationTimestamp is the epoch-millisecond timestamp of the last reconciliation.
	ReconciliationTimestamp int64 `json:"reconciliationTimestamp,omitempty"`
	// LastReconciledSpec is the operator's JSON-serialized last reconciled spec.
	LastReconciledSpec string `json:"lastReconciledSpec,omitempty"`
	// LastStableSpec is the operator's JSON-serialized last stable spec.
	LastStableSpec string `json:"lastStableSpec,omitempty"`
	// State is the current reconciliation state (DEPLOYED, UPGRADING, ROLLING_BACK, ROLLED_BACK).
	State ReconciliationState `json:"state,omitempty"`
}

// TaskManagerStatusInfo reports observed TaskManager replica and selector details in status.taskManager.
type TaskManagerStatusInfo struct {
	// LabelSelector is the label selector matching TaskManager pods.
	LabelSelector string `json:"labelSelector,omitempty"`
	// Replicas is the observed TaskManager replica count.
	Replicas int64 `json:"replicas,omitempty"`
}

// DeploymentCondition represents an entry in status.conditions of a FlinkDeployment.
// Note: Fabric8 serializes lastTransitionTime as a plain string without format: date-time.
type DeploymentCondition struct {
	// Type of condition.
	Type string `json:"type,omitempty"`
	// Status of the condition (True, False, Unknown).
	Status string `json:"status,omitempty"`
	// Reason is a machine-readable reason code for the transition.
	Reason string `json:"reason,omitempty"`
	// Message is a human-readable description of the transition.
	Message string `json:"message,omitempty"`
	// LastTransitionTime is the timestamp string of the last condition transition.
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	// ObservedGeneration is the metadata.generation observed for this condition.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// FlinkDeploymentStatus defines the observed state of a FlinkDeployment.
type FlinkDeploymentStatus struct {
	// LifecycleState is the overall lifecycle phase of the FlinkDeployment.
	LifecycleState LifecycleState `json:"lifecycleState,omitempty"`
	// Error records the latest reconciliation or deployment error message from the Flink operator.
	Error string `json:"error,omitempty"`
	// JobStatus holds the observed Flink job state and snapshot metadata.
	JobStatus JobStatus `json:"jobStatus,omitempty,omitzero"`
	// JobManagerDeploymentStatus reports the readiness of the JobManager Deployment.
	JobManagerDeploymentStatus JobManagerDeploymentStatus `json:"jobManagerDeploymentStatus,omitempty"`
	// ReconciliationStatus tracks spec reconciliation progress and serialized snapshots.
	ReconciliationStatus ReconciliationStatus `json:"reconciliationStatus,omitempty,omitzero"`
	// TaskManager reports observed TaskManager selector and replica count.
	TaskManager *TaskManagerStatusInfo `json:"taskManager,omitempty"`
	// ClusterInfo stores Flink runtime version and cluster metadata reported by the operator.
	ClusterInfo map[string]string `json:"clusterInfo,omitempty"`
	// Conditions holds deployment conditions reported on the resource.
	Conditions []DeploymentCondition `json:"conditions,omitempty"`
	// ObservedGeneration is the most recent metadata.generation observed by the Flink operator.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// FlinkDeployment is the Schema for the flinkdeployments API in flink.apache.org/v1beta1.
//
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type FlinkDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of the FlinkDeployment.
	Spec FlinkDeploymentSpec `json:"spec,omitempty"`
	// Status represents the observed state of the FlinkDeployment.
	Status FlinkDeploymentStatus `json:"status,omitempty,omitzero"`
}

// FlinkDeploymentList contains a list of FlinkDeployment resources.
//
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type FlinkDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the slice of FlinkDeployment objects.
	Items []FlinkDeployment `json:"items"`
}
