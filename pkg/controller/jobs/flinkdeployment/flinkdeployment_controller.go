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
	"math"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	clientutil "sigs.k8s.io/kueue/pkg/util/client"
)

const (
	// FrameworkName is the integration identifier used in Kueue's configuration.integrations.frameworks.
	FrameworkName = "flink.apache.org/flinkdeployment"
)

var (
	gvk = flinkv1beta1.SchemeGroupVersion.WithKind("FlinkDeployment")

	// NewReconciler creates the GenericReconciler factory for FlinkDeployment resources.
	NewReconciler = jobframework.NewGenericReconcilerFactory(newJob)
)

// RegisterIntegration registers the FlinkDeployment integration with the given IntegrationManager.
//
// Note: Apache Flink Kubernetes Operator creates JobManager and standalone TaskManager
// Deployments with controller=false OwnerReferences, which metav1.GetControllerOf ignores, so
// the deployment and pod integrations cannot resolve a FlinkDeployment ancestor in Alpha. See
// site/content/en/docs/tasks/run/flinkdeployments.md for co-enabled integration guidance.
func RegisterIntegration(m *jobframework.IntegrationManager) error {
	return m.RegisterIntegration(FrameworkName, jobframework.IntegrationCallbacks{
		SetupIndexes:          SetupIndexes,
		NewJob:                newJob,
		NewReconciler:         NewReconciler,
		SetupWebhook:          SetupWebhook,
		JobType:               &flinkv1beta1.FlinkDeployment{},
		AddToScheme:           flinkv1beta1.AddToScheme,
		CanSupportIntegration: canSupportIntegration,
	})
}

// +kubebuilder:rbac:groups=flink.apache.org,resources=flinkdeployments,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=flink.apache.org,resources=flinkdeployments/status,verbs=get
// +kubebuilder:rbac:groups=flink.apache.org,resources=flinkdeployments/finalizers,verbs=get;update

type flinkDeployment flinkv1beta1.FlinkDeployment

var _ jobframework.GenericJob = (*flinkDeployment)(nil)
var _ jobframework.JobWithSkip = (*flinkDeployment)(nil)
var _ jobframework.JobWithCustomStop = (*flinkDeployment)(nil)

func newJob() jobframework.GenericJob {
	return &flinkDeployment{}
}

func fromObject(obj runtime.Object) *flinkDeployment {
	fd, ok := obj.(*flinkv1beta1.FlinkDeployment)
	if !ok {
		return nil
	}
	return (*flinkDeployment)(fd)
}

func (j *flinkDeployment) Object() client.Object {
	return (*flinkv1beta1.FlinkDeployment)(j)
}

// Skip bypasses Kueue reconciliation for all live Session clusters (spec.job == nil),
// including pre-existing Session clusters carrying a stale queue-name label and clusters
// in namespaces where manageJobsWithoutQueueName is enabled globally.
//
// Name is empty when loadJob hit NotFound (a zero-valued object also has spec.job == nil)
// and DeletionTimestamp is set while finalizers are still pending. ReconcileGenericJob calls
// Skip before it finalizes orphaned Workloads, so both cases must fall through to finalization
// instead of being skipped.
func (j *flinkDeployment) Skip(_ context.Context) bool {
	if j.Name == "" || !j.DeletionTimestamp.IsZero() {
		return false
	}
	return j.Spec.Job == nil
}

func (j *flinkDeployment) IsSuspended() bool {
	return j.Spec.Job != nil && j.Spec.Job.State == flinkv1beta1.JobStateSuspended
}

// IsActive reports whether the Flink Operator still has JobManager or TaskManager pods for
// this deployment, based on pod evidence in status (JobManager Deployment presence,
// status.taskManager.replicas, and a non-terminal Flink job state). It ignores UPGRADING
// lifecycle/reconciliation states so a suspended deployment parked in UPGRADING without pods
// does not hold an evicted Workload's admission forever. In native mode (where TaskManager pods
// are owned by the JobManager Deployment), a post-deployment FAILED lifecycle state with a
// MISSING JobManager Deployment also returns false because the operator aborts before refreshing
// stale status.taskManager.replicas and leaves status.jobStatus.state in RECONCILING.
func (j *flinkDeployment) IsActive() bool {
	switch j.Status.JobManagerDeploymentStatus {
	case flinkv1beta1.JobManagerDeploymentStatusReady,
		flinkv1beta1.JobManagerDeploymentStatusDeploying,
		flinkv1beta1.JobManagerDeploymentStatusDeployedNotReady,
		flinkv1beta1.JobManagerDeploymentStatusError:
		return true
	}

	if j.Spec.Mode != flinkv1beta1.KubernetesDeploymentModeStandalone &&
		j.Status.LifecycleState == flinkv1beta1.LifecycleStateFailed &&
		j.Status.JobManagerDeploymentStatus == flinkv1beta1.JobManagerDeploymentStatusMissing &&
		(j.Status.ReconciliationStatus.State == flinkv1beta1.ReconciliationStateDeployed ||
			j.Status.ReconciliationStatus.State == flinkv1beta1.ReconciliationStateRolledBack) {
		return false
	}

	if j.Status.TaskManager != nil && j.Status.TaskManager.Replicas > 0 {
		return true
	}

	switch j.Status.JobStatus.State {
	case flinkv1beta1.JobStatusStateInitializing,
		flinkv1beta1.JobStatusStateCreated,
		flinkv1beta1.JobStatusStateRunning,
		flinkv1beta1.JobStatusStateFailing,
		flinkv1beta1.JobStatusStateCancelling,
		flinkv1beta1.JobStatusStateRestarting,
		flinkv1beta1.JobStatusStateReconciling:
		return true
	}

	return false
}

func (j *flinkDeployment) Suspend() {
	if j.Spec.Job != nil {
		j.Spec.Job.State = flinkv1beta1.JobStateSuspended
	}
}

// Stop suspends the FlinkDeployment (setting spec.job.state to suspended while preserving
// spec.job.upgradeMode) and restores PodSet overlays from podSetsInfo even when the deployment
// was already manually suspended before eviction or Workload deletion, avoiding no-op API patches
// on repeat reconciles once already suspended and restored.
func (j *flinkDeployment) Stop(ctx context.Context, c client.Client, podSetsInfo []podset.PodSetInfo, _ jobframework.StopReason, _ string) (bool, error) {
	wasSuspended := j.IsSuspended()
	var stoppedNow bool
	if err := clientutil.Patch(ctx, c, j.Object(), func() (bool, error) {
		j.Suspend()
		stoppedNow = !wasSuspended && j.IsSuspended()
		restored := false
		if podSetsInfo != nil {
			restored = j.RestorePodSetsInfo(ctx, podSetsInfo)
		}
		return stoppedNow || restored, nil
	}); err != nil {
		return false, err
	}
	return stoppedNow, nil
}

func (j *flinkDeployment) GVK() schema.GroupVersionKind {
	return gvk
}

// resolveRolePodSetsInfo routes PodSetInfo entries strictly by PodSet name. jobframework always
// builds PodSetInfo from Workload.spec.podSets, so an empty or unknown name is an error rather
// than a positional fallback.
func resolveRolePodSetsInfo(podSetsInfo []podset.PodSetInfo) (podset.PodSetInfo, podset.PodSetInfo, error) {
	if len(podSetsInfo) != podSetsCount {
		return podset.PodSetInfo{}, podset.PodSetInfo{}, podset.BadPodSetsInfoLenError(podSetsCount, len(podSetsInfo))
	}

	var (
		jmInfo  podset.PodSetInfo
		tmInfo  podset.PodSetInfo
		jmFound bool
		tmFound bool
	)
	for _, info := range podSetsInfo {
		switch info.Name {
		case jobManagerPodSetName:
			if jmFound {
				return podset.PodSetInfo{}, podset.PodSetInfo{}, fmt.Errorf("%w: duplicate podSet %q", podset.ErrInvalidPodsetInfo, info.Name)
			}
			jmInfo = info
			jmFound = true
		case taskManagerPodSetName:
			if tmFound {
				return podset.PodSetInfo{}, podset.PodSetInfo{}, fmt.Errorf("%w: duplicate podSet %q", podset.ErrInvalidPodsetInfo, info.Name)
			}
			tmInfo = info
			tmFound = true
		default:
			return podset.PodSetInfo{}, podset.PodSetInfo{}, fmt.Errorf("%w: unknown podSet %q", podset.ErrInvalidPodsetInfo, info.Name)
		}
	}
	return jmInfo, tmInfo, nil
}

func allSchedulingGatesNamed(gates []corev1.PodSchedulingGate) bool {
	for _, g := range gates {
		if g.Name == "" {
			return false
		}
	}
	return true
}

func omitInheritedBaseMapEntries(src, baseMap, roleMap map[string]string) map[string]string {
	if len(src) == 0 || len(baseMap) == 0 {
		return src
	}
	var out map[string]string
	for k, v := range src {
		if _, inRole := roleMap[k]; !inRole {
			if baseVal, inBase := baseMap[k]; inBase && baseVal == v {
				if out == nil {
					out = maps.Clone(src)
				}
				delete(out, k)
			}
		}
	}
	if out != nil {
		return out
	}
	return src
}

func applyPodSetInfoToComponent(
	ctx context.Context,
	baseTemplate *flinkv1beta1.PodTemplateSpec,
	comp *flinkv1beta1.ComponentPodSpec,
	info podset.PodSetInfo,
	mergeArraysByName bool,
) error {
	log := ctrl.LoggerFrom(ctx)

	// Validate against the effective merged template first so conflicts with root
	// spec.podTemplate (for example conflicting nodeSelector keys) return ErrInvalidPodSetUpdate.
	merged, err := mergePodTemplates(baseTemplate, comp.PodTemplate, mergeArraysByName)
	if err != nil {
		return fmt.Errorf("%w: %w", podset.ErrInvalidPodsetInfo, err)
	}
	preMergeTols := slices.Clone(merged.Spec.Tolerations)
	preMergeGates := slices.Clone(merged.Spec.SchedulingGates)
	if err := podset.Merge(log, &merged.ObjectMeta, &merged.Spec, info); err != nil {
		return err
	}
	addedTols := merged.Spec.Tolerations[len(preMergeTols):]
	addedGates := merged.Spec.SchedulingGates[len(preMergeGates):]

	wasNil := comp.PodTemplate == nil
	if comp.PodTemplate == nil {
		comp.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
	}

	// Seed role tolerations (and schedulingGates when merging positionally) up to the
	// effective pre-admission slice length whenever 0 <= len(role) < len(base), preserving any
	// existing partial positional role overrides at indices < len(role) so Flink Operator's
	// positional array merge appends Kueue's additions at index >= len(base).
	if len(addedTols) > 0 {
		if len(comp.PodTemplate.Spec.Tolerations) < len(preMergeTols) {
			comp.PodTemplate.Spec.Tolerations = append(
				comp.PodTemplate.Spec.Tolerations,
				preMergeTols[len(comp.PodTemplate.Spec.Tolerations):]...,
			)
		}
		comp.PodTemplate.Spec.Tolerations = append(comp.PodTemplate.Spec.Tolerations, addedTols...)
	}
	var baseGates []corev1.PodSchedulingGate
	if baseTemplate != nil {
		baseGates = baseTemplate.Spec.SchedulingGates
	}
	gatesMergeByName := mergeArraysByName &&
		allSchedulingGatesNamed(baseGates) &&
		allSchedulingGatesNamed(comp.PodTemplate.Spec.SchedulingGates) &&
		allSchedulingGatesNamed(info.SchedulingGates)
	if !gatesMergeByName && len(addedGates) > 0 {
		if len(comp.PodTemplate.Spec.SchedulingGates) < len(preMergeGates) {
			comp.PodTemplate.Spec.SchedulingGates = append(
				comp.PodTemplate.Spec.SchedulingGates,
				preMergeGates[len(comp.PodTemplate.Spec.SchedulingGates):]...,
			)
		}
		comp.PodTemplate.Spec.SchedulingGates = append(comp.PodTemplate.Spec.SchedulingGates, addedGates...)
	}

	infoWithoutTols := info
	infoWithoutTols.Tolerations = nil
	if !gatesMergeByName {
		infoWithoutTols.SchedulingGates = nil
	}
	if baseTemplate != nil {
		infoWithoutTols.Annotations = omitInheritedBaseMapEntries(info.Annotations, baseTemplate.Annotations, comp.PodTemplate.Annotations)
		infoWithoutTols.Labels = omitInheritedBaseMapEntries(info.Labels, baseTemplate.Labels, comp.PodTemplate.Labels)
		infoWithoutTols.NodeSelector = omitInheritedBaseMapEntries(info.NodeSelector, baseTemplate.Spec.NodeSelector, comp.PodTemplate.Spec.NodeSelector)
		if gatesMergeByName && len(info.SchedulingGates) > 0 && len(baseGates) > 0 {
			filteredGates := make([]corev1.PodSchedulingGate, 0, len(info.SchedulingGates))
			for _, g := range info.SchedulingGates {
				if !slices.Contains(comp.PodTemplate.Spec.SchedulingGates, g) && slices.Contains(baseGates, g) {
					continue
				}
				filteredGates = append(filteredGates, g)
			}
			infoWithoutTols.SchedulingGates = filteredGates
		}
	}
	if err := podset.Merge(log, &comp.PodTemplate.ObjectMeta, &comp.PodTemplate.Spec, infoWithoutTols); err != nil {
		return err
	}
	if wasNil && podTemplatesEqualWithRawFidelity(comp.PodTemplate, &flinkv1beta1.PodTemplateSpec{}) {
		comp.PodTemplate = nil
	}
	return nil
}

func (j *flinkDeployment) RunWithPodSetsInfo(ctx context.Context, _ client.Client, podSetsInfo []podset.PodSetInfo) error {
	jmInfo, tmInfo, err := resolveRolePodSetsInfo(podSetsInfo)
	if err != nil {
		return err
	}
	if j.Spec.Job == nil {
		return fmt.Errorf("%w: spec.job is required for Application mode FlinkDeployment", podset.ErrInvalidPodsetInfo)
	}
	if j.Spec.JobManager != nil && j.Spec.JobManager.Replicas != nil && (*j.Spec.JobManager.Replicas < 1 || *j.Spec.JobManager.Replicas > math.MaxInt32) {
		return fmt.Errorf("%w: spec.jobManager.replicas must be between 1 and %d, got %d", podset.ErrInvalidPodsetInfo, math.MaxInt32, *j.Spec.JobManager.Replicas)
	}
	if j.Spec.TaskManager != nil && j.Spec.TaskManager.Replicas != nil && (*j.Spec.TaskManager.Replicas < 1 || *j.Spec.TaskManager.Replicas > math.MaxInt32) {
		return fmt.Errorf("%w: spec.taskManager.replicas must be between 1 and %d, got %d", podset.ErrInvalidPodsetInfo, math.MaxInt32, *j.Spec.TaskManager.Replicas)
	}
	if j.Spec.Job.Parallelism != nil && (*j.Spec.Job.Parallelism < 1 || *j.Spec.Job.Parallelism > math.MaxInt32) {
		return fmt.Errorf("%w: spec.job.parallelism must be between 1 and %d, got %d", podset.ErrInvalidPodsetInfo, math.MaxInt32, *j.Spec.Job.Parallelism)
	}
	if jmInfo.Count < 1 || tmInfo.Count < 1 {
		return fmt.Errorf("%w: job-manager and task-manager podSet counts must be greater than or equal to 1, got (%d, %d)", podset.ErrInvalidPodsetInfo, jmInfo.Count, tmInfo.Count)
	}

	flatCfg, err := j.Spec.FlinkConfiguration.FlatMap()
	if err != nil {
		return fmt.Errorf("%w: %w", podset.ErrInvalidPodsetInfo, err)
	}
	mergeArraysByName, err := podTemplateMergeArraysByNameFromConfig(flatCfg)
	if err != nil {
		return fmt.Errorf("%w: %w", podset.ErrInvalidPodsetInfo, err)
	}

	if j.Spec.JobManager == nil {
		j.Spec.JobManager = &flinkv1beta1.JobManagerSpec{}
	}
	if err := applyPodSetInfoToComponent(ctx, j.Spec.PodTemplate, &j.Spec.JobManager.ComponentPodSpec, jmInfo, mergeArraysByName); err != nil {
		return err
	}

	if j.Spec.TaskManager == nil {
		j.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{}
	}
	if err := applyPodSetInfoToComponent(ctx, j.Spec.PodTemplate, &j.Spec.TaskManager.ComponentPodSpec, tmInfo, mergeArraysByName); err != nil {
		return err
	}

	j.Spec.Job.State = flinkv1beta1.JobStateRunning
	return nil
}

func retainMapKeysFromBefore(restored, beforeMap map[string]string) map[string]string {
	if len(restored) == 0 || len(beforeMap) == 0 {
		return nil
	}
	for k := range restored {
		if _, ok := beforeMap[k]; !ok {
			delete(restored, k)
		}
	}
	if len(restored) == 0 {
		return nil
	}
	return restored
}

func trimTrailingInheritedSlice[T any](roleSlice, baseSlice []T) []T {
	for len(roleSlice) > 0 && len(roleSlice) <= len(baseSlice) {
		last := len(roleSlice) - 1
		if !apiequality.Semantic.DeepEqual(roleSlice[last], baseSlice[last]) {
			break
		}
		roleSlice = roleSlice[:last]
	}
	if len(roleSlice) == 0 {
		return nil
	}
	return roleSlice
}

func mergePositionalToleration(base, override corev1.Toleration, rawOverride map[string]any) corev1.Toleration {
	out := *base.DeepCopy()
	if rawOverride != nil {
		if _, ok := rawOverride["key"]; ok {
			out.Key = override.Key
		}
		if _, ok := rawOverride["operator"]; ok {
			out.Operator = override.Operator
		}
		if _, ok := rawOverride["value"]; ok {
			out.Value = override.Value
		}
		if _, ok := rawOverride["effect"]; ok {
			out.Effect = override.Effect
		}
		if _, ok := rawOverride["tolerationSeconds"]; ok {
			if override.TolerationSeconds != nil {
				sec := *override.TolerationSeconds
				out.TolerationSeconds = &sec
			} else {
				out.TolerationSeconds = nil
			}
		}
		return out
	}
	if override.Key != "" {
		out.Key = override.Key
	}
	if override.Operator != "" {
		out.Operator = override.Operator
	}
	if override.Value != "" {
		out.Value = override.Value
	}
	if override.Effect != "" {
		out.Effect = override.Effect
	}
	if override.TolerationSeconds != nil {
		sec := *override.TolerationSeconds
		out.TolerationSeconds = &sec
	}
	return out
}

func beforeTolerationsMatchRestoredPrefix(
	baseTols, beforeTolerations []corev1.Toleration,
	beforeRawTolerations []map[string]any,
	restoredTolerations []corev1.Toleration,
) bool {
	if len(beforeTolerations) > len(restoredTolerations) {
		return false
	}
	for i := range beforeTolerations {
		expected := beforeTolerations[i]
		if i < len(baseTols) {
			var rawTol map[string]any
			if i < len(beforeRawTolerations) {
				rawTol = beforeRawTolerations[i]
			}
			expected = mergePositionalToleration(baseTols[i], beforeTolerations[i], rawTol)
		}
		if !apiequality.Semantic.DeepEqual(expected, restoredTolerations[i]) {
			return false
		}
	}
	return true
}

func pruneInheritedComponentPodTemplate(
	baseTemplate *flinkv1beta1.PodTemplateSpec,
	before *flinkv1beta1.PodTemplateSpec,
	comp *flinkv1beta1.ComponentPodSpec,
	mergeArraysByName bool,
) {
	if comp == nil || comp.PodTemplate == nil {
		return
	}

	var (
		beforeAnnotations    map[string]string
		beforeLabels         map[string]string
		beforeNodeSelector   map[string]string
		beforeTolerations    []corev1.Toleration
		beforeRawTolerations []map[string]any
		beforeGates          []corev1.PodSchedulingGate
		baseTols             []corev1.Toleration
		baseGates            []corev1.PodSchedulingGate
	)
	if before != nil {
		beforeAnnotations = before.Annotations
		beforeLabels = before.Labels
		beforeNodeSelector = before.Spec.NodeSelector
		beforeTolerations = before.Spec.Tolerations
		beforeRawTolerations = before.RawSpecTolerations()
		beforeGates = before.Spec.SchedulingGates
	}
	if baseTemplate != nil {
		baseTols = baseTemplate.Spec.Tolerations
		baseGates = baseTemplate.Spec.SchedulingGates
	}

	comp.PodTemplate.Annotations = retainMapKeysFromBefore(comp.PodTemplate.Annotations, beforeAnnotations)
	comp.PodTemplate.Labels = retainMapKeysFromBefore(comp.PodTemplate.Labels, beforeLabels)
	comp.PodTemplate.Spec.NodeSelector = retainMapKeysFromBefore(comp.PodTemplate.Spec.NodeSelector, beforeNodeSelector)

	if beforeTolerationsMatchRestoredPrefix(baseTols, beforeTolerations, beforeRawTolerations, comp.PodTemplate.Spec.Tolerations) {
		if len(beforeTolerations) == 0 {
			comp.PodTemplate.Spec.Tolerations = nil
		} else {
			comp.PodTemplate.Spec.Tolerations = slices.Clone(beforeTolerations)
		}
	} else {
		for i := 0; i < len(baseTols) && i < len(beforeTolerations) && i < len(comp.PodTemplate.Spec.Tolerations); i++ {
			var rawTol map[string]any
			if i < len(beforeRawTolerations) {
				rawTol = beforeRawTolerations[i]
			}
			mergedTol := mergePositionalToleration(baseTols[i], beforeTolerations[i], rawTol)
			if apiequality.Semantic.DeepEqual(mergedTol, comp.PodTemplate.Spec.Tolerations[i]) {
				comp.PodTemplate.Spec.Tolerations[i] = *beforeTolerations[i].DeepCopy()
			}
		}
		comp.PodTemplate.Spec.Tolerations = trimTrailingInheritedSlice(comp.PodTemplate.Spec.Tolerations, baseTols)
	}

	gatesMergeByName := mergeArraysByName &&
		allSchedulingGatesNamed(baseGates) &&
		allSchedulingGatesNamed(beforeGates) &&
		allSchedulingGatesNamed(comp.PodTemplate.Spec.SchedulingGates)
	filteredGates := slices.DeleteFunc(comp.PodTemplate.Spec.SchedulingGates, func(g corev1.PodSchedulingGate) bool {
		return !slices.Contains(beforeGates, g)
	})
	if !gatesMergeByName && len(beforeGates) > len(baseGates) {
		filteredGates = trimTrailingInheritedSlice(filteredGates, baseGates)
	}
	if len(filteredGates) == 0 {
		comp.PodTemplate.Spec.SchedulingGates = nil
	} else {
		comp.PodTemplate.Spec.SchedulingGates = filteredGates
	}

	if podTemplatesEqualWithRawFidelity(comp.PodTemplate, &flinkv1beta1.PodTemplateSpec{}) {
		comp.PodTemplate = nil
	}
}

func restoreComponentPodSetInfo(
	baseTemplate *flinkv1beta1.PodTemplateSpec,
	comp *flinkv1beta1.ComponentPodSpec,
	info podset.PodSetInfo,
	mergeArraysByName bool,
) bool {
	if comp == nil || baseTemplate.DecodeError() != nil || comp.PodTemplate.DecodeError() != nil {
		return false
	}
	before := comp.PodTemplate.DeepCopy()
	if comp.PodTemplate == nil {
		comp.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
	}
	_ = podset.RestorePodSpec(&comp.PodTemplate.ObjectMeta, &comp.PodTemplate.Spec, info)
	pruneInheritedComponentPodTemplate(baseTemplate, before, comp, mergeArraysByName)
	return !podTemplatesEqualWithRawFidelity(before, comp.PodTemplate)
}

func (j *flinkDeployment) RestorePodSetsInfo(ctx context.Context, podSetsInfo []podset.PodSetInfo) bool {
	jmInfo, tmInfo, err := resolveRolePodSetsInfo(podSetsInfo)
	if err != nil {
		ctrl.LoggerFrom(ctx).V(2).Info(
			"Skipping pod set info restore because the pod set info is invalid",
			"expectedCount", podSetsCount,
			"gotCount", len(podSetsInfo),
			"error", err,
		)
		return false
	}

	var mergeArraysByName bool
	if flatCfg, err := j.Spec.FlinkConfiguration.FlatMap(); err == nil {
		mergeArraysByName, _ = podTemplateMergeArraysByNameFromConfig(flatCfg)
	}

	var changed bool
	if j.Spec.JobManager != nil {
		changed = restoreComponentPodSetInfo(j.Spec.PodTemplate, &j.Spec.JobManager.ComponentPodSpec, jmInfo, mergeArraysByName) || changed
		if apiequality.Semantic.DeepEqual(j.Spec.JobManager, &flinkv1beta1.JobManagerSpec{}) {
			j.Spec.JobManager = nil
			changed = true
		}
	}
	if j.Spec.TaskManager != nil {
		changed = restoreComponentPodSetInfo(j.Spec.PodTemplate, &j.Spec.TaskManager.ComponentPodSpec, tmInfo, mergeArraysByName) || changed
		if apiequality.Semantic.DeepEqual(j.Spec.TaskManager, &flinkv1beta1.TaskManagerSpec{}) {
			j.Spec.TaskManager = nil
			changed = true
		}
	}
	return changed
}

// Finished maps a terminal Flink job state (or an unrecoverable pod-free post-deployment FAILED
// lifecycle state, such as native-mode missing JobManager/HA metadata where status.jobStatus.state
// stays RECONCILING) to a finished Workload only once the Flink Operator reports that the current
// spec is deployed. The operator persists status.jobStatus.state FINISHED (or FAILED when the
// JobManager never started) upon suspension, before every deployment attempt while
// status.reconciliationStatus.state is UPGRADING (which lifecycleState mirrors, ranking UPGRADING
// and ROLLING_BACK above SUSPENDED), and while a never-deployed resource is rejected by operator
// validation (lifecycleState FAILED with reconciliationStatus.state unset or UPGRADING). All of
// those states (as well as post-deployment FAILED lifecycle states where JobManager or standalone
// TaskManager pods remain active) must not release quota prematurely.
func (j *flinkDeployment) Finished(_ context.Context) (message string, success, finished bool) {
	if j.IsSuspended() {
		return "", false, false
	}

	switch j.Status.LifecycleState {
	case flinkv1beta1.LifecycleStateSuspended,
		flinkv1beta1.LifecycleStateUpgrading,
		flinkv1beta1.LifecycleStateRollingBack:
		return "", false, false
	}

	switch j.Status.ReconciliationStatus.State {
	case flinkv1beta1.ReconciliationStateDeployed, flinkv1beta1.ReconciliationStateRolledBack:
	default:
		return "", false, false
	}

	switch j.Status.JobStatus.State {
	case flinkv1beta1.JobStatusStateFinished:
		return "FlinkDeployment job finished successfully", true, true
	case flinkv1beta1.JobStatusStateFailed:
		msg := "FlinkDeployment job failed"
		if j.Status.Error != "" {
			msg = fmt.Sprintf("%s: %s", msg, j.Status.Error)
		}
		return msg, false, true
	case flinkv1beta1.JobStatusStateCanceled:
		return "FlinkDeployment job was canceled", false, true
	}

	if j.Status.LifecycleState == flinkv1beta1.LifecycleStateFailed && !j.IsActive() {
		msg := "FlinkDeployment failed"
		if j.Status.Error != "" {
			msg = fmt.Sprintf("%s: %s", msg, j.Status.Error)
		}
		return msg, false, true
	}

	return "", false, false
}

func (j *flinkDeployment) PodsReady(_ context.Context, _ client.Client, _ ...jobframework.PodsReadyOption) bool {
	return j.Status.LifecycleState == flinkv1beta1.LifecycleStateStable &&
		j.Status.JobManagerDeploymentStatus == flinkv1beta1.JobManagerDeploymentStatusReady &&
		j.Status.JobStatus.State == flinkv1beta1.JobStatusStateRunning
}

// SetupIndexes registers field indexes for Workloads owned by FlinkDeployment resources.
func SetupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	return jobframework.SetupWorkloadOwnerIndex(ctx, indexer, gvk)
}

// GetWorkloadNameForFlinkDeployment returns the deterministic Workload name for a FlinkDeployment owner.
func GetWorkloadNameForFlinkDeployment(jobName string, jobUID types.UID) string {
	return jobframework.GetWorkloadNameForOwnerWithGVK(jobName, jobUID, gvk)
}

func canSupportIntegration(opts ...jobframework.Option) (bool, error) {
	if features.Enabled(features.FlinkDeploymentIntegration) {
		return true, nil
	}
	return false, fmt.Errorf("%s integration is an alpha feature; please enable the %s feature gate", FrameworkName, features.FlinkDeploymentIntegration)
}
