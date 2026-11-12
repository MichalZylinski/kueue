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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metavalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	clientutil "sigs.k8s.io/kueue/pkg/util/client"
	"sigs.k8s.io/kueue/pkg/workload"
)

const (
	FrameworkName                                = "flink.apache.org/flinkdeployment"
	jobManagerPodSetName                         = kueue.PodSetReference("job-manager")
	taskManagerPodSetName                        = kueue.PodSetReference("task-manager")
	flinkMainContainerName                       = "flink-main-container"
	podSetsCount                                 = 2
	flinkConfigNumberOfTaskSlots                 = "taskmanager.numberOfTaskSlots"
	flinkConfigPodTemplateMergeArraysByName      = "kubernetes.operator.pod-template.merge-arrays-by-name"
	flinkConfigAutoscalerEnabled                 = "job.autoscaler.enabled"
	flinkConfigOperatorAutoscalerEnabled         = "kubernetes.operator.job.autoscaler.enabled"
	flinkConfigExternalResources                 = "external-resources"
	flinkConfigExternalResourcePrefix            = "external-resource."
	flinkConfigVertexParallelismOverrides        = "pipeline.jobvertex-parallelism-overrides"
	flinkConfigOperatorJobRestartFailed          = "kubernetes.operator.job.restart.failed"
	flinkConfigOperatorDeploymentRollbackEnabled = "kubernetes.operator.deployment.rollback.enabled"
	// maxFlinkMemoryBytes caps component memory at Integer.MAX_VALUE MiB (2,147,483,647 MiB);
	// Flink's MemorySize.getMebiBytes() (`(int) (bytes >> 20)`) overflows at 2^31 MiB ("2Pi").
	maxFlinkMemoryBytes = int64(math.MaxInt32) << 20
)

var (
	gvk                              = flinkv1beta1.SchemeGroupVersion.WithKind("FlinkDeployment")
	NewReconciler                    = jobframework.NewGenericReconcilerFactory(newJob)
	unsupportedFlinkConfigSizingKeys = sets.New(
		"taskmanager.network.memory.min", "taskmanager.network.memory.max", "taskmanager.cpu.cores", "kubernetes.jobmanager.cpu", "kubernetes.jobmanager.cpu.amount", "kubernetes.taskmanager.cpu",
		"kubernetes.taskmanager.cpu.amount", "kubernetes.jobmanager.replicas", "kubernetes.taskmanager.replicas", "kubernetes.internal.taskmanager.replicas", "kubernetes.jobmanager.cpu.limit-factor",
		"kubernetes.jobmanager.cpu.request-factor", "kubernetes.jobmanager.memory.limit-factor", "kubernetes.jobmanager.memory.request-factor", "kubernetes.taskmanager.cpu.limit-factor",
		"kubernetes.taskmanager.cpu.request-factor", "kubernetes.taskmanager.memory.limit-factor", "kubernetes.taskmanager.memory.request-factor", "slotmanager.redundant-taskmanager-num",
		"slotmanager.number-of-slots.min", "slotmanager.min-total-resource.cpu", "slotmanager.min-total-resource.memory",
	)
	standaloneOnlyParallelismConfigKeys = sets.New(
		flinkConfigVertexParallelismOverrides, "execution.batch.adaptive.auto-parallelism.max-parallelism", "execution.batch.adaptive.auto-parallelism.min-parallelism",
		"execution.batch.adaptive.auto-parallelism.default-source-parallelism", "jobmanager.adaptive-batch-scheduler.max-parallelism", "jobmanager.adaptive-batch-scheduler.min-parallelism",
		"jobmanager.adaptive-batch-scheduler.default-source-parallelism",
	)
	forbiddenFlinkPodPlacementAndMetadataKeys = []string{
		"kubernetes.jobmanager.node-selector", "kubernetes.taskmanager.node-selector", "kubernetes.jobmanager.tolerations", "kubernetes.taskmanager.tolerations", "kubernetes.jobmanager.annotations",
		"kubernetes.taskmanager.annotations", "kubernetes.jobmanager.labels", "kubernetes.taskmanager.labels", "kubernetes.pod-template-file",
	}
	forbiddenFlinkPodIdentityImageAndNetworkKeys = sets.New(
		"kubernetes.service-account", "kubernetes.jobmanager.service-account", "kubernetes.taskmanager.service-account",
		"kubernetes.container.image.ref", "kubernetes.container.image", "kubernetes.container.image.pull-policy", "kubernetes.container.image.pull-secrets", "kubernetes.hostnetwork.enabled",
	)
	supportedPullPolicies = []string{string(corev1.PullAlways), string(corev1.PullNever), string(corev1.PullIfNotPresent)}
	binarySIMultipliers   = map[string]uint64{"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40, "Pi": 1 << 50}
	flinkMemMultipliers   = map[string]uint64{
		"": 1, "b": 1, "bytes": 1, "k": 1 << 10, "kb": 1 << 10, "kibibytes": 1 << 10, "ki": 1 << 10, "kib": 1 << 10, "m": 1 << 20, "mb": 1 << 20, "mebibytes": 1 << 20, "mi": 1 << 20, "mib": 1 << 20,
		"g": 1 << 30, "gb": 1 << 30, "gibibytes": 1 << 30, "gi": 1 << 30, "gib": 1 << 30, "t": 1 << 40, "tb": 1 << 40, "tebibytes": 1 << 40, "ti": 1 << 40, "tib": 1 << 40,
		"pi": 1 << 50, "pib": 1 << 50, "p": 1_000_000_000_000_000, "pb": 1_000_000_000_000_000,
	}
)

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

var (
	_ jobframework.GenericJob        = (*flinkDeployment)(nil)
	_ jobframework.JobWithSkip       = (*flinkDeployment)(nil)
	_ jobframework.JobWithCustomStop = (*flinkDeployment)(nil)
)

func newJob() jobframework.GenericJob { return &flinkDeployment{} }
func fromObject(obj runtime.Object) *flinkDeployment {
	fd, _ := obj.(*flinkv1beta1.FlinkDeployment)
	return (*flinkDeployment)(fd)
}

func (j *flinkDeployment) Object() client.Object        { return (*flinkv1beta1.FlinkDeployment)(j) }
func (j *flinkDeployment) GVK() schema.GroupVersionKind { return gvk }
func (j *flinkDeployment) IsSuspended() bool {
	return j.Spec.Job != nil && j.Spec.Job.State == flinkv1beta1.JobStateSuspended
}
func (j *flinkDeployment) Skip(_ context.Context) bool {
	return j.Name != "" && j.DeletionTimestamp.IsZero() && j.Spec.Job == nil
}
func (j *flinkDeployment) Suspend() {
	if j.Spec.Job != nil {
		j.Spec.Job.State = flinkv1beta1.JobStateSuspended
	}
}

func (j *flinkDeployment) IsActive() bool {
	if slices.Contains([]flinkv1beta1.JobManagerDeploymentStatus{
		flinkv1beta1.JobManagerDeploymentStatusReady, flinkv1beta1.JobManagerDeploymentStatusDeploying,
		flinkv1beta1.JobManagerDeploymentStatusDeployedNotReady, flinkv1beta1.JobManagerDeploymentStatusError,
	}, j.Status.JobManagerDeploymentStatus) {
		return true
	}
	if j.Spec.Mode != flinkv1beta1.KubernetesDeploymentModeStandalone &&
		j.Status.LifecycleState == flinkv1beta1.LifecycleStateFailed &&
		j.Status.JobManagerDeploymentStatus == flinkv1beta1.JobManagerDeploymentStatusMissing &&
		slices.Contains([]flinkv1beta1.ReconciliationState{flinkv1beta1.ReconciliationStateDeployed, flinkv1beta1.ReconciliationStateRolledBack}, j.Status.ReconciliationStatus.State) {
		return false
	}
	if j.Status.TaskManager != nil && j.Status.TaskManager.Replicas > 0 {
		return true
	}
	return slices.Contains([]flinkv1beta1.JobStatusState{
		flinkv1beta1.JobStatusStateInitializing, flinkv1beta1.JobStatusStateCreated, flinkv1beta1.JobStatusStateRunning, flinkv1beta1.JobStatusStateFailing,
		flinkv1beta1.JobStatusStateCancelling, flinkv1beta1.JobStatusStateRestarting, flinkv1beta1.JobStatusStateReconciling,
	}, j.Status.JobStatus.State)
}

func (j *flinkDeployment) Stop(ctx context.Context, c client.Client, podSetsInfo []podset.PodSetInfo, _ jobframework.StopReason, _ string) (bool, error) {
	wasSuspended := j.IsSuspended()
	var stoppedNow bool
	err := clientutil.Patch(ctx, c, j.Object(), func() (bool, error) {
		j.Suspend()
		stoppedNow = !wasSuspended && j.IsSuspended()
		restored := podSetsInfo != nil && j.RestorePodSetsInfo(ctx, podSetsInfo)
		return stoppedNow || restored, nil
	})
	return stoppedNow && err == nil, err
}

func components(spec *flinkv1beta1.FlinkDeploymentSpec) (jm, tm *flinkv1beta1.ComponentPodSpec) {
	if spec.JobManager != nil {
		jm = &spec.JobManager.ComponentPodSpec
	}
	if spec.TaskManager != nil {
		tm = &spec.TaskManager.ComponentPodSpec
	}
	return jm, tm
}

func checkBoundedInt32(name string, v, minVal int64) (int32, error) {
	if v < minVal || v > math.MaxInt32 {
		return 0, fmt.Errorf("%s must be between %d and %d, got %d", name, minVal, math.MaxInt32, v)
	}
	return int32(v), nil
}

// mergePodTemplates mirrors FlinkUtils.mergePodTemplates (positional array merge by default, or by "name"
// when mergeArraysByName is true and every element in both arrays carries a non-empty name).
func mergePodTemplates(base, override *flinkv1beta1.PodTemplateSpec, mergeArraysByName bool) (*corev1.PodTemplateSpec, error) {
	if err := base.DecodeError(); err != nil {
		return nil, fmt.Errorf("decoding base podTemplate: %w", err)
	}
	if err := override.DecodeError(); err != nil {
		return nil, fmt.Errorf("decoding component podTemplate: %w", err)
	}
	if base == nil || override == nil {
		return cmp.Or(base.ToCorePodTemplateSpec(), override.ToCorePodTemplateSpec(), &corev1.PodTemplateSpec{}), nil
	}
	toMap := func(pt *flinkv1beta1.PodTemplateSpec, label string) (map[string]any, error) {
		raw, err := json.Marshal(pt)
		var m map[string]any
		if err == nil {
			m, err = flinkv1beta1.DecodeJSONMapUseNumber(raw)
		}
		if err != nil {
			return nil, fmt.Errorf("decoding %s podTemplate: %w", label, err)
		}
		delete(m, "apiVersion")
		delete(m, "kind")
		delete(m, "status")
		return m, nil
	}
	baseMap, err1 := toMap(base, "base")
	overrideMap, err2 := toMap(override, "component")
	if err := cmp.Or(err1, err2); err != nil {
		return nil, err
	}
	mergeJSONObjects(baseMap, overrideMap, mergeArraysByName)
	mergedBytes, err := json.Marshal(baseMap)
	if err != nil {
		return nil, fmt.Errorf("marshaling merged podTemplate: %w", err)
	}
	merged := &corev1.PodTemplateSpec{}
	return merged, json.Unmarshal(mergedBytes, merged)
}

func mergeJSONObjects(toMap, fromMap map[string]any, mergeArraysByName bool) {
	for k, fromVal := range fromMap {
		toObj, ok1 := toMap[k].(map[string]any)
		fromObj, ok2 := fromVal.(map[string]any)
		toArr, ok3 := toMap[k].([]any)
		fromArr, ok4 := fromVal.([]any)
		switch {
		case ok1 && ok2:
			mergeJSONObjects(toObj, fromObj, mergeArraysByName)
		case ok3 && ok4:
			toMap[k] = mergeJSONArrays(toArr, fromArr, mergeArraysByName)
		default:
			toMap[k] = fromVal
		}
	}
}

func podTemplateMergeArraysByNameFromConfig(flatCfg map[string]string) (bool, error) {
	if val, ok := flatCfg[flinkConfigPodTemplateMergeArraysByName]; ok {
		return parseFlinkBool(val)
	}
	return false, nil
}

func allObjectsHaveNonEmptyName(arr []any) bool {
	return !slices.ContainsFunc(arr, func(elem any) bool {
		obj, _ := elem.(map[string]any)
		name, _ := obj["name"].(string)
		return name == ""
	})
}

func mergeJSONArrays(toArr, fromArr []any, mergeArraysByName bool) []any {
	out := slices.Clone(toArr)
	byName := mergeArraysByName && allObjectsHaveNonEmptyName(toArr) && allObjectsHaveNonEmptyName(fromArr)
	for i, fromElem := range fromArr {
		switch {
		case byName:
			fromObj := fromElem.(map[string]any)
			if idx := slices.IndexFunc(out, func(e any) bool { m, ok := e.(map[string]any); return ok && m["name"] == fromObj["name"] }); idx >= 0 {
				mergeJSONObjects(out[idx].(map[string]any), fromObj, mergeArraysByName)
			} else {
				out = append(out, fromObj)
			}
		case i >= len(out):
			out = append(out, fromElem)
		default:
			toObj, ok1 := out[i].(map[string]any)
			fromObj, ok2 := fromElem.(map[string]any)
			if ok1 && ok2 {
				mergeJSONObjects(toObj, fromObj, mergeArraysByName)
			} else {
				out[i] = fromElem
			}
		}
	}
	return out
}

func (j *flinkDeployment) buildComponentPodTemplateSpec(role kueue.PodSetReference, comp *flinkv1beta1.ComponentPodSpec, mergeByName bool, defaultCPU int64) (*corev1.PodTemplateSpec, error) {
	roleTemplate := ptr.Deref(comp, flinkv1beta1.ComponentPodSpec{}).PodTemplate
	podTemplate, err := mergePodTemplates(j.Spec.PodTemplate, roleTemplate, mergeByName)
	if err != nil {
		return nil, err
	}
	if err := cmp.Or(validateRolePositionalSlices(j.Spec.PodTemplate, roleTemplate, mergeByName), validatePodTemplateContainerNames(&podTemplate.Spec)); err != nil {
		return nil, err
	}

	mainIdx := slices.IndexFunc(podTemplate.Spec.Containers, func(c corev1.Container) bool { return c.Name == flinkMainContainerName })
	if mainIdx < 0 {
		podTemplate.Spec.Containers = append(podTemplate.Spec.Containers, corev1.Container{Name: flinkMainContainerName})
		mainIdx = len(podTemplate.Spec.Containers) - 1
	}
	mainContainer := &podTemplate.Spec.Containers[mainIdx]
	if strings.TrimSpace(j.Spec.Image) != "" {
		mainContainer.Image = j.Spec.Image
	}
	isStandaloneTM := role == taskManagerPodSetName && j.Spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone
	if strings.TrimSpace(string(j.Spec.ImagePullPolicy)) != "" {
		if !isSupportedPullPolicy(j.Spec.ImagePullPolicy) {
			return nil, fmt.Errorf("unsupported spec.imagePullPolicy %q", j.Spec.ImagePullPolicy)
		}
		mainContainer.ImagePullPolicy = j.Spec.ImagePullPolicy
	} else if isStandaloneTM {
		mainContainer.ImagePullPolicy = corev1.PullIfNotPresent
	}
	switch {
	case j.Spec.ServiceAccount != "":
		podTemplate.Spec.ServiceAccountName = j.Spec.ServiceAccount
	case isStandaloneTM:
		podTemplate.Spec.ServiceAccountName = "default"
	case podTemplate.Spec.DeprecatedServiceAccount != "":
		podTemplate.Spec.ServiceAccountName = podTemplate.Spec.DeprecatedServiceAccount
	}
	podTemplate.Spec.DeprecatedServiceAccount = ""

	if err := cmp.Or(validatePodTemplateMetadataAndPlacement(podTemplate), validatePodTolerations(podTemplate.Spec.Tolerations)); err != nil {
		return nil, err
	}
	if err := validateResourceRequirements(podTemplate.Spec.Resources); err != nil {
		return nil, fmt.Errorf("pod resources: %w", err)
	}
	if err := validateCoreResourceList(podTemplate.Spec.Overhead, "overhead resource"); err != nil {
		return nil, fmt.Errorf("pod overhead: %w", err)
	}
	validateContainers := func(kind string, cs []corev1.Container) error {
		for i := range cs {
			if err := validateResolvedContainer(&cs[i]); err != nil {
				return fmt.Errorf("%s %q: %w", kind, cs[i].Name, err)
			}
			applyContainerOpenAPIDefaults(cs[i].Ports, cs[i].Env, cs[i].LivenessProbe, cs[i].ReadinessProbe, cs[i].StartupProbe)
		}
		return nil
	}
	if err := validateContainers("initContainer", podTemplate.Spec.InitContainers); err != nil {
		return nil, err
	}
	if errs := validateAndApplyComponentResources(mainContainer, comp, field.NewPath(string(role))); len(errs) > 0 {
		return nil, errors.New(errs[0].Detail)
	}
	if !hasExplicitComponentCPU(comp) {
		setContainerResourceReqAndLim(mainContainer, corev1.ResourceCPU, *resource.NewQuantity(defaultCPU, resource.DecimalSI))
	}
	if err := validateContainers("container", podTemplate.Spec.Containers); err != nil {
		return nil, err
	}
	for i := range podTemplate.Spec.EphemeralContainers {
		ec := &podTemplate.Spec.EphemeralContainers[i]
		applyContainerOpenAPIDefaults(ec.Ports, ec.Env, ec.LivenessProbe, ec.ReadinessProbe, ec.StartupProbe)
	}
	workload.UseLimitsAsMissingRequestsInPod(&podTemplate.Spec)
	return podTemplate, nil
}

func applyContainerOpenAPIDefaults(ports []corev1.ContainerPort, env []corev1.EnvVar, probes ...*corev1.Probe) {
	for j := range ports {
		if ports[j].Protocol == "" {
			ports[j].Protocol = corev1.ProtocolTCP
		}
	}
	for j := range env {
		if env[j].ValueFrom != nil && env[j].ValueFrom.FileKeyRef != nil && env[j].ValueFrom.FileKeyRef.Optional == nil {
			env[j].ValueFrom.FileKeyRef.Optional = new(false)
		}
	}
	for _, p := range probes {
		if p != nil && p.GRPC != nil && p.GRPC.Service == nil {
			p.GRPC.Service = new("")
		}
	}
}

func setTopologyRequestIfEnabled(ps *kueue.PodSet) (err error) {
	if features.Enabled(features.TopologyAwareScheduling) {
		ps.TopologyRequest, err = jobframework.NewPodSetTopologyRequest(&ps.Template.ObjectMeta).Build()
	}
	return err
}

func (j *flinkDeployment) buildPodSets() ([]kueue.PodSet, error) {
	jmComp, tmComp := components(&j.Spec)
	if j.Spec.Job == nil || !hasExplicitComponentMemory(jmComp) || !hasExplicitComponentMemory(tmComp) {
		return nil, errors.New("spec.job and explicit jobManager and taskManager memory are required")
	}
	if errs := validateFlinkConfiguration(&j.Spec); len(errs) > 0 {
		return nil, errs[0]
	}
	flatCfg, _ := j.Spec.FlinkConfiguration.FlatMap()
	mergeArraysByName, _ := podTemplateMergeArraysByNameFromConfig(flatCfg)
	taskSlots := int32(1)
	if val, ok := flatCfg[flinkConfigNumberOfTaskSlots]; ok {
		taskSlots, _ = parsePositiveInt32(val, flinkConfigNumberOfTaskSlots)
	}
	if j.Spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone && strings.TrimSpace(j.Spec.Image) == "" {
		return nil, errors.New("spec.image must be specified when mode is standalone")
	}
	jmTemplate, err := j.buildComponentPodTemplateSpec(jobManagerPodSetName, jmComp, mergeArraysByName, 1)
	if err != nil {
		return nil, fmt.Errorf("job-manager podSet: %w", err)
	}
	jmCount := int32(1)
	if jmComp != nil && jmComp.Replicas != nil {
		if jmCount, err = checkBoundedInt32("jobManager.replicas", *jmComp.Replicas, 1); err != nil {
			return nil, err
		}
	}
	if j.Spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone && !hasExplicitComponentCPU(tmComp) {
		return nil, errors.New("taskManager cpu must be specified via taskManager.resource.cpu or taskManager.resources when mode is standalone")
	}
	tmTemplate, err := j.buildComponentPodTemplateSpec(taskManagerPodSetName, tmComp, mergeArraysByName, int64(taskSlots))
	if err != nil {
		return nil, fmt.Errorf("task-manager podSet: %w", err)
	}
	var tmCount int32
	if tmComp != nil && tmComp.Replicas != nil {
		if tmCount, err = checkBoundedInt32("taskManager.replicas", *tmComp.Replicas, 0); err != nil {
			return nil, err
		}
	} else {
		if j.Spec.Job.Parallelism == nil {
			return nil, errors.New("job.parallelism must be specified when taskManager.replicas is not set")
		}
		if _, err := checkBoundedInt32("job.parallelism", *j.Spec.Job.Parallelism, 1); err != nil {
			return nil, err
		}
		tmCount = int32((*j.Spec.Job.Parallelism + int64(taskSlots) - 1) / int64(taskSlots))
	}
	return []kueue.PodSet{
		{Name: jobManagerPodSetName, Template: *jmTemplate, Count: jmCount},
		{Name: taskManagerPodSetName, Template: *tmTemplate, Count: tmCount},
	}, nil
}

func (j *flinkDeployment) PodSets(_ context.Context, _ client.Client) ([]kueue.PodSet, error) {
	podSets, err := j.buildPodSets()
	for i := range podSets {
		if err = setTopologyRequestIfEnabled(&podSets[i]); err != nil {
			return nil, err
		}
	}
	return podSets, err
}

func resolveRolePodSetsInfo(podSetsInfo []podset.PodSetInfo) (podset.PodSetInfo, podset.PodSetInfo, error) {
	if len(podSetsInfo) != podSetsCount {
		return podset.PodSetInfo{}, podset.PodSetInfo{}, podset.BadPodSetsInfoLenError(podSetsCount, len(podSetsInfo))
	}
	for _, info := range podSetsInfo {
		if info.Name != jobManagerPodSetName && info.Name != taskManagerPodSetName {
			return podset.PodSetInfo{}, podset.PodSetInfo{}, fmt.Errorf("%w: unknown podSet %q", podset.ErrInvalidPodsetInfo, info.Name)
		}
	}
	if podSetsInfo[0].Name == podSetsInfo[1].Name {
		return podset.PodSetInfo{}, podset.PodSetInfo{}, fmt.Errorf("%w: duplicate podSet %q", podset.ErrInvalidPodsetInfo, podSetsInfo[0].Name)
	}
	if podSetsInfo[0].Name == jobManagerPodSetName {
		return podSetsInfo[0], podSetsInfo[1], nil
	}
	return podSetsInfo[1], podSetsInfo[0], nil
}

func omitInheritedBaseMapEntries(src, baseMap, roleMap map[string]string) map[string]string {
	if len(src) == 0 || len(baseMap) == 0 {
		return src
	}
	out := maps.Clone(src)
	maps.DeleteFunc(out, func(k, v string) bool {
		_, inRole := roleMap[k]
		bv, inBase := baseMap[k]
		return !inRole && inBase && bv == v
	})
	return out
}

func hasUnnamedGate(lists ...[]corev1.PodSchedulingGate) bool {
	return slices.ContainsFunc(lists, func(l []corev1.PodSchedulingGate) bool {
		return slices.ContainsFunc(l, func(g corev1.PodSchedulingGate) bool { return g.Name == "" })
	})
}

func appendAddedPositional[T any](cur, pre, post []T) []T {
	if added := post[len(pre):]; len(added) > 0 {
		if len(cur) == 0 && len(pre) > 0 {
			cur = slices.Clone(pre)
		}
		return append(cur, added...)
	}
	return cur
}

func applyPodSetInfoToComponent(ctx context.Context, baseTemplate *flinkv1beta1.PodTemplateSpec, comp *flinkv1beta1.ComponentPodSpec, info podset.PodSetInfo, mergeByName bool) error {
	merged, err := mergePodTemplates(baseTemplate, comp.PodTemplate, mergeByName)
	if err == nil {
		err = validateRolePositionalSlices(baseTemplate, comp.PodTemplate, mergeByName)
	}
	if err != nil {
		return fmt.Errorf("%w: %w", podset.ErrInvalidPodsetInfo, err)
	}
	preTols, preGates := slices.Clone(merged.Spec.Tolerations), slices.Clone(merged.Spec.SchedulingGates)
	if err := podset.Merge(ctrl.LoggerFrom(ctx), &merged.ObjectMeta, &merged.Spec, info); err != nil {
		return err
	}
	wasNil := comp.PodTemplate == nil
	if wasNil {
		comp.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
	}
	comp.PodTemplate.Spec.Tolerations = appendAddedPositional(comp.PodTemplate.Spec.Tolerations, preTols, merged.Spec.Tolerations)
	base := ptr.Deref(baseTemplate, flinkv1beta1.PodTemplateSpec{})
	gatesByName := mergeByName && !hasUnnamedGate(base.Spec.SchedulingGates, comp.PodTemplate.Spec.SchedulingGates, info.SchedulingGates)
	if !gatesByName {
		comp.PodTemplate.Spec.SchedulingGates = appendAddedPositional(comp.PodTemplate.Spec.SchedulingGates, preGates, merged.Spec.SchedulingGates)
	}

	filtered := info
	filtered.Tolerations = nil
	if !gatesByName {
		filtered.SchedulingGates = nil
	} else if len(info.SchedulingGates) > 0 && len(base.Spec.SchedulingGates) > 0 {
		filtered.SchedulingGates = slices.DeleteFunc(slices.Clone(info.SchedulingGates), func(g corev1.PodSchedulingGate) bool {
			return !slices.Contains(comp.PodTemplate.Spec.SchedulingGates, g) && slices.Contains(base.Spec.SchedulingGates, g)
		})
	}
	filtered.Annotations = omitInheritedBaseMapEntries(info.Annotations, base.Annotations, comp.PodTemplate.Annotations)
	filtered.Labels = omitInheritedBaseMapEntries(info.Labels, base.Labels, comp.PodTemplate.Labels)
	filtered.NodeSelector = omitInheritedBaseMapEntries(info.NodeSelector, base.Spec.NodeSelector, comp.PodTemplate.Spec.NodeSelector)
	if err := podset.Merge(ctrl.LoggerFrom(ctx), &comp.PodTemplate.ObjectMeta, &comp.PodTemplate.Spec, filtered); err != nil {
		return err
	}
	if wasNil && apiequality.Semantic.DeepEqual(comp.PodTemplate, &flinkv1beta1.PodTemplateSpec{}) {
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
	jmComp, tmComp := components(&j.Spec)
	checkOpt := func(name string, val *int64) error {
		if val == nil {
			return nil
		}
		_, err := checkBoundedInt32(name, *val, 1)
		return err
	}
	if err := cmp.Or(
		checkOpt("spec.jobManager.replicas", ptr.Deref(jmComp, flinkv1beta1.ComponentPodSpec{}).Replicas),
		checkOpt("spec.taskManager.replicas", ptr.Deref(tmComp, flinkv1beta1.ComponentPodSpec{}).Replicas),
		checkOpt("spec.job.parallelism", j.Spec.Job.Parallelism),
	); err != nil {
		return fmt.Errorf("%w: %w", podset.ErrInvalidPodsetInfo, err)
	}
	if jmInfo.Count < 1 || tmInfo.Count < 1 {
		return fmt.Errorf("%w: job-manager and task-manager podSet counts must be greater than or equal to 1, got (%d, %d)", podset.ErrInvalidPodsetInfo, jmInfo.Count, tmInfo.Count)
	}
	flatCfg, err := j.Spec.FlinkConfiguration.FlatMap()
	var mergeByName bool
	if err == nil {
		mergeByName, err = podTemplateMergeArraysByNameFromConfig(flatCfg)
	}
	if err != nil {
		return fmt.Errorf("%w: %w", podset.ErrInvalidPodsetInfo, err)
	}
	if j.Spec.JobManager == nil {
		j.Spec.JobManager = &flinkv1beta1.JobManagerSpec{}
	}
	if j.Spec.TaskManager == nil {
		j.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{}
	}
	if err := cmp.Or(
		applyPodSetInfoToComponent(ctx, j.Spec.PodTemplate, &j.Spec.JobManager.ComponentPodSpec, jmInfo, mergeByName),
		applyPodSetInfoToComponent(ctx, j.Spec.PodTemplate, &j.Spec.TaskManager.ComponentPodSpec, tmInfo, mergeByName),
	); err != nil {
		return err
	}
	j.Spec.Job.State = flinkv1beta1.JobStateRunning
	return nil
}

func retainMapKeysFromBefore(restored, beforeMap map[string]string) map[string]string {
	if maps.DeleteFunc(restored, func(k, _ string) bool { _, ok := beforeMap[k]; return !ok }); len(restored) == 0 {
		return nil
	}
	return restored
}

func restoreRoleSlice[T any](s, beforeSlice, baseSlice []T, positional bool) []T {
	eq := func(a, b T) bool { return apiequality.Semantic.DeepEqual(a, b) }
	hasPrefix := func(x, p []T) bool { return len(x) >= len(p) && slices.EqualFunc(x[:len(p)], p, eq) }
	if len(beforeSlice) == 0 {
		return nil
	}
	if positional && len(baseSlice) > 0 && !hasPrefix(s, baseSlice) && hasPrefix(beforeSlice, baseSlice) {
		s = slices.DeleteFunc(s, func(v T) bool {
			return !slices.ContainsFunc(beforeSlice[len(baseSlice):], func(bv T) bool { return eq(v, bv) })
		})
		if len(s) > 0 || len(beforeSlice) == len(baseSlice) {
			s = append(slices.Clone(baseSlice), s...)
		}
	}
	if len(s) == 0 || (positional && !slices.EqualFunc(beforeSlice, baseSlice, eq) && hasPrefix(baseSlice, s)) {
		return nil
	}
	return s
}

func restoreComponentPodSetInfo(baseTemplate *flinkv1beta1.PodTemplateSpec, comp *flinkv1beta1.ComponentPodSpec, info podset.PodSetInfo, mergeByName bool) bool {
	if comp == nil || baseTemplate.DecodeError() != nil || comp.PodTemplate.DecodeError() != nil {
		return false
	}
	before := comp.PodTemplate.DeepCopy()
	comp.PodTemplate = cmp.Or(comp.PodTemplate, &flinkv1beta1.PodTemplateSpec{})
	_ = podset.RestorePodSpec(&comp.PodTemplate.ObjectMeta, &comp.PodTemplate.Spec, info)
	b, base := ptr.Deref(before, flinkv1beta1.PodTemplateSpec{}), ptr.Deref(baseTemplate, flinkv1beta1.PodTemplateSpec{})
	comp.PodTemplate.Annotations, comp.PodTemplate.Labels = retainMapKeysFromBefore(comp.PodTemplate.Annotations, b.Annotations), retainMapKeysFromBefore(comp.PodTemplate.Labels, b.Labels)
	comp.PodTemplate.Spec.NodeSelector = retainMapKeysFromBefore(comp.PodTemplate.Spec.NodeSelector, b.Spec.NodeSelector)
	comp.PodTemplate.Spec.Tolerations = restoreRoleSlice(comp.PodTemplate.Spec.Tolerations, b.Spec.Tolerations, base.Spec.Tolerations, true)
	gatesByName := mergeByName && !hasUnnamedGate(base.Spec.SchedulingGates, b.Spec.SchedulingGates, comp.PodTemplate.Spec.SchedulingGates)
	gates := slices.DeleteFunc(comp.PodTemplate.Spec.SchedulingGates, func(g corev1.PodSchedulingGate) bool { return !slices.Contains(b.Spec.SchedulingGates, g) })
	comp.PodTemplate.Spec.SchedulingGates = restoreRoleSlice(gates, b.Spec.SchedulingGates, base.Spec.SchedulingGates, !gatesByName)
	if raw, err := json.Marshal(comp.PodTemplate); err == nil && string(raw) == "{}" && apiequality.Semantic.DeepEqual(comp.PodTemplate, &flinkv1beta1.PodTemplateSpec{}) {
		comp.PodTemplate = nil
	}
	return !apiequality.Semantic.DeepEqual(before, comp.PodTemplate)
}

func (j *flinkDeployment) RestorePodSetsInfo(ctx context.Context, podSetsInfo []podset.PodSetInfo) bool {
	jmInfo, tmInfo, err := resolveRolePodSetsInfo(podSetsInfo)
	if err != nil {
		ctrl.LoggerFrom(ctx).V(2).Info("Skipping pod set info restore because the pod set info is invalid", "error", err)
		return false
	}
	flatCfg, _ := j.Spec.FlinkConfiguration.FlatMap()
	mergeArraysByName, _ := podTemplateMergeArraysByNameFromConfig(flatCfg)
	var changed bool
	if j.Spec.JobManager != nil {
		changed = restoreComponentPodSetInfo(j.Spec.PodTemplate, &j.Spec.JobManager.ComponentPodSpec, jmInfo, mergeArraysByName)
		if apiequality.Semantic.DeepEqual(j.Spec.JobManager, &flinkv1beta1.JobManagerSpec{}) {
			j.Spec.JobManager, changed = nil, true
		}
	}
	if j.Spec.TaskManager != nil {
		changed = restoreComponentPodSetInfo(j.Spec.PodTemplate, &j.Spec.TaskManager.ComponentPodSpec, tmInfo, mergeArraysByName) || changed
		if apiequality.Semantic.DeepEqual(j.Spec.TaskManager, &flinkv1beta1.TaskManagerSpec{}) {
			j.Spec.TaskManager, changed = nil, true
		}
	}
	return changed
}

func (j *flinkDeployment) Finished(_ context.Context) (message string, success, finished bool) {
	if j.IsSuspended() ||
		slices.Contains([]flinkv1beta1.LifecycleState{flinkv1beta1.LifecycleStateSuspended, flinkv1beta1.LifecycleStateUpgrading, flinkv1beta1.LifecycleStateRollingBack}, j.Status.LifecycleState) ||
		(j.Status.ReconciliationStatus.State != flinkv1beta1.ReconciliationStateDeployed && j.Status.ReconciliationStatus.State != flinkv1beta1.ReconciliationStateRolledBack) {
		return "", false, false
	}
	withErr := func(prefix string) string {
		if j.Status.Error != "" {
			return fmt.Sprintf("%s: %s", prefix, j.Status.Error)
		}
		return prefix
	}
	switch {
	case j.Status.JobStatus.State == flinkv1beta1.JobStatusStateFinished:
		return "FlinkDeployment job finished successfully", true, true
	case j.Status.JobStatus.State == flinkv1beta1.JobStatusStateFailed:
		return withErr("FlinkDeployment job failed"), false, true
	case j.Status.JobStatus.State == flinkv1beta1.JobStatusStateCanceled:
		return "FlinkDeployment job was canceled", false, true
	case j.Status.LifecycleState == flinkv1beta1.LifecycleStateFailed && !j.IsActive():
		return withErr("FlinkDeployment failed"), false, true
	default:
		return "", false, false
	}
}

func (j *flinkDeployment) PodsReady(_ context.Context, _ client.Client, _ ...jobframework.PodsReadyOption) bool {
	return j.Status.LifecycleState == flinkv1beta1.LifecycleStateStable &&
		j.Status.JobManagerDeploymentStatus == flinkv1beta1.JobManagerDeploymentStatusReady &&
		j.Status.JobStatus.State == flinkv1beta1.JobStatusStateRunning
}

func SetupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	return jobframework.SetupWorkloadOwnerIndex(ctx, indexer, gvk)
}

func GetWorkloadNameForFlinkDeployment(jobName string, jobUID types.UID) string {
	return jobframework.GetWorkloadNameForOwnerWithGVK(jobName, jobUID, gvk)
}

func canSupportIntegration(_ ...jobframework.Option) (bool, error) {
	if features.Enabled(features.FlinkDeploymentIntegration) {
		return true, nil
	}
	return false, fmt.Errorf("%s integration is an alpha feature; please enable the %s feature gate", FrameworkName, features.FlinkDeploymentIntegration)
}

func isForbiddenFlinkMemorySizingKey(key string) bool {
	return (strings.HasPrefix(key, "jobmanager.memory.") || strings.HasPrefix(key, "taskmanager.memory.")) &&
		(strings.HasSuffix(key, ".size") || strings.HasSuffix(key, ".min") || strings.HasSuffix(key, ".max"))
}

func isStandaloneOnlyParallelismConfigKey(key string) bool {
	return standaloneOnlyParallelismConfigKeys.Has(key) || strings.HasPrefix(key, flinkConfigVertexParallelismOverrides+".")
}

func isForbiddenFlinkPodPlacementOrMetadataKey(key string) bool {
	return slices.ContainsFunc(forbiddenFlinkPodPlacementAndMetadataKeys, func(p string) bool { return key == p || strings.HasPrefix(key, p+".") })
}

func parseBoundedMemoryValue(digits, raw string, multiplier uint64) (int64, error) {
	val, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || val > uint64(maxFlinkMemoryBytes)/multiplier {
		return 0, fmt.Errorf("memory size %q exceeds maximum supported Flink memory size (%d MiB)", raw, math.MaxInt32)
	}
	if val == 0 {
		return 0, fmt.Errorf("memory size %q must be greater than zero", raw)
	}
	return int64(val * multiplier), nil
}

// parseFlinkMemoryQuantity mirrors Flink Operator's ResourceConfigUtils.parseResourceMemoryString
// on spec.<role>.resource.memory (Flink MemorySize 1024-based k/m/g/t + plain integer bytes, plus integer K8s suffixes).
func parseFlinkMemoryQuantity(raw string) (resource.Quantity, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return resource.Quantity{}, errors.New("memory size cannot be empty")
	}
	if strings.Contains(trimmed, ".") {
		return resource.Quantity{}, fmt.Errorf("memory size %q must not contain a decimal point; use an integer quantity such as \"1536Mi\" or \"512Mi\"", raw)
	}
	if len(trimmed) > 1 && trimmed[0] == '-' && trimmed[1] >= '0' && trimmed[1] <= '9' {
		return resource.Quantity{}, fmt.Errorf("memory size %q must be greater than zero", raw)
	}
	digitEnd := strings.IndexFunc(trimmed, func(r rune) bool { return r < '0' || r > '9' })
	if digitEnd < 0 {
		digitEnd = len(trimmed)
	}
	unit := strings.ToLower(strings.TrimSpace(trimmed[digitEnd:]))
	multiplier, ok := flinkMemMultipliers[unit]
	if digitEnd == 0 || !ok {
		return resource.Quantity{}, fmt.Errorf("invalid memory quantity %q: must use Flink MemorySize units or an integer Kubernetes suffix (Ki, Mi, Gi, Ti, Pi, K, M, G, T, P)", raw)
	}
	totalBytes, err := parseBoundedMemoryValue(trimmed[:digitEnd], raw, multiplier)
	if err != nil {
		return resource.Quantity{}, err
	}
	format := resource.DecimalSI
	if unit != "p" && unit != "pb" && totalBytes%(1<<10) == 0 {
		format = resource.BinarySI
	}
	return *resource.NewQuantity(totalBytes, format), nil
}

func parseFlinkCPUQuantity(cpu float64) (resource.Quantity, error) {
	switch {
	case math.IsNaN(cpu) || math.IsInf(cpu, 0):
		return resource.Quantity{}, fmt.Errorf("cpu value %v must be a finite number", cpu)
	case cpu <= 0:
		return resource.Quantity{}, fmt.Errorf("cpu value %v must be greater than 0", cpu)
	case cpu >= float64(math.MaxInt64/1000):
		return resource.Quantity{}, fmt.Errorf("cpu value %v exceeds maximum supported cores", cpu)
	}
	qty, err := resource.ParseQuantity(strconv.FormatFloat(cpu, 'f', -1, 64))
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid cpu value %v: %w", cpu, err)
	}
	return *resource.NewMilliQuantity(max(qty.MilliValue(), 1), resource.DecimalSI), nil
}

func parseEphemeralStorageQuantity(raw string) (resource.Quantity, error) {
	if strings.ContainsFunc(raw, unicode.IsSpace) {
		return resource.Quantity{}, fmt.Errorf("ephemeralStorage %q must not contain whitespace", raw)
	}
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid ephemeralStorage %q: %w", raw, err)
	} else if q.Sign() <= 0 {
		return resource.Quantity{}, fmt.Errorf("ephemeralStorage %q must be greater than zero", raw)
	}
	return q, nil
}

func parseFlinkBool(raw string) (bool, error) {
	if strings.EqualFold(raw, "true") || strings.EqualFold(raw, "false") {
		return strings.EqualFold(raw, "true"), nil
	}
	return false, fmt.Errorf("invalid boolean value %q: must be \"true\" or \"false\"", raw)
}

func parsePositiveInt32(raw, name string) (int32, error) {
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", name, raw, err)
	}
	if n < 1 {
		return 0, fmt.Errorf("%s must be greater than or equal to 1, got %d", name, n)
	}
	return int32(n), nil
}

func validateCoreResourceList(list corev1.ResourceList, label string) error {
	for _, resName := range slices.Sorted(maps.Keys(list)) {
		if resName == corev1.ResourcePods {
			return fmt.Errorf("resource %q is reserved for internal Kueue use", corev1.ResourcePods)
		}
		if qty := list[resName]; qty.Sign() < 0 {
			return fmt.Errorf("%s %s (%s) must be non-negative", label, resName, qty.String())
		}
	}
	return nil
}

func validateResourceRequirements(reqs *corev1.ResourceRequirements) error {
	if reqs == nil {
		return nil
	}
	if err := errors.Join(validateCoreResourceList(reqs.Requests, "resource request"), validateCoreResourceList(reqs.Limits, "resource limit")); err != nil {
		return err
	}
	for _, resName := range slices.Sorted(maps.Keys(reqs.Limits)) {
		limitQty := reqs.Limits[resName]
		if reqQty, hasReq := reqs.Requests[resName]; hasReq && reqQty.Cmp(limitQty) > 0 {
			return fmt.Errorf("resource request %s (%s) must be less than or equal to limit (%s)", resName, reqQty.String(), limitQty.String())
		}
	}
	return nil
}

// validateComponentMemoryQuantity requires spec.<role>.resources memory to use a positive integer
// BinarySI suffix (Ki, Mi, Gi, Ti, Pi, up to 2147483647Mi) without "." or whitespace.
func validateComponentMemoryQuantity(memQty flinkv1beta1.Quantity) error {
	raw := memQty.RawLiteral()
	switch {
	case raw == "":
		return errors.New("memory size cannot be empty")
	case strings.ContainsFunc(raw, unicode.IsSpace):
		return fmt.Errorf("memory size %q must not contain whitespace", raw)
	case strings.Contains(raw, "."):
		return fmt.Errorf("memory size %q must not contain a decimal point; must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi)", raw)
	}
	digitEnd := strings.IndexFunc(raw, func(r rune) bool { return r < '0' || r > '9' })
	if digitEnd == 0 {
		return fmt.Errorf("must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi), got %q", raw)
	}
	if digitEnd < 0 || binarySIMultipliers[raw[digitEnd:]] == 0 {
		return fmt.Errorf("must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) because Flink interprets %q incompatibly or rejects it", raw)
	}
	_, err := parseBoundedMemoryValue(raw[:digitEnd], raw, binarySIMultipliers[raw[digitEnd:]])
	return err
}

func validateComponentResourceRequirements(reqs *flinkv1beta1.ResourceRequirements) error {
	if reqs == nil {
		return nil
	}
	for i, list := range []flinkv1beta1.ResourceList{reqs.Requests, reqs.Limits} {
		kind := [2]string{"request", "limit"}[i]
		for _, resName := range slices.Sorted(maps.Keys(list)) {
			qty, raw := list[resName], list[resName].RawLiteral()
			switch {
			case resName == corev1.ResourcePods:
				return fmt.Errorf("resource %q is reserved for internal Kueue use", corev1.ResourcePods)
			case strings.ContainsFunc(raw, unicode.IsSpace):
				return fmt.Errorf("resource %s %s (%q) must not contain whitespace", kind, resName, raw)
			case resName == corev1.ResourceMemory:
				if err := validateComponentMemoryQuantity(qty); err != nil {
					return fmt.Errorf("resource %s memory (%s): %w", kind, raw, err)
				}
			}
			if err := qty.ParseError(); err != nil {
				return fmt.Errorf("invalid resource %s %s (%s): %w", kind, resName, raw, err)
			}
			if q := qty.AsQuantity(); resName == corev1.ResourceCPU && q.Sign() <= 0 {
				return fmt.Errorf("resource %s cpu (%s) must be greater than 0", kind, raw)
			}
		}
	}
	return validateResourceRequirements(reqs.ToCore())
}

func isSupportedPullPolicy(policy corev1.PullPolicy) bool {
	return policy == corev1.PullAlways || policy == corev1.PullNever || policy == corev1.PullIfNotPresent
}

func validateResolvedContainer(c *corev1.Container) error {
	if strings.TrimSpace(c.Image) == "" {
		return errors.New("image must be specified")
	}
	if c.ImagePullPolicy != "" && !isSupportedPullPolicy(c.ImagePullPolicy) {
		return fmt.Errorf("unsupported imagePullPolicy %q", c.ImagePullPolicy)
	}
	return validateResourceRequirements(&c.Resources)
}

func validatePodTemplateContainerNames(podSpec *corev1.PodSpec) error {
	check := func(kind string, count int, nameAt func(int) string) error {
		seen := sets.New[string]()
		for i := range count {
			name := nameAt(i)
			if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
				return fmt.Errorf("%s[%d].name %q: %s", kind, i, name, strings.Join(errs, "; "))
			} else if seen.Has(name) {
				return fmt.Errorf("duplicate %s name %q", kind, name)
			}
			seen.Insert(name)
		}
		return nil
	}
	return errors.Join(
		check("initContainers", len(podSpec.InitContainers), func(i int) string { return podSpec.InitContainers[i].Name }),
		check("containers", len(podSpec.Containers), func(i int) string { return podSpec.Containers[i].Name }),
		check("ephemeralContainers", len(podSpec.EphemeralContainers), func(i int) string { return podSpec.EphemeralContainers[i].Name }),
	)
}

func validatePodTemplateMetadataAndPlacement(podTemplate *corev1.PodTemplateSpec) error {
	wrap := func(prefix string, errs field.ErrorList) error {
		if len(errs) == 0 {
			return nil
		}
		return fmt.Errorf("%s: %w", prefix, errs.ToAggregate())
	}
	if features.Enabled(features.WorkloadValidationForPodSetMetadata) {
		if err := cmp.Or(
			wrap("pod labels", metavalidation.ValidateLabels(podTemplate.Labels, labelsPath)),
			wrap("pod annotations", apivalidation.ValidateAnnotations(podTemplate.Annotations, field.NewPath("metadata", "annotations"))),
		); err != nil {
			return err
		}
	}
	return wrap("pod nodeSelector", metavalidation.ValidateLabels(podTemplate.Spec.NodeSelector, field.NewPath("spec", "nodeSelector")))
}

func validateRolePositionalSlices(baseTemplate, roleTemplate *flinkv1beta1.PodTemplateSpec, mergeByName bool) error {
	if baseTemplate == nil || roleTemplate == nil {
		return nil
	}
	baseTols, roleTols := baseTemplate.Spec.Tolerations, roleTemplate.Spec.Tolerations
	if len(baseTols) > 0 && len(roleTols) > 0 && (len(roleTols) < len(baseTols) || !apiequality.Semantic.DeepEqual(roleTols[:len(baseTols)], baseTols)) {
		return errors.New("component podTemplate.spec.tolerations must include spec.podTemplate.spec.tolerations as a prefix when both are specified")
	}
	baseGates, roleGates := baseTemplate.Spec.SchedulingGates, roleTemplate.Spec.SchedulingGates
	if (!mergeByName || hasUnnamedGate(baseGates, roleGates)) && len(baseGates) > 0 && len(roleGates) > 0 &&
		(len(roleGates) < len(baseGates) || !slices.Equal(roleGates[:len(baseGates)], baseGates)) {
		return errors.New("component podTemplate.spec.schedulingGates must include spec.podTemplate.spec.schedulingGates as a prefix when positional array merging is used")
	}
	return nil
}

func validatePodTolerations(tolerations []corev1.Toleration) error {
	for i, t := range tolerations {
		if t.Key != "" && len(metavalidation.ValidateLabelName(t.Key, field.NewPath("tolerations").Index(i).Child("key"))) > 0 {
			return fmt.Errorf("toleration[%d]: invalid key %q", i, t.Key)
		}
		switch t.Operator {
		case "", corev1.TolerationOpEqual:
			if t.Key == "" || (t.Value != "" && len(validation.IsValidLabelValue(t.Value)) > 0) {
				return fmt.Errorf("toleration[%d]: invalid Equal toleration", i)
			}
		case corev1.TolerationOpExists:
			if t.Value != "" {
				return fmt.Errorf("toleration[%d]: value must be empty when operator is Exists", i)
			}
		default:
			return fmt.Errorf("toleration[%d]: unsupported operator %q", i, t.Operator)
		}
		if !slices.Contains([]corev1.TaintEffect{"", corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute}, t.Effect) {
			return fmt.Errorf("toleration[%d]: unsupported effect %q", i, t.Effect)
		}
		if t.TolerationSeconds != nil && t.Effect != corev1.TaintEffectNoExecute {
			return fmt.Errorf("toleration[%d]: effect must be NoExecute when tolerationSeconds is set", i)
		}
	}
	return nil
}

func hasResourceInRequirements(reqs *flinkv1beta1.ResourceRequirements, name corev1.ResourceName) bool {
	if reqs == nil {
		return false
	}
	_, inReq := reqs.Requests[name]
	_, inLim := reqs.Limits[name]
	return inReq || inLim
}

func hasExplicitComponentMemory(comp *flinkv1beta1.ComponentPodSpec) bool {
	return comp != nil && ((comp.Resource != nil && strings.TrimSpace(comp.Resource.Memory) != "") || hasResourceInRequirements(comp.Resources, corev1.ResourceMemory))
}

func hasExplicitComponentCPU(comp *flinkv1beta1.ComponentPodSpec) bool {
	return comp != nil && ((comp.Resource != nil && comp.Resource.CPU != nil) || hasResourceInRequirements(comp.Resources, corev1.ResourceCPU))
}

func setResourceEntries(target *corev1.ResourceList, entries corev1.ResourceList) {
	if len(entries) > 0 && *target == nil {
		*target = make(corev1.ResourceList, len(entries))
	}
	for k, v := range entries {
		(*target)[k] = v.DeepCopy()
	}
}

func setContainerResourceReqAndLim(container *corev1.Container, name corev1.ResourceName, qty resource.Quantity) {
	entry := corev1.ResourceList{name: qty}
	setResourceEntries(&container.Resources.Requests, entry)
	setResourceEntries(&container.Resources.Limits, entry)
}

// validateAndApplyComponentResources validates component resource/resources fields and applies them onto container.
func validateAndApplyComponentResources(container *corev1.Container, comp *flinkv1beta1.ComponentPodSpec, fldPath *field.Path) field.ErrorList {
	if comp == nil {
		return nil
	}
	var allErrs field.ErrorList
	if comp.Resource != nil && comp.Resources != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("resources"), "may not be specified when resource is also specified"))
	}
	if comp.Resources != nil {
		if len(comp.Resources.Claims) > 0 {
			allErrs = append(allErrs, field.Forbidden(
				fldPath.Child("resources", "claims"),
				"Flink Kubernetes Operator ignores claims in component resources; configure container resources.claims and pod spec.resourceClaims in spec.podTemplate or component podTemplate instead",
			))
		}
		if err := validateComponentResourceRequirements(comp.Resources); err != nil {
			return append(allErrs, field.Invalid(fldPath.Child("resources"), comp.Resources, err.Error()))
		}
		coreReqs := comp.Resources.ToCore()
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
			reqQty, hasReq := coreReqs.Requests[name]
			limQty, hasLim := coreReqs.Limits[name]
			if hasReq && !hasLim {
				setResourceEntries(&coreReqs.Limits, corev1.ResourceList{name: reqQty})
			} else if hasLim && !hasReq {
				setResourceEntries(&coreReqs.Requests, corev1.ResourceList{name: limQty})
			}
		}
		setResourceEntries(&container.Resources.Requests, coreReqs.Requests)
		setResourceEntries(&container.Resources.Limits, coreReqs.Limits)
		return allErrs
	}
	if comp.Resource == nil {
		return allErrs
	}
	applyRes := func(sub string, val any, name corev1.ResourceName, qty resource.Quantity, err error) {
		if err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("resource", sub), val, err.Error()))
		} else {
			setContainerResourceReqAndLim(container, name, qty)
		}
	}
	if comp.Resource.CPU != nil {
		qty, err := parseFlinkCPUQuantity(*comp.Resource.CPU)
		applyRes("cpu", *comp.Resource.CPU, corev1.ResourceCPU, qty, err)
	}
	if strings.TrimSpace(comp.Resource.Memory) != "" {
		qty, err := parseFlinkMemoryQuantity(comp.Resource.Memory)
		applyRes("memory", comp.Resource.Memory, corev1.ResourceMemory, qty, err)
	}
	if comp.Resource.EphemeralStorage != "" {
		qty, err := parseEphemeralStorageQuantity(comp.Resource.EphemeralStorage)
		applyRes("ephemeralStorage", comp.Resource.EphemeralStorage, corev1.ResourceEphemeralStorage, qty, err)
	}
	return allErrs
}
