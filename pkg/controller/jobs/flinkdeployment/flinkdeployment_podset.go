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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/workload"
)

const (
	jobManagerPodSetName   = kueue.PodSetReference("job-manager")
	taskManagerPodSetName  = kueue.PodSetReference("task-manager")
	flinkMainContainerName = "flink-main-container"

	podSetsCount = 2
)

func (j *flinkDeployment) numJobManagers() (int32, error) {
	if j.Spec.JobManager != nil && j.Spec.JobManager.Replicas != nil {
		replicas := *j.Spec.JobManager.Replicas
		if replicas < 1 {
			return 0, fmt.Errorf("jobManager.replicas must be greater than or equal to 1, got %d", replicas)
		}
		if replicas > math.MaxInt32 {
			return 0, fmt.Errorf("jobManager.replicas must be less than or equal to %d, got %d", math.MaxInt32, replicas)
		}
		return int32(replicas), nil
	}
	return 1, nil
}

func taskSlotsFromConfig(flatCfg map[string]string) (int32, error) {
	if val, ok := flatCfg[flinkConfigNumberOfTaskSlots]; ok {
		return parsePositiveInt32(val, flinkConfigNumberOfTaskSlots)
	}
	return 1, nil
}

func podTemplateMergeArraysByNameFromConfig(flatCfg map[string]string) (bool, error) {
	val, ok := flatCfg[flinkConfigPodTemplateMergeArraysByName]
	if !ok {
		return false, nil
	}
	parsed, err := parseFlinkBool(val)
	if err != nil {
		return false, fmt.Errorf("invalid %s %q: %w", flinkConfigPodTemplateMergeArraysByName, val, err)
	}
	return parsed, nil
}

func (j *flinkDeployment) numTaskManagers(taskSlots int32) (int32, error) {
	if j.Spec.TaskManager != nil && j.Spec.TaskManager.Replicas != nil {
		replicas := *j.Spec.TaskManager.Replicas
		if replicas < 0 {
			return 0, fmt.Errorf("taskManager.replicas must be greater than or equal to 0, got %d", replicas)
		}
		if replicas > math.MaxInt32 {
			return 0, fmt.Errorf("taskManager.replicas must be less than or equal to %d, got %d", math.MaxInt32, replicas)
		}
		return int32(replicas), nil
	}

	if j.Spec.Job == nil || j.Spec.Job.Parallelism == nil {
		return 0, errors.New("job.parallelism must be specified when taskManager.replicas is not set")
	}
	parallelism := *j.Spec.Job.Parallelism
	if parallelism < 1 {
		return 0, fmt.Errorf("job.parallelism must be greater than or equal to 1, got %d", parallelism)
	}
	if parallelism > math.MaxInt32 {
		return 0, fmt.Errorf("job.parallelism must be less than or equal to %d, got %d", math.MaxInt32, parallelism)
	}

	// Equivalent to ceil(parallelism / taskSlots) using 64-bit arithmetic to avoid int32 overflow.
	replicas := (parallelism + int64(taskSlots) - 1) / int64(taskSlots)
	return int32(replicas), nil
}

// mergePodTemplates merges a role-specific PodTemplateSpec on top of the shared root
// spec.podTemplate, matching Apache Flink Kubernetes Operator's FlinkUtils.mergePodTemplates:
//   - When mergeArraysByName is false (default), all JSON arrays merge positionally index-by-index.
//   - When mergeArraysByName is true, arrays where every element in both base and override is an
//     object carrying a non-empty "name" field (such as named containers, initContainers, volumes,
//     and schedulingGates) merge by "name", while arrays containing any unnamed element (such as
//     tolerations or positional container overrides omitting name) merge positionally index-by-index.
func mergePodTemplates(base, override *flinkv1beta1.PodTemplateSpec, mergeArraysByName bool) (*corev1.PodTemplateSpec, error) {
	if err := base.DecodeError(); err != nil {
		return nil, fmt.Errorf("decoding base podTemplate: %w", err)
	}
	if err := override.DecodeError(); err != nil {
		return nil, fmt.Errorf("decoding component podTemplate: %w", err)
	}
	switch {
	case base == nil && override == nil:
		return &corev1.PodTemplateSpec{}, nil
	case base == nil:
		return override.ToCorePodTemplateSpec(), nil
	case override == nil:
		return base.ToCorePodTemplateSpec(), nil
	}

	baseMap, err := podTemplateToJSONMap(base)
	if err != nil {
		return nil, fmt.Errorf("decoding base podTemplate: %w", err)
	}
	overrideMap, err := podTemplateToJSONMap(override)
	if err != nil {
		return nil, fmt.Errorf("decoding component podTemplate: %w", err)
	}

	mergeJSONObjects(baseMap, overrideMap, mergeArraysByName)

	mergedBytes, err := json.Marshal(baseMap)
	if err != nil {
		return nil, fmt.Errorf("marshaling merged podTemplate: %w", err)
	}

	merged := &corev1.PodTemplateSpec{}
	if err := json.Unmarshal(mergedBytes, merged); err != nil {
		return nil, fmt.Errorf("unmarshaling merged podTemplate: %w", err)
	}

	return merged, nil
}

// podTemplateToJSONMap marshals a PodTemplateSpec (dropping apiVersion, kind, and status,
// which Flink's merge ignores) through PodTemplateSpec.MarshalJSON so raw JSON bytes are
// preserved when unmutated and non-nullable null entries are stripped otherwise, decodes it
// with UseNumber so 64-bit integers survive the round trip, and prunes Go zero-valued struct
// fields under spec when spec is not backed by unmodified raw JSON to match Fabric8's
// @JsonInclude(NON_EMPTY) ObjectNode serialization in FlinkUtils.mergePodTemplates.
func podTemplateToJSONMap(pt *flinkv1beta1.PodTemplateSpec) (map[string]any, error) {
	raw, err := json.Marshal(pt)
	if err != nil {
		return nil, err
	}
	ptMap, err := flinkv1beta1.DecodeJSONMapUseNumber(raw)
	if err != nil {
		return nil, err
	}
	delete(ptMap, "apiVersion")
	delete(ptMap, "kind")
	delete(ptMap, "status")
	if !pt.HasUnmodifiedRawSpec() {
		if specMap, ok := ptMap["spec"].(map[string]any); ok {
			flinkv1beta1.PruneEmptyPodSpecFields(specMap)
			if len(specMap) == 0 {
				delete(ptMap, "spec")
			}
		}
	}
	return ptMap, nil
}

func mergeJSONObjects(toMap, fromMap map[string]any, mergeArraysByName bool) {
	for k, fromVal := range fromMap {
		toVal, exists := toMap[k]
		if !exists {
			toMap[k] = fromVal
			continue
		}
		if toObj, ok1 := toVal.(map[string]any); ok1 {
			if fromObj, ok2 := fromVal.(map[string]any); ok2 {
				mergeJSONObjects(toObj, fromObj, mergeArraysByName)
				continue
			}
		}
		if toArr, ok1 := toVal.([]any); ok1 {
			if fromArr, ok2 := fromVal.([]any); ok2 {
				toMap[k] = mergeJSONArrays(toArr, fromArr, mergeArraysByName)
				continue
			}
		}
		toMap[k] = fromVal
	}
}

func allObjectsHaveNonEmptyName(arr []any) bool {
	for _, elem := range arr {
		obj, ok := elem.(map[string]any)
		if !ok {
			return false
		}
		nameVal, ok := obj["name"].(string)
		if !ok || nameVal == "" {
			return false
		}
	}
	return true
}

func mergeJSONArrays(toArr, fromArr []any, mergeArraysByName bool) []any {
	out := slices.Clone(toArr)
	byName := mergeArraysByName && allObjectsHaveNonEmptyName(toArr) && allObjectsHaveNonEmptyName(fromArr)
	for i, fromElem := range fromArr {
		if byName {
			fromObj := fromElem.(map[string]any)
			nameVal := fromObj["name"].(string)
			matchIdx := slices.IndexFunc(out, func(existing any) bool {
				existingObj, isMap := existing.(map[string]any)
				if !isMap {
					return false
				}
				existingName, ok := existingObj["name"].(string)
				return ok && existingName == nameVal
			})
			if matchIdx >= 0 {
				mergeJSONObjects(out[matchIdx].(map[string]any), fromObj, mergeArraysByName)
			} else {
				out = append(out, fromObj)
			}
			continue
		}

		if i < len(out) {
			if toObj, ok1 := out[i].(map[string]any); ok1 {
				if fromObj, ok2 := fromElem.(map[string]any); ok2 {
					mergeJSONObjects(toObj, fromObj, mergeArraysByName)
					continue
				}
			}
			out[i] = fromElem
		} else {
			out = append(out, fromElem)
		}
	}
	return out
}

func (j *flinkDeployment) buildComponentPodTemplateSpec(
	role kueue.PodSetReference,
	comp *flinkv1beta1.ComponentPodSpec,
	mergeArraysByName bool,
	defaultCPUCores int64,
) (*corev1.PodTemplateSpec, error) {
	var roleTemplate *flinkv1beta1.PodTemplateSpec
	if comp != nil {
		roleTemplate = comp.PodTemplate
	}

	podTemplate, err := mergePodTemplates(j.Spec.PodTemplate, roleTemplate, mergeArraysByName)
	if err != nil {
		return nil, err
	}
	if err := validatePodTemplateContainerNames(&podTemplate.Spec); err != nil {
		return nil, err
	}

	mainIdx := slices.IndexFunc(podTemplate.Spec.Containers, func(c corev1.Container) bool {
		return c.Name == flinkMainContainerName
	})
	if mainIdx < 0 {
		podTemplate.Spec.Containers = append(podTemplate.Spec.Containers, corev1.Container{
			Name: flinkMainContainerName,
		})
		mainIdx = len(podTemplate.Spec.Containers) - 1
	}

	// Match FlinkConfigBuilder.applyImage / applyImagePullPolicy / applyServiceAccount,
	// InitJobManagerDecorator / InitTaskManagerDecorator (KubernetesUtils.resolveUserDefinedValue
	// + KubernetesUtils.getServiceAccount), and InitStandaloneTaskManagerDecorator:
	//   - Non-whitespace spec.image and spec.imagePullPolicy overwrite flink-main-container's
	//     podTemplate values across all modes and roles.
	//   - When spec.image or spec.imagePullPolicy is empty/whitespace, job-manager (both modes)
	//     and native task-manager preserve podTemplate flink-main-container values, whereas
	//     standalone task-manager ignores podTemplate (requiring spec.image in standalone mode
	//     and setting flink-main-container imagePullPolicy to IfNotPresent).
	//   - Non-empty spec.serviceAccount sets kubernetes.service-account (a fallback key on both
	//     JOB_MANAGER_SERVICE_ACCOUNT and TASK_MANAGER_SERVICE_ACCOUNT checked by
	//     Configuration.contains), overwriting podTemplate serviceAccount / serviceAccountName
	//     across all modes and roles.
	//   - When spec.serviceAccount is empty, job-manager (both modes) and native task-manager
	//     fall back from podTemplate.spec.serviceAccount to podTemplate.spec.serviceAccountName,
	//     whereas standalone task-manager ignores podTemplate and uses "default".
	mainContainer := &podTemplate.Spec.Containers[mainIdx]
	if strings.TrimSpace(j.Spec.Image) != "" {
		mainContainer.Image = j.Spec.Image
	}
	switch {
	case strings.TrimSpace(string(j.Spec.ImagePullPolicy)) != "":
		if !isSupportedPullPolicy(j.Spec.ImagePullPolicy) {
			return nil, fmt.Errorf("unsupported spec.imagePullPolicy %q: supported values: %q, %q, %q",
				j.Spec.ImagePullPolicy, corev1.PullAlways, corev1.PullNever, corev1.PullIfNotPresent)
		}
		mainContainer.ImagePullPolicy = j.Spec.ImagePullPolicy
	case role == taskManagerPodSetName && j.Spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone:
		mainContainer.ImagePullPolicy = corev1.PullIfNotPresent
	}
	switch {
	case j.Spec.ServiceAccount != "":
		podTemplate.Spec.ServiceAccountName = j.Spec.ServiceAccount
		podTemplate.Spec.DeprecatedServiceAccount = ""
	case role == taskManagerPodSetName && j.Spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone:
		podTemplate.Spec.ServiceAccountName = "default"
		podTemplate.Spec.DeprecatedServiceAccount = ""
	case podTemplate.Spec.DeprecatedServiceAccount != "":
		podTemplate.Spec.ServiceAccountName = podTemplate.Spec.DeprecatedServiceAccount
		podTemplate.Spec.DeprecatedServiceAccount = ""
	}

	if err := validatePodTemplateMetadataAndPlacement(podTemplate, features.Enabled(features.WorkloadValidationForPodSetMetadata)); err != nil {
		return nil, err
	}
	if err := validateResourceRequirements(podTemplate.Spec.Resources); err != nil {
		return nil, fmt.Errorf("pod resources: %w", err)
	}
	if err := validateResourceOverhead(podTemplate.Spec.Overhead); err != nil {
		return nil, fmt.Errorf("pod overhead: %w", err)
	}
	if err := validatePodTolerations(podTemplate.Spec.Tolerations); err != nil {
		return nil, err
	}

	for i := range podTemplate.Spec.InitContainers {
		if err := validateResolvedContainer(&podTemplate.Spec.InitContainers[i]); err != nil {
			return nil, fmt.Errorf("initContainer %q: %w", podTemplate.Spec.InitContainers[i].Name, err)
		}
	}

	if err := applyComponentResources(mainContainer, comp); err != nil {
		return nil, err
	}
	applyDefaultComponentCPU(mainContainer, comp, defaultCPUCores)

	for i := range podTemplate.Spec.Containers {
		if err := validateResolvedContainer(&podTemplate.Spec.Containers[i]); err != nil {
			return nil, fmt.Errorf("container %q: %w", podTemplate.Spec.Containers[i].Name, err)
		}
	}

	workload.UseLimitsAsMissingRequestsInPod(&podTemplate.Spec)
	applyWorkloadContainerDefaults(&podTemplate.Spec)
	if err := validatePodTemplateSpecListMaps(&podTemplate.Spec); err != nil {
		return nil, err
	}
	return podTemplate, nil
}

// applyWorkloadContainerDefaults normalizes container fields on derived PodSets to match the
// OpenAPI schema defaults applied by kube-apiserver when persisting a Kueue Workload CR
// (config/components/crd/bases/kueue.x-k8s.io_workloads.yaml). Unlike Kubebuilder-generated
// CRDs, the Fabric8-generated FlinkDeployment CRD defines no OpenAPI defaults on podTemplate,
// so without this normalization EquivalentToWorkload (equality.ComparePodTemplate) would report
// a mismatch after Workload creation whenever ports[].protocol, grpc.service, or
// fileKeyRef.optional is omitted in FlinkDeployment.
func applyWorkloadContainerDefaults(podSpec *corev1.PodSpec) {
	defaultContainerCommon := func(ports []corev1.ContainerPort, env []corev1.EnvVar, probes ...*corev1.Probe) {
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
		for _, probe := range probes {
			if probe != nil && probe.GRPC != nil && probe.GRPC.Service == nil {
				probe.GRPC.Service = new("")
			}
		}
	}
	for i := range podSpec.InitContainers {
		c := &podSpec.InitContainers[i]
		defaultContainerCommon(c.Ports, c.Env, c.LivenessProbe, c.ReadinessProbe, c.StartupProbe)
	}
	for i := range podSpec.Containers {
		c := &podSpec.Containers[i]
		defaultContainerCommon(c.Ports, c.Env, c.LivenessProbe, c.ReadinessProbe, c.StartupProbe)
	}
	for i := range podSpec.EphemeralContainers {
		ec := &podSpec.EphemeralContainers[i]
		defaultContainerCommon(ec.Ports, ec.Env, ec.LivenessProbe, ec.ReadinessProbe, ec.StartupProbe)
	}
}

func setTopologyRequestIfEnabled(ps *kueue.PodSet) error {
	if !features.Enabled(features.TopologyAwareScheduling) {
		return nil
	}
	topologyRequest, err := jobframework.NewPodSetTopologyRequest(&ps.Template.ObjectMeta).Build()
	if err != nil {
		return err
	}
	ps.TopologyRequest = topologyRequest
	return nil
}

func (j *flinkDeployment) buildPodSets() ([]kueue.PodSet, error) {
	flatCfg, err := j.Spec.FlinkConfiguration.FlatMap()
	if err != nil {
		return nil, err
	}
	mergeArraysByName, err := podTemplateMergeArraysByNameFromConfig(flatCfg)
	if err != nil {
		return nil, err
	}
	taskSlots, err := taskSlotsFromConfig(flatCfg)
	if err != nil {
		return nil, err
	}

	// Flink's InitStandaloneTaskManagerDecorator ignores podTemplate image and falls back to the
	// operator JVM's runtime CONTAINER_IMAGE default when spec.image is omitted, and renders
	// Quantity("-1.0") when standalone TaskManager CPU is omitted; the webhook reports both as
	// field.Required and buildPodSets rejects them on webhook-bypass paths.
	if j.Spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone && strings.TrimSpace(j.Spec.Image) == "" {
		return nil, errors.New("spec.image must be specified when mode is standalone")
	}

	var jmComp *flinkv1beta1.ComponentPodSpec
	if j.Spec.JobManager != nil {
		jmComp = &j.Spec.JobManager.ComponentPodSpec
	}
	jmTemplate, err := j.buildComponentPodTemplateSpec(jobManagerPodSetName, jmComp, mergeArraysByName, 1)
	if err != nil {
		return nil, fmt.Errorf("job-manager podSet: %w", err)
	}
	jmCount, err := j.numJobManagers()
	if err != nil {
		return nil, err
	}

	var tmComp *flinkv1beta1.ComponentPodSpec
	if j.Spec.TaskManager != nil {
		tmComp = &j.Spec.TaskManager.ComponentPodSpec
	}
	// Flink's InitStandaloneTaskManagerDecorator renders Quantity("-1.0") when standalone
	// TaskManager CPU is omitted, so there is no honest default to mirror; the webhook reports
	// this as field.Required and buildPodSets rejects it on webhook-bypass paths.
	if j.Spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone && !hasExplicitComponentCPU(tmComp) {
		return nil, errors.New("taskManager cpu must be specified via taskManager.resource.cpu or taskManager.resources when mode is standalone")
	}
	tmTemplate, err := j.buildComponentPodTemplateSpec(taskManagerPodSetName, tmComp, mergeArraysByName, int64(taskSlots))
	if err != nil {
		return nil, fmt.Errorf("task-manager podSet: %w", err)
	}
	tmCount, err := j.numTaskManagers(taskSlots)
	if err != nil {
		return nil, err
	}

	return []kueue.PodSet{
		{
			Name:     jobManagerPodSetName,
			Template: *jmTemplate,
			Count:    jmCount,
		},
		{
			Name:     taskManagerPodSetName,
			Template: *tmTemplate,
			Count:    tmCount,
		},
	}, nil
}

func (j *flinkDeployment) PodSets(_ context.Context, _ client.Client) ([]kueue.PodSet, error) {
	podSets, err := j.buildPodSets()
	if err != nil {
		return nil, err
	}

	for i := range podSets {
		if err := setTopologyRequestIfEnabled(&podSets[i]); err != nil {
			return nil, err
		}
	}

	return podSets, nil
}
