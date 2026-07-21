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

package sparkapplication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	sparkutil "github.com/kubeflow/spark-operator/v2/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

const (
	// defaultSparkCores mirrors Spark's default for spark.driver.cores /
	// spark.executor.cores when neither that nor coreRequest is set.
	defaultSparkCores = 1

	// defaultExecutorInstances mirrors Spark's default for spark.executor.instances
	// in static (non dynamic-allocation) mode when the field is unset.
	defaultExecutorInstances = 2

	// defaultMemoryOverheadFactorJVM and defaultMemoryOverheadFactorNonJVM mirror
	// spark.{driver,executor}.memoryOverheadFactor defaults: 10% for JVM
	// languages (Java/Scala), 40% for non-JVM languages (Python/R), which need
	// more off-heap headroom.
	defaultMemoryOverheadFactorJVM    = 0.1
	defaultMemoryOverheadFactorNonJVM = 0.4

	// minMemoryOverheadMiB is the floor Spark applies to the computed memory
	// overhead regardless of the factor.
	minMemoryOverheadMiB = 384
)

var (
	emptyDriverPodTemplateSpec = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: sparkcommon.SparkDriverContainerName,
			}},
		},
	}
	emptyExecutorPodTemplateSpec = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: sparkcommon.Spark3DefaultExecutorContainerName,
			}},
		},
	}
)

// numInitialExecutors returns the number of executor pods Spark will create at
// startup, so the initial Workload PodSet count matches reality.
//
// In static (non dynamic-allocation) mode, it mirrors Spark's own default of 2
// executors when spec.executor.instances is unset.
//
// When dynamic allocation is enabled, Spark ignores the static default and
// instead ramps from max(instances, initialExecutors, minExecutors) — see
// "If .spec.executor.instances is also set, the initial number of executors is
// set to the bigger of that and this option" in the spark-operator API doc for
// dynamicAllocation.initialExecutors.
func (j *SparkApplication) numInitialExecutors() int32 {
	da := ptr.Deref(j.Spec.DynamicAllocation, sparkv1beta2.DynamicAllocation{})
	if !da.Enabled {
		return ptr.Deref(j.Spec.Executor.Instances, defaultExecutorInstances)
	}

	target := ptr.Deref(j.Spec.Executor.Instances, 0)
	if v := ptr.Deref(da.InitialExecutors, 0); v > target {
		target = v
	}
	if v := ptr.Deref(da.MinExecutors, 0); v > target {
		target = v
	}
	return target
}

// numExecutors returns the executor PodSet count for the Workload.
//
// For non-elastic applications this is simply numInitialExecutors(): Spark
// never resizes an application whose spec doesn't change.
//
// For elastic applications, the SparkApplication spec never reflects
// dynamic-allocation scaling (the driver creates and deletes executor pods
// directly, without touching the spec), so the count is instead derived from
// observed executor pod demand: the number of live (non-terminal,
// non-deleting) executor pods, floored at the initial target before the
// driver starts and at minExecutors once it's running (so demand never
// implies fewer executors than Spark itself will always re-request), and
// capped at maxExecutors (required by webhook validation for elastic jobs).
func (j *SparkApplication) numExecutors(ctx context.Context, c client.Client) (int32, error) {
	if !workloadslicing.Enabled(j) {
		return j.numInitialExecutors(), nil
	}

	floor := j.numInitialExecutors()
	if j.Status.AppState.State == sparkv1beta2.ApplicationStateRunning {
		da := ptr.Deref(j.Spec.DynamicAllocation, sparkv1beta2.DynamicAllocation{})
		floor = ptr.Deref(da.MinExecutors, 0)
	}

	if c == nil {
		// No client available (e.g. webhook validation building a PodSet
		// template outside of admission). Fall back to the floor; the
		// reconciler always has a client and will observe live demand.
		return floor, nil
	}

	delegated, err := j.isMultiKueueDelegated(ctx, c)
	if err != nil {
		return 0, err
	}

	var observed int32
	if delegated {
		// The driver and its executors run on the worker cluster, not here:
		// there are no local executor pods to observe. Use the demand the
		// worker last synced back onto this object instead (see the
		// SparkApplication MultiKueue adapter's SyncJob).
		observed, err = j.mirroredExecutorCount()
	} else {
		observed, err = j.observedExecutorCount(ctx, c)
	}
	if err != nil {
		return 0, err
	}

	desired := max(observed, floor)

	maxExecutors := ptr.Deref(ptr.Deref(j.Spec.DynamicAllocation, sparkv1beta2.DynamicAllocation{}).MaxExecutors, 0)
	if maxExecutors > 0 && desired > maxExecutors {
		desired = maxExecutors
	}

	return desired, nil
}

// observedExecutorCount lists the application's executor pods and counts
// those that are neither terminal (Succeeded/Failed) nor being deleted.
// Pending (including gated) executor pods are counted, since a gated pod is
// itself the signal of scale-up demand.
func (j *SparkApplication) observedExecutorCount(ctx context.Context, c client.Client) (int32, error) {
	var podList corev1.PodList
	if err := c.List(ctx, &podList,
		client.InNamespace(j.Namespace),
		client.MatchingLabels{
			sparkcommon.LabelSparkAppName: j.Name,
			sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
		},
	); err != nil {
		return 0, fmt.Errorf("failed to list executor pods: %w", err)
	}

	var count int32
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		count++
	}
	return count, nil
}

// isMultiKueueDelegated reports whether this SparkApplication is the
// management-cluster copy of an application admitted through MultiKueue,
// meaning the driver and its executors actually run on a worker cluster and
// no local executor pods will ever exist to observe. It is derived fresh
// from the workload's current admission-check state on every call, rather
// than from a locally-stamped marker, so that it correctly flips back to
// false once the workload is no longer MultiKueue-delegated (e.g. after
// eviction) instead of permanently pinning numExecutors to the mirrored
// annotation.
func (j *SparkApplication) isMultiKueueDelegated(ctx context.Context, c client.Client) (bool, error) {
	if !features.Enabled(features.MultiKueue) {
		return false, nil
	}

	workloads, err := workloadslicing.FindNotFinishedWorkloads(ctx, c, (*sparkv1beta2.SparkApplication)(j), gvk)
	if err != nil {
		return false, fmt.Errorf("failed to list workloads for sparkapplication: %w", err)
	}

	for i := range workloads {
		skip, err := admissioncheck.ShouldSkipLocalExecution(ctx, c, &workloads[i])
		if err != nil {
			return false, err
		}
		if skip {
			return true, nil
		}
	}
	return false, nil
}

// mirroredExecutorCount returns the executor PodSet count most recently
// synced from the remote worker cluster's copy of
// SparkApplicationPodSetReplicaSizesAnnotation (see the SparkApplication
// MultiKueue adapter's SyncJob), or 0 if the annotation is absent or has no
// executor entry yet.
func (j *SparkApplication) mirroredExecutorCount() (int32, error) {
	replicaSizes := j.Annotations[SparkApplicationPodSetReplicaSizesAnnotation]
	if replicaSizes == "" {
		return 0, nil
	}

	var sizes []jobframework.PodSetReplicaSize
	if err := json.Unmarshal([]byte(replicaSizes), &sizes); err != nil {
		return 0, fmt.Errorf("failed to unmarshal PodSet replica sizes: %w", err)
	}

	for _, size := range sizes {
		if size.Name == executorPodSetName {
			return size.Count, nil
		}
	}
	return 0, nil
}

func (j *SparkApplication) buildDriverPodTemplateSpec() (*corev1.PodTemplateSpec, error) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				sparkcommon.LabelSparkApplicationSelector: j.Name,
				sparkcommon.LabelSparkRole:                sparkcommon.SparkRoleDriver,
			},
		},
		Spec: *emptyDriverPodTemplateSpec.Spec.DeepCopy(),
	}

	if err := mutateSparkPod((*sparkv1beta2.SparkApplication)(j), &pod); err != nil {
		return nil, err
	}

	return &corev1.PodTemplateSpec{
		ObjectMeta: pod.ObjectMeta,
		Spec:       pod.Spec,
	}, nil
}

func (j *SparkApplication) buildExecutorPodTemplateSpec() (*corev1.PodTemplateSpec, error) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				sparkcommon.LabelSparkApplicationSelector: j.Name,
				sparkcommon.LabelSparkRole:                sparkcommon.SparkRoleExecutor,
			},
		},
		Spec: *emptyExecutorPodTemplateSpec.Spec.DeepCopy(),
	}

	if err := mutateSparkPod((*sparkv1beta2.SparkApplication)(j), &pod); err != nil {
		return nil, err
	}

	return &corev1.PodTemplateSpec{
		ObjectMeta: pod.ObjectMeta,
		Spec:       pod.Spec,
	}, nil
}

// NOTE: Most of the below code is adapted from kubeflow/spark-operator internal code
// ref: https://github.com/kubeflow/spark-operator/blob/v2.4.0/internal/webhook/sparkpod_defaulter.go
// TODO: replace them with exported functions once the below PR is merged and released.
// PR: https://github.com/kubeflow/spark-operator/pull/2857

func hasContainer(pod *corev1.Pod, container *corev1.Container) bool {
	return slices.ContainsFunc(pod.Spec.Containers, func(c corev1.Container) bool {
		return container.Name == c.Name && container.Image == c.Image
	})
}

func hasInitContainer(pod *corev1.Pod, container *corev1.Container) bool {
	return slices.ContainsFunc(pod.Spec.InitContainers, func(c corev1.Container) bool {
		return container.Name == c.Name && container.Image == c.Image
	})
}

func findContainer(pod *corev1.Pod) int {
	switch {
	case sparkutil.IsDriverPod(pod):
		// if no containers match the driver container name, assume the first container is the one
		return max(slices.IndexFunc(pod.Spec.Containers, func(c corev1.Container) bool {
			return c.Name == sparkcommon.SparkDriverContainerName
		}), 0)
	case sparkutil.IsExecutorPod(pod):
		// if no containers match the executor container name, assume the first container is the one
		return max(slices.IndexFunc(pod.Spec.Containers, func(c corev1.Container) bool {
			return c.Name == sparkcommon.SparkExecutorContainerName
		}), 0)
	default:
		return -1
	}
}

func mutateSparkPod(app *sparkv1beta2.SparkApplication, pod *corev1.Pod) error {
	// this focuses on resource related fields only
	options := []mutateSparkPodOption{
		addVolumes,
		addInitContainers,
		addSidecarContainers,
		addPriorityClassName,
		addNodeSelectors,
		addAffinity,
		addTolerations,
		addCPURequests,
		addCPULimit,
		addMemoryRequests,
		addMemoryLimit,
		addGPU,
		addObjectMeta,
	}

	for _, option := range options {
		if err := option(pod, app); err != nil {
			return err
		}
	}

	return nil
}

type mutateSparkPodOption func(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error

func addObjectMeta(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	mergeMetadata := func(base *corev1.PodTemplateSpec, labels, annotations map[string]string) metav1.ObjectMeta {
		if base == nil {
			return metav1.ObjectMeta{
				Labels:      labels,
				Annotations: annotations,
			}
		}

		meta := base.ObjectMeta.DeepCopy()
		if base.Labels == nil {
			meta.Labels = map[string]string{}
		}
		maps.Copy(meta.Labels, labels)

		if meta.Annotations == nil {
			meta.Annotations = map[string]string{}
		}
		maps.Copy(meta.Annotations, annotations)

		return *meta
	}

	if sparkutil.IsDriverPod(pod) {
		pod.ObjectMeta = mergeMetadata(app.Spec.Driver.Template, app.Spec.Driver.Labels, app.Spec.Driver.Annotations)
	} else if sparkutil.IsExecutorPod(pod) {
		pod.ObjectMeta = mergeMetadata(app.Spec.Executor.Template, app.Spec.Executor.Labels, app.Spec.Executor.Annotations)
	}

	return nil
}

func addVolume(pod *corev1.Pod, volume corev1.Volume) error {
	pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
	return nil
}

func addVolumeMount(pod *corev1.Pod, mount corev1.VolumeMount) error {
	i := findContainer(pod)
	if i < 0 {
		return errors.New("failed to add volumeMounts as Spark container not found")
	}

	pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, mount)
	return nil
}

func addVolumes(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	volumes := app.Spec.Volumes

	volumeMap := make(map[string]corev1.Volume)
	for _, v := range volumes {
		volumeMap[v.Name] = v
	}

	var volumeMounts []corev1.VolumeMount
	if sparkutil.IsDriverPod(pod) {
		volumeMounts = app.Spec.Driver.VolumeMounts
	} else if sparkutil.IsExecutorPod(pod) {
		volumeMounts = app.Spec.Executor.VolumeMounts
	}

	addedVolumeMap := make(map[string]corev1.Volume)
	for _, m := range volumeMounts {
		// Skip adding localDirVolumes
		if strings.HasPrefix(m.Name, sparkcommon.SparkLocalDirVolumePrefix) {
			continue
		}

		if v, ok := volumeMap[m.Name]; ok {
			if _, ok := addedVolumeMap[m.Name]; !ok {
				_ = addVolume(pod, v)
				addedVolumeMap[m.Name] = v
			}
			_ = addVolumeMount(pod, m)
		}
	}
	return nil
}

func addInitContainers(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	var initContainers []corev1.Container
	if sparkutil.IsDriverPod(pod) {
		initContainers = app.Spec.Driver.InitContainers
	} else if sparkutil.IsExecutorPod(pod) {
		initContainers = app.Spec.Executor.InitContainers
	}

	if pod.Spec.InitContainers == nil {
		pod.Spec.InitContainers = []corev1.Container{}
	}

	for _, container := range initContainers {
		if !hasInitContainer(pod, &container) {
			pod.Spec.InitContainers = append(pod.Spec.InitContainers, *container.DeepCopy())
		}
	}
	return nil
}

func addSidecarContainers(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	var sidecars []corev1.Container
	if sparkutil.IsDriverPod(pod) {
		sidecars = app.Spec.Driver.Sidecars
	} else if sparkutil.IsExecutorPod(pod) {
		sidecars = app.Spec.Executor.Sidecars
	}

	for _, sidecar := range sidecars {
		if !hasContainer(pod, &sidecar) {
			pod.Spec.Containers = append(pod.Spec.Containers, *sidecar.DeepCopy())
		}
	}
	return nil
}

func addPriorityClassName(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	var priorityClassName *string

	if sparkutil.IsDriverPod(pod) {
		priorityClassName = app.Spec.Driver.PriorityClassName
	} else if sparkutil.IsExecutorPod(pod) {
		priorityClassName = app.Spec.Executor.PriorityClassName
	}

	if priorityClassName != nil && *priorityClassName != "" {
		pod.Spec.PriorityClassName = *priorityClassName
		pod.Spec.Priority = nil
		pod.Spec.PreemptionPolicy = nil
	}

	return nil
}

func addNodeSelectors(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	var nodeSelector map[string]string

	if sparkutil.IsDriverPod(pod) {
		nodeSelector = app.Spec.Driver.NodeSelector
	} else if sparkutil.IsExecutorPod(pod) {
		nodeSelector = app.Spec.Executor.NodeSelector
	}

	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}

	maps.Copy(pod.Spec.NodeSelector, nodeSelector)

	return nil
}

func addAffinity(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	var affinity *corev1.Affinity
	if sparkutil.IsDriverPod(pod) {
		affinity = app.Spec.Driver.Affinity
	} else if sparkutil.IsExecutorPod(pod) {
		affinity = app.Spec.Executor.Affinity
	}
	if affinity == nil {
		return nil
	}
	pod.Spec.Affinity = affinity.DeepCopy()
	return nil
}

func addTolerations(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	var tolerations []corev1.Toleration
	if sparkutil.IsDriverPod(pod) {
		tolerations = app.Spec.Driver.Tolerations
	} else if sparkutil.IsExecutorPod(pod) {
		tolerations = app.Spec.Executor.Tolerations
	}

	if pod.Spec.Tolerations == nil {
		pod.Spec.Tolerations = []corev1.Toleration{}
	}

	pod.Spec.Tolerations = append(pod.Spec.Tolerations, tolerations...)
	return nil
}

// addCPURequests sets the container's CPU request to what Spark will actually
// request for the real pod: coreRequest if set, else an integer quantity built
// from cores (spark.{driver,executor}.cores), else Spark's own default of 1
// core. Unlike coreRequest (a Kubernetes-style quantity string, e.g. "500m"),
// cores is a whole-core count.
func addCPURequests(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	i := findContainer(pod)
	if i < 0 {
		return fmt.Errorf("failed to add CPU requests as Spark container was not found in pod %s", pod.Name)
	}

	var cpuRequests *string
	var cores *int32
	if sparkutil.IsDriverPod(pod) {
		cpuRequests = app.Spec.Driver.CoreRequest
		cores = app.Spec.Driver.Cores
	} else if sparkutil.IsExecutorPod(pod) {
		cpuRequests = app.Spec.Executor.CoreRequest
		cores = app.Spec.Executor.Cores
	}

	var requestsQuantity resource.Quantity
	switch {
	case cpuRequests != nil:
		var err error
		requestsQuantity, err = resource.ParseQuantity(*cpuRequests)
		if err != nil {
			return fmt.Errorf("failed to parse CPU requests %s: %w", *cpuRequests, err)
		}
	case cores != nil && *cores > 0:
		requestsQuantity = *resource.NewQuantity(int64(*cores), resource.DecimalSI)
	default:
		requestsQuantity = *resource.NewQuantity(defaultSparkCores, resource.DecimalSI)
	}

	if pod.Spec.Containers[i].Resources.Requests == nil {
		pod.Spec.Containers[i].Resources.Requests = corev1.ResourceList{}
	}

	// Apply the CPU requests to the container's resources
	pod.Spec.Containers[i].Resources.Requests[corev1.ResourceCPU] = requestsQuantity
	return nil
}

func addCPULimit(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	i := findContainer(pod)
	if i < 0 {
		return fmt.Errorf("failed to add CPU limit as Spark container was not found in pod %s", pod.Name)
	}

	var cpuLimit *string
	if sparkutil.IsDriverPod(pod) {
		cpuLimit = app.Spec.Driver.CoreLimit
	} else if sparkutil.IsExecutorPod(pod) {
		cpuLimit = app.Spec.Executor.CoreLimit
	}

	if cpuLimit == nil {
		return nil
	}

	// Convert CPU limit to a Kubernetes-style unit
	limitQuantity, err := resource.ParseQuantity(*cpuLimit)
	if err != nil {
		return fmt.Errorf("failed to parse CPU limit %s: %v", *cpuLimit, err)
	}

	if pod.Spec.Containers[i].Resources.Limits == nil {
		pod.Spec.Containers[i].Resources.Limits = corev1.ResourceList{}
	}

	// Apply the CPU limit to the container's resources
	pod.Spec.Containers[i].Resources.Limits[corev1.ResourceCPU] = limitQuantity
	return nil
}

// addMemoryRequests sets the container's memory request to what Spark will
// actually request for the real pod: the configured memory plus the computed
// off-heap overhead (see computeMemoryOverhead). Spark always adds overhead on
// top of `memory` for the real pod, so accounting for `memory` alone
// under-counts by at least the overhead floor (384Mi).
func addMemoryRequests(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	i := findContainer(pod)
	if i < 0 {
		return fmt.Errorf("failed to add memory requests as Spark container was not found in pod %s", pod.Name)
	}

	var memoryRequests, memoryOverhead *string
	if sparkutil.IsDriverPod(pod) {
		memoryRequests = app.Spec.Driver.Memory
		memoryOverhead = app.Spec.Driver.MemoryOverhead
	} else if sparkutil.IsExecutorPod(pod) {
		memoryRequests = app.Spec.Executor.Memory
		memoryOverhead = app.Spec.Executor.MemoryOverhead
	}

	if memoryRequests == nil {
		return nil
	}

	// Convert memory requests to a Kubernetes-style unit
	requestsQuantity, err := resource.ParseQuantity(sparkutil.ConvertJavaMemoryStringToK8sMemoryString(*memoryRequests))
	if err != nil {
		return fmt.Errorf("failed to parse memory requests %s: %w", *memoryRequests, err)
	}

	overheadQuantity, err := computeMemoryOverhead(requestsQuantity, memoryOverhead, app.Spec.MemoryOverheadFactor, app.Spec.Type)
	if err != nil {
		return err
	}
	requestsQuantity.Add(overheadQuantity)

	if pod.Spec.Containers[i].Resources.Requests == nil {
		pod.Spec.Containers[i].Resources.Requests = corev1.ResourceList{}
	}

	// Apply the memory requests (including overhead) to the container's resources
	pod.Spec.Containers[i].Resources.Requests[corev1.ResourceMemory] = requestsQuantity
	return nil
}

// computeMemoryOverhead mirrors Spark's off-heap memory overhead computation:
// the explicit memoryOverhead value if set, else max(factor * memory, 384Mi),
// where factor is memoryOverheadFactor if set, else 0.1 for JVM languages
// (Java/Scala) or 0.4 for non-JVM languages (Python/R), which need more
// off-heap headroom.
func computeMemoryOverhead(memory resource.Quantity, explicitOverhead, factorOverride *string, appType sparkv1beta2.SparkApplicationType) (resource.Quantity, error) {
	if explicitOverhead != nil {
		overheadQuantity, err := resource.ParseQuantity(sparkutil.ConvertJavaMemoryStringToK8sMemoryString(*explicitOverhead))
		if err != nil {
			return resource.Quantity{}, fmt.Errorf("failed to parse memory overhead %s: %w", *explicitOverhead, err)
		}
		return overheadQuantity, nil
	}

	factor := defaultMemoryOverheadFactorJVM
	if appType == sparkv1beta2.SparkApplicationTypePython || appType == sparkv1beta2.SparkApplicationTypeR {
		factor = defaultMemoryOverheadFactorNonJVM
	}
	if factorOverride != nil {
		parsed, err := strconv.ParseFloat(*factorOverride, 64)
		if err != nil {
			return resource.Quantity{}, fmt.Errorf("failed to parse memoryOverheadFactor %s: %w", *factorOverride, err)
		}
		factor = parsed
	}

	floorBytes := int64(minMemoryOverheadMiB) * 1024 * 1024
	overheadBytes := int64(float64(memory.Value()) * factor)
	if overheadBytes < floorBytes {
		overheadBytes = floorBytes
	}
	return *resource.NewQuantity(overheadBytes, resource.BinarySI), nil
}

func addMemoryLimit(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	i := findContainer(pod)
	if i < 0 {
		return fmt.Errorf("failed to add memory limit as Spark container was not found in pod %s", pod.Name)
	}

	var memoryLimit *string
	if sparkutil.IsDriverPod(pod) {
		memoryLimit = app.Spec.Driver.MemoryLimit
	} else if sparkutil.IsExecutorPod(pod) {
		memoryLimit = app.Spec.Executor.MemoryLimit
	}

	if memoryLimit == nil {
		return nil
	}

	// Convert memory limit to a Kubernetes-style unit
	limitQuantity, err := resource.ParseQuantity(sparkutil.ConvertJavaMemoryStringToK8sMemoryString(*memoryLimit))
	if err != nil {
		return fmt.Errorf("failed to parse memory limit %s: %v", *memoryLimit, err)
	}

	if pod.Spec.Containers[i].Resources.Limits == nil {
		pod.Spec.Containers[i].Resources.Limits = corev1.ResourceList{}
	}

	// Apply the memory limit to the container's resources
	pod.Spec.Containers[i].Resources.Limits[corev1.ResourceMemory] = limitQuantity
	return nil
}

func addGPU(pod *corev1.Pod, app *sparkv1beta2.SparkApplication) error {
	var gpu *sparkv1beta2.GPUSpec
	if sparkutil.IsDriverPod(pod) {
		gpu = app.Spec.Driver.GPU
	}
	if sparkutil.IsExecutorPod(pod) {
		gpu = app.Spec.Executor.GPU
	}
	if gpu == nil {
		return nil
	}
	if gpu.Name == "" {
		return nil
	}
	if gpu.Quantity <= 0 {
		return nil
	}

	i := findContainer(pod)
	if i < 0 {
		return fmt.Errorf("failed to add GPU as Spark container was not found in pod %s", pod.Name)
	}
	if pod.Spec.Containers[i].Resources.Limits == nil {
		pod.Spec.Containers[i].Resources.Limits = make(corev1.ResourceList)
	}
	pod.Spec.Containers[i].Resources.Limits[corev1.ResourceName(gpu.Name)] = *resource.NewQuantity(gpu.Quantity, resource.DecimalSI)
	return nil
}
