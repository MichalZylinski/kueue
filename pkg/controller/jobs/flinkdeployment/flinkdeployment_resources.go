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
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metavalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
)

const (
	flinkConfigNumberOfTaskSlots                        = "taskmanager.numberOfTaskSlots"
	flinkConfigPodTemplateMergeArraysByName             = "kubernetes.operator.pod-template.merge-arrays-by-name"
	flinkConfigAutoscalerEnabled                        = "job.autoscaler.enabled"
	flinkConfigOperatorAutoscalerEnabled                = "kubernetes.operator.job.autoscaler.enabled"
	flinkConfigExternalResources                        = "external-resources"
	flinkConfigExternalResourcePrefix                   = "external-resource."
	flinkConfigJobManagerMemoryPrefix                   = "jobmanager.memory."
	flinkConfigTaskManagerMemoryPrefix                  = "taskmanager.memory."
	flinkConfigTaskManagerNetworkMemoryMin              = "taskmanager.network.memory.min"
	flinkConfigTaskManagerNetworkMemoryMax              = "taskmanager.network.memory.max"
	flinkConfigVertexParallelismOverrides               = "pipeline.jobvertex-parallelism-overrides"
	flinkConfigBatchAdaptiveMaxParallelism              = "execution.batch.adaptive.auto-parallelism.max-parallelism"
	flinkConfigBatchAdaptiveMinParallelism              = "execution.batch.adaptive.auto-parallelism.min-parallelism"
	flinkConfigBatchAdaptiveDefaultSourceParallelism    = "execution.batch.adaptive.auto-parallelism.default-source-parallelism"
	flinkConfigDeprecatedAdaptiveBatchMaxParallelism    = "jobmanager.adaptive-batch-scheduler.max-parallelism"
	flinkConfigDeprecatedAdaptiveBatchMinParallelism    = "jobmanager.adaptive-batch-scheduler.min-parallelism"
	flinkConfigDeprecatedAdaptiveBatchDefaultSourcePara = "jobmanager.adaptive-batch-scheduler.default-source-parallelism"
	flinkConfigTaskManagerCPUCores                      = "taskmanager.cpu.cores"
	flinkConfigJobManagerCPU                            = "kubernetes.jobmanager.cpu"
	flinkConfigJobManagerCPUAmount                      = "kubernetes.jobmanager.cpu.amount"
	flinkConfigTaskManagerCPU                           = "kubernetes.taskmanager.cpu"
	flinkConfigTaskManagerCPUAmount                     = "kubernetes.taskmanager.cpu.amount"
	flinkConfigJobManagerReplicas                       = "kubernetes.jobmanager.replicas"
	flinkConfigTaskManagerReplicas                      = "kubernetes.taskmanager.replicas"
	flinkConfigInternalTaskManagerReplicas              = "kubernetes.internal.taskmanager.replicas"
	flinkConfigJobManagerCPULimitFactor                 = "kubernetes.jobmanager.cpu.limit-factor"
	flinkConfigJobManagerCPURequestFactor               = "kubernetes.jobmanager.cpu.request-factor"
	flinkConfigJobManagerMemoryLimitFactor              = "kubernetes.jobmanager.memory.limit-factor"
	flinkConfigJobManagerMemoryRequestFactor            = "kubernetes.jobmanager.memory.request-factor"
	flinkConfigTaskManagerCPULimitFactor                = "kubernetes.taskmanager.cpu.limit-factor"
	flinkConfigTaskManagerCPURequestFactor              = "kubernetes.taskmanager.cpu.request-factor"
	flinkConfigTaskManagerMemoryLimitFactor             = "kubernetes.taskmanager.memory.limit-factor"
	flinkConfigTaskManagerMemoryRequestFactor           = "kubernetes.taskmanager.memory.request-factor"
	flinkConfigSlotManagerRedundantTaskManagers         = "slotmanager.redundant-taskmanager-num"
	flinkConfigSlotManagerMinSlots                      = "slotmanager.number-of-slots.min"
	flinkConfigSlotManagerMinTotalCPU                   = "slotmanager.min-total-resource.cpu"
	flinkConfigSlotManagerMinTotalMemory                = "slotmanager.min-total-resource.memory"
	flinkConfigPodTemplateFile                          = "kubernetes.pod-template-file"
	flinkConfigServiceAccount                           = "kubernetes.service-account"
	flinkConfigJobManagerServiceAccount                 = "kubernetes.jobmanager.service-account"
	flinkConfigTaskManagerServiceAccount                = "kubernetes.taskmanager.service-account"
	flinkConfigContainerImageRef                        = "kubernetes.container.image.ref"
	flinkConfigDeprecatedContainerImage                 = "kubernetes.container.image"
	flinkConfigContainerImagePullPolicy                 = "kubernetes.container.image.pull-policy"
	flinkConfigContainerImagePullSecrets                = "kubernetes.container.image.pull-secrets"
	flinkConfigHostNetworkEnabled                       = "kubernetes.hostnetwork.enabled"
	flinkConfigOperatorJobRestartFailed                 = "kubernetes.operator.job.restart.failed"
	flinkConfigOperatorDeploymentRollbackEnabled        = "kubernetes.operator.deployment.rollback.enabled"

	// maxFlinkMemoryBytes caps component memory at Integer.MAX_VALUE MiB (2,147,483,647 MiB =
	// 2,251,799,812,636,672 bytes). Apache Flink converts TOTAL_PROCESS_MEMORY to Pod memory
	// via MemorySize.getMebiBytes() (`(int) (bytes >> 20)`), which overflows to negative MiB at
	// 2^31 MiB ("2Pi"), and Fabric8 Quantity switches to MathContext.DECIMAL64 scientific
	// notation at >= 1e16 bytes.
	maxFlinkMemoryBytes = int64(math.MaxInt32) << 20
)

var (
	minMemoryByteQuantity = *resource.NewQuantity(1, resource.DecimalSI)
	maxMemoryByteQuantity = *resource.NewQuantity(maxFlinkMemoryBytes, resource.BinarySI)

	// unsupportedFlinkConfigSizingKeys lists exact flinkConfiguration keys that alter pod
	// CPU, memory, or replica counts outside spec.jobManager and spec.taskManager, including
	// deprecated network memory aliases and the Native-mode FineGrainedSlotManager keys that
	// make ActiveResourceManager allocate TaskManager pods beyond the job slot demand reflected
	// in the TaskManager PodSet.
	unsupportedFlinkConfigSizingKeys = sets.New(
		flinkConfigTaskManagerNetworkMemoryMin,
		flinkConfigTaskManagerNetworkMemoryMax,
		flinkConfigTaskManagerCPUCores,
		flinkConfigJobManagerCPU,
		flinkConfigJobManagerCPUAmount,
		flinkConfigTaskManagerCPU,
		flinkConfigTaskManagerCPUAmount,
		flinkConfigJobManagerReplicas,
		flinkConfigTaskManagerReplicas,
		flinkConfigInternalTaskManagerReplicas,
		flinkConfigJobManagerCPULimitFactor,
		flinkConfigJobManagerCPURequestFactor,
		flinkConfigJobManagerMemoryLimitFactor,
		flinkConfigJobManagerMemoryRequestFactor,
		flinkConfigTaskManagerCPULimitFactor,
		flinkConfigTaskManagerCPURequestFactor,
		flinkConfigTaskManagerMemoryLimitFactor,
		flinkConfigTaskManagerMemoryRequestFactor,
		flinkConfigSlotManagerRedundantTaskManagers,
		flinkConfigSlotManagerMinSlots,
		flinkConfigSlotManagerMinTotalCPU,
		flinkConfigSlotManagerMinTotalMemory,
	)

	// standaloneOnlyParallelismConfigKeys lists Flink parallelism override and
	// AdaptiveBatchScheduler auto-parallelism keys (along with their deprecated aliases) that
	// rescale operator parallelism and Native-mode TaskManager pod demand independently of
	// spec.job.parallelism unless TaskManager pod count is pinned in standalone mode via
	// spec.taskManager.replicas.
	standaloneOnlyParallelismConfigKeys = sets.New(
		flinkConfigVertexParallelismOverrides,
		flinkConfigBatchAdaptiveMaxParallelism,
		flinkConfigBatchAdaptiveMinParallelism,
		flinkConfigBatchAdaptiveDefaultSourceParallelism,
		flinkConfigDeprecatedAdaptiveBatchMaxParallelism,
		flinkConfigDeprecatedAdaptiveBatchMinParallelism,
		flinkConfigDeprecatedAdaptiveBatchDefaultSourcePara,
	)

	// forbiddenFlinkPodPlacementAndMetadataKeys lists Flink decorator keys that mutate pod
	// nodeSelector, tolerations, annotations, or labels after podTemplate loading, plus the
	// operator-local kubernetes.pod-template-file* keys whose contents are invisible to
	// PodSets() and get overwritten by FlinkConfigBuilder.applyPodTemplate once Kueue
	// materializes spec.jobManager.podTemplate and spec.taskManager.podTemplate on admission.
	forbiddenFlinkPodPlacementAndMetadataKeys = []string{
		"kubernetes.jobmanager.node-selector",
		"kubernetes.taskmanager.node-selector",
		"kubernetes.jobmanager.tolerations",
		"kubernetes.taskmanager.tolerations",
		"kubernetes.jobmanager.annotations",
		"kubernetes.taskmanager.annotations",
		"kubernetes.jobmanager.labels",
		"kubernetes.taskmanager.labels",
		flinkConfigPodTemplateFile,
	}

	// forbiddenFlinkPodIdentityImageAndNetworkKeys lists Flink decorator keys that overwrite pod
	// serviceAccountName, container image, imagePullPolicy, imagePullSecrets, or hostNetwork/dnsPolicy
	// after podTemplate loading without being reflected in PodSets().
	forbiddenFlinkPodIdentityImageAndNetworkKeys = sets.New(
		flinkConfigServiceAccount,
		flinkConfigJobManagerServiceAccount,
		flinkConfigTaskManagerServiceAccount,
		flinkConfigContainerImageRef,
		flinkConfigDeprecatedContainerImage,
		flinkConfigContainerImagePullPolicy,
		flinkConfigContainerImagePullSecrets,
		flinkConfigHostNetworkEnabled,
	)
)

func isForbiddenFlinkMemorySizingKey(key string) bool {
	if !strings.HasPrefix(key, flinkConfigJobManagerMemoryPrefix) && !strings.HasPrefix(key, flinkConfigTaskManagerMemoryPrefix) {
		return false
	}
	return strings.HasSuffix(key, ".size") || strings.HasSuffix(key, ".min") || strings.HasSuffix(key, ".max")
}

func isStandaloneOnlyParallelismConfigKey(key string) bool {
	return standaloneOnlyParallelismConfigKeys.Has(key) ||
		strings.HasPrefix(key, flinkConfigVertexParallelismOverrides+".")
}

func isForbiddenFlinkPodPlacementOrMetadataKey(key string) bool {
	for _, prefix := range forbiddenFlinkPodPlacementAndMetadataKeys {
		if key == prefix || strings.HasPrefix(key, prefix+".") {
			return true
		}
	}
	return false
}

// parseFlinkMemoryQuantity mirrors Apache Flink Kubernetes Operator's
// ResourceConfigUtils.parseResourceMemoryString:
//  1. Attempt Apache Flink org.apache.flink.configuration.MemorySize parsing first,
//     preserving 1024-based k/m/g/t semantics (e.g. "2048m" -> 2Gi, "2g" -> 2Gi).
//  2. Otherwise normalize via formatMemoryStringForK8sSpec (strip whitespace, uppercase,
//     strip trailing "B", rewrite trailing "I" to "i") and fall back to Kubernetes
//     resource.ParseQuantity (accepting "2Gi", "512Mi", "2gib", etc.).
//
// Fractional quantities such as "1.5Gi" are rejected: the operator's Fabric8 fallback renders
// them as "1610612736.0", which Flink's MemorySize.parse rejects with unit '.0', so Flink
// Operator would permanently refuse a FlinkDeployment that Kueue already admitted.
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

	if qty, matched, err := tryParseFlinkMemorySize(trimmed, raw); matched {
		return qty, err
	}

	normalized := formatMemoryStringForK8sSpec(trimmed)
	if !isSupportedNormalizedK8sMemory(normalized) {
		return resource.Quantity{}, fmt.Errorf("invalid memory quantity %q: must use Flink MemorySize units or an integer Kubernetes suffix (Ki, Mi, Gi, Ti, Pi, K, M, G, T, P)", raw)
	}
	qty, err := resource.ParseQuantity(normalized)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid memory quantity %q: %w", raw, err)
	}
	if qty.Cmp(minMemoryByteQuantity) < 0 {
		return resource.Quantity{}, fmt.Errorf("memory size %q must be greater than zero", raw)
	}
	if qty.Cmp(maxMemoryByteQuantity) > 0 {
		return resource.Quantity{}, fmt.Errorf("memory size %q exceeds maximum supported Flink memory size (%d MiB)", raw, math.MaxInt32)
	}
	return qty, nil
}

func isSupportedNormalizedK8sMemory(normalized string) bool {
	digitEnd := 0
	for digitEnd < len(normalized) && normalized[digitEnd] >= '0' && normalized[digitEnd] <= '9' {
		digitEnd++
	}
	if digitEnd == 0 {
		return false
	}
	switch normalized[digitEnd:] {
	case "", "K", "M", "G", "T", "P", "Ki", "Mi", "Gi", "Ti", "Pi":
		return true
	default:
		return false
	}
}

func tryParseFlinkMemorySize(trimmed, raw string) (resource.Quantity, bool, error) {
	digitEnd := 0
	for digitEnd < len(trimmed) && trimmed[digitEnd] >= '0' && trimmed[digitEnd] <= '9' {
		digitEnd++
	}
	if digitEnd == 0 {
		return resource.Quantity{}, false, nil
	}

	unitPart := strings.TrimSpace(trimmed[digitEnd:])
	for _, r := range unitPart {
		if !unicode.IsLetter(r) {
			return resource.Quantity{}, false, nil
		}
	}

	var multiplier uint64
	switch strings.ToLower(unitPart) {
	case "", "b", "bytes":
		multiplier = 1
	case "k", "kb", "kibibytes":
		multiplier = 1 << 10
	case "m", "mb", "mebibytes":
		multiplier = 1 << 20
	case "g", "gb", "gibibytes":
		multiplier = 1 << 30
	case "t", "tb", "tebibytes":
		multiplier = 1 << 40
	default:
		return resource.Quantity{}, false, nil
	}

	val, err := strconv.ParseUint(trimmed[:digitEnd], 10, 64)
	if err != nil {
		return resource.Quantity{}, true, fmt.Errorf("memory size %q exceeds maximum supported Flink memory size (%d MiB): %w", raw, math.MaxInt32, err)
	}
	if val == 0 {
		return resource.Quantity{}, true, fmt.Errorf("memory size %q must be greater than zero", raw)
	}
	if val > uint64(maxFlinkMemoryBytes)/multiplier {
		return resource.Quantity{}, true, fmt.Errorf("memory size %q exceeds maximum supported Flink memory size (%d MiB)", raw, math.MaxInt32)
	}

	totalBytes := int64(val * multiplier)
	format := resource.DecimalSI
	if totalBytes%(1<<10) == 0 {
		format = resource.BinarySI
	}
	return *resource.NewQuantity(totalBytes, format), true, nil
}

func formatMemoryStringForK8sSpec(trimmed string) string {
	noSpaces := strings.Join(strings.Fields(trimmed), "")
	upper := strings.ToUpper(noSpaces)
	upper = strings.TrimSuffix(upper, "B")
	if before, ok := strings.CutSuffix(upper, "I"); ok {
		upper = before + "i"
	}
	return upper
}

// parseFlinkCPUQuantity converts a Flink floating-point CPU core value into a
// positive Kubernetes CPU resource.Quantity with milli-core precision.
func parseFlinkCPUQuantity(cpu float64) (resource.Quantity, error) {
	if math.IsNaN(cpu) || math.IsInf(cpu, 0) {
		return resource.Quantity{}, fmt.Errorf("cpu value %v must be a finite number", cpu)
	}
	if cpu <= 0 {
		return resource.Quantity{}, fmt.Errorf("cpu value %v must be greater than 0", cpu)
	}
	if cpu >= float64(math.MaxInt64/1000) {
		return resource.Quantity{}, fmt.Errorf("cpu value %v exceeds maximum supported cores", cpu)
	}
	formatted := strconv.FormatFloat(cpu, 'f', -1, 64)
	qty, err := resource.ParseQuantity(formatted)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid cpu value %v: %w", cpu, err)
	}
	milli := max(qty.MilliValue(), 1)
	return *resource.NewMilliQuantity(milli, resource.DecimalSI), nil
}

func parseEphemeralStorageQuantity(raw string) (resource.Quantity, error) {
	if strings.ContainsFunc(raw, unicode.IsSpace) {
		return resource.Quantity{}, fmt.Errorf("ephemeralStorage %q must not contain whitespace", raw)
	}
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid ephemeralStorage %q: %w", raw, err)
	}
	if q.Sign() <= 0 {
		return resource.Quantity{}, fmt.Errorf("ephemeralStorage %q must be greater than zero", raw)
	}
	return q, nil
}

// parseFlinkBool mirrors Apache Flink's ConfigurationUtils.convertToBoolean,
// accepting only exact "true" and "false" strings case-insensitively without
// trimming whitespace and rejecting Go strconv.ParseBool shorthands such as "1", "0", "t", or "f".
func parseFlinkBool(raw string) (bool, error) {
	switch strings.ToUpper(raw) {
	case "TRUE":
		return true, nil
	case "FALSE":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value %q: must be \"true\" or \"false\"", raw)
	}
}

func parsePositiveInt32(raw string, name string) (int32, error) {
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", name, raw, err)
	}
	if n < 1 {
		return 0, fmt.Errorf("%s must be greater than or equal to 1, got %d", name, n)
	}
	return int32(n), nil
}

func validateResourceRequirements(reqs *corev1.ResourceRequirements) error {
	if reqs == nil {
		return nil
	}
	for _, resName := range slices.Sorted(maps.Keys(reqs.Requests)) {
		if resName == corev1.ResourcePods {
			return fmt.Errorf("resource %q is reserved for internal Kueue use", corev1.ResourcePods)
		}
		qty := reqs.Requests[resName]
		if qty.Sign() < 0 {
			return fmt.Errorf("resource request %s (%s) must be non-negative", resName, qty.String())
		}
	}
	for _, resName := range slices.Sorted(maps.Keys(reqs.Limits)) {
		if resName == corev1.ResourcePods {
			return fmt.Errorf("resource %q is reserved for internal Kueue use", corev1.ResourcePods)
		}
		limitQty := reqs.Limits[resName]
		if limitQty.Sign() < 0 {
			return fmt.Errorf("resource limit %s (%s) must be non-negative", resName, limitQty.String())
		}
		if reqQty, hasReq := reqs.Requests[resName]; hasReq && reqQty.Cmp(limitQty) > 0 {
			return fmt.Errorf("resource request %s (%s) must be less than or equal to limit (%s)", resName, reqQty.String(), limitQty.String())
		}
	}
	return nil
}

// validateComponentMemoryQuantity requires spec.<role>.resources memory quantities to use a
// BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes so Kubernetes resource.Quantity
// (charged by Kueue), Apache Flink MemorySize (used by FlinkConfigBuilder.setResourceRequirements),
// and Fabric8 computeLimitFactor agree byte-for-byte. Flink rejects fractional literals (such as
// "1.5Gi") with MemorySize unit '.0', and parses "2048m", "2G", and "2k" as 1024-based powers
// whereas Kubernetes parses them as 2.048 bytes, 2e9 bytes, and 2000 bytes respectively.
func validateComponentMemoryQuantity(memQty flinkv1beta1.Quantity) error {
	raw := memQty.RawLiteral()
	if raw == "" {
		return errors.New("memory size cannot be empty")
	}
	if strings.ContainsFunc(raw, unicode.IsSpace) {
		return fmt.Errorf("memory size %q must not contain whitespace", raw)
	}
	if strings.Contains(raw, ".") {
		return fmt.Errorf("memory size %q must not contain a decimal point; must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes", raw)
	}

	digitEnd := 0
	for digitEnd < len(raw) && raw[digitEnd] >= '0' && raw[digitEnd] <= '9' {
		digitEnd++
	}
	if digitEnd == 0 {
		return fmt.Errorf("must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes, got %q", raw)
	}

	var multiplier uint64
	switch raw[digitEnd:] {
	case "":
		multiplier = 1
	case "Ki":
		multiplier = 1 << 10
	case "Mi":
		multiplier = 1 << 20
	case "Gi":
		multiplier = 1 << 30
	case "Ti":
		multiplier = 1 << 40
	case "Pi":
		multiplier = 1 << 50
	default:
		return fmt.Errorf("must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes because Flink interprets %q incompatibly or rejects it", raw)
	}

	val, err := strconv.ParseUint(raw[:digitEnd], 10, 64)
	if err != nil {
		return fmt.Errorf("memory size %q exceeds maximum supported Flink memory size (%d MiB): %w", raw, math.MaxInt32, err)
	}
	if val == 0 {
		return errors.New("must be at least 1 byte")
	}
	if val > uint64(maxFlinkMemoryBytes)/multiplier {
		return fmt.Errorf("memory size %q exceeds maximum supported Flink memory size (%d MiB)", raw, math.MaxInt32)
	}
	return nil
}

func validateComponentResourceListQuantities(list flinkv1beta1.ResourceList, kind string) error {
	for _, resName := range slices.Sorted(maps.Keys(list)) {
		if resName == corev1.ResourcePods {
			return fmt.Errorf("resource %q is reserved for internal Kueue use", corev1.ResourcePods)
		}
		qty := list[resName]
		if strings.ContainsFunc(qty.RawLiteral(), unicode.IsSpace) {
			return fmt.Errorf("resource %s %s (%q) must not contain whitespace", kind, resName, qty.RawLiteral())
		}
		if resName == corev1.ResourceMemory {
			if qty.ParseError() == nil {
				q := qty.AsQuantity()
				if q.Cmp(minMemoryByteQuantity) < 0 {
					return fmt.Errorf("resource %s memory (%s) must be at least 1 byte", kind, qty.RawLiteral())
				}
			}
			if err := validateComponentMemoryQuantity(qty); err != nil {
				return fmt.Errorf("resource %s memory (%s): %w", kind, qty.RawLiteral(), err)
			}
		}
		if err := qty.ParseError(); err != nil {
			return fmt.Errorf("invalid resource %s %s (%s): %w", kind, resName, qty.RawLiteral(), err)
		}
	}
	if cpuQty, ok := list[corev1.ResourceCPU]; ok {
		if cpuQty.Sign() <= 0 {
			return fmt.Errorf("resource %s cpu (%s) must be greater than 0", kind, cpuQty.RawLiteral())
		}
	}
	return nil
}

func validateComponentResourceRequirements(reqs *flinkv1beta1.ResourceRequirements) error {
	if reqs == nil {
		return nil
	}
	if err := validateComponentResourceListQuantities(reqs.Requests, "request"); err != nil {
		return err
	}
	if err := validateComponentResourceListQuantities(reqs.Limits, "limit"); err != nil {
		return err
	}
	return validateResourceRequirements(reqs.ToCore())
}

func validateResourceOverhead(overhead corev1.ResourceList) error {
	for _, resName := range slices.Sorted(maps.Keys(overhead)) {
		if resName == corev1.ResourcePods {
			return fmt.Errorf("overhead resource %q is reserved for internal Kueue use", corev1.ResourcePods)
		}
		qty := overhead[resName]
		if qty.Sign() < 0 {
			return fmt.Errorf("overhead resource %s (%s) must be non-negative", resName, qty.String())
		}
	}
	return nil
}

var supportedPullPolicies = []string{
	string(corev1.PullAlways),
	string(corev1.PullNever),
	string(corev1.PullIfNotPresent),
}

func isSupportedPullPolicy(policy corev1.PullPolicy) bool {
	switch policy {
	case corev1.PullAlways, corev1.PullNever, corev1.PullIfNotPresent:
		return true
	default:
		return false
	}
}

func validateContainerPullPolicy(c *corev1.Container) error {
	if c.ImagePullPolicy != "" && !isSupportedPullPolicy(c.ImagePullPolicy) {
		return fmt.Errorf("unsupported imagePullPolicy %q: supported values: %q, %q, %q",
			c.ImagePullPolicy, corev1.PullAlways, corev1.PullNever, corev1.PullIfNotPresent)
	}
	return nil
}

func validateResolvedContainer(c *corev1.Container) error {
	if strings.TrimSpace(c.Image) == "" {
		return errors.New("image must be specified")
	}
	if err := validateContainerPullPolicy(c); err != nil {
		return err
	}
	return validateResourceRequirements(&c.Resources)
}

func isSupportedPortProtocol(proto corev1.Protocol) bool {
	switch proto {
	case corev1.ProtocolTCP, corev1.ProtocolUDP, corev1.ProtocolSCTP:
		return true
	default:
		return false
	}
}

func validateContainerNameList(kind string, count int, nameAt func(int) string) error {
	if count == 0 {
		return nil
	}
	seen := make(map[string]struct{}, count)
	for i := range count {
		name := nameAt(i)
		if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
			return fmt.Errorf("%s[%d].name %q: %s", kind, i, name, strings.Join(errs, "; "))
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate %s name %q", kind, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validatePodTemplateContainerNames(podSpec *corev1.PodSpec) error {
	if err := validateContainerNameList("initContainers", len(podSpec.InitContainers), func(i int) string {
		return podSpec.InitContainers[i].Name
	}); err != nil {
		return err
	}
	if err := validateContainerNameList("containers", len(podSpec.Containers), func(i int) string {
		return podSpec.Containers[i].Name
	}); err != nil {
		return err
	}
	return validateContainerNameList("ephemeralContainers", len(podSpec.EphemeralContainers), func(i int) string {
		return podSpec.EphemeralContainers[i].Name
	})
}

func validateUniqueRequiredStringKey(listName, keyField string, count int, keyAt func(int) string) error {
	if count == 0 {
		return nil
	}
	seen := make(map[string]struct{}, count)
	for i := range count {
		k := keyAt(i)
		if k == "" {
			return fmt.Errorf("%s[%d].%s must not be empty", listName, i, keyField)
		}
		if _, exists := seen[k]; exists {
			return fmt.Errorf("duplicate %s %s %q", listName, keyField, k)
		}
		seen[k] = struct{}{}
	}
	return nil
}

func validateContainerListMaps(
	kind, containerName string,
	ports []corev1.ContainerPort,
	volumeMounts []corev1.VolumeMount,
	volumeDevices []corev1.VolumeDevice,
	claims []corev1.ResourceClaim,
	restartRules []corev1.ContainerRestartRule,
) error {
	type portKey struct {
		port     int32
		protocol corev1.Protocol
	}
	if len(ports) > 0 {
		seenPorts := make(map[portKey]struct{}, len(ports))
		for i, p := range ports {
			if p.ContainerPort < 1 || p.ContainerPort > 65535 {
				return fmt.Errorf("%s %q ports[%d].containerPort (%d) must be between 1 and 65535", kind, containerName, i, p.ContainerPort)
			}
			proto := p.Protocol
			if proto == "" {
				proto = corev1.ProtocolTCP
			}
			if !isSupportedPortProtocol(proto) {
				return fmt.Errorf("%s %q ports[%d].protocol %q: supported values: %q, %q, %q",
					kind, containerName, i, p.Protocol, corev1.ProtocolTCP, corev1.ProtocolUDP, corev1.ProtocolSCTP)
			}
			pk := portKey{port: p.ContainerPort, protocol: proto}
			if _, exists := seenPorts[pk]; exists {
				return fmt.Errorf("%s %q has duplicate port %d/%s", kind, containerName, p.ContainerPort, proto)
			}
			seenPorts[pk] = struct{}{}
		}
	}

	if len(volumeMounts) > 0 {
		seenMounts := make(map[string]struct{}, len(volumeMounts))
		for i, vm := range volumeMounts {
			if vm.Name == "" {
				return fmt.Errorf("%s %q volumeMounts[%d].name must not be empty", kind, containerName, i)
			}
			if vm.MountPath == "" {
				return fmt.Errorf("%s %q volumeMounts[%d].mountPath must not be empty", kind, containerName, i)
			}
			if _, exists := seenMounts[vm.MountPath]; exists {
				return fmt.Errorf("%s %q has duplicate volumeMount mountPath %q", kind, containerName, vm.MountPath)
			}
			seenMounts[vm.MountPath] = struct{}{}
			if len(vm.BindMountOptions) > 0 {
				seenOpts := make(map[string]struct{}, len(vm.BindMountOptions))
				for _, opt := range vm.BindMountOptions {
					if _, exists := seenOpts[opt]; exists {
						return fmt.Errorf("%s %q volumeMounts[%d].bindMountOptions has duplicate value %q", kind, containerName, i, opt)
					}
					seenOpts[opt] = struct{}{}
				}
			}
		}
	}

	if len(volumeDevices) > 0 {
		seenDevices := make(map[string]struct{}, len(volumeDevices))
		for i, vd := range volumeDevices {
			if vd.Name == "" {
				return fmt.Errorf("%s %q volumeDevices[%d].name must not be empty", kind, containerName, i)
			}
			if vd.DevicePath == "" {
				return fmt.Errorf("%s %q volumeDevices[%d].devicePath must not be empty", kind, containerName, i)
			}
			if _, exists := seenDevices[vd.DevicePath]; exists {
				return fmt.Errorf("%s %q has duplicate volumeDevice devicePath %q", kind, containerName, vd.DevicePath)
			}
			seenDevices[vd.DevicePath] = struct{}{}
		}
	}

	if err := validateUniqueRequiredStringKey(
		fmt.Sprintf("%s %q resources.claims", kind, containerName),
		"name",
		len(claims),
		func(i int) string { return claims[i].Name },
	); err != nil {
		return err
	}

	for i, rule := range restartRules {
		if rule.Action == "" {
			return fmt.Errorf("%s %q restartPolicyRules[%d].action must not be empty", kind, containerName, i)
		}
		if rule.ExitCodes != nil {
			if rule.ExitCodes.Operator == "" {
				return fmt.Errorf("%s %q restartPolicyRules[%d].exitCodes.operator must not be empty", kind, containerName, i)
			}
			seenCodes := make(map[int32]struct{}, len(rule.ExitCodes.Values))
			for _, code := range rule.ExitCodes.Values {
				if _, exists := seenCodes[code]; exists {
					return fmt.Errorf("%s %q restartPolicyRules[%d].exitCodes.values has duplicate value %d", kind, containerName, i, code)
				}
				seenCodes[code] = struct{}{}
			}
		}
	}

	return nil
}

func validatePodTemplateSpecListMaps(podSpec *corev1.PodSpec) error {
	for i := range podSpec.InitContainers {
		c := &podSpec.InitContainers[i]
		if err := validateContainerListMaps("initContainer", c.Name, c.Ports, c.VolumeMounts, c.VolumeDevices, c.Resources.Claims, c.RestartPolicyRules); err != nil {
			return err
		}
	}
	for i := range podSpec.Containers {
		c := &podSpec.Containers[i]
		if err := validateContainerListMaps("container", c.Name, c.Ports, c.VolumeMounts, c.VolumeDevices, c.Resources.Claims, c.RestartPolicyRules); err != nil {
			return err
		}
	}
	for i := range podSpec.EphemeralContainers {
		ec := &podSpec.EphemeralContainers[i]
		if err := validateContainerListMaps("ephemeralContainer", ec.Name, ec.Ports, ec.VolumeMounts, ec.VolumeDevices, ec.Resources.Claims, ec.RestartPolicyRules); err != nil {
			return err
		}
		if err := validateUniqueRequiredStringKey(
			fmt.Sprintf("ephemeralContainer %q env", ec.Name),
			"name",
			len(ec.Env),
			func(j int) string { return ec.Env[j].Name },
		); err != nil {
			return err
		}
	}

	if podSpec.Affinity != nil &&
		podSpec.Affinity.NodeAffinity != nil &&
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil &&
		len(podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
		return errors.New("affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms must not be empty")
	}

	if err := validateUniqueRequiredStringKey("volumes", "name", len(podSpec.Volumes), func(i int) string {
		return podSpec.Volumes[i].Name
	}); err != nil {
		return err
	}
	for i := range podSpec.Volumes {
		v := &podSpec.Volumes[i]
		if v.RBD != nil && len(v.RBD.CephMonitors) == 0 {
			return fmt.Errorf("volumes[%d].rbd.monitors must not be empty", i)
		}
		if v.CephFS != nil && len(v.CephFS.Monitors) == 0 {
			return fmt.Errorf("volumes[%d].cephfs.monitors must not be empty", i)
		}
		if v.ScaleIO != nil && v.ScaleIO.SecretRef == nil {
			return fmt.Errorf("volumes[%d].scaleIO.secretRef is required", i)
		}
		if v.Projected != nil {
			for j := range v.Projected.Sources {
				pc := v.Projected.Sources[j].PodCertificate
				if pc == nil {
					continue
				}
				if pc.KeyType == "" {
					return fmt.Errorf("volumes[%d].projected.sources[%d].podCertificate.keyType must not be empty", i, j)
				}
				if pc.SignerName == "" {
					return fmt.Errorf("volumes[%d].projected.sources[%d].podCertificate.signerName must not be empty", i, j)
				}
			}
		}
	}
	if err := validateUniqueRequiredStringKey("schedulingGates", "name", len(podSpec.SchedulingGates), func(i int) string {
		return podSpec.SchedulingGates[i].Name
	}); err != nil {
		return err
	}
	if err := validateUniqueRequiredStringKey("resourceClaims", "name", len(podSpec.ResourceClaims), func(i int) string {
		return podSpec.ResourceClaims[i].Name
	}); err != nil {
		return err
	}
	if podSpec.Resources != nil {
		if err := validateUniqueRequiredStringKey("resources.claims", "name", len(podSpec.Resources.Claims), func(i int) string {
			return podSpec.Resources.Claims[i].Name
		}); err != nil {
			return err
		}
	}
	if err := validateUniqueRequiredStringKey("imagePullSecrets", "name", len(podSpec.ImagePullSecrets), func(i int) string {
		return podSpec.ImagePullSecrets[i].Name
	}); err != nil {
		return err
	}
	if len(podSpec.EvictionResponders) > 10 {
		return fmt.Errorf("evictionResponders must have at most 10 items, got %d", len(podSpec.EvictionResponders))
	}
	if err := validateUniqueRequiredStringKey("evictionResponders", "name", len(podSpec.EvictionResponders), func(i int) string {
		return podSpec.EvictionResponders[i].Name
	}); err != nil {
		return err
	}
	for i, er := range podSpec.EvictionResponders {
		if er.Priority == nil {
			return fmt.Errorf("evictionResponders[%d].priority is required", i)
		}
		if *er.Priority < 0 || *er.Priority > 100000 {
			return fmt.Errorf("evictionResponders[%d].priority (%d) must be between 0 and 100000", i, *er.Priority)
		}
	}
	if err := validateUniqueRequiredStringKey("hostAliases", "ip", len(podSpec.HostAliases), func(i int) string {
		return podSpec.HostAliases[i].IP
	}); err != nil {
		return err
	}

	if len(podSpec.TopologySpreadConstraints) > 0 {
		type tscKey struct {
			topologyKey       string
			whenUnsatisfiable corev1.UnsatisfiableConstraintAction
		}
		seenTSC := make(map[tscKey]struct{}, len(podSpec.TopologySpreadConstraints))
		for i, tsc := range podSpec.TopologySpreadConstraints {
			if tsc.MaxSkew < 1 {
				return fmt.Errorf("topologySpreadConstraints[%d].maxSkew (%d) must be greater than or equal to 1", i, tsc.MaxSkew)
			}
			if tsc.TopologyKey == "" {
				return fmt.Errorf("topologySpreadConstraints[%d].topologyKey must not be empty", i)
			}
			if tsc.WhenUnsatisfiable == "" {
				return fmt.Errorf("topologySpreadConstraints[%d].whenUnsatisfiable must not be empty", i)
			}
			if tsc.WhenUnsatisfiable != corev1.DoNotSchedule && tsc.WhenUnsatisfiable != corev1.ScheduleAnyway {
				return fmt.Errorf("topologySpreadConstraints[%d].whenUnsatisfiable %q: supported values: %q, %q",
					i, tsc.WhenUnsatisfiable, corev1.DoNotSchedule, corev1.ScheduleAnyway)
			}
			tk := tscKey{topologyKey: tsc.TopologyKey, whenUnsatisfiable: tsc.WhenUnsatisfiable}
			if _, exists := seenTSC[tk]; exists {
				return fmt.Errorf("duplicate topologySpreadConstraints entry for topologyKey %q and whenUnsatisfiable %q",
					tsc.TopologyKey, tsc.WhenUnsatisfiable)
			}
			seenTSC[tk] = struct{}{}
		}
	}

	return nil
}

func validatePodTemplateMetadataAndPlacement(podTemplate *corev1.PodTemplateSpec, validatePodSetMeta bool) error {
	if validatePodSetMeta {
		if errs := metavalidation.ValidateLabels(podTemplate.Labels, field.NewPath("metadata", "labels")); len(errs) > 0 {
			return fmt.Errorf("pod labels: %w", errs.ToAggregate())
		}
		if errs := apivalidation.ValidateAnnotations(podTemplate.Annotations, field.NewPath("metadata", "annotations")); len(errs) > 0 {
			return fmt.Errorf("pod annotations: %w", errs.ToAggregate())
		}
	}
	if errs := metavalidation.ValidateLabels(podTemplate.Spec.NodeSelector, field.NewPath("spec", "nodeSelector")); len(errs) > 0 {
		return fmt.Errorf("pod nodeSelector: %w", errs.ToAggregate())
	}
	return nil
}

func validatePodTolerations(tolerations []corev1.Toleration) error {
	tolPath := field.NewPath("tolerations")
	for i, t := range tolerations {
		idxPath := tolPath.Index(i)
		if len(t.Key) > 0 {
			if errs := metavalidation.ValidateLabelName(t.Key, idxPath.Child("key")); len(errs) > 0 {
				return fmt.Errorf("toleration[%d]: %w", i, errs.ToAggregate())
			}
		}
		switch t.Operator {
		case "", corev1.TolerationOpEqual:
			if len(t.Key) == 0 {
				return fmt.Errorf("toleration[%d]: %w", i, field.Invalid(
					idxPath.Child("operator"),
					t.Operator,
					"operator must be Exists when key is empty",
				))
			}
			if len(t.Value) > 0 {
				if errs := validation.IsValidLabelValue(t.Value); len(errs) > 0 {
					return fmt.Errorf("toleration[%d]: %w", i, field.Invalid(
						idxPath.Child("value"),
						t.Value,
						strings.Join(errs, "; "),
					))
				}
			}
		case corev1.TolerationOpExists:
			if len(t.Value) > 0 {
				return fmt.Errorf("toleration[%d]: value must be empty when operator is Exists", i)
			}
		default:
			return fmt.Errorf("toleration[%d]: %w", i, field.NotSupported(
				idxPath.Child("operator"),
				t.Operator,
				[]string{string(corev1.TolerationOpEqual), string(corev1.TolerationOpExists)},
			))
		}
		switch t.Effect {
		case "", corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule:
			if t.TolerationSeconds != nil {
				return fmt.Errorf("toleration[%d]: %w", i, field.Invalid(
					idxPath.Child("effect"),
					t.Effect,
					"effect must be NoExecute when tolerationSeconds is set",
				))
			}
		case corev1.TaintEffectNoExecute:
		default:
			return fmt.Errorf("toleration[%d]: %w", i, field.NotSupported(
				idxPath.Child("effect"),
				t.Effect,
				[]string{
					string(corev1.TaintEffectNoSchedule),
					string(corev1.TaintEffectPreferNoSchedule),
					string(corev1.TaintEffectNoExecute),
				},
			))
		}
	}
	return nil
}

func hasResourceInRequirements(reqs *flinkv1beta1.ResourceRequirements, name corev1.ResourceName) bool {
	if reqs == nil {
		return false
	}
	if _, ok := reqs.Requests[name]; ok {
		return true
	}
	_, ok := reqs.Limits[name]
	return ok
}

// applyComponentResources applies component-level resource settings onto the target
// container. Setting both spec.<role>.resource and spec.<role>.resources on the same
// component is rejected; otherwise spec.<role>.resources (requests and limits) or
// spec.<role>.resource overrides the corresponding resource entries on "flink-main-container".
//
// For cpu and memory, spec.<role>.resources mirrors Apache Flink's
// KubernetesUtils.getResourceRequirements, which unconditionally overwrites both requests and
// limits on flink-main-container: an omitted request falls back to the limit
// (ResourceConfigUtils.getRequestOrLimit) and an omitted limit defaults to limitFactor 1.0,
// so inherited podTemplate cpu and memory entries are discarded rather than merged.
func applyComponentResources(container *corev1.Container, comp *flinkv1beta1.ComponentPodSpec) error {
	if comp == nil {
		return nil
	}
	if comp.Resource != nil && comp.Resources != nil {
		return errors.New("resource and resources cannot both be specified on the same component")
	}
	if comp.Resources != nil {
		if err := validateComponentResourceRequirements(comp.Resources); err != nil {
			return err
		}
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
			if hasResourceInRequirements(comp.Resources, name) {
				delete(container.Resources.Requests, name)
				delete(container.Resources.Limits, name)
			}
		}
		if len(comp.Resources.Requests) > 0 {
			if container.Resources.Requests == nil {
				container.Resources.Requests = make(corev1.ResourceList, len(comp.Resources.Requests))
			}
			for k, v := range comp.Resources.Requests {
				container.Resources.Requests[k] = v.AsQuantity()
			}
		}
		if len(comp.Resources.Limits) > 0 {
			if container.Resources.Limits == nil {
				container.Resources.Limits = make(corev1.ResourceList, len(comp.Resources.Limits))
			}
			for k, v := range comp.Resources.Limits {
				container.Resources.Limits[k] = v.AsQuantity()
			}
		}
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
			if !hasResourceInRequirements(comp.Resources, name) {
				continue
			}
			reqQty, hasReq := comp.Resources.Requests[name]
			limQty, hasLim := comp.Resources.Limits[name]
			switch {
			case hasReq && !hasLim:
				if container.Resources.Limits == nil {
					container.Resources.Limits = make(corev1.ResourceList)
				}
				container.Resources.Limits[name] = reqQty.AsQuantity()
			case hasLim && !hasReq:
				if container.Resources.Requests == nil {
					container.Resources.Requests = make(corev1.ResourceList)
				}
				container.Resources.Requests[name] = limQty.AsQuantity()
			}
		}
		return nil
	}

	if comp.Resource == nil {
		return nil
	}

	ensureReqLim := func() {
		if container.Resources.Requests == nil {
			container.Resources.Requests = make(corev1.ResourceList)
		}
		if container.Resources.Limits == nil {
			container.Resources.Limits = make(corev1.ResourceList)
		}
	}

	if comp.Resource.CPU != nil {
		cpuQty, err := parseFlinkCPUQuantity(*comp.Resource.CPU)
		if err != nil {
			return err
		}
		ensureReqLim()
		container.Resources.Requests[corev1.ResourceCPU] = cpuQty
		container.Resources.Limits[corev1.ResourceCPU] = cpuQty.DeepCopy()
	}

	if strings.TrimSpace(comp.Resource.Memory) != "" {
		memQty, err := parseFlinkMemoryQuantity(comp.Resource.Memory)
		if err != nil {
			return err
		}
		ensureReqLim()
		container.Resources.Requests[corev1.ResourceMemory] = memQty
		container.Resources.Limits[corev1.ResourceMemory] = memQty.DeepCopy()
	}

	if comp.Resource.EphemeralStorage != "" {
		storageQty, err := parseEphemeralStorageQuantity(comp.Resource.EphemeralStorage)
		if err != nil {
			return err
		}
		ensureReqLim()
		container.Resources.Requests[corev1.ResourceEphemeralStorage] = storageQty
		container.Resources.Limits[corev1.ResourceEphemeralStorage] = storageQty.DeepCopy()
	}

	return nil
}

// applyDefaultComponentCPU mirrors Apache Flink's Kubernetes pod decorators when neither
// spec.<role>.resource.cpu nor spec.<role>.resources CPU is specified:
//   - JobManager defaults to 1 CPU core (in both native and standalone modes).
//   - TaskManager defaults to taskmanager.numberOfTaskSlots CPU cores in native mode
//     (standalone mode requires explicit TaskManager CPU in webhook validation).
func applyDefaultComponentCPU(
	container *corev1.Container,
	comp *flinkv1beta1.ComponentPodSpec,
	defaultCores int64,
) {
	if comp != nil {
		if comp.Resource != nil && comp.Resource.CPU != nil {
			return
		}
		if hasResourceInRequirements(comp.Resources, corev1.ResourceCPU) {
			return
		}
	}

	cpuQty := *resource.NewQuantity(defaultCores, resource.DecimalSI)
	if container.Resources.Requests == nil {
		container.Resources.Requests = make(corev1.ResourceList)
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = make(corev1.ResourceList)
	}
	container.Resources.Requests[corev1.ResourceCPU] = cpuQty
	container.Resources.Limits[corev1.ResourceCPU] = cpuQty.DeepCopy()
}
