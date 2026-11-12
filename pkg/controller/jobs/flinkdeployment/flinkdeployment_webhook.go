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
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	kueueconstants "sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	"sigs.k8s.io/kueue/pkg/util/equality"
	utilslices "sigs.k8s.io/kueue/pkg/util/slices"
	"sigs.k8s.io/kueue/pkg/util/webhook"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var (
	labelsPath             = field.NewPath("metadata", "labels")
	specPath               = field.NewPath("spec")
	jobSpecPath            = specPath.Child("job")
	jobManagerSpecPath     = specPath.Child("jobManager")
	taskManagerSpecPath    = specPath.Child("taskManager")
	podTemplateSpecPath    = specPath.Child("podTemplate")
	flinkConfigurationPath = specPath.Child("flinkConfiguration")
)

type flinkDeploymentWebhook struct {
	integrationManager           *jobframework.IntegrationManager
	client                       client.Client
	queues                       *qcache.Manager
	manageJobsWithoutQueueName   bool
	managedJobsNamespaceSelector labels.Selector
	maxTimeoutOnWorkload         *metav1.Duration
}

// SetupWebhook configures the mutating and validating admission webhooks for FlinkDeployment.
func SetupWebhook(mgr ctrl.Manager, opts ...jobframework.Option) error {
	options := jobframework.ProcessOptions(opts...)
	if options.IntegrationManager == nil {
		options.IntegrationManager = jobframework.NewIntegrationManager()
	}
	wh := &flinkDeploymentWebhook{
		integrationManager:           options.IntegrationManager,
		client:                       mgr.GetClient(),
		queues:                       options.Queues,
		manageJobsWithoutQueueName:   options.ManageJobsWithoutQueueName,
		managedJobsNamespaceSelector: options.ManagedJobsNamespaceSelector,
		maxTimeoutOnWorkload:         options.MaxTimeoutOnWorkload,
	}
	obj := &flinkv1beta1.FlinkDeployment{}
	if options.NoopWebhook {
		return webhook.SetupNoopWebhook(mgr, obj)
	}
	return ctrl.NewWebhookManagedBy(mgr, obj).
		WithValidator(wh).
		WithDefaulter(wh).
		WithLogConstructor(jobframework.WebhookLogConstructor(gvk, options.RoleTracker)).
		Complete()
}

func (w *flinkDeploymentWebhook) effectiveIntegrationManager() *jobframework.IntegrationManager {
	if w.integrationManager != nil {
		return w.integrationManager
	}
	return jobframework.NewIntegrationManager()
}

func (w *flinkDeploymentWebhook) defaultQueueExistFunc() func(string) bool {
	if w.queues != nil {
		return w.queues.DefaultLocalQueueExist
	}
	return func(string) bool {
		return false
	}
}

// +kubebuilder:webhook:path=/mutate-flink-apache-org-v1beta1-flinkdeployment,mutating=true,failurePolicy=fail,sideEffects=None,groups=flink.apache.org,resources=flinkdeployments,verbs=create,versions=v1beta1,name=mflinkdeployment.kb.io,admissionReviewVersions=v1

var _ admission.Defaulter[*flinkv1beta1.FlinkDeployment] = &flinkDeploymentWebhook{}

// Default implements admission.Defaulter for FlinkDeployment.
func (w *flinkDeploymentWebhook) Default(ctx context.Context, obj *flinkv1beta1.FlinkDeployment) error {
	if obj.Spec.Job == nil {
		return nil
	}

	job := fromObject(obj)
	log := ctrl.LoggerFrom(ctx).WithName("flinkdeployment-webhook")
	log.V(5).Info("Applying defaults")

	integrationMgr := w.effectiveIntegrationManager()
	if err := integrationMgr.ApplyDefaultLocalQueue(ctx, w.client, job.Object(), w.defaultQueueExistFunc(), w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	if err := integrationMgr.ApplyDefaultWorkloadPriorityClass(ctx, w.client, job.Object(), w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	if err := integrationMgr.ApplyDefaultForSuspend(ctx, job, w.client, w.manageJobsWithoutQueueName, w.managedJobsNamespaceSelector); err != nil {
		return err
	}

	return nil
}

// +kubebuilder:webhook:path=/validate-flink-apache-org-v1beta1-flinkdeployment,mutating=false,failurePolicy=fail,sideEffects=None,groups=flink.apache.org,resources=flinkdeployments,verbs=create;update,versions=v1beta1,name=vflinkdeployment.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*flinkv1beta1.FlinkDeployment] = &flinkDeploymentWebhook{}

// ValidateCreate implements admission.Validator for FlinkDeployment creation.
func (w *flinkDeploymentWebhook) ValidateCreate(ctx context.Context, obj *flinkv1beta1.FlinkDeployment) (admission.Warnings, error) {
	log := ctrl.LoggerFrom(ctx).WithName("flinkdeployment-webhook")
	log.V(5).Info("Validating create")
	validationErrs, err := w.validateCreate(ctx, obj, w.maxTimeoutOnWorkload)
	if err != nil {
		return nil, err
	}
	return nil, validationErrs.ToAggregate()
}

func validateComponentSpec(comp *flinkv1beta1.ComponentPodSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if comp == nil {
		return nil
	}

	if comp.Replicas != nil {
		switch {
		case *comp.Replicas < 1:
			allErrs = append(allErrs, field.Invalid(fldPath.Child("replicas"), *comp.Replicas, "must be greater than or equal to 1"))
		case *comp.Replicas > math.MaxInt32:
			allErrs = append(allErrs, field.Invalid(fldPath.Child("replicas"), *comp.Replicas, fmt.Sprintf("must be less than or equal to %d", math.MaxInt32)))
		}
	}

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
			allErrs = append(allErrs, field.Invalid(fldPath.Child("resources"), comp.Resources, err.Error()))
		}
	} else if comp.Resource != nil {
		resPath := fldPath.Child("resource")
		if comp.Resource.CPU != nil {
			if _, err := parseFlinkCPUQuantity(*comp.Resource.CPU); err != nil {
				allErrs = append(allErrs, field.Invalid(resPath.Child("cpu"), *comp.Resource.CPU, err.Error()))
			}
		}
		if strings.TrimSpace(comp.Resource.Memory) != "" {
			if _, err := parseFlinkMemoryQuantity(comp.Resource.Memory); err != nil {
				allErrs = append(allErrs, field.Invalid(resPath.Child("memory"), comp.Resource.Memory, err.Error()))
			}
		}
		if comp.Resource.EphemeralStorage != "" {
			if _, err := parseEphemeralStorageQuantity(comp.Resource.EphemeralStorage); err != nil {
				allErrs = append(allErrs, field.Invalid(resPath.Child("ephemeralStorage"), comp.Resource.EphemeralStorage, err.Error()))
			}
		}
	}

	return allErrs
}

func validateFlinkConfiguration(spec *flinkv1beta1.FlinkDeploymentSpec) field.ErrorList {
	flatCfg, err := spec.FlinkConfiguration.FlatMap()
	if err != nil {
		return field.ErrorList{field.Invalid(flinkConfigurationPath, field.OmitValueType{}, err.Error())}
	}
	if len(flatCfg) == 0 {
		return nil
	}

	var allErrs field.ErrorList
	for _, key := range slices.Sorted(maps.Keys(flatCfg)) {
		val := flatCfg[key]

		switch {
		case key == flinkConfigAutoscalerEnabled || key == flinkConfigOperatorAutoscalerEnabled:
			if fieldErr := validateForbiddenFlinkBoolKey(
				key,
				val,
				"dynamic Flink autoscaling is not supported for a Kueue-managed FlinkDeployment",
			); fieldErr != nil {
				allErrs = append(allErrs, fieldErr)
			}
		case unsupportedFlinkConfigSizingKeys.Has(key) || isForbiddenFlinkMemorySizingKey(key):
			allErrs = append(allErrs, field.Forbidden(
				flinkConfigurationPath.Key(key),
				"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
			))
		case isForbiddenFlinkPodPlacementOrMetadataKey(key):
			allErrs = append(allErrs, field.Forbidden(
				flinkConfigurationPath.Key(key),
				"configure pod templates, placement, tolerations, labels, and annotations in spec.podTemplate, spec.jobManager.podTemplate, or spec.taskManager.podTemplate instead of spec.flinkConfiguration",
			))
		case forbiddenFlinkPodIdentityImageAndNetworkKeys.Has(key):
			allErrs = append(allErrs, field.Forbidden(
				flinkConfigurationPath.Key(key),
				"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
			))
		case key == flinkConfigExternalResources || strings.HasPrefix(key, flinkConfigExternalResourcePrefix):
			allErrs = append(allErrs, field.Forbidden(
				flinkConfigurationPath.Key(key),
				"configure extended resources such as GPUs in spec.jobManager.resources, spec.taskManager.resources, or podTemplate instead of spec.flinkConfiguration",
			))
		case isStandaloneOnlyParallelismConfigKey(key):
			if spec.Mode != flinkv1beta1.KubernetesDeploymentModeStandalone || spec.TaskManager == nil || spec.TaskManager.Replicas == nil {
				allErrs = append(allErrs, field.Forbidden(
					flinkConfigurationPath.Key(key),
					fmt.Sprintf("spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when %s is configured", key),
				))
			}
		case key == flinkConfigNumberOfTaskSlots:
			if _, err := parsePositiveInt32(val, flinkConfigNumberOfTaskSlots); err != nil {
				allErrs = append(allErrs, field.Invalid(flinkConfigurationPath.Key(key), val, err.Error()))
			}
		case key == flinkConfigPodTemplateMergeArraysByName:
			if _, err := parseFlinkBool(val); err != nil {
				allErrs = append(allErrs, field.Invalid(flinkConfigurationPath.Key(key), val, err.Error()))
			}
		case key == flinkConfigOperatorJobRestartFailed:
			if fieldErr := validateForbiddenFlinkBoolKey(
				key,
				val,
				"the Flink Operator would resubmit a FAILED job after Kueue has released its quota; rely on Kueue requeueing and backoff (for example via waitForPodsReady) instead",
			); fieldErr != nil {
				allErrs = append(allErrs, fieldErr)
			}
		case key == flinkConfigOperatorDeploymentRollbackEnabled:
			if fieldErr := validateForbiddenFlinkBoolKey(
				key,
				val,
				"automatic Flink Operator deployment rollback is not supported for a Kueue-managed FlinkDeployment because in-memory rollback restores lastStableSpec placement and parallelism without updating spec",
			); fieldErr != nil {
				allErrs = append(allErrs, fieldErr)
			}
		}
	}

	return allErrs
}

func validateForbiddenFlinkBoolKey(key, val, forbiddenMsg string) *field.Error {
	enabled, err := parseFlinkBool(val)
	if err != nil {
		return field.Invalid(flinkConfigurationPath.Key(key), val, err.Error())
	}
	if enabled {
		return field.Forbidden(flinkConfigurationPath.Key(key), forbiddenMsg)
	}
	return nil
}

func componentMissingMemoryPath(comp *flinkv1beta1.ComponentPodSpec, rolePath *field.Path) *field.Path {
	if comp != nil && comp.Resources != nil {
		return rolePath.Child("resources", "requests").Key(string(corev1.ResourceMemory))
	}
	return rolePath.Child("resource", "memory")
}

func componentMissingCPUPath(comp *flinkv1beta1.ComponentPodSpec, rolePath *field.Path) *field.Path {
	if comp != nil && comp.Resources != nil {
		return rolePath.Child("resources", "requests").Key(string(corev1.ResourceCPU))
	}
	return rolePath.Child("resource", "cpu")
}

func hasExplicitComponentMemory(comp *flinkv1beta1.ComponentPodSpec) bool {
	if comp == nil {
		return false
	}
	if comp.Resource != nil && strings.TrimSpace(comp.Resource.Memory) != "" {
		return true
	}
	return hasResourceInRequirements(comp.Resources, corev1.ResourceMemory)
}

func hasExplicitComponentCPU(comp *flinkv1beta1.ComponentPodSpec) bool {
	if comp == nil {
		return false
	}
	if comp.Resource != nil && comp.Resource.CPU != nil {
		return true
	}
	return hasResourceInRequirements(comp.Resources, corev1.ResourceCPU)
}

func validateComponentMemorySpecified(comp *flinkv1beta1.ComponentPodSpec, rolePath *field.Path) field.ErrorList {
	if hasExplicitComponentMemory(comp) {
		return nil
	}
	return field.ErrorList{field.Required(
		componentMissingMemoryPath(comp, rolePath),
		fmt.Sprintf("memory must be specified via %s.resource.memory or %s.resources", rolePath, rolePath),
	)}
}

func hasMainContainerImageInPodTemplates(spec *flinkv1beta1.FlinkDeploymentSpec) bool {
	var jmTemplate, tmTemplate *flinkv1beta1.PodTemplateSpec
	if spec.JobManager != nil {
		jmTemplate = spec.JobManager.PodTemplate
	}
	if spec.TaskManager != nil {
		tmTemplate = spec.TaskManager.PodTemplate
	}
	if spec.PodTemplate == nil && (jmTemplate == nil || tmTemplate == nil) {
		return false
	}
	var mergeArraysByName bool
	if flatCfg, err := spec.FlinkConfiguration.FlatMap(); err == nil {
		if parsed, mergeErr := podTemplateMergeArraysByNameFromConfig(flatCfg); mergeErr == nil {
			mergeArraysByName = parsed
		}
	}
	for _, roleTemplate := range []*flinkv1beta1.PodTemplateSpec{jmTemplate, tmTemplate} {
		merged, err := mergePodTemplates(spec.PodTemplate, roleTemplate, mergeArraysByName)
		if err != nil {
			// Defer malformed podTemplate JSON errors to buildPodSets.
			return true
		}
		idx := slices.IndexFunc(merged.Spec.Containers, func(c corev1.Container) bool {
			return c.Name == flinkMainContainerName
		})
		if idx < 0 || strings.TrimSpace(merged.Spec.Containers[idx].Image) == "" {
			return false
		}
	}
	return true
}

func validateManagedFlinkSpec(kueueJob *flinkDeployment) field.ErrorList {
	var allErrs field.ErrorList
	spec := &kueueJob.Spec

	if spec.Job == nil {
		allErrs = append(allErrs, field.Required(
			jobSpecPath,
			"only Application mode (spec.job != nil) is supported for a Kueue-managed FlinkDeployment",
		))
		return allErrs
	}

	if strings.TrimSpace(spec.Image) == "" {
		if spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone {
			allErrs = append(allErrs, field.Required(specPath.Child("image"), "image must be specified when spec.mode is standalone"))
		} else if !hasMainContainerImageInPodTemplates(spec) {
			allErrs = append(allErrs, field.Required(
				specPath.Child("image"),
				fmt.Sprintf("image must be specified via spec.image or %s in podTemplate", flinkMainContainerName),
			))
		}
	}
	if strings.TrimSpace(string(spec.ImagePullPolicy)) != "" && !isSupportedPullPolicy(spec.ImagePullPolicy) {
		allErrs = append(allErrs, field.NotSupported(specPath.Child("imagePullPolicy"), spec.ImagePullPolicy, supportedPullPolicies))
	}

	hasExplicitTMReplicas := spec.TaskManager != nil && spec.TaskManager.Replicas != nil
	if spec.Job.Parallelism == nil {
		if !hasExplicitTMReplicas {
			allErrs = append(allErrs, field.Required(
				jobSpecPath.Child("parallelism"),
				"must be specified when spec.taskManager.replicas is not set",
			))
		}
	} else {
		switch {
		case *spec.Job.Parallelism < 1:
			allErrs = append(allErrs, field.Invalid(jobSpecPath.Child("parallelism"), *spec.Job.Parallelism, "must be greater than or equal to 1"))
		case *spec.Job.Parallelism > math.MaxInt32:
			allErrs = append(allErrs, field.Invalid(jobSpecPath.Child("parallelism"), *spec.Job.Parallelism, fmt.Sprintf("must be less than or equal to %d", math.MaxInt32)))
		}
	}

	var jmComp, tmComp *flinkv1beta1.ComponentPodSpec
	if spec.JobManager != nil {
		jmComp = &spec.JobManager.ComponentPodSpec
		allErrs = append(allErrs, validateComponentSpec(jmComp, jobManagerSpecPath)...)
	}
	if spec.TaskManager != nil {
		tmComp = &spec.TaskManager.ComponentPodSpec
		allErrs = append(allErrs, validateComponentSpec(tmComp, taskManagerSpecPath)...)
	}

	allErrs = append(allErrs, validateComponentMemorySpecified(jmComp, jobManagerSpecPath)...)
	allErrs = append(allErrs, validateComponentMemorySpecified(tmComp, taskManagerSpecPath)...)
	if spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone && !hasExplicitComponentCPU(tmComp) {
		allErrs = append(allErrs, field.Required(
			componentMissingCPUPath(tmComp, taskManagerSpecPath),
			"cpu must be specified via spec.taskManager.resource.cpu or spec.taskManager.resources when spec.mode is standalone",
		))
	}

	allErrs = append(allErrs, validateFlinkConfiguration(spec)...)

	if len(allErrs) == 0 {
		if _, err := kueueJob.buildPodSets(); err != nil {
			allErrs = append(allErrs, field.Invalid(specPath, field.OmitValueType{}, err.Error()))
		}
	}

	return allErrs
}

func (w *flinkDeploymentWebhook) isManagedByKueue(ctx context.Context, kueueJob *flinkDeployment) (bool, error) {
	if kueueJob.Spec.Job == nil {
		return jobframework.QueueName(kueueJob) != "", nil
	}
	if !w.manageJobsWithoutQueueName && jobframework.QueueName(kueueJob) == "" {
		return false, nil
	}
	managed, err := w.effectiveIntegrationManager().WorkloadShouldBeSuspended(
		ctx,
		kueueJob.Object(),
		w.client,
		w.manageJobsWithoutQueueName,
		w.managedJobsNamespaceSelector,
	)
	if err != nil {
		if errors.Is(err, jobframework.ErrWorkloadOwnerNotFound) &&
			!kueueJob.DeletionTimestamp.IsZero() &&
			features.Enabled(features.SkipAncestorCheckForDeletedWorkloads) {
			return false, nil
		}
		return false, err
	}
	return managed, nil
}

func (w *flinkDeploymentWebhook) validateCreate(ctx context.Context, obj *flinkv1beta1.FlinkDeployment, maxTimeoutOnWorkload *metav1.Duration) (field.ErrorList, error) {
	if obj.Spec.Job == nil {
		if _, hasQueueLabel := obj.Labels[constants.QueueLabel]; !hasQueueLabel {
			return nil, nil
		}
	}
	kueueJob := fromObject(obj)
	isManaged, err := w.isManagedByKueue(ctx, kueueJob)
	if err != nil {
		return nil, err
	}
	return w.validateJobAndSpec(kueueJob, isManaged, maxTimeoutOnWorkload)
}

func (w *flinkDeploymentWebhook) validateJobAndSpec(kueueJob *flinkDeployment, isManaged bool, maxTimeoutOnWorkload *metav1.Duration) (field.ErrorList, error) {
	var allErrors field.ErrorList
	var specErrs field.ErrorList
	if isManaged {
		specErrs = validateManagedFlinkSpec(kueueJob)
		allErrors = append(allErrors, specErrs...)
		if kueueJob.Spec.Job == nil {
			return allErrors, nil
		}
	}

	allErrors = append(allErrors, jobframework.ValidateJobOnCreate(kueueJob, maxTimeoutOnWorkload)...)
	if isManaged && len(specErrs) == 0 && features.Enabled(features.TopologyAwareScheduling) {
		validationErrs, err := w.validateTopologyRequest(kueueJob)
		if err != nil {
			return nil, err
		}
		allErrors = append(allErrors, validationErrs...)
	}

	return allErrors, nil
}

var tasPodSetAnnotationKeys = []string{
	kueue.PodSetRequiredTopologyAnnotation,
	kueue.PodSetPreferredTopologyAnnotation,
	kueue.PodSetUnconstrainedTopologyAnnotation,
	kueue.PodSetSliceRequiredTopologyAnnotation,
	kueue.PodSetSliceSizeAnnotation,
	kueue.PodSetSliceRequiredTopologyConstraintsAnnotation,
	kueue.PodSetGroupName,
	kueue.PodIndexOffsetAnnotation,
	kueue.PodSetTopologySpreadingAnnotation,
}

func hasTASPodSetAnnotation(annotations map[string]string) bool {
	for _, key := range tasPodSetAnnotationKeys {
		if _, ok := annotations[key]; ok {
			return true
		}
	}
	return false
}

// tasPodSetMetaPath attributes TAS validation errors for a PodSet to the template that supplies
// its TAS annotations: the role podTemplate when it carries any, otherwise the root spec.podTemplate.
func tasPodSetMetaPath(baseTemplate, roleTemplate *flinkv1beta1.PodTemplateSpec, rolePath *field.Path) *field.Path {
	roleMetaPath := rolePath.Child("podTemplate", "metadata")
	if roleTemplate != nil && hasTASPodSetAnnotation(roleTemplate.Annotations) {
		return roleMetaPath
	}
	if baseTemplate != nil && hasTASPodSetAnnotation(baseTemplate.Annotations) {
		return podTemplateSpecPath.Child("metadata")
	}
	return roleMetaPath
}

// deduplicateFieldErrors drops repeated errors: both PodSets inherit the root spec.podTemplate,
// so an invalid root TAS annotation would otherwise be reported once per PodSet.
func deduplicateFieldErrors(errs field.ErrorList) field.ErrorList {
	type errKey struct {
		errType field.ErrorType
		field   string
		detail  string
	}
	seen := make(map[errKey]struct{}, len(errs))
	out := make(field.ErrorList, 0, len(errs))
	for _, fe := range errs {
		k := errKey{errType: fe.Type, field: fe.Field, detail: fe.Detail}
		if _, exists := seen[k]; exists {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, fe)
	}
	return out
}

func (w *flinkDeploymentWebhook) validateTopologyRequest(flinkDep *flinkDeployment) (field.ErrorList, error) {
	podSets, podSetsErr := flinkDep.buildPodSets()
	if podSetsErr != nil {
		return field.ErrorList{field.Invalid(specPath, field.OmitValueType{}, podSetsErr.Error())}, nil
	}

	var allErrs field.ErrorList
	podSetAnnotationsByName := make(map[kueue.PodSetReference]*field.Path, podSetsCount)
	for i := range podSets {
		ps := &podSets[i]
		_ = setTopologyRequestIfEnabled(ps)

		var roleTemplate *flinkv1beta1.PodTemplateSpec
		var rolePath *field.Path
		switch ps.Name {
		case jobManagerPodSetName:
			rolePath = jobManagerSpecPath
			if flinkDep.Spec.JobManager != nil {
				roleTemplate = flinkDep.Spec.JobManager.PodTemplate
			}
		case taskManagerPodSetName:
			rolePath = taskManagerSpecPath
			if flinkDep.Spec.TaskManager != nil {
				roleTemplate = flinkDep.Spec.TaskManager.PodTemplate
			}
		default:
			continue
		}

		metaPath := tasPodSetMetaPath(flinkDep.Spec.PodTemplate, roleTemplate, rolePath)
		podSetAnnotationsByName[ps.Name] = metaPath.Child("annotations")
		allErrs = append(allErrs, jobframework.ValidateTASPodSetRequest(metaPath, &ps.Template.ObjectMeta)...)
		allErrs = append(allErrs, jobframework.ValidateSliceSizeAnnotationUpperBound(metaPath, &ps.Template.ObjectMeta, ps)...)
	}

	allErrs = append(allErrs, jobframework.ValidatePodSetGroupingTopology(podSets, podSetAnnotationsByName)...)
	return deduplicateFieldErrors(allErrs), nil
}

// ValidateUpdate implements admission.Validator for FlinkDeployment updates.
func (w *flinkDeploymentWebhook) ValidateUpdate(ctx context.Context, oldObj, newObj *flinkv1beta1.FlinkDeployment) (admission.Warnings, error) {
	oldJob := fromObject(oldObj)
	newJob := fromObject(newObj)
	oldManaged, err := w.isManagedByKueue(ctx, oldJob)
	if err != nil {
		return nil, err
	}
	newManaged, err := w.isManagedByKueue(ctx, newJob)
	if err != nil {
		return nil, err
	}
	if !oldManaged && !newManaged {
		return nil, nil
	}

	log := ctrl.LoggerFrom(ctx).WithName("flinkdeployment-webhook")
	log.V(5).Info("Validating update")

	// A pre-existing Session cluster (spec.job == nil) that still carries a queue-name label
	// (for example, created before the webhook was enabled) can never be managed by Kueue, and
	// IsSuspended() is always false for it, so the queue-name immutability and default-queue
	// rules in ValidateJobOnUpdate would wedge the label forever. Nothing can re-attach the label
	// on UPDATE either: Default() returns early for spec.job == nil and the mutating webhook is
	// create-only. Allow dropping the label outright, regardless of a default LocalQueue.
	if oldObj.Spec.Job == nil && newObj.Spec.Job == nil && jobframework.QueueName(newJob) == "" &&
		(newObj.DeletionTimestamp.IsZero() || specsEqualWithRawFidelity(oldObj.Spec, newObj.Spec)) {
		return nil, nil
	}

	var wl *kueue.Workload
	var hasFinishedWorkload bool
	if oldManaged && oldObj.Spec.Job != nil {
		wl, hasFinishedWorkload, err = w.findOwnedNonFinishedWorkload(ctx, oldObj)
		if err != nil {
			return nil, err
		}
	}
	_, _, oldFinished := oldJob.Finished(ctx)
	isFinished := hasFinishedWorkload || oldFinished
	isEvicted := wl != nil && apimeta.IsStatusConditionTrue(wl.Status.Conditions, kueue.WorkloadEvicted)
	isAdmitted := wl != nil && workload.IsAdmitted(wl) && !isEvicted
	hasQuotaReservation := (wl != nil && workload.HasQuotaReservation(wl)) || isAdmitted

	allErrors := jobframework.ValidateJobOnUpdate(oldJob, newJob, w.defaultQueueExistFunc(), w.maxTimeoutOnWorkload)
	if (!oldJob.IsSuspended() || hasQuotaReservation) && newJob.IsSuspended() {
		allErrors = append(allErrors, validateRunningToSuspendedJobMeta(oldJob, newJob)...)
	}
	maxExecImmutable := !oldJob.IsSuspended() || !newJob.IsSuspended() || hasQuotaReservation || (newManaged && isFinished)
	if oldJob.IsSuspended() && newJob.IsSuspended() && (hasQuotaReservation || (newManaged && isFinished)) {
		allErrors = append(allErrors, apivalidation.ValidateImmutableField(
			newObj.GetLabels()[constants.MaxExecTimeSecondsLabel],
			oldObj.GetLabels()[constants.MaxExecTimeSecondsLabel],
			labelsPath.Key(constants.MaxExecTimeSecondsLabel),
		)...)
	}
	if (oldObj.Spec.Job == nil) != (newObj.Spec.Job == nil) {
		allErrors = append(allErrors, field.Invalid(
			jobSpecPath,
			field.OmitValueType{},
			"cannot switch between Session and Application mode",
		))
		return nil, allErrors.ToAggregate()
	}

	if !newObj.DeletionTimestamp.IsZero() {
		if !oldManaged && newManaged {
			allErrors = deduplicateFieldErrors(append(allErrors, jobframework.ValidateWaitForPodsReadyAnnotation(newObj, w.maxTimeoutOnWorkload)...))
		}
		allErrors = append(allErrors, validateEarlyReturnMeta(oldJob, newJob, oldManaged, newManaged, maxExecImmutable)...)
		if !specsEqualWithRawFidelity(oldObj.Spec, newObj.Spec) {
			allErrors = append(allErrors, field.Invalid(
				specPath,
				field.OmitValueType{},
				fmt.Sprintf("%s when FlinkDeployment is being deleted", apivalidation.FieldImmutableErrorMsg),
			))
		}
		return nil, allErrors.ToAggregate()
	}

	if oldManaged && isFinished {
		allErrors = append(allErrors, validateEarlyReturnMeta(oldJob, newJob, oldManaged, newManaged, maxExecImmutable)...)
		isDeletingWorkloadStop := wl != nil && !wl.DeletionTimestamp.IsZero() && newJob.IsSuspended() &&
			isWorkloadRestoreStopSpec(ctx, oldObj, newObj, wl)
		if !isDeletingWorkloadStop && !specsEqualWithRawFidelity(oldObj.Spec, newObj.Spec) {
			allErrors = append(allErrors, field.Invalid(
				specPath,
				field.OmitValueType{},
				fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
			))
		}
		return nil, allErrors.ToAggregate()
	}

	canRestoreSuspended := oldJob.IsSuspended() && wl != nil && (isEvicted || !wl.DeletionTimestamp.IsZero())
	if oldManaged && (!oldJob.IsSuspended() || canRestoreSuspended) && newJob.IsSuspended() &&
		isPureSuspensionSpec(ctx, oldObj, newObj, wl, canRestoreSuspended) {
		allErrors = append(allErrors, validateEarlyReturnMeta(oldJob, newJob, oldManaged, newManaged, maxExecImmutable)...)
		return nil, allErrors.ToAggregate()
	}

	// When both oldJob and newJob are suspended and the owned Workload holds no quota reservation,
	// only the new object's management state decides whether the Kueue-managed spec and TAS
	// restrictions apply, so detaching a queued suspended job from Kueue by dropping the
	// queue-name label does not re-validate the now-unmanaged standalone Flink spec. Whenever
	// oldJob or newJob is running (or the owned Workload holds a quota reservation),
	// Kueue-managed validation applies if either object is managed.
	var maxTimeoutForCreate *metav1.Duration
	if !oldManaged && newManaged {
		maxTimeoutForCreate = w.maxTimeoutOnWorkload
	}
	enforceManagedSpec := newManaged || (oldManaged && (!oldJob.IsSuspended() || !newJob.IsSuspended() || hasQuotaReservation))
	validationErrs, err := w.validateJobAndSpec(newJob, enforceManagedSpec, maxTimeoutForCreate)
	if err != nil {
		return nil, err
	}
	allErrors = deduplicateFieldErrors(append(allErrors, validationErrs...))

	if len(validationErrs) == 0 && (!oldJob.IsSuspended() || !newJob.IsSuspended() || hasQuotaReservation) {
		immutableReason := "when FlinkDeployment is not suspended"
		if oldJob.IsSuspended() && newJob.IsSuspended() {
			if isAdmitted {
				immutableReason = "while Workload is admitted"
			} else {
				immutableReason = "while Workload has quota reserved"
			}
		}
		immutableMsg := fmt.Sprintf("%s %s", apivalidation.FieldImmutableErrorMsg, immutableReason)
		// Top-level fields and flinkConfiguration keys that govern pod identity, image, PodTemplate
		// array merging, and default parallelism/slots are never mutated by RunWithPodSetsInfo or
		// RestorePodSetsInfo and remain immutable whenever oldJob or newJob is running or the owned
		// Workload holds a quota reservation.
		if oldObj.Spec.Mode != newObj.Spec.Mode {
			allErrors = append(allErrors, field.Invalid(specPath.Child("mode"), newObj.Spec.Mode, immutableMsg))
		}
		if oldObj.Spec.Image != newObj.Spec.Image {
			allErrors = append(allErrors, field.Invalid(specPath.Child("image"), newObj.Spec.Image, immutableMsg))
		}
		if oldObj.Spec.ImagePullPolicy != newObj.Spec.ImagePullPolicy {
			allErrors = append(allErrors, field.Invalid(specPath.Child("imagePullPolicy"), newObj.Spec.ImagePullPolicy, immutableMsg))
		}
		if oldObj.Spec.ServiceAccount != newObj.Spec.ServiceAccount {
			allErrors = append(allErrors, field.Invalid(specPath.Child("serviceAccount"), newObj.Spec.ServiceAccount, immutableMsg))
		}
		if !podTemplatesEqualWithRawFidelity(oldObj.Spec.PodTemplate, newObj.Spec.PodTemplate) {
			allErrors = append(allErrors, field.Invalid(podTemplateSpecPath, field.OmitValueType{}, immutableMsg))
		}
		if oldObj.Spec.Job != nil && newObj.Spec.Job != nil && !ptr.Equal(oldObj.Spec.Job.Parallelism, newObj.Spec.Job.Parallelism) {
			allErrors = append(allErrors, field.Invalid(jobSpecPath.Child("parallelism"), ptr.Deref(newObj.Spec.Job.Parallelism, 0), immutableMsg))
		}
		oldFlatCfg, oldFlatErr := oldObj.Spec.FlinkConfiguration.FlatMap()
		newFlatCfg, newFlatErr := newObj.Spec.FlinkConfiguration.FlatMap()
		if oldFlatErr == nil && newFlatErr == nil {
			for _, cfgKey := range []string{flinkConfigNumberOfTaskSlots, flinkConfigPodTemplateMergeArraysByName} {
				oldVal, oldOK := oldFlatCfg[cfgKey]
				newVal, newOK := newFlatCfg[cfgKey]
				if oldOK != newOK || oldVal != newVal {
					allErrors = append(allErrors, field.Invalid(flinkConfigurationPath.Key(cfgKey), newVal, immutableMsg))
				}
			}
		}
		newPodSets, newErr := newJob.PodSets(ctx, nil)
		if newErr == nil {
			oldPodSets, oldErr := oldJob.PodSets(ctx, nil)
			var podSetsAllowed bool
			switch {
			case oldErr != nil:
				podSetsAllowed = false
			case oldJob.IsSuspended() == newJob.IsSuspended():
				podSetsAllowed = apiequality.Semantic.DeepEqual(oldPodSets, newPodSets) ||
					(canRestoreSuspended && canRestorePodSetsFromWorkload(ctx, oldObj, newObj, wl, oldPodSets, newPodSets))
			case oldJob.IsSuspended() && !newJob.IsSuspended():
				var startErr error
				podSetsAllowed, startErr = w.canStartSuspendedPodSetsFromWorkload(
					ctx, oldObj, wl, isAdmitted, newPodSets,
				)
				if startErr != nil {
					return nil, startErr
				}
			default:
				podSetsAllowed = apiequality.Semantic.DeepEqual(oldPodSets, newPodSets) ||
					canRestorePodSetsFromWorkload(ctx, oldObj, newObj, wl, oldPodSets, newPodSets)
			}
			if !podSetsAllowed {
				allErrors = append(allErrors, field.Invalid(
					specPath,
					field.OmitValueType{},
					immutableMsg,
				))
			}
		}
	}

	return nil, allErrors.ToAggregate()
}

func validateRunningToSuspendedJobMeta(oldJob, newJob *flinkDeployment) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(
		jobframework.QueueName(newJob),
		jobframework.QueueName(oldJob),
		labelsPath.Key(constants.QueueLabel),
	)...)
	if !workloadslicing.Enabled(oldJob.Object()) {
		oldWlName := jobframework.PrebuiltWorkloadNameFor(oldJob.Object())
		newWlName := jobframework.PrebuiltWorkloadNameFor(newJob.Object())
		wlPath := jobframework.GetPrebuiltWorkloadPath(newJob.Object())
		if newWlName == "" {
			wlPath = jobframework.GetPrebuiltWorkloadPath(oldJob.Object())
		}
		allErrs = append(allErrs, apivalidation.ValidateImmutableField(newWlName, oldWlName, wlPath)...)
	}
	if !jobframework.CanSetWorkloadPriorityClassName(false, oldJob.Object()) &&
		!jobframework.IsWorkloadPriorityClassNameEmpty(newJob.Object()) {
		allErrs = append(allErrs, field.Invalid(
			labelsPath.Key(constants.WorkloadPriorityClassLabel),
			jobframework.WorkloadPriorityClassName(newJob.Object()),
			"WorkloadPriorityClass cannot be added to a non-suspended workload",
		))
	}
	return allErrs
}

func podSetsMatchOutsideOverlay(a, b []kueue.PodSet) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Count != b[i].Count {
			return false
		}
		if !apiequality.Semantic.DeepEqual(
			flinkv1beta1.PodSpecWithoutOverlay(&a[i].Template.Spec),
			flinkv1beta1.PodSpecWithoutOverlay(&b[i].Template.Spec),
		) {
			return false
		}
	}
	return true
}

var (
	admittedOverlayAnnotationKeys = []string{
		kueue.WorkloadAnnotation,
		kueue.WorkloadSliceNameAnnotation,
		kueue.PodSetUnconstrainedTopologyAnnotation,
	}
	admittedOverlayLabelKeys = []string{
		kueueconstants.PodSetLabel,
		kueueconstants.LocalQueueLabel,
		kueueconstants.ClusterQueueLabel,
		kueue.MultiKueueWorkerWorkloadPodLabel,
	}
)

func hasAddedOverlayEntry(ownerMap, wlMap map[string]string, keys []string) bool {
	for _, k := range keys {
		ownerVal, inOwner := ownerMap[k]
		if !inOwner {
			continue
		}
		if wlVal, inWl := wlMap[k]; !inWl || ownerVal != wlVal {
			return true
		}
	}
	return false
}

func hasAdmittedPodSetOverlay(ownerPodSet, wlPodSet *kueue.PodSet) bool {
	if hasAddedOverlayEntry(ownerPodSet.Template.Annotations, wlPodSet.Template.Annotations, admittedOverlayAnnotationKeys) ||
		hasAddedOverlayEntry(ownerPodSet.Template.Labels, wlPodSet.Template.Labels, admittedOverlayLabelKeys) {
		return true
	}
	topologyGate := corev1.PodSchedulingGate{Name: kueue.TopologySchedulingGate}
	return slices.Contains(ownerPodSet.Template.Spec.SchedulingGates, topologyGate) &&
		!slices.Contains(wlPodSet.Template.Spec.SchedulingGates, topologyGate)
}

// isUnstartedFailedToStartWorkload reports whether wl finished with FailedToStart before
// RunWithPodSetsInfo ever patched owner (for example when taskManager.replicas was scaled to 0
// via /scale or podTemplate had a conflicting nodeSelector), including when legitimate edits to
// Labels, Annotations, NodeSelector, or SchedulingGates while queued left wl.Spec.PodSets with
// prior values, while still blocking resurrection when owner carries admitted overlays.
func isUnstartedFailedToStartWorkload(ctx context.Context, owner *flinkv1beta1.FlinkDeployment, wl *kueue.Workload) bool {
	ownerJob := fromObject(owner)
	if !ownerJob.IsSuspended() {
		return false
	}
	cond := apimeta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadFinished)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != jobframework.FailedToStartFinishedReason {
		return false
	}
	ownerPodSets, err := jobframework.JobPodSets(ctx, ownerJob, nil)
	if err != nil || len(ownerPodSets) != len(wl.Spec.PodSets) {
		return false
	}
	for i := range ownerPodSets {
		if ownerPodSets[i].Name != wl.Spec.PodSets[i].Name ||
			hasAdmittedPodSetOverlay(&ownerPodSets[i], &wl.Spec.PodSets[i]) {
			return false
		}
	}
	var opts []equality.ComparePodSetsOption
	if !features.Enabled(features.TopologyAwareScheduling) {
		opts = append(opts, equality.WithIgnoreTopologyRequest())
	}
	return equality.ComparePodSetSlices(ownerPodSets, wl.Spec.PodSets, opts...)
}

// findOwnedNonFinishedWorkload looks up the controller-owned non-finished Workload for owner,
// attempting a deterministic O(1) Get by GetWorkloadNameForFlinkDeployment first and falling
// back to an indexed, zero-deepcopy WorkloadList query whenever the deterministic Get does not
// return a non-finished Workload controlled by owner (for example when a custom Workload name
// is used or the deterministic-name Workload is already finished). hasFinishedWorkload is true
// when no non-finished controller-owned Workload exists and at least one controller-owned
// post-start finished Workload was encountered during the lookup.
func (w *flinkDeploymentWebhook) findOwnedNonFinishedWorkload(
	ctx context.Context,
	owner *flinkv1beta1.FlinkDeployment,
) (*kueue.Workload, bool, error) {
	if w.client == nil {
		return nil, false, nil
	}

	var hasFinishedWorkload bool
	var deterministicWl kueue.Workload
	wlKey := types.NamespacedName{
		Namespace: owner.Namespace,
		Name:      GetWorkloadNameForFlinkDeployment(owner.Name, owner.UID),
	}
	if err := w.client.Get(ctx, wlKey, &deterministicWl); err == nil {
		if isControlledByFlinkDeployment(&deterministicWl, owner) {
			if !apimeta.IsStatusConditionTrue(deterministicWl.Status.Conditions, kueue.WorkloadFinished) {
				return &deterministicWl, false, nil
			}
			if !isUnstartedFailedToStartWorkload(ctx, owner, &deterministicWl) {
				hasFinishedWorkload = true
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, err
	}

	var workloads kueue.WorkloadList
	if err := w.client.List(
		ctx,
		&workloads,
		client.InNamespace(owner.Namespace),
		jobframework.OwnerReferenceIndexFieldMatcher(gvk, owner.Name),
		client.UnsafeDisableDeepCopy,
	); err != nil {
		return nil, false, err
	}
	for i := range workloads.Items {
		candidate := &workloads.Items[i]
		if !isControlledByFlinkDeployment(candidate, owner) {
			continue
		}
		if !apimeta.IsStatusConditionTrue(candidate.Status.Conditions, kueue.WorkloadFinished) {
			return candidate, false, nil
		}
		if !isUnstartedFailedToStartWorkload(ctx, owner, candidate) {
			hasFinishedWorkload = true
		}
	}
	return nil, hasFinishedWorkload, nil
}

// expectedAdmittedPodSetsInfo computes the admitted PodSetInfo slice for wl matching
// JobReconciler.startJob / getPodSetsInfoFromStatus.
func expectedAdmittedPodSetsInfo(ctx context.Context, c client.Client, wl *kueue.Workload) ([]podset.PodSetInfo, error) {
	if wl.Status.Admission == nil || len(wl.Status.Admission.PodSetAssignments) == 0 {
		return nil, podset.ErrInvalidPodsetInfo
	}

	byName := utilslices.ToRefMap(wl.Status.Admission.PodSetAssignments, func(psa *kueue.PodSetAssignment) kueue.PodSetReference {
		return psa.Name
	})

	podSetsInfo := make([]podset.PodSetInfo, len(wl.Spec.PodSets))
	for i := range wl.Spec.PodSets {
		psa, found := byName[wl.Spec.PodSets[i].Name]
		if !found {
			return nil, fmt.Errorf("%w: admission has no assignment for PodSet %q", podset.ErrInvalidPodsetInfo, wl.Spec.PodSets[i].Name)
		}
		info, err := podset.FromAssignment(ctx, c, psa, &wl.Spec.PodSets[i])
		if err != nil {
			return nil, err
		}
		info.Annotations[kueue.WorkloadAnnotation] = wl.Name
		if workloadslicing.IsElasticWorkload(wl) {
			info.Annotations[kueue.WorkloadSliceNameAnnotation] = workloadslicing.SliceName(wl)
		}
		info.Labels[kueueconstants.PodSetLabel] = string(psa.Name)
		if _, exists := wl.Labels[kueue.MultiKueueOriginLabel]; exists {
			info.Labels[kueue.MultiKueueWorkerWorkloadPodLabel] = kueue.MultiKueueWorkerWorkloadPodValue
		}
		info.Labels[kueueconstants.LocalQueueLabel] = string(wl.Spec.QueueName)
		if clusterQueueName := string(wl.Status.Admission.ClusterQueue); len(validation.IsDNS1123Label(clusterQueueName)) == 0 {
			info.Labels[kueueconstants.ClusterQueueLabel] = clusterQueueName
		}
		for _, admissionCheck := range wl.Status.AdmissionChecks {
			for _, podSetUpdate := range admissionCheck.PodSetUpdates {
				if podSetUpdate.Name == info.Name {
					if err := info.Merge(podset.FromUpdate(&podSetUpdate)); err != nil {
						return nil, fmt.Errorf("in admission check %q: %w", admissionCheck.Name, err)
					}
					break
				}
			}
		}
		podSetsInfo[i] = info
	}
	return podSetsInfo, nil
}

func (w *flinkDeploymentWebhook) canStartSuspendedPodSetsFromWorkload(
	ctx context.Context,
	oldObj *flinkv1beta1.FlinkDeployment,
	wl *kueue.Workload,
	isAdmitted bool,
	newPodSets []kueue.PodSet,
) (bool, error) {
	if !isAdmitted {
		return false, nil
	}
	equivalent, err := jobframework.EquivalentToWorkload(ctx, w.client, fromObject(oldObj), wl)
	if err != nil {
		return false, err
	}
	if !equivalent {
		return false, nil
	}
	podSetsInfo, err := expectedAdmittedPodSetsInfo(ctx, w.client, wl)
	if err != nil {
		if podset.IsPermanent(err) || apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	candidate := oldObj.DeepCopy()
	candidateJob := fromObject(candidate)
	if err := candidateJob.RunWithPodSetsInfo(ctx, w.client, podSetsInfo); err != nil {
		if podset.IsPermanent(err) {
			return false, nil
		}
		return false, err
	}
	candidatePodSets, err := candidateJob.PodSets(ctx, nil)
	if err != nil {
		return false, err
	}
	return apiequality.Semantic.DeepEqual(candidatePodSets, newPodSets), nil
}

func quantitiesEqualWithRawFidelity(a, b flinkv1beta1.Quantity) bool {
	return (a.ParseError() == nil) == (b.ParseError() == nil) &&
		a.RawLiteral() == b.RawLiteral() &&
		a.Equal(b)
}

func resourceListsEqualWithRawFidelity(a, b flinkv1beta1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || !quantitiesEqualWithRawFidelity(va, vb) {
			return false
		}
	}
	return true
}

func componentResourcesEqualWithRawFidelity(a, b *flinkv1beta1.ComponentPodSpec) bool {
	var ar, br *flinkv1beta1.ResourceRequirements
	if a != nil {
		ar = a.Resources
	}
	if b != nil {
		br = b.Resources
	}
	if (ar == nil) != (br == nil) {
		return false
	}
	if ar == nil {
		return true
	}
	return resourceListsEqualWithRawFidelity(ar.Requests, br.Requests) &&
		resourceListsEqualWithRawFidelity(ar.Limits, br.Limits) &&
		apiequality.Semantic.DeepEqual(ar.Claims, br.Claims)
}

func podTemplatesEqualWithRawFidelity(a, b *flinkv1beta1.PodTemplateSpec) bool {
	if !apiequality.Semantic.DeepEqual(a, b) {
		return false
	}
	if a == nil || b == nil {
		return a == b
	}
	if a.DecodeError() != nil || b.DecodeError() != nil {
		return true
	}
	aMap, aErr := podTemplateToJSONMap(a)
	bMap, bErr := podTemplateToJSONMap(b)
	if aErr != nil || bErr != nil {
		return false
	}
	return reflect.DeepEqual(aMap, bMap)
}

func componentPodSpecsEqualWithRawFidelity(a, b *flinkv1beta1.ComponentPodSpec) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	return componentResourcesEqualWithRawFidelity(a, b) &&
		podTemplatesEqualWithRawFidelity(a.PodTemplate, b.PodTemplate)
}

// specsEqualWithRawFidelity requires both apimachinery semantic equality and raw-literal /
// JSON-map fidelity on raw-preserving Quantity and PodTemplateSpec subtrees so mutations like
// "1536Mi" -> "1.5Gi", "1000000000" -> "1G", "1" -> " 1 ", or injecting explicit zero-valued JSON
// overrides ("image": "", "hostNetwork": false) cannot slip past webhook immutability checks.
func specsEqualWithRawFidelity(a, b flinkv1beta1.FlinkDeploymentSpec) bool {
	if !apiequality.Semantic.DeepEqual(a, b) {
		return false
	}
	if !podTemplatesEqualWithRawFidelity(a.PodTemplate, b.PodTemplate) {
		return false
	}
	var aJM, bJM *flinkv1beta1.ComponentPodSpec
	if a.JobManager != nil {
		aJM = &a.JobManager.ComponentPodSpec
	}
	if b.JobManager != nil {
		bJM = &b.JobManager.ComponentPodSpec
	}
	if !componentPodSpecsEqualWithRawFidelity(aJM, bJM) {
		return false
	}
	var aTM, bTM *flinkv1beta1.ComponentPodSpec
	if a.TaskManager != nil {
		aTM = &a.TaskManager.ComponentPodSpec
	}
	if b.TaskManager != nil {
		bTM = &b.TaskManager.ComponentPodSpec
	}
	return componentPodSpecsEqualWithRawFidelity(aTM, bTM)
}

func validateMaxExecTimeLabelValue(obj *flinkv1beta1.FlinkDeployment) field.ErrorList {
	strVal, found := obj.GetLabels()[constants.MaxExecTimeSecondsLabel]
	if !found {
		return nil
	}
	labelPath := labelsPath.Key(constants.MaxExecTimeSecondsLabel)
	v, err := strconv.Atoi(strVal)
	if err != nil {
		return field.ErrorList{field.Invalid(labelPath, strVal, err.Error())}
	}
	if v <= 0 {
		return field.ErrorList{field.Invalid(labelPath, v, "should be greater than 0")}
	}
	if v > math.MaxInt32 {
		return field.ErrorList{field.Invalid(labelPath, v, fmt.Sprintf("should be less than or equal to %d", math.MaxInt32))}
	}
	return nil
}

func validateEarlyReturnMeta(
	oldJob, newJob *flinkDeployment,
	oldManaged, newManaged, maxExecImmutable bool,
) field.ErrorList {
	var allErrs field.ErrorList
	if newManaged {
		oldVal, oldFound := oldJob.GetLabels()[constants.MaxExecTimeSecondsLabel]
		newVal, newFound := newJob.GetLabels()[constants.MaxExecTimeSecondsLabel]
		if (!oldManaged || oldFound != newFound || oldVal != newVal) &&
			(!maxExecImmutable || oldVal == newVal) {
			allErrs = append(allErrs, validateMaxExecTimeLabelValue((*flinkv1beta1.FlinkDeployment)(newJob))...)
		}
	}
	if newJob.GetAnnotations()[kueueconstants.ElasticJobScaleUpStrategyAnnotationKey] !=
		oldJob.GetAnnotations()[kueueconstants.ElasticJobScaleUpStrategyAnnotationKey] {
		allErrs = append(allErrs, jobframework.ValidateElasticJobAnnotation(newJob.Object(), newJob.GVK())...)
	}
	return allErrs
}

func isWorkloadRestoreStopSpec(
	ctx context.Context,
	oldObj, newObj *flinkv1beta1.FlinkDeployment,
	wl *kueue.Workload,
) bool {
	if wl == nil {
		return false
	}
	if fromObject(oldObj).IsSuspended() && specsEqualWithRawFidelity(oldObj.Spec, newObj.Spec) {
		return false
	}
	restoredSuspended := oldObj.DeepCopy()
	candidateJob := fromObject(restoredSuspended)
	candidateJob.Suspend()
	candidateJob.RestorePodSetsInfo(ctx, jobframework.GetPodSetsInfoFromWorkload(wl))
	return specsEqualWithRawFidelity(restoredSuspended.Spec, newObj.Spec)
}

// isPureSuspensionSpec reports whether newObj.Spec is an exact suspension (or post-eviction /
// Workload-deletion overlay restore) of oldObj.Spec:
//   - on running -> suspended, either setting only spec.job.state to suspended or applying
//     Suspend + RestorePodSetsInfo from wl;
//   - on suspended -> suspended when canRestoreSuspended is true, applying a non-noop Suspend +
//     RestorePodSetsInfo from wl so JobWithCustomStop.Stop can strip admitted overlays from a
//     deployment that was manually suspended before eviction or Workload deletion.
func isPureSuspensionSpec(
	ctx context.Context,
	oldObj, newObj *flinkv1beta1.FlinkDeployment,
	wl *kueue.Workload,
	canRestoreSuspended bool,
) bool {
	wasSuspended := fromObject(oldObj).IsSuspended()
	if !wasSuspended {
		plainSuspended := oldObj.DeepCopy()
		fromObject(plainSuspended).Suspend()
		if specsEqualWithRawFidelity(plainSuspended.Spec, newObj.Spec) {
			return true
		}
	}
	if !wasSuspended || canRestoreSuspended {
		return isWorkloadRestoreStopSpec(ctx, oldObj, newObj, wl)
	}
	return false
}

// canRestorePodSetsFromWorkload permits JobReconciler.stopJob (and JobWithCustomStop.Stop on an
// evicted or deleting Workload) to suspend oldObj and restore PodSet overlays from its owned
// non-finished Workload, including when legitimate edits to Labels, Annotations, NodeSelector, or
// SchedulingGates while queued and suspended kept EquivalentToWorkload == true and left
// Workload.Spec.PodSets with the prior overlay values.
func canRestorePodSetsFromWorkload(
	ctx context.Context,
	oldObj, newObj *flinkv1beta1.FlinkDeployment,
	wl *kueue.Workload,
	oldPodSets, newPodSets []kueue.PodSet,
) bool {
	var oldJM, newJM, oldTM, newTM *flinkv1beta1.ComponentPodSpec
	if oldObj.Spec.JobManager != nil {
		oldJM = &oldObj.Spec.JobManager.ComponentPodSpec
	}
	if newObj.Spec.JobManager != nil {
		newJM = &newObj.Spec.JobManager.ComponentPodSpec
	}
	if oldObj.Spec.TaskManager != nil {
		oldTM = &oldObj.Spec.TaskManager.ComponentPodSpec
	}
	if newObj.Spec.TaskManager != nil {
		newTM = &newObj.Spec.TaskManager.ComponentPodSpec
	}
	if wl == nil ||
		oldObj.Spec.Mode != newObj.Spec.Mode ||
		oldObj.Spec.Image != newObj.Spec.Image ||
		oldObj.Spec.ImagePullPolicy != newObj.Spec.ImagePullPolicy ||
		oldObj.Spec.ServiceAccount != newObj.Spec.ServiceAccount ||
		!podTemplatesEqualWithRawFidelity(oldObj.Spec.PodTemplate, newObj.Spec.PodTemplate) ||
		!componentResourcesEqualWithRawFidelity(oldJM, newJM) ||
		!componentResourcesEqualWithRawFidelity(oldTM, newTM) ||
		!podSetsMatchOutsideOverlay(oldPodSets, newPodSets) {
		return false
	}

	candidate := oldObj.DeepCopy()
	candidateJob := fromObject(candidate)
	candidateJob.Suspend()
	candidateJob.RestorePodSetsInfo(ctx, jobframework.GetPodSetsInfoFromWorkload(wl))
	candidatePodSets, err := candidateJob.PodSets(ctx, nil)
	return err == nil && apiequality.Semantic.DeepEqual(candidatePodSets, newPodSets)
}

func isControlledByFlinkDeployment(wl *kueue.Workload, owner *flinkv1beta1.FlinkDeployment) bool {
	if !metav1.IsControlledBy(wl, owner) {
		return false
	}
	ref := metav1.GetControllerOf(wl)
	return ref != nil &&
		ref.APIVersion == gvk.GroupVersion().String() &&
		ref.Kind == gvk.Kind &&
		ref.Name == owner.Name
}

// ValidateDelete implements admission.Validator for FlinkDeployment deletion.
func (w *flinkDeploymentWebhook) ValidateDelete(_ context.Context, _ *flinkv1beta1.FlinkDeployment) (admission.Warnings, error) {
	return nil, nil
}
