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
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	"sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/util/webhook"
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

func SetupWebhook(mgr ctrl.Manager, opts ...jobframework.Option) error {
	options := jobframework.ProcessOptions(opts...)
	obj := &flinkv1beta1.FlinkDeployment{}
	if options.NoopWebhook {
		return webhook.SetupNoopWebhook(mgr, obj)
	}
	wh := &flinkDeploymentWebhook{
		integrationManager:           options.IntegrationManager,
		client:                       mgr.GetClient(),
		queues:                       options.Queues,
		manageJobsWithoutQueueName:   options.ManageJobsWithoutQueueName,
		managedJobsNamespaceSelector: options.ManagedJobsNamespaceSelector,
		maxTimeoutOnWorkload:         options.MaxTimeoutOnWorkload,
	}
	return ctrl.NewWebhookManagedBy(mgr, obj).WithValidator(wh).WithDefaulter(wh).
		WithLogConstructor(jobframework.WebhookLogConstructor(gvk, options.RoleTracker)).Complete()
}

func (w *flinkDeploymentWebhook) im() *jobframework.IntegrationManager {
	return cmp.Or(w.integrationManager, jobframework.NewIntegrationManager())
}

func (w *flinkDeploymentWebhook) defaultQueueExist(ns string) bool {
	return w.queues != nil && w.queues.DefaultLocalQueueExist(ns)
}

// +kubebuilder:webhook:path=/mutate-flink-apache-org-v1beta1-flinkdeployment,mutating=true,failurePolicy=fail,sideEffects=None,groups=flink.apache.org,resources=flinkdeployments,verbs=create,versions=v1beta1,name=mflinkdeployment.kb.io,admissionReviewVersions=v1

var _ admission.Defaulter[*flinkv1beta1.FlinkDeployment] = &flinkDeploymentWebhook{}

func (w *flinkDeploymentWebhook) Default(ctx context.Context, obj *flinkv1beta1.FlinkDeployment) error {
	if obj.Spec.Job == nil {
		return nil
	}
	job, im := fromObject(obj), w.im()
	ctrl.LoggerFrom(ctx).WithName("flinkdeployment-webhook").V(5).Info("Applying defaults")
	if err := errors.Join(
		im.ApplyDefaultLocalQueue(ctx, w.client, job.Object(), w.defaultQueueExist, w.managedJobsNamespaceSelector),
		im.ApplyDefaultWorkloadPriorityClass(ctx, w.client, job.Object(), w.managedJobsNamespaceSelector),
	); err != nil {
		return err
	}
	return im.ApplyDefaultForSuspend(ctx, job, w.client, w.manageJobsWithoutQueueName, w.managedJobsNamespaceSelector)
}

// +kubebuilder:webhook:path=/validate-flink-apache-org-v1beta1-flinkdeployment,mutating=false,failurePolicy=fail,sideEffects=None,groups=flink.apache.org,resources=flinkdeployments,verbs=create;update,versions=v1beta1,name=vflinkdeployment.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*flinkv1beta1.FlinkDeployment] = &flinkDeploymentWebhook{}

func (w *flinkDeploymentWebhook) ValidateCreate(ctx context.Context, obj *flinkv1beta1.FlinkDeployment) (admission.Warnings, error) {
	ctrl.LoggerFrom(ctx).WithName("flinkdeployment-webhook").V(5).Info("Validating create")
	if _, hasLabel := obj.Labels[constants.QueueLabel]; obj.Spec.Job == nil && !hasLabel {
		return nil, nil
	}
	kueueJob := fromObject(obj)
	isManaged, err := w.isManagedByKueue(ctx, kueueJob)
	if err != nil {
		return nil, err
	}
	validationErrs, err := w.validateJobAndSpec(ctx, kueueJob, nil, isManaged, w.maxTimeoutOnWorkload)
	return nil, errors.Join(err, validationErrs.ToAggregate())
}

func validatePositiveInt32Field(val *int64, fldPath *field.Path) *field.Error {
	if val != nil && *val < 1 {
		return field.Invalid(fldPath, *val, "must be greater than or equal to 1")
	} else if val != nil && *val > math.MaxInt32 {
		return field.Invalid(fldPath, *val, fmt.Sprintf("must be less than or equal to %d", math.MaxInt32))
	}
	return nil
}

func missingResourceFieldPath(fldPath *field.Path, comp *flinkv1beta1.ComponentPodSpec, sub string, res corev1.ResourceName) *field.Path {
	if comp != nil && comp.Resources != nil {
		return fldPath.Child("resources", "requests").Key(string(res))
	}
	return fldPath.Child("resource", sub)
}

func validateComponentSpec(comp, oldSuspendedComp *flinkv1beta1.ComponentPodSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if comp != nil {
		if oldSuspendedComp == nil || !ptr.Equal(oldSuspendedComp.Replicas, comp.Replicas) || comp.Replicas == nil || *comp.Replicas != 0 {
			if fe := validatePositiveInt32Field(comp.Replicas, fldPath.Child("replicas")); fe != nil {
				allErrs = append(allErrs, fe)
			}
		}
		allErrs = append(allErrs, validateAndApplyComponentResources(&corev1.Container{}, comp, fldPath)...)
	}
	if !hasExplicitComponentMemory(comp) {
		allErrs = append(allErrs, field.Required(missingResourceFieldPath(fldPath, comp, "memory", corev1.ResourceMemory),
			fmt.Sprintf("memory must be specified via %s.resource.memory or %s.resources", fldPath, fldPath)))
	}
	return allErrs
}

func validateFlinkConfiguration(spec *flinkv1beta1.FlinkDeploymentSpec) field.ErrorList {
	flatCfg, err := spec.FlinkConfiguration.FlatMap()
	if err != nil {
		return field.ErrorList{field.Invalid(flinkConfigurationPath, field.OmitValueType{}, err.Error())}
	}
	var allErrs field.ErrorList
	for _, key := range slices.Sorted(maps.Keys(flatCfg)) {
		val, kp := flatCfg[key], flinkConfigurationPath.Key(key)
		forbid := func(msg string) { allErrs = append(allErrs, field.Forbidden(kp, msg)) }
		forbidBool := func(msg string) {
			if enabled, bErr := parseFlinkBool(val); bErr != nil {
				allErrs = append(allErrs, field.Invalid(kp, val, bErr.Error()))
			} else if enabled {
				forbid(msg)
			}
		}
		switch {
		case key == flinkConfigAutoscalerEnabled || key == flinkConfigOperatorAutoscalerEnabled:
			forbidBool("dynamic Flink autoscaling is not supported for a Kueue-managed FlinkDeployment")
		case unsupportedFlinkConfigSizingKeys.Has(key) || isForbiddenFlinkMemorySizingKey(key):
			forbid("configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration")
		case isForbiddenFlinkPodPlacementOrMetadataKey(key):
			forbid(
				"configure pod templates, placement, tolerations, labels, and annotations in spec.podTemplate, spec.jobManager.podTemplate, or spec.taskManager.podTemplate instead of spec.flinkConfiguration",
			)
		case forbiddenFlinkPodIdentityImageAndNetworkKeys.Has(key):
			forbid(
				"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
			)
		case key == flinkConfigExternalResources || strings.HasPrefix(key, flinkConfigExternalResourcePrefix):
			forbid("configure extended resources such as GPUs in spec.jobManager.resources, spec.taskManager.resources, or podTemplate instead of spec.flinkConfiguration")
		case isStandaloneOnlyParallelismConfigKey(key):
			if spec.Mode != flinkv1beta1.KubernetesDeploymentModeStandalone || spec.TaskManager == nil || spec.TaskManager.Replicas == nil {
				forbid(fmt.Sprintf("spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when %s is configured", key))
			}
		case key == flinkConfigNumberOfTaskSlots:
			if _, sErr := parsePositiveInt32(val, flinkConfigNumberOfTaskSlots); sErr != nil {
				allErrs = append(allErrs, field.Invalid(kp, val, sErr.Error()))
			}
		case key == flinkConfigPodTemplateMergeArraysByName:
			if _, bErr := parseFlinkBool(val); bErr != nil {
				allErrs = append(allErrs, field.Invalid(kp, val, bErr.Error()))
			}
		case key == flinkConfigOperatorJobRestartFailed:
			forbidBool("the Flink Operator would resubmit a FAILED job after Kueue has released its quota; rely on Kueue requeueing and backoff (for example via waitForPodsReady) instead")
		case key == flinkConfigOperatorDeploymentRollbackEnabled:
			forbidBool("automatic Flink Operator deployment rollback is not supported for a Kueue-managed FlinkDeployment")
		}
	}
	return allErrs
}

func validateManagedFlinkSpec(kueueJob *flinkDeployment, oldTM *flinkv1beta1.ComponentPodSpec) ([]kueue.PodSet, field.ErrorList) {
	spec := &kueueJob.Spec
	if spec.Job == nil {
		return nil, field.ErrorList{field.Required(jobSpecPath, "only Application mode (spec.job != nil) is supported for a Kueue-managed FlinkDeployment")}
	}
	var allErrs field.ErrorList
	jmComp, tmComp := components(spec)
	missingMainImgMsg := fmt.Sprintf("image must be specified via spec.image or %s in podTemplate", flinkMainContainerName)
	if strings.TrimSpace(spec.Image) == "" {
		if spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone {
			allErrs = append(allErrs, field.Required(specPath.Child("image"), "image must be specified when spec.mode is standalone"))
		} else if spec.PodTemplate == nil && (jmComp == nil || jmComp.PodTemplate == nil || tmComp == nil || tmComp.PodTemplate == nil) {
			allErrs = append(allErrs, field.Required(specPath.Child("image"), missingMainImgMsg))
		}
	}
	if strings.TrimSpace(string(spec.ImagePullPolicy)) != "" && !isSupportedPullPolicy(spec.ImagePullPolicy) {
		allErrs = append(allErrs, field.NotSupported(specPath.Child("imagePullPolicy"), spec.ImagePullPolicy, supportedPullPolicies))
	}
	if spec.Job.Parallelism == nil {
		if spec.TaskManager == nil || spec.TaskManager.Replicas == nil {
			allErrs = append(allErrs, field.Required(jobSpecPath.Child("parallelism"), "must be specified when spec.taskManager.replicas is not set"))
		}
	} else if fe := validatePositiveInt32Field(spec.Job.Parallelism, jobSpecPath.Child("parallelism")); fe != nil {
		allErrs = append(allErrs, fe)
	}

	allErrs = append(allErrs, validateComponentSpec(jmComp, nil, jobManagerSpecPath)...)
	allErrs = append(allErrs, validateComponentSpec(tmComp, oldTM, taskManagerSpecPath)...)
	if spec.Mode == flinkv1beta1.KubernetesDeploymentModeStandalone && !hasExplicitComponentCPU(tmComp) {
		allErrs = append(allErrs, field.Required(missingResourceFieldPath(taskManagerSpecPath, tmComp, "cpu", corev1.ResourceCPU),
			"cpu must be specified via spec.taskManager.resource.cpu or spec.taskManager.resources when spec.mode is standalone"))
	}
	allErrs = append(allErrs, validateFlinkConfiguration(spec)...)
	if len(allErrs) > 0 {
		return nil, allErrs
	}
	podSets, err := kueueJob.buildPodSets()
	if err != nil {
		if strings.TrimSpace(spec.Image) == "" && strings.Contains(err.Error(), fmt.Sprintf("container %q: image must be specified", flinkMainContainerName)) {
			return nil, field.ErrorList{field.Required(specPath.Child("image"), missingMainImgMsg)}
		}
		return nil, field.ErrorList{field.Invalid(specPath, field.OmitValueType{}, err.Error())}
	}
	return podSets, nil
}

func (w *flinkDeploymentWebhook) isManagedByKueue(ctx context.Context, kueueJob *flinkDeployment) (bool, error) {
	if kueueJob.Spec.Job == nil {
		return jobframework.QueueName(kueueJob) != "", nil
	}
	if !w.manageJobsWithoutQueueName && jobframework.QueueName(kueueJob) == "" {
		return false, nil
	}
	managed, err := w.im().WorkloadShouldBeSuspended(ctx, kueueJob.Object(), w.client, w.manageJobsWithoutQueueName, w.managedJobsNamespaceSelector)
	if err != nil && errors.Is(err, jobframework.ErrWorkloadOwnerNotFound) && !kueueJob.DeletionTimestamp.IsZero() && features.Enabled(features.SkipAncestorCheckForDeletedWorkloads) {
		return false, nil
	}
	return managed, err
}

func dedupFieldErrors(errs field.ErrorList) field.ErrorList {
	seen := make(map[string]struct{}, len(errs))
	return slices.DeleteFunc(errs, func(fe *field.Error) bool {
		k := string(fe.Type) + "|" + fe.Field + "|" + fe.Detail
		_, dup := seen[k]
		seen[k] = struct{}{}
		return dup
	})
}

func hasTASAnnotation(ann map[string]string) bool {
	for k := range ann {
		if strings.HasPrefix(k, "kueue.x-k8s.io/podset-") || k == kueue.PodIndexOffsetAnnotation {
			return true
		}
	}
	return false
}

func (w *flinkDeploymentWebhook) validateJobAndSpec(ctx context.Context, kueueJob, oldForSuspend *flinkDeployment, isManaged bool, maxTimeoutOnWorkload *metav1.Duration) (field.ErrorList, error) {
	var allErrors field.ErrorList
	var podSets, oldPodSets []kueue.PodSet
	if isManaged {
		var oldTM *flinkv1beta1.ComponentPodSpec
		if oldForSuspend != nil {
			_, oldTM = components(&oldForSuspend.Spec)
			oldPodSets, _ = oldForSuspend.PodSets(ctx, nil)
		}
		if podSets, allErrors = validateManagedFlinkSpec(kueueJob, oldTM); kueueJob.Spec.Job == nil {
			return allErrors, nil
		}
	}
	specOK := len(allErrors) == 0
	allErrors = append(allErrors, jobframework.ValidateJobOnCreate(kueueJob, maxTimeoutOnWorkload)...)
	if isManaged && specOK && features.Enabled(features.TopologyAwareScheduling) {
		jmComp, tmComp := components(&kueueJob.Spec)
		rolePaths, roleComps := [2]*field.Path{jobManagerSpecPath, taskManagerSpecPath}, [2]*flinkv1beta1.ComponentPodSpec{jmComp, tmComp}
		podSetAnnotationsByName, sameGroup := make(map[kueue.PodSetReference]*field.Path, podSetsCount), len(oldPodSets) == len(podSets)
		var tasErrs field.ErrorList
		for i := range podSets {
			ps, sameOld := &podSets[i], i < len(oldPodSets) && oldPodSets[i].Count == podSets[i].Count
			_ = setTopologyRequestIfEnabled(ps)
			metaPath := rolePaths[i].Child("podTemplate", "metadata")
			if (roleComps[i] == nil || roleComps[i].PodTemplate == nil || !hasTASAnnotation(roleComps[i].PodTemplate.Annotations)) &&
				kueueJob.Spec.PodTemplate != nil && hasTASAnnotation(kueueJob.Spec.PodTemplate.Annotations) {
				metaPath = podTemplateSpecPath.Child("metadata")
			}
			podSetAnnotationsByName[ps.Name] = metaPath.Child("annotations")
			tasErrs = append(tasErrs, jobframework.ValidateTASPodSetRequest(metaPath, &ps.Template.ObjectMeta)...)
			sameGroup = sameGroup && sameOld && apiequality.Semantic.DeepEqual(oldPodSets[i].TopologyRequest, ps.TopologyRequest) &&
				oldPodSets[i].Template.Annotations[kueue.PodSetTopologySpreadingAnnotation] == ps.Template.Annotations[kueue.PodSetTopologySpreadingAnnotation]
			if !sameOld || oldPodSets[i].Template.Annotations[kueue.PodSetSliceSizeAnnotation] != ps.Template.Annotations[kueue.PodSetSliceSizeAnnotation] ||
				oldPodSets[i].Template.Annotations[kueue.PodSetSliceRequiredTopologyConstraintsAnnotation] != ps.Template.Annotations[kueue.PodSetSliceRequiredTopologyConstraintsAnnotation] ||
				len(jobframework.ValidateSliceSizeAnnotationUpperBound(metaPath, &oldPodSets[i].Template.ObjectMeta, &oldPodSets[i])) == 0 {
				tasErrs = append(tasErrs, jobframework.ValidateSliceSizeAnnotationUpperBound(metaPath, &ps.Template.ObjectMeta, ps)...)
			}
		}
		if !sameGroup || len(jobframework.ValidatePodSetGroupingTopology(oldPodSets, podSetAnnotationsByName)) == 0 {
			tasErrs = append(tasErrs, jobframework.ValidatePodSetGroupingTopology(podSets, podSetAnnotationsByName)...)
		}
		allErrors = append(allErrors, dedupFieldErrors(tasErrs)...)
	}
	return allErrors, nil
}

func specsEqual(a, b *flinkv1beta1.FlinkDeploymentSpec) bool {
	b1, e1 := json.Marshal(a)
	b2, e2 := json.Marshal(b)
	return e1 == nil && e2 == nil && bytes.Equal(b1, b2) && apiequality.Semantic.DeepEqual(a, b)
}

func (w *flinkDeploymentWebhook) ValidateUpdate(ctx context.Context, oldObj, newObj *flinkv1beta1.FlinkDeployment) (admission.Warnings, error) {
	oldJob, newJob := fromObject(oldObj), fromObject(newObj)
	oldManaged, err1 := w.isManagedByKueue(ctx, oldJob)
	newManaged, err2 := w.isManagedByKueue(ctx, newJob)
	if err := errors.Join(err1, err2); err != nil || (!oldManaged && !newManaged) {
		return nil, err
	}
	ctrl.LoggerFrom(ctx).WithName("flinkdeployment-webhook").V(5).Info("Validating update")

	if (oldObj.Spec.Job == nil) != (newObj.Spec.Job == nil) {
		return nil, field.ErrorList{field.Invalid(jobSpecPath, field.OmitValueType{}, "cannot switch between Session and Application mode")}.ToAggregate()
	}
	if oldObj.Spec.Job == nil {
		if jobframework.QueueName(newJob) == "" && (newObj.DeletionTimestamp.IsZero() || specsEqual(&oldObj.Spec, &newObj.Spec)) {
			return nil, nil
		}
		if newObj.DeletionTimestamp.IsZero() {
			return nil, field.ErrorList{field.Required(jobSpecPath, "only Application mode (spec.job != nil) is supported for a Kueue-managed FlinkDeployment")}.ToAggregate()
		}
	}

	allErrors := jobframework.ValidateJobOnUpdate(oldJob, newJob, w.defaultQueueExist, w.maxTimeoutOnWorkload)
	if oldManaged && !oldJob.IsSuspended() && newJob.IsSuspended() {
		allErrors = append(allErrors, apivalidation.ValidateImmutableField(jobframework.QueueName(newJob), jobframework.QueueName(oldJob), labelsPath.Key(constants.QueueLabel))...)
		if !workloadslicing.Enabled(oldJob.Object()) {
			oldWl, newWl := jobframework.PrebuiltWorkloadNameFor(oldJob.Object()), jobframework.PrebuiltWorkloadNameFor(newJob.Object())
			wlObj := newJob.Object()
			if newWl == "" {
				wlObj = oldJob.Object()
			}
			allErrors = append(allErrors, apivalidation.ValidateImmutableField(newWl, oldWl, jobframework.GetPrebuiltWorkloadPath(wlObj))...)
		}
		allErrors = append(allErrors, jobframework.ValidateUpdateForWorkloadPriorityClassName(false, oldJob.Object(), newJob.Object())...)
	}
	if !newObj.DeletionTimestamp.IsZero() {
		if !specsEqual(&oldObj.Spec, &newObj.Spec) {
			allErrors = append(allErrors, field.Invalid(specPath, field.OmitValueType{}, fmt.Sprintf("%s when FlinkDeployment is being deleted", apivalidation.FieldImmutableErrorMsg)))
		}
		return nil, allErrors.ToAggregate()
	}
	if oldManaged && !oldJob.IsSuspended() && newJob.IsSuspended() {
		suspendedCopy := oldObj.Spec.DeepCopy()
		suspendedCopy.Job.State = flinkv1beta1.JobStateSuspended
		if specsEqual(suspendedCopy, &newObj.Spec) {
			return nil, allErrors.ToAggregate()
		}
	}

	var createTimeout *metav1.Duration
	var oldForSuspend *flinkDeployment
	if !oldManaged && newManaged {
		createTimeout = w.maxTimeoutOnWorkload
	} else if oldManaged && newJob.IsSuspended() {
		oldForSuspend = oldJob
	}
	validationErrs, err := w.validateJobAndSpec(ctx, newJob, oldForSuspend, newManaged, createTimeout)
	if err == nil {
		allErrors = dedupFieldErrors(append(allErrors, validationErrs...))
		if len(validationErrs) == 0 && !oldJob.IsSuspended() && !newJob.IsSuspended() {
			allErrors = append(allErrors, validateRunningImmutability(ctx, oldObj, newObj)...)
		}
	}
	return nil, errors.Join(err, allErrors.ToAggregate())
}

func validateRunningImmutability(ctx context.Context, oldObj, newObj *flinkv1beta1.FlinkDeployment) field.ErrorList {
	var allErrs field.ErrorList
	immutableMsg := fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg)
	addIf := func(changed bool, p *field.Path, val any) {
		if changed {
			allErrs = append(allErrs, field.Invalid(p, val, immutableMsg))
		}
	}
	addIf(oldObj.Spec.Mode != newObj.Spec.Mode, specPath.Child("mode"), newObj.Spec.Mode)
	addIf(oldObj.Spec.Image != newObj.Spec.Image, specPath.Child("image"), newObj.Spec.Image)
	addIf(oldObj.Spec.ImagePullPolicy != newObj.Spec.ImagePullPolicy, specPath.Child("imagePullPolicy"), newObj.Spec.ImagePullPolicy)
	addIf(oldObj.Spec.ServiceAccount != newObj.Spec.ServiceAccount, specPath.Child("serviceAccount"), newObj.Spec.ServiceAccount)
	addIf(!specsEqual(&flinkv1beta1.FlinkDeploymentSpec{PodTemplate: oldObj.Spec.PodTemplate}, &flinkv1beta1.FlinkDeploymentSpec{PodTemplate: newObj.Spec.PodTemplate}),
		podTemplateSpecPath, field.OmitValueType{})
	addIf(oldObj.Spec.Job != nil && newObj.Spec.Job != nil && !ptr.Equal(oldObj.Spec.Job.Parallelism, newObj.Spec.Job.Parallelism),
		jobSpecPath.Child("parallelism"), ptr.Deref(newObj.Spec.Job.Parallelism, 0))
	oldFlatCfg, oldFlatErr := oldObj.Spec.FlinkConfiguration.FlatMap()
	newFlatCfg, newFlatErr := newObj.Spec.FlinkConfiguration.FlatMap()
	if oldFlatErr == nil && newFlatErr == nil {
		for _, k := range []string{flinkConfigNumberOfTaskSlots, flinkConfigPodTemplateMergeArraysByName} {
			v1, ok1 := oldFlatCfg[k]
			v2, ok2 := newFlatCfg[k]
			addIf(ok1 != ok2 || v1 != v2, flinkConfigurationPath.Key(k), v2)
		}
	}
	if newPodSets, newErr := fromObject(newObj).PodSets(ctx, nil); newErr == nil {
		oldPodSets, oldErr := fromObject(oldObj).PodSets(ctx, nil)
		addIf(oldErr != nil || !apiequality.Semantic.DeepEqual(oldPodSets, newPodSets), specPath, field.OmitValueType{})
	}
	return allErrs
}

func (w *flinkDeploymentWebhook) ValidateDelete(_ context.Context, _ *flinkv1beta1.FlinkDeployment) (admission.Warnings, error) {
	return nil, nil
}
