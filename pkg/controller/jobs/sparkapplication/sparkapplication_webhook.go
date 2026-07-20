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
	"slices"

	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	"sigs.k8s.io/kueue/pkg/util/podset"
	"sigs.k8s.io/kueue/pkg/util/webhook"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var (
	specPath                     = field.NewPath("spec")
	dynamicAllocationPath        = specPath.Child("dynamicAllocation")
	dynamicAllocationEnabledPath = dynamicAllocationPath.Child("enabled")
	maxExecutorsPath             = dynamicAllocationPath.Child("maxExecutors")
	driverSpecPath               = specPath.Child("driver")
	driverSchedulingGatesPath    = driverSpecPath.Child("template", "spec", "schedulingGates")
	executorSpecPath             = specPath.Child("executor")
	executorSchedulingGatesPath  = executorSpecPath.Child("template", "spec", "schedulingGates")
	restartPolicyTypePath        = specPath.Child("restartPolicy").Child("type")
	batchSchedulerPath           = specPath.Child("batchScheduler")
)

type SparkApplicationWebhook struct {
	client                       client.Client
	queues                       *qcache.Manager
	manageJobsWithoutQueueName   bool
	managedJobsNamespaceSelector labels.Selector
	cache                        *schdcache.Cache
}

func SetupWebhook(mgr ctrl.Manager, opts ...jobframework.Option) error {
	options := jobframework.ProcessOptions(opts...)
	wh := &SparkApplicationWebhook{
		client:                       mgr.GetClient(),
		queues:                       options.Queues,
		manageJobsWithoutQueueName:   options.ManageJobsWithoutQueueName,
		managedJobsNamespaceSelector: options.ManagedJobsNamespaceSelector,
		cache:                        options.Cache,
	}
	obj := &sparkv1beta2.SparkApplication{}
	if options.NoopWebhook {
		return webhook.SetupNoopWebhook(mgr, obj)
	}
	return ctrl.NewWebhookManagedBy(mgr, obj).
		WithValidator(wh).
		WithDefaulter(wh).
		WithLogConstructor(jobframework.WebhookLogConstructor(fromObject(obj).GVK(), options.RoleTracker)).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-sparkoperator-k8s-io-v1beta2-sparkapplication,mutating=true,failurePolicy=fail,sideEffects=None,groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=create,versions=v1beta2,name=msparkapplication.kb.io,admissionReviewVersions=v1

var _ admission.Defaulter[*sparkv1beta2.SparkApplication] = &SparkApplicationWebhook{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the type
func (w *SparkApplicationWebhook) Default(ctx context.Context, obj *sparkv1beta2.SparkApplication) error {
	job := fromObject(obj)
	log := ctrl.LoggerFrom(ctx).WithName("sparkapplication-webhook")
	log.V(5).Info("Applying defaults")

	jobframework.ApplyDefaultLocalQueue(job.Object(), w.queues.DefaultLocalQueueExist)
	jobframework.ApplyDefaultWorkloadPriorityClass(ctx, w.client, job.Object())
	if err := jobframework.ApplyDefaultForSuspend(ctx, job, w.client, w.manageJobsWithoutQueueName, w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	jobframework.ApplyDefaultForManagedBy(job, w.queues, w.cache, log)

	if isAnElasticJob(obj) {
		// Ensure the elastic-job scheduling gate is present in both the driver
		// and executor pod templates. The operator renders these templates
		// (spec.driver.template / spec.executor.template) into the pod
		// template files passed to spark-submit, so every executor pod the
		// driver creates at runtime — including scale-ups — is born gated.
		if obj.Spec.Driver.Template == nil {
			obj.Spec.Driver.Template = emptyDriverPodTemplateSpec.DeepCopy()
		}
		utilpod.GateTemplate(obj.Spec.Driver.Template, kueue.ElasticJobSchedulingGate)

		if obj.Spec.Executor.Template == nil {
			obj.Spec.Executor.Template = emptyExecutorPodTemplateSpec.DeepCopy()
		}
		utilpod.GateTemplate(obj.Spec.Executor.Template, kueue.ElasticJobSchedulingGate)
	}

	return nil
}

// +kubebuilder:webhook:path=/validate-sparkoperator-k8s-io-v1beta2-sparkapplication,mutating=false,failurePolicy=fail,sideEffects=None,groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=create;update,versions=v1beta2,name=vsparkapplication.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*sparkv1beta2.SparkApplication] = &SparkApplicationWebhook{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type
func (w *SparkApplicationWebhook) ValidateCreate(ctx context.Context, obj *sparkv1beta2.SparkApplication) (admission.Warnings, error) {
	log := ctrl.LoggerFrom(ctx).WithName("sparkapplication-webhook")
	log.Info("Validating create")
	validationErrs, err := w.validateCreate(ctx, obj)
	if err != nil {
		return nil, err
	}
	return nil, validationErrs.ToAggregate()
}

// returns whether the SparkApplication is an elastic job or not
func isAnElasticJob(sparkApp *sparkv1beta2.SparkApplication) bool {
	return workloadslicing.Enabled(sparkApp)
}

func (w *SparkApplicationWebhook) validateCreate(ctx context.Context, job *sparkv1beta2.SparkApplication) (field.ErrorList, error) {
	var allErrors field.ErrorList
	kueueJob := (*SparkApplication)(job)

	if w.manageJobsWithoutQueueName || jobframework.QueueName(kueueJob) != "" {
		spec := &job.Spec

		if spec.Mode != sparkv1beta2.DeployModeCluster {
			allErrors = append(allErrors, field.Invalid(specPath.Child("mode"), spec.Mode, "only Cluster mode is supported for a kueue managed job"))
		}

		if isAnElasticJob(job) {
			allErrors = append(allErrors, validateElasticJob(job)...)
		} else if ptr.Deref(spec.DynamicAllocation, sparkv1beta2.DynamicAllocation{}).Enabled {
			allErrors = append(allErrors,
				field.Invalid(dynamicAllocationEnabledPath,
					ptr.Deref(spec.DynamicAllocation, sparkv1beta2.DynamicAllocation{}).Enabled,
					"a kueue managed job can use dynamicAllocation only when the ElasticJobsViaWorkloadSlices feature gate is on and the job is an elastic job",
				),
			)
		}

		// The operator resubmits a FAILED application when restartPolicy allows
		// it (Always/OnFailure), which happens after Kueue has already released
		// quota via Finished(). Retries must be owned by Kueue's own requeue
		// machinery, not the operator's, to keep quota accounting correct across
		// attempts. An unset type ("") behaves like Never at runtime and is
		// accepted; only the two restart-triggering values are rejected.
		if spec.RestartPolicy.Type == sparkv1beta2.RestartPolicyAlways || spec.RestartPolicy.Type == sparkv1beta2.RestartPolicyOnFailure {
			allErrors = append(allErrors,
				field.Invalid(restartPolicyTypePath, spec.RestartPolicy.Type,
					"a kueue managed job must use restartPolicy.type=Never (or leave it unset); retries are managed by Kueue",
				),
			)
		}

		// spec.batchScheduler delegates gang scheduling to another scheduler
		// (e.g. Volcano, YuniKorn), which conflicts with Kueue's own admission
		// control over the same pods.
		if ptr.Deref(spec.BatchScheduler, "") != "" {
			allErrors = append(allErrors,
				field.Invalid(batchSchedulerPath, *spec.BatchScheduler,
					"a kueue managed job cannot set batchScheduler; it conflicts with Kueue's admission control",
				),
			)
		}
	}

	allErrors = append(allErrors, jobframework.ValidateJobOnCreate(kueueJob)...)
	if features.Enabled(features.TopologyAwareScheduling) {
		validationErrs, err := w.validateTopologyRequest(ctx, kueueJob)
		if err != nil {
			return nil, err
		}
		allErrors = append(allErrors, validationErrs...)
	}

	return allErrors, nil
}

// validateElasticJob validates the additional constraints an elastic
// SparkApplication must satisfy on top of the generic elastic-job validation
// (annotation-supported-GVK check, annotation immutability) already applied
// by jobframework.ValidateJobOnCreate/ValidateJobOnUpdate.
func validateElasticJob(job *sparkv1beta2.SparkApplication) field.ErrorList {
	var allErrors field.ErrorList

	// An elastic SparkApplication without dynamic allocation has no scaling
	// mechanism: any spec edit restarts the whole application via the
	// operator's INVALIDATING state, so the elastic annotation would be
	// meaningless. Require dynamicAllocation explicitly rather than silently
	// tolerating it.
	da := ptr.Deref(job.Spec.DynamicAllocation, sparkv1beta2.DynamicAllocation{})
	if !da.Enabled {
		allErrors = append(allErrors,
			field.Invalid(dynamicAllocationEnabledPath, da.Enabled,
				"an elastic job must enable dynamicAllocation",
			),
		)
	}

	// maxExecutors bounds the workload's growth and caps the demand
	// observation loop; without it there is no ceiling on how much quota a
	// single elastic application can eventually claim.
	if ptr.Deref(da.MaxExecutors, 0) <= 0 {
		allErrors = append(allErrors,
			field.Invalid(maxExecutorsPath, ptr.Deref(da.MaxExecutors, 0),
				"an elastic job must set dynamicAllocation.maxExecutors",
			),
		)
	} else if minExecutors := ptr.Deref(da.MinExecutors, 0); minExecutors > *da.MaxExecutors {
		allErrors = append(allErrors,
			field.Invalid(maxExecutorsPath, *da.MaxExecutors,
				"must be greater than or equal to dynamicAllocation.minExecutors",
			),
		)
	}

	workloadSliceSchedulingGate := corev1.PodSchedulingGate{Name: kueue.ElasticJobSchedulingGate}

	if job.Spec.Driver.Template == nil || !slices.Contains(job.Spec.Driver.Template.Spec.SchedulingGates, workloadSliceSchedulingGate) {
		allErrors = append(allErrors,
			field.Invalid(driverSchedulingGatesPath, job.Spec.Driver.Template,
				"an elastic job must have the ElasticJobSchedulingGate",
			),
		)
	}

	if job.Spec.Executor.Template == nil || !slices.Contains(job.Spec.Executor.Template.Spec.SchedulingGates, workloadSliceSchedulingGate) {
		allErrors = append(allErrors,
			field.Invalid(executorSchedulingGatesPath, job.Spec.Executor.Template,
				"an elastic job must have the ElasticJobSchedulingGate",
			),
		)
	}

	return allErrors
}

func (w *SparkApplicationWebhook) validateTopologyRequest(ctx context.Context, sparkApp *SparkApplication) (field.ErrorList, error) {
	var allErrs field.ErrorList

	// Reject elastic jobs with required/preferred topology (only unconstrained
	// is supported for the initial iteration of elastic TAS integration).
	if isAnElasticJob((*sparkv1beta2.SparkApplication)(sparkApp)) {
		for _, roleAnnotations := range []struct {
			path        *field.Path
			annotations map[string]string
		}{
			{driverSpecPath, sparkApp.Spec.Driver.Annotations},
			{executorSpecPath, sparkApp.Spec.Executor.Annotations},
		} {
			if _, hasRequired := roleAnnotations.annotations[kueue.PodSetRequiredTopologyAnnotation]; hasRequired {
				return field.ErrorList{field.Forbidden(
					roleAnnotations.path.Child("annotations", kueue.PodSetRequiredTopologyAnnotation),
					"required topology is not supported with elastic jobs",
				)}, nil
			}
			if _, hasPreferred := roleAnnotations.annotations[kueue.PodSetPreferredTopologyAnnotation]; hasPreferred {
				return field.ErrorList{field.Forbidden(
					roleAnnotations.path.Child("annotations", kueue.PodSetPreferredTopologyAnnotation),
					"preferred topology is not supported with elastic jobs",
				)}, nil
			}
		}
	}

	podSets, podSetsErr := sparkApp.PodSets(ctx, nil)

	if podSetsErr == nil {
		driverPodSet := podset.FindPodSetByName(podSets, driverPodSetName)
		allErrs = append(allErrs, jobframework.ValidateTASPodSetRequest(driverSpecPath, &driverPodSet.Template.ObjectMeta)...)
		allErrs = append(allErrs, jobframework.ValidateSliceSizeAnnotationUpperBound(driverSpecPath, &driverPodSet.Template.ObjectMeta, driverPodSet)...)

		executorPodSet := podset.FindPodSetByName(podSets, executorPodSetName)
		allErrs = append(allErrs, jobframework.ValidateTASPodSetRequest(executorSpecPath, &executorPodSet.Template.ObjectMeta)...)
		allErrs = append(allErrs, jobframework.ValidateSliceSizeAnnotationUpperBound(executorSpecPath, &executorPodSet.Template.ObjectMeta, executorPodSet)...)
	}

	if len(allErrs) > 0 {
		return allErrs, nil
	}

	return nil, podSetsErr
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type
func (w *SparkApplicationWebhook) ValidateUpdate(ctx context.Context, oldSparkApp, newSparkApp *sparkv1beta2.SparkApplication) (admission.Warnings, error) {
	log := ctrl.LoggerFrom(ctx).WithName("sparkapplication-webhook")
	if w.manageJobsWithoutQueueName || jobframework.QueueName(fromObject(newSparkApp)) != "" {
		log.Info("Validating update")
		allErrors := jobframework.ValidateJobOnUpdate(fromObject(oldSparkApp), fromObject(newSparkApp), w.queues.DefaultLocalQueueExist)
		validationErrs, err := w.validateCreate(ctx, newSparkApp)
		if err != nil {
			return nil, err
		}
		allErrors = append(allErrors, validationErrs...)
		return nil, allErrors.ToAggregate()
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type
func (w *SparkApplicationWebhook) ValidateDelete(_ context.Context, _ *sparkv1beta2.SparkApplication) (admission.Warnings, error) {
	return nil, nil
}
