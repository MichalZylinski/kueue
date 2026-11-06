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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	utilpodset "sigs.k8s.io/kueue/pkg/util/podset"
	"sigs.k8s.io/kueue/pkg/util/webhook"
)

type FlinkDeploymentWebhook struct {
	integrationManager           *jobframework.IntegrationManager
	client                       client.Client
	queues                       *qcache.Manager
	manageJobsWithoutQueueName   bool
	managedJobsNamespaceSelector labels.Selector
	cache                        *schdcache.Cache
	maxTimeoutOnWorkload         *metav1.Duration
}

func SetupWebhook(mgr ctrl.Manager, opts ...jobframework.Option) error {
	options := jobframework.ProcessOptions(opts...)
	wh := &FlinkDeploymentWebhook{
		integrationManager:           options.IntegrationManager,
		client:                       mgr.GetClient(),
		queues:                       options.Queues,
		manageJobsWithoutQueueName:   options.ManageJobsWithoutQueueName,
		managedJobsNamespaceSelector: options.ManagedJobsNamespaceSelector,
		cache:                        options.Cache,
		maxTimeoutOnWorkload:         options.MaxTimeoutOnWorkload,
	}
	obj := &flinkv1beta1.FlinkDeployment{}
	if options.NoopWebhook {
		return webhook.SetupNoopWebhook(mgr, obj)
	}
	return ctrl.NewWebhookManagedBy(mgr, obj).
		WithValidator(wh).
		WithDefaulter(wh).
		WithLogConstructor(jobframework.WebhookLogConstructor(fromObject(obj).GVK(), options.RoleTracker)).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-flink-apache-org-v1beta1-flinkdeployment,mutating=true,failurePolicy=fail,sideEffects=None,groups=flink.apache.org,resources=flinkdeployments,verbs=create,versions=v1beta1,name=mflinkdeployment.kb.io,admissionReviewVersions=v1

var _ admission.Defaulter[*flinkv1beta1.FlinkDeployment] = &FlinkDeploymentWebhook{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the type
func (w *FlinkDeploymentWebhook) Default(ctx context.Context, obj *flinkv1beta1.FlinkDeployment) error {
	job := fromObject(obj)
	log := ctrl.LoggerFrom(ctx).WithName("flinkdeployment-webhook")
	log.V(5).Info("Applying defaults")

	if err := w.integrationManager.ApplyDefaultLocalQueue(ctx, w.client, job.Object(), w.queues.DefaultLocalQueueExist, w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	if err := w.integrationManager.ApplyDefaultWorkloadPriorityClass(ctx, w.client, job.Object(), w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	if err := w.integrationManager.ApplyDefaultForSuspend(ctx, job, w.client, w.manageJobsWithoutQueueName, w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	jobframework.ApplyDefaultForManagedBy(job, w.queues, w.cache, log)

	return nil
}

// +kubebuilder:webhook:path=/validate-flink-apache-org-v1beta1-flinkdeployment,mutating=false,failurePolicy=fail,sideEffects=None,groups=flink.apache.org,resources=flinkdeployments,verbs=create;update,versions=v1beta1,name=vflinkdeployment.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*flinkv1beta1.FlinkDeployment] = &FlinkDeploymentWebhook{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type
func (w *FlinkDeploymentWebhook) ValidateCreate(ctx context.Context, obj *flinkv1beta1.FlinkDeployment) (admission.Warnings, error) {
	log := ctrl.LoggerFrom(ctx).WithName("flinkdeployment-webhook")
	log.Info("Validating create")
	validationErrs, err := w.validateCreate(ctx, obj, w.maxTimeoutOnWorkload)
	if err != nil {
		return nil, err
	}
	return nil, validationErrs.ToAggregate()
}

func (w *FlinkDeploymentWebhook) validateCreate(ctx context.Context, job *flinkv1beta1.FlinkDeployment, maxTimeoutOnWorkload *metav1.Duration) (field.ErrorList, error) {
	var allErrors field.ErrorList
	kueueJob := (*FlinkDeployment)(job)

	allErrors = append(allErrors, jobframework.ValidateJobOnCreate(kueueJob, maxTimeoutOnWorkload)...)
	if features.Enabled(features.TopologyAwareScheduling) {
		validationErrs, err := w.validateTopologyRequest(ctx, kueueJob)
		if err != nil {
			return nil, err
		}
		allErrors = append(allErrors, validationErrs...)
	}

	return allErrors, nil
}

func (w *FlinkDeploymentWebhook) validateTopologyRequest(ctx context.Context, flinkApp *FlinkDeployment) (field.ErrorList, error) {
	var allErrs field.ErrorList

	podSets, podSetsErr := flinkApp.PodSets(ctx, nil)
	if podSetsErr == nil {
		jmPodSet := utilpodset.FindPodSetByName(podSets, jobManagerPodSetName)
		allErrs = append(allErrs, jobframework.ValidateTASPodSetRequest(field.NewPath("spec", "jobManager", "podTemplate"), &jmPodSet.Template.ObjectMeta)...)
		allErrs = append(allErrs, jobframework.ValidateSliceSizeAnnotationUpperBound(field.NewPath("spec", "jobManager", "podTemplate"), &jmPodSet.Template.ObjectMeta, jmPodSet)...)

		tmPodSet := utilpodset.FindPodSetByName(podSets, taskManagerPodSetName)
		allErrs = append(allErrs, jobframework.ValidateTASPodSetRequest(field.NewPath("spec", "taskManager", "podTemplate"), &tmPodSet.Template.ObjectMeta)...)
		allErrs = append(allErrs, jobframework.ValidateSliceSizeAnnotationUpperBound(field.NewPath("spec", "taskManager", "podTemplate"), &tmPodSet.Template.ObjectMeta, tmPodSet)...)
	}

	if len(allErrs) > 0 {
		return allErrs, nil
	}

	return nil, podSetsErr
}

func (w *FlinkDeploymentWebhook) ValidateUpdate(ctx context.Context, oldJob, newJob *flinkv1beta1.FlinkDeployment) (admission.Warnings, error) {
	log := ctrl.LoggerFrom(ctx).WithName("flinkdeployment-webhook")
	log.Info("Validating update")
	kueueOldJob := (*FlinkDeployment)(oldJob)
	kueueNewJob := (*FlinkDeployment)(newJob)

	allErrors := jobframework.ValidateJobOnUpdate(kueueOldJob, kueueNewJob, w.queues.DefaultLocalQueueExist, w.maxTimeoutOnWorkload)
	validationErrs, err := w.validateCreate(ctx, newJob, nil)
	if err != nil {
		return nil, err
	}
	allErrors = append(allErrors, validationErrs...)

	return nil, allErrors.ToAggregate()
}

func (w *FlinkDeploymentWebhook) ValidateDelete(_ context.Context, _ *flinkv1beta1.FlinkDeployment) (admission.Warnings, error) {
	return nil, nil
}
