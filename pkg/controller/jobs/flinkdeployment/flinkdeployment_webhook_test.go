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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"

	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

func TestDefaulting(t *testing.T) {
	ctx := context.Background()
	kClient := utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme).Build()
	cqCache := schdcache.New(kClient)
	queueManager := qcache.NewManagerForUnitTests(kClient, cqCache)

	integrationMgr := jobframework.NewIntegrationManager()
	if err := RegisterIntegration(integrationMgr); err != nil {
		t.Fatalf("RegisterIntegration error: %v", err)
	}

	wh := &FlinkDeploymentWebhook{
		integrationManager:           integrationMgr,
		client:                       kClient,
		queues:                       queueManager,
		cache:                        cqCache,
		manageJobsWithoutQueueName:   false,
		managedJobsNamespaceSelector: labels.Everything(),
	}

	flinkDep := &flinkv1beta1.FlinkDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-flink",
			Namespace: "default",
			Labels: map[string]string{
				"kueue.x-k8s.io/queue-name": "user-queue",
			},
		},
		Spec: flinkv1beta1.FlinkDeploymentSpec{
			Image: "flink:1.18",
			Job: &flinkv1beta1.JobSpec{
				State: flinkv1beta1.JobStateRunning,
			},
		},
	}

	if err := wh.Default(ctx, flinkDep); err != nil {
		t.Fatalf("Defaulting error: %v", err)
	}

	if flinkDep.Spec.Job.State != flinkv1beta1.JobStateSuspended {
		t.Errorf("Expected job to be defaulted to suspended, got %s", flinkDep.Spec.Job.State)
	}
}

func TestValidation(t *testing.T) {
	ctx := context.Background()
	clientBuilder := utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme)
	kClient := clientBuilder.Build()

	integrationMgr := jobframework.NewIntegrationManager()
	if err := RegisterIntegration(integrationMgr); err != nil {
		t.Fatalf("RegisterIntegration error: %v", err)
	}

	wh := &FlinkDeploymentWebhook{
		integrationManager:           integrationMgr,
		client:                       kClient,
		manageJobsWithoutQueueName:   false,
		managedJobsNamespaceSelector: labels.Everything(),
	}

	flinkDep := &flinkv1beta1.FlinkDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-flink",
			Namespace: "default",
			Labels: map[string]string{
				"kueue.x-k8s.io/queue-name": "user-queue",
			},
		},
		Spec: flinkv1beta1.FlinkDeploymentSpec{
			Image: "flink:1.18",
			JobManager: &flinkv1beta1.JobManagerSpec{
				Replicas: ptr.To[int32](1),
			},
			TaskManager: &flinkv1beta1.TaskManagerSpec{
				Replicas: ptr.To[int32](2),
			},
			Job: &flinkv1beta1.JobSpec{
				State: flinkv1beta1.JobStateSuspended,
			},
		},
	}

	if _, err := wh.ValidateCreate(ctx, flinkDep); err != nil {
		t.Errorf("ValidateCreate unexpectedly failed: %v", err)
	}
}
