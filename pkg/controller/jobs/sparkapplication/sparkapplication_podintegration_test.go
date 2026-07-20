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
	"testing"

	"github.com/google/go-cmp/cmp"
	sparkappv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"

	// Blank-imported so this test binary registers the plain-pod integration
	// too, matching a real kueue-controller-manager (which always compiles in
	// every built-in integration via pkg/controller/jobs). Without this,
	// isKnownOwner would never recognize the intermediate driver-Pod hop and
	// this test would give a false negative that doesn't reflect production
	// behavior.
	_ "sigs.k8s.io/kueue/pkg/controller/jobs/pod"

	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
)

// TestFindAncestorJobManagedByKueueForExecutorPod is a regression test proving
// that a Spark executor pod — owned by the driver pod, which is in turn owned
// by the SparkApplication (a two-hop Pod -> Pod -> SparkApplication chain,
// unusual among Kueue integrations) — is correctly attributed to its
// SparkApplication ancestor by the shared jobframework ownership walk.
//
// This matters because the plain-pod integration's webhook skips suspending
// and queue-labeling any pod whose ancestor is already Kueue-managed
// (jobframework.FindAncestorJobManagedByKueue). If that walk stopped at the
// intermediate driver Pod instead of continuing to the SparkApplication,
// executor pods could be independently gated/suspended and double-counted
// whenever the plain-pod integration is also active in the cluster.
func TestFindAncestorJobManagedByKueueForExecutorPod(t *testing.T) {
	sparkApp := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns").
		Queue("local-queue").
		Obj()
	sparkApp.UID = types.UID("sparkapp-uid")

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sparkapp-driver",
			Namespace: "ns",
			UID:       types.UID("driver-uid"),
		},
	}
	utiltesting.AppendOwnerReference(driverPod, gvk, sparkApp.Name, string(sparkApp.UID), new(true), new(true))

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sparkapp-exec-1",
			Namespace: "ns",
			UID:       types.UID("executor-uid"),
		},
	}
	utiltesting.AppendOwnerReference(executorPod, corev1.SchemeGroupVersion.WithKind("Pod"), driverPod.Name, string(driverPod.UID), new(true), new(true))

	testCases := map[string]struct {
		queueName string
		want      *sparkappv1beta2.SparkApplication
	}{
		"executor pod resolves to its SparkApplication ancestor through the driver pod": {
			queueName: "local-queue",
			want:      sparkApp,
		},
		"no ancestor is reported when the SparkApplication has no queue-name": {
			queueName: "",
			want:      nil,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(jobframework.EnableIntegrationsForTest(t, FrameworkName))

			app := sparkApp.DeepCopy()
			if tc.queueName == "" {
				delete(app.Labels, controllerconstants.QueueLabel)
			} else {
				app.Labels[controllerconstants.QueueLabel] = tc.queueName
			}

			kClient := utiltesting.NewClientBuilder(sparkappv1beta2.AddToScheme).
				WithObjects(app, driverPod).
				Build()

			ctx, _ := utiltesting.ContextWithLog(t)
			got, err := jobframework.FindAncestorJobManagedByKueue(ctx, kClient, executorPod, false)
			if err != nil {
				t.Fatalf("FindAncestorJobManagedByKueue() returned error: %v", err)
			}

			var gotSparkApp *sparkappv1beta2.SparkApplication
			if got != nil {
				gotSparkApp = got.(*sparkappv1beta2.SparkApplication)
			}
			if diff := cmp.Diff(tc.want, gotSparkApp, sparkAppCmpOpts); diff != "" {
				t.Errorf("FindAncestorJobManagedByKueue() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
