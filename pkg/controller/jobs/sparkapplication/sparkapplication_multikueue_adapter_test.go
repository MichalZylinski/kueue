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
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/featuregate"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/util/slices"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
)

const testNamespace = "ns"

func TestMultiKueueAdapter(t *testing.T) {
	objCheckOpts := cmp.Options{
		cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion"),
		cmpopts.EquateEmpty(),
	}

	baseAppBuilder := sparkapplicationtesting.MakeSparkApplication("app1", testNamespace)

	cases := map[string]struct {
		managersApps []sparkv1beta2.SparkApplication
		workerApps   []sparkv1beta2.SparkApplication

		operation func(ctx context.Context, adapter *multiKueueAdapter, managerClient, workerClient client.Client) error

		wantError       error
		wantManagerApps []sparkv1beta2.SparkApplication
		wantWorkerApps  []sparkv1beta2.SparkApplication
		featureGates    map[featuregate.Feature]bool
	}{
		"sync creates missing remote sparkapplication": {
			featureGates: map[featuregate.Feature]bool{features.WorkloadIdentifierAnnotations: false},
			managersApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().Obj(),
			},
			operation: func(ctx context.Context, adapter *multiKueueAdapter, managerClient, workerClient client.Client) error {
				_, err := adapter.SyncJob(ctx, managerClient, workerClient, types.NamespacedName{Name: "app1", Namespace: testNamespace}, "wl1", "origin1")
				return err
			},
			wantManagerApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().Obj(),
			},
			wantWorkerApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().
					PrebuiltWorkloadLabel("wl1").
					Label(kueue.MultiKueueOriginLabel, "origin1").
					Obj(),
			},
		},
		"sync creates missing remote sparkapplication, WorkloadIdentifierAnnotations enabled": {
			featureGates: map[featuregate.Feature]bool{features.WorkloadIdentifierAnnotations: true},
			managersApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().Obj(),
			},
			operation: func(ctx context.Context, adapter *multiKueueAdapter, managerClient, workerClient client.Client) error {
				_, err := adapter.SyncJob(ctx, managerClient, workerClient, types.NamespacedName{Name: "app1", Namespace: testNamespace}, "wl1", "origin1")
				return err
			},
			wantManagerApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().Obj(),
			},
			wantWorkerApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().
					PrebuiltWorkloadAnnotation("wl1").
					Label(kueue.MultiKueueOriginLabel, "origin1").
					Obj(),
			},
		},
		"sync copies status and replica-sizes annotation from remote sparkapplication": {
			featureGates: map[featuregate.Feature]bool{features.WorkloadIdentifierAnnotations: false},
			managersApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().Obj(),
			},
			workerApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().
					PrebuiltWorkloadLabel("wl1").
					Label(kueue.MultiKueueOriginLabel, "origin1").
					AppState(sparkv1beta2.ApplicationStateRunning).
					Annotation(SparkApplicationPodSetReplicaSizesAnnotation, `[{"name":"executor","count":5}]`).
					Obj(),
			},
			operation: func(ctx context.Context, adapter *multiKueueAdapter, managerClient, workerClient client.Client) error {
				_, err := adapter.SyncJob(ctx, managerClient, workerClient, types.NamespacedName{Name: "app1", Namespace: testNamespace}, "wl1", "origin1")
				return err
			},
			wantManagerApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().
					AppState(sparkv1beta2.ApplicationStateRunning).
					Annotation(SparkApplicationPodSetReplicaSizesAnnotation, `[{"name":"executor","count":5}]`).
					Obj(),
			},
			wantWorkerApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().
					PrebuiltWorkloadLabel("wl1").
					Label(kueue.MultiKueueOriginLabel, "origin1").
					AppState(sparkv1beta2.ApplicationStateRunning).
					Annotation(SparkApplicationPodSetReplicaSizesAnnotation, `[{"name":"executor","count":5}]`).
					Obj(),
			},
		},
		"remote sparkapplication is deleted": {
			featureGates: map[featuregate.Feature]bool{features.WorkloadIdentifierAnnotations: false},
			workerApps: []sparkv1beta2.SparkApplication{
				*baseAppBuilder.Clone().
					PrebuiltWorkloadLabel("wl1").
					Label(kueue.MultiKueueOriginLabel, "origin1").
					Obj(),
			},
			operation: func(ctx context.Context, adapter *multiKueueAdapter, managerClient, workerClient client.Client) error {
				return adapter.DeleteRemoteObject(ctx, managerClient, workerClient, types.NamespacedName{Name: "app1", Namespace: testNamespace})
			},
		},
		"missing sparkapplication is still considered managed": {
			// SparkApplication has no spec.managedBy field, so unlike most other
			// adapters, IsJobManagedByKueue cannot verify ownership from the object
			// itself; it always returns true (see the doc comment on the
			// implementation), matching the Pod adapter's equivalent behavior.
			featureGates: map[featuregate.Feature]bool{features.WorkloadIdentifierAnnotations: false},
			operation: func(ctx context.Context, adapter *multiKueueAdapter, managerClient, workerClient client.Client) error {
				if isManaged, _, _ := adapter.IsJobManagedByKueue(ctx, managerClient, types.NamespacedName{Name: "app1", Namespace: testNamespace}); !isManaged {
					return errors.New("expecting true")
				}
				return nil
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.featureGates)
			managerBuilder := utiltesting.NewClientBuilder(sparkv1beta2.AddToScheme).WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: utiltesting.TreatSSAAsStrategicMerge})
			managerBuilder = managerBuilder.WithLists(&sparkv1beta2.SparkApplicationList{Items: tc.managersApps})
			managerBuilder = managerBuilder.WithStatusSubresource(slices.Map(tc.managersApps, func(w *sparkv1beta2.SparkApplication) client.Object { return w })...)
			managerClient := managerBuilder.Build()

			workerBuilder := utiltesting.NewClientBuilder(sparkv1beta2.AddToScheme).WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: utiltesting.TreatSSAAsStrategicMerge})
			workerBuilder = workerBuilder.WithLists(&sparkv1beta2.SparkApplicationList{Items: tc.workerApps})
			workerClient := workerBuilder.Build()

			ctx, _ := utiltesting.ContextWithLog(t)

			adapter := &multiKueueAdapter{}

			gotErr := tc.operation(ctx, adapter, managerClient, workerClient)

			if diff := cmp.Diff(tc.wantError, gotErr, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("unexpected error (-want/+got):\n%s", diff)
			}

			gotManagerApps := &sparkv1beta2.SparkApplicationList{}
			if err := managerClient.List(ctx, gotManagerApps); err != nil {
				t.Errorf("unexpected list manager's sparkapplication error %s", err)
			} else if diff := cmp.Diff(tc.wantManagerApps, gotManagerApps.Items, objCheckOpts...); diff != "" {
				t.Errorf("unexpected manager's sparkapplication (-want/+got):\n%s", diff)
			}

			gotWorkerApps := &sparkv1beta2.SparkApplicationList{}
			if err := workerClient.List(ctx, gotWorkerApps); err != nil {
				t.Errorf("unexpected list worker's sparkapplication error %s", err)
			} else if diff := cmp.Diff(tc.wantWorkerApps, gotWorkerApps.Items, objCheckOpts...); diff != "" {
				t.Errorf("unexpected worker's sparkapplication (-want/+got):\n%s", diff)
			}
		})
	}
}

func TestMultiKueueAdapterGetEmptyList(t *testing.T) {
	adapter := &multiKueueAdapter{}
	if _, ok := adapter.GetEmptyList().(*sparkv1beta2.SparkApplicationList); !ok {
		t.Error("GetEmptyList did not return a *SparkApplicationList")
	}
}

func TestMultiKueueAdapterWorkloadKeysFor(t *testing.T) {
	adapter := &multiKueueAdapter{}

	t.Run("no prebuilt workload", func(t *testing.T) {
		app := sparkapplicationtesting.MakeSparkApplication("app1", testNamespace).Obj()
		if _, err := adapter.WorkloadKeysFor(app); err == nil {
			t.Error("expected an error for a sparkapplication with no prebuilt workload")
		}
	})

	t.Run("with prebuilt workload", func(t *testing.T) {
		app := sparkapplicationtesting.MakeSparkApplication("app1", testNamespace).PrebuiltWorkloadLabel("wl1").Obj()
		got, err := adapter.WorkloadKeysFor(app)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		want := []types.NamespacedName{{Name: "wl1", Namespace: testNamespace}}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("unexpected workload keys (-want/+got):\n%s", diff)
		}
	})
}
