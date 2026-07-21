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
	"fmt"

	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/util/api"
	clientutil "sigs.k8s.io/kueue/pkg/util/client"
)

type multiKueueAdapter struct{}

var _ jobframework.MultiKueueAdapter = (*multiKueueAdapter)(nil)

func (b *multiKueueAdapter) SyncJob(ctx context.Context, localClient client.Client, remoteClient client.Client, key types.NamespacedName, workloadName, origin string) (bool, error) {
	localApp := sparkv1beta2.SparkApplication{}
	if err := localClient.Get(ctx, key, &localApp); err != nil {
		return false, err
	}

	remoteApp := sparkv1beta2.SparkApplication{}
	err := remoteClient.Get(ctx, key, &remoteApp)
	if client.IgnoreNotFound(err) != nil {
		return false, err
	}

	// The remote SparkApplication exists: sync status, plus the observed-demand
	// annotation, back to the local copy. This is the only direction the
	// annotation ever syncs in — see the write-once note on the create path.
	// The status sync is a full Status().Update() rather than a diff-based
	// patch: SparkApplicationStatus.DriverInfo is a required, non-defaulted
	// CRD field, so a merge-patch that omits it (because it happens to be
	// unchanged from its zero value) leaves it entirely absent on an object
	// that has never had status written before, and the apiserver rejects
	// the write. A full replace always includes it.
	if err == nil {
		if !equality.Semantic.DeepEqual(localApp.Status, remoteApp.Status) {
			localApp.Status = remoteApp.Status
			if err := localClient.Status().Update(ctx, &localApp); err != nil {
				return false, err
			}
		}

		if remoteReplicaSizes, ok := remoteApp.Annotations[SparkApplicationPodSetReplicaSizesAnnotation]; ok &&
			localApp.Annotations[SparkApplicationPodSetReplicaSizesAnnotation] != remoteReplicaSizes {
			if err := clientutil.Patch(ctx, localClient, &localApp, func() (bool, error) {
				if localApp.Annotations == nil {
					localApp.Annotations = make(map[string]string, 1)
				}
				localApp.Annotations[SparkApplicationPodSetReplicaSizesAnnotation] = remoteReplicaSizes
				return true, nil
			}); err != nil {
				return false, err
			}
		}

		return false, nil
	}

	// The remote SparkApplication does not exist yet: create it as a full copy
	// of the local spec. Unlike batch/v1.Job, the remote spec is write-once —
	// the spark-operator moves an application to INVALIDATING and resubmits it
	// on any spec update after creation — so this create path is the only spec
	// mutation the adapter ever performs on the remote object.
	remoteApp = sparkv1beta2.SparkApplication{
		ObjectMeta: api.CloneObjectMetaForCreation(&localApp.ObjectMeta),
		Spec:       *localApp.Spec.DeepCopy(),
	}

	jobframework.SetMultiKueueMeta(&remoteApp, workloadName, origin)

	return false, remoteClient.Create(ctx, &remoteApp)
}

func (b *multiKueueAdapter) DeleteRemoteObject(ctx context.Context, _ client.Client, remoteClient client.Client, key types.NamespacedName) error {
	app := sparkv1beta2.SparkApplication{}
	app.SetName(key.Name)
	app.SetNamespace(key.Namespace)
	return client.IgnoreNotFound(remoteClient.Delete(ctx, &app))
}

// IsJobManagedByKueue implements jobframework.MultiKueueAdapter. Unlike
// batch/v1.Job, JobSet, RayCluster, and TrainJob, SparkApplication has no
// spec.managedBy field for Kueue to have set and later verify here (tracked
// upstream as a spark-operator API gap; see the SparkApplication MultiKueue
// KEP's "interim pattern"). The local application is instead kept
// permanently suspended by the startJob guard in jobframework's reconciler,
// so there is no spec-level signal to check. This mirrors the Pod adapter,
// the only other integration without a managedBy field, which also returns
// unconditionally true.
func (b *multiKueueAdapter) IsJobManagedByKueue(_ context.Context, _ client.Client, _ types.NamespacedName) (bool, string, error) {
	return true, "", nil
}

func (b *multiKueueAdapter) GVK() schema.GroupVersionKind {
	return gvk
}

var _ jobframework.MultiKueueWatcher = (*multiKueueAdapter)(nil)

func (*multiKueueAdapter) GetEmptyList() client.ObjectList {
	return &sparkv1beta2.SparkApplicationList{}
}

func (*multiKueueAdapter) WorkloadKeysFor(o runtime.Object) ([]types.NamespacedName, error) {
	app, ok := o.(*sparkv1beta2.SparkApplication)
	if !ok {
		return nil, errors.New("not a sparkapplication")
	}

	prebuiltWorkload := jobframework.PrebuiltWorkloadNameFor(app)
	if prebuiltWorkload == "" {
		return nil, fmt.Errorf("no prebuilt workload found for sparkapplication: %s", klog.KObj(app))
	}

	return []types.NamespacedName{{Name: prebuiltWorkload, Namespace: app.Namespace}}, nil
}
