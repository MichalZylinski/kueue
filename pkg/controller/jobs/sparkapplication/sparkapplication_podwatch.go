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

	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
)

// NewReconciler adds a watch on executor pods on top of the generic job
// reconciler. Elastic SparkApplications derive their executor PodSet count
// from observed pod demand (see numExecutors), and unlike other elastic
// integrations, that demand changes without any corresponding SparkApplication
// spec update — the driver creates and deletes executor pods directly. The
// generic reconciler's default watch (on the SparkApplication itself) would
// never re-trigger on that pod churn, so this factory adds the pod watch
// needed to keep the elastic Workload slice in sync.
var NewReconciler = jobframework.NewGenericReconcilerFactory(NewJob,
	func(b *builder.Builder, c client.Client) *builder.Builder {
		return b.Watches(&corev1.Pod{}, &executorPodHandler{})
	})

var _ handler.EventHandler = (*executorPodHandler)(nil)

// executorPodHandler maps executor pod create/delete events, and phase or
// termination-in-progress transitions, to a reconcile request for the owning
// SparkApplication. Executor pods are matched by the operator-set
// sparkoperator.k8s.io/app-name and spark-role labels rather than by
// ownership, since the driver — not the operator — creates them, without any
// ownerReference back to the SparkApplication (only to the driver pod).
type executorPodHandler struct{}

func (h *executorPodHandler) Create(_ context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForPod(e.Object, q)
}

func (h *executorPodHandler) Update(_ context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	oldPod, ok := e.ObjectOld.(*corev1.Pod)
	if !ok {
		return
	}
	newPod, ok := e.ObjectNew.(*corev1.Pod)
	if !ok {
		return
	}
	// Only phase transitions and the start of termination affect observed
	// executor demand (see observedExecutorCount); other updates (e.g.
	// resource version churn, condition updates) are not worth a reconcile.
	if oldPod.Status.Phase == newPod.Status.Phase && (oldPod.DeletionTimestamp != nil) == (newPod.DeletionTimestamp != nil) {
		return
	}
	h.queueReconcileForPod(e.ObjectNew, q)
}

func (h *executorPodHandler) Delete(_ context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.queueReconcileForPod(e.Object, q)
}

func (h *executorPodHandler) Generic(context.Context, event.GenericEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *executorPodHandler) queueReconcileForPod(object client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pod, isPod := object.(*corev1.Pod)
	if !isPod {
		return
	}
	if pod.Labels[sparkcommon.LabelSparkRole] != sparkcommon.SparkRoleExecutor {
		return
	}
	appName, found := pod.Labels[sparkcommon.LabelSparkAppName]
	if !found {
		return
	}
	// Batch pod-creation bursts (a single Spark allocation batch) into one
	// reconcile, matching the ElasticJobUngater's pod handler.
	q.AddAfter(reconcile.Request{NamespacedName: types.NamespacedName{
		Name:      appName,
		Namespace: pod.Namespace,
	}}, constants.UpdatesBatchPeriod)
}
