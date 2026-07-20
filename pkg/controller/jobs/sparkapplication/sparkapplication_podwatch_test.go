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
	"time"

	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/kueue/pkg/constants"
)

// waitForQueuedRequest waits up to twice the batching period for an item to
// appear on the queue (executorPodHandler enqueues with AddAfter, so items
// are not visible via Len()/Get() until the delay elapses). Returns the item
// and true if one arrived, or the zero value and false on timeout.
func waitForQueuedRequest(t *testing.T, q workqueue.TypedRateLimitingInterface[reconcile.Request]) (reconcile.Request, bool) {
	t.Helper()
	type result struct {
		req reconcile.Request
		ok  bool
	}
	ch := make(chan result, 1)
	go func() {
		item, shutdown := q.Get()
		ch <- result{item, !shutdown}
	}()
	select {
	case r := <-ch:
		return r.req, r.ok
	case <-time.After(2 * constants.UpdatesBatchPeriod):
		return reconcile.Request{}, false
	}
}

func executorPod(name string, phase corev1.PodPhase, deleting bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				sparkcommon.LabelSparkAppName: "sparkapp",
				sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
	if deleting {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		pod.Finalizers = []string{"kueue.x-k8s.io/test-finalizer"}
	}
	return pod
}

func driverPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				sparkcommon.LabelSparkAppName: "sparkapp",
				sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleDriver,
			},
		},
	}
}

func TestExecutorPodHandlerCreateDelete(t *testing.T) {
	testCases := map[string]struct {
		pod        *corev1.Pod
		wantQueued bool
	}{
		"executor pod is queued": {
			pod:        executorPod("exec-1", corev1.PodPending, false),
			wantQueued: true,
		},
		"driver pod is not queued": {
			pod:        driverPod("driver"),
			wantQueued: false,
		},
		"pod without the app-name label is not queued": {
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "stray", Namespace: "ns",
					Labels: map[string]string{sparkcommon.LabelSparkRole: sparkcommon.SparkRoleExecutor},
				},
			},
			wantQueued: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			h := &executorPodHandler{}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Create(t.Context(), event.CreateEvent{Object: tc.pod}, q)

			item, queued := waitForQueuedRequest(t, q)
			if queued != tc.wantQueued {
				t.Errorf("queued = %v, wantQueued = %v", queued, tc.wantQueued)
			}
			if tc.wantQueued && queued {
				want := reconcile.Request{NamespacedName: types.NamespacedName{Name: "sparkapp", Namespace: "ns"}}
				if item != want {
					t.Errorf("queued request = %v, want %v", item, want)
				}
			}
		})
	}
}

func TestExecutorPodHandlerUpdate(t *testing.T) {
	testCases := map[string]struct {
		oldPod     *corev1.Pod
		newPod     *corev1.Pod
		wantQueued bool
	}{
		"phase change is queued": {
			oldPod:     executorPod("exec-1", corev1.PodPending, false),
			newPod:     executorPod("exec-1", corev1.PodRunning, false),
			wantQueued: true,
		},
		"deletion started is queued": {
			oldPod:     executorPod("exec-1", corev1.PodRunning, false),
			newPod:     executorPod("exec-1", corev1.PodRunning, true),
			wantQueued: true,
		},
		"unrelated update (same phase, not deleting) is not queued": {
			oldPod:     executorPod("exec-1", corev1.PodRunning, false),
			newPod:     executorPod("exec-1", corev1.PodRunning, false),
			wantQueued: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			h := &executorPodHandler{}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Update(t.Context(), event.UpdateEvent{ObjectOld: tc.oldPod, ObjectNew: tc.newPod}, q)

			_, queued := waitForQueuedRequest(t, q)
			if queued != tc.wantQueued {
				t.Errorf("queued = %v, wantQueued = %v", queued, tc.wantQueued)
			}
		})
	}
}
