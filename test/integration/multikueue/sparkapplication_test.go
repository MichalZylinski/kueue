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

package multikueue

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	workloadsparkapplication "sigs.k8s.io/kueue/pkg/controller/jobs/sparkapplication"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingsparkapplication "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
	"sigs.k8s.io/kueue/test/util"
)

// sparkApplicationEnabledIntegrations mirrors defaultEnabledIntegrations plus
// the SparkApplication framework, which is alpha and therefore excluded from
// the shared default set used by the rest of this suite.
var sparkApplicationEnabledIntegrations = sets.New("batch/job", workloadsparkapplication.FrameworkName)

var _ = ginkgo.Describe("MultiKueue SparkApplication", ginkgo.Label("area:multikueue", "feature:multikueue"), ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		managerNs *corev1.Namespace
		worker1Ns *corev1.Namespace

		managerMultiKueueSecret1 *corev1.Secret
		workerCluster1           *kueue.MultiKueueCluster
		managerMultiKueueConfig  *kueue.MultiKueueConfig
		multiKueueAC             *kueue.AdmissionCheck
		managerCq                *kueue.ClusterQueue
		managerLq                *kueue.LocalQueue
		managerFlavor            *kueue.ResourceFlavor

		worker1Cq     *kueue.ClusterQueue
		worker1Lq     *kueue.LocalQueue
		worker1Flavor *kueue.ResourceFlavor
	)

	ginkgo.BeforeAll(func() {
		managerTestCluster.fwk.StartManager(managerTestCluster.ctx, managerTestCluster.cfg, func(ctx context.Context, mgr manager.Manager) {
			managerAndMultiKueueSetup(ctx, mgr, 2*time.Second, sparkApplicationEnabledIntegrations, config.MultiKueueDispatcherModeAllAtOnce)
		})
	})

	ginkgo.AfterAll(func() {
		managerTestCluster.fwk.StopManager(managerTestCluster.ctx)
	})

	ginkgo.BeforeEach(func() {
		managerNs = util.CreateNamespaceFromPrefixWithLog(managerTestCluster.ctx, managerTestCluster.client, "multikueue-spark-")
		worker1Ns = util.CreateNamespaceWithLog(worker1TestCluster.ctx, worker1TestCluster.client, managerNs.Name)

		w1Kubeconfig, err := worker1TestCluster.kubeConfigBytes()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		managerMultiKueueSecret1 = utiltesting.MakeSecret("multikueue1", managersConfigNamespace.Name).Data(kueue.MultiKueueConfigSecretKey, w1Kubeconfig).Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, managerMultiKueueSecret1)

		workerCluster1 = utiltestingapi.MakeMultiKueueCluster("worker1").KubeConfig(kueue.SecretLocationType, managerMultiKueueSecret1.Name).Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, workerCluster1)

		managerMultiKueueConfig = utiltestingapi.MakeMultiKueueConfig("multikueueconfig").Clusters(workerCluster1.Name).Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, managerMultiKueueConfig)

		multiKueueAC = utiltestingapi.MakeAdmissionCheck("ac1").
			ControllerName(kueue.MultiKueueControllerName).
			Parameters(kueue.SchemeGroupVersion.Group, "MultiKueueConfig", managerMultiKueueConfig.Name).
			Obj()
		util.CreateAdmissionChecksAndWaitForActive(managerTestCluster.ctx, managerTestCluster.client, multiKueueAC)

		managerFlavor = utiltestingapi.MakeResourceFlavor(string(multikueueTestFlavor)).Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, managerFlavor)

		managerCq = utiltestingapi.MakeClusterQueue("q1").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(string(multikueueTestFlavor)).Resource(corev1.ResourceCPU, "5").Obj()).
			AdmissionChecks(kueue.AdmissionCheckReference(multiKueueAC.Name)).
			Obj()
		util.CreateClusterQueuesAndWaitForActive(managerTestCluster.ctx, managerTestCluster.client, managerCq)

		managerLq = utiltestingapi.MakeLocalQueue(managerCq.Name, managerNs.Name).ClusterQueue(managerCq.Name).Obj()
		util.CreateLocalQueuesAndWaitForActive(managerTestCluster.ctx, managerTestCluster.client, managerLq)

		worker1Flavor = utiltestingapi.MakeResourceFlavor(string(multikueueTestFlavor)).Obj()
		util.MustCreate(worker1TestCluster.ctx, worker1TestCluster.client, worker1Flavor)
		worker1Cq = utiltestingapi.MakeClusterQueue("q1").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(string(multikueueTestFlavor)).Resource(corev1.ResourceCPU, "5").Obj()).
			Obj()
		util.CreateClusterQueuesAndWaitForActive(worker1TestCluster.ctx, worker1TestCluster.client, worker1Cq)
		worker1Lq = utiltestingapi.MakeLocalQueue(worker1Cq.Name, worker1Ns.Name).ClusterQueue(worker1Cq.Name).Obj()
		util.CreateLocalQueuesAndWaitForActive(worker1TestCluster.ctx, worker1TestCluster.client, worker1Lq)
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(util.DeleteNamespace(managerTestCluster.ctx, managerTestCluster.client, managerNs)).To(gomega.Succeed())
		gomega.Expect(util.DeleteNamespace(worker1TestCluster.ctx, worker1TestCluster.client, worker1Ns)).To(gomega.Succeed())
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, managerCq, true)
		util.ExpectObjectToBeDeleted(worker1TestCluster.ctx, worker1TestCluster.client, worker1Cq, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, managerFlavor, true)
		util.ExpectObjectToBeDeleted(worker1TestCluster.ctx, worker1TestCluster.client, worker1Flavor, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, multiKueueAC, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, managerMultiKueueConfig, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, workerCluster1, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, managerMultiKueueSecret1, true)
	})

	ginkgo.It("Should run a SparkApplication on worker if admitted, and finish when the worker application completes", func() {
		admission := utiltestingapi.MakeAdmission(kueue.ClusterQueueReference(managerCq.Name)).PodSets(
			utiltestingapi.MakePodSetAssignment("driver").Flavor(corev1.ResourceCPU, multikueueTestFlavor).Obj(),
			utiltestingapi.MakePodSetAssignment("executor").Flavor(corev1.ResourceCPU, multikueueTestFlavor).Obj(),
		)
		sparkApp := testingsparkapplication.MakeSparkApplication("sparkapp1", managerNs.Name).
			Queue(managerLq.Name).
			Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, sparkApp)

		wlLookupKey := types.NamespacedName{
			Name:      workloadsparkapplication.GetWorkloadNameForSparkApplication(sparkApp.Name, sparkApp.UID),
			Namespace: managerNs.Name,
		}

		ginkgo.By("setting workload reservation in the management cluster", func() {
			util.SetQuotaReservation(managerTestCluster.ctx, managerTestCluster.client, wlLookupKey, admission.Obj())
		})

		ginkgo.By("checking the workload creation in the worker cluster", func() {
			// Only the placeholder Workload is synced to the worker at this
			// point; the MultiKueue reconciler only creates the remote
			// SparkApplication once that placeholder Workload is itself
			// admitted on the worker (see the next step), mirroring how the
			// analogous RayJob/Job MultiKueue tests are structured.
			managerWl := &kueue.Workload{}
			gomega.Expect(managerTestCluster.client.Get(managerTestCluster.ctx, wlLookupKey, managerWl)).To(gomega.Succeed())
			createdWorkload := &kueue.Workload{}
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(worker1TestCluster.client.Get(worker1TestCluster.ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
				g.Expect(createdWorkload.Spec).To(gomega.BeComparableTo(managerWl.Spec))
			}, util.MediumTimeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("the local SparkApplication is never unsuspended, since local execution is delegated to the worker", func() {
			gomega.Consistently(func(g gomega.Gomega) {
				localApp := sparkv1beta2.SparkApplication{}
				g.Expect(managerTestCluster.client.Get(managerTestCluster.ctx, client.ObjectKeyFromObject(sparkApp), &localApp)).To(gomega.Succeed())
				g.Expect(localApp.Spec.Suspend).To(gomega.HaveValue(gomega.BeTrue()))
			}, util.ConsistentDuration, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("setting workload reservation in worker1, the SparkApplication is synced and unsuspended on the worker, and AC state is updated in manager", func() {
			util.SetQuotaReservation(worker1TestCluster.ctx, worker1TestCluster.client, wlLookupKey, admission.Obj())

			// The remote copy's driver/executor pod templates legitimately
			// diverge from the local spec: worker1's own mutating webhook
			// stamps MultiKueue-worker pod-tracking labels onto them (as it
			// would for any locally admitted SparkApplication), so only
			// Suspend is asserted here, matching how the other MultiKueue
			// job tests in this suite compare the prebuilt Workload's spec
			// rather than diffing the raw remote CR against the local one.
			gomega.Eventually(func(g gomega.Gomega) {
				createdApp := sparkv1beta2.SparkApplication{}
				g.Expect(worker1TestCluster.client.Get(worker1TestCluster.ctx, client.ObjectKeyFromObject(sparkApp), &createdApp)).To(gomega.Succeed())
				g.Expect(createdApp.Spec.Suspend).To(gomega.HaveValue(gomega.BeFalse()))
			}, util.MediumTimeout, util.Interval).Should(gomega.Succeed())

			util.ExpectAdmissionCheckStateWithMessage(
				managerTestCluster.ctx, managerTestCluster.client, wlLookupKey,
				multiKueueAC.Name,
				kueue.CheckStateReady,
				`The workload was admitted on "worker1"`,
			)
			util.ExpectEventAppeared(managerTestCluster.ctx, managerTestCluster.client, eventsv1.Event{
				Reason: "MultiKueue",
				Type:   corev1.EventTypeNormal,
				Note:   `The workload was admitted on "worker1"`,
			})
		})

		ginkgo.By("completing the worker SparkApplication, the manager's workload is marked finished and the worker workload removed", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				createdApp := sparkv1beta2.SparkApplication{}
				g.Expect(worker1TestCluster.client.Get(worker1TestCluster.ctx, client.ObjectKeyFromObject(sparkApp), &createdApp)).To(gomega.Succeed())
				createdApp.Status.AppState.State = sparkv1beta2.ApplicationStateCompleted
				g.Expect(worker1TestCluster.client.Status().Update(worker1TestCluster.ctx, &createdApp)).To(gomega.Succeed())
			}, util.Timeout, util.Interval).Should(gomega.Succeed())

			gomega.Eventually(func(g gomega.Gomega) {
				createdWorkload := &kueue.Workload{}
				g.Expect(managerTestCluster.client.Get(managerTestCluster.ctx, wlLookupKey, createdWorkload)).To(gomega.Succeed())
				g.Expect(apimeta.FindStatusCondition(createdWorkload.Status.Conditions, kueue.WorkloadFinished)).To(gomega.BeComparableTo(&metav1.Condition{
					Type:   kueue.WorkloadFinished,
					Status: metav1.ConditionTrue,
					Reason: string(kueue.WorkloadFinishedReasonSucceeded),
				}, util.IgnoreConditionTimestampsAndObservedGeneration, util.IgnoreConditionMessage))
			}, util.MediumTimeout, util.Interval).Should(gomega.Succeed())

			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(worker1TestCluster.client.Get(worker1TestCluster.ctx, wlLookupKey, &kueue.Workload{})).To(utiltesting.BeNotFoundError())
			}, util.MediumTimeout, util.Interval).Should(gomega.Succeed())
		})
	})
})
