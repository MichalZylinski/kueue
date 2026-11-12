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
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	workloadflinkdeployment "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingflinkdeployment "sigs.k8s.io/kueue/pkg/util/testingjobs/flinkdeployment"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	"sigs.k8s.io/kueue/test/util/behavioral"
	"sigs.k8s.io/kueue/test/util/behavioral/integration"
)

const (
	instanceKey = "cloud.provider.com/instance"
)

var _ = ginkgo.Describe("FlinkDeployment controller with manageJobsWithoutQueueName", ginkgo.Label("job:flinkdeployment", "area:jobs"), ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		ns                 *corev1.Namespace
		unmanagedNamespace *corev1.Namespace
	)

	ginkgo.BeforeAll(func() {
		fwk.StartManager(ctx, cfg, managerSetup(
			jobframework.WithManageJobsWithoutQueueName(true),
			jobframework.WithManagedJobsNamespaceSelector(behavioral.NewNamespaceSelectorExcluding("unmanaged-flink-ns")),
		))
		unmanagedNamespace = utiltesting.MakeNamespace("unmanaged-flink-ns")
		behavioral.MustCreate(ctx, k8sClient, unmanagedNamespace)
	})

	ginkgo.AfterAll(func() {
		gomega.Expect(behavioral.DeleteNamespace(ctx, k8sClient, unmanagedNamespace)).To(gomega.Succeed())
		fwk.StopManager(ctx)
	})

	ginkgo.BeforeEach(func() {
		ns = behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "flink-managed-")
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(behavioral.DeleteAllFlinkDeploymentsInNamespace(ctx, k8sClient, unmanagedNamespace)).To(gomega.Succeed())
		gomega.Expect(behavioral.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
	})

	ginkgo.It("Should suspend and create a Workload for an unsuspended FlinkDeployment without queue-name in a managed namespace", func() {
		flinkDep := testingflinkdeployment.MakeFlinkDeployment("managed-app", ns.Name).
			Suspend(false).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(flinkDep), created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job).NotTo(gomega.BeNil())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		wlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(created.Name, created.UID),
		}
		wl := &kueue.Workload{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Allowing an unlabeled Session cluster (spec.job == nil) in a managed namespace without creating a phantom Workload")
		unmanagedSession := testingflinkdeployment.MakeFlinkDeployment("managed-ns-session", ns.Name).
			SessionCluster().
			Obj()
		behavioral.MustCreate(ctx, k8sClient, unmanagedSession)

		wlList := &kueue.WorkloadList{}
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.List(ctx, wlList, client.InNamespace(ns.Name))).To(gomega.Succeed())
			g.Expect(wlList.Items).To(gomega.HaveLen(1))
			g.Expect(wlList.Items[0].Name).To(gomega.Equal(wlKey.Name))
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())
	})

	ginkgo.It("Should not suspend or create a Workload for an unsuspended FlinkDeployment in an unmanaged namespace", func() {
		flinkDep := testingflinkdeployment.MakeFlinkDeployment("unmanaged-app", unmanagedNamespace.Name).
			Suspend(false).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		created := &flinkv1beta1.FlinkDeployment{}
		wlList := &kueue.WorkloadList{}
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(flinkDep), created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job).NotTo(gomega.BeNil())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(k8sClient.List(ctx, wlList, client.InNamespace(unmanagedNamespace.Name))).To(gomega.Succeed())
			g.Expect(wlList.Items).To(gomega.BeEmpty())
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())

		ginkgo.By("Allowing an unmanaged Session cluster (spec.job == nil) in an excluded namespace even with manageJobsWithoutQueueName enabled")
		unmanagedSession := testingflinkdeployment.MakeFlinkDeployment("unmanaged-session", unmanagedNamespace.Name).
			SessionCluster().
			Obj()
		behavioral.MustCreate(ctx, k8sClient, unmanagedSession)
	})
})

var _ = ginkgo.Describe("FlinkDeployment controller and scheduler", ginkgo.Label("job:flinkdeployment", "area:jobs"), ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		ns           *corev1.Namespace
		flavor       *kueue.ResourceFlavor
		clusterQueue *kueue.ClusterQueue
		localQueue   *kueue.LocalQueue
		extraFlavor  *kueue.ResourceFlavor
	)

	ginkgo.BeforeAll(func() {
		fwk.StartManager(ctx, cfg, managerAndSchedulerSetup(
			false,
			jobframework.WithWaitForPodsReady(&config.WaitForPodsReady{}),
		))
	})

	ginkgo.AfterAll(func() {
		fwk.StopManager(ctx)
	})

	ginkgo.BeforeEach(func() {
		ns = behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "flink-")

		flavor = utiltestingapi.MakeResourceFlavor("default-flavor").
			NodeLabel(instanceKey, "spot-pool").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flavor)

		clusterQueue = utiltestingapi.MakeClusterQueue("cluster-queue").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(flavor.Name).
				Resource(corev1.ResourceCPU, "4").
				Resource(corev1.ResourceMemory, "8Gi").
				Obj()).
			Preemption(kueue.ClusterQueuePreemption{
				WithinClusterQueue: kueue.PreemptionPolicyLowerPriority,
			}).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, clusterQueue)
		behavioral.ExpectClusterQueuesToBeActive(ctx, k8sClient, clusterQueue)

		localQueue = utiltestingapi.MakeLocalQueue("local-queue", ns.Name).
			ClusterQueue(clusterQueue.Name).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, localQueue)
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(behavioral.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, clusterQueue, true)
		if extraFlavor != nil {
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, extraFlavor, true)
			extraFlavor = nil
		}
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, flavor, true)
	})

	ginkgo.It("Should suspend via webhook, compute Flink memory/slot PodSets, admit, report PodsReady, and finish", func() {
		ginkgo.By("Holding the ClusterQueue initially so webhook suspension can be observed before admission")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.StopPolicy = new(kueue.Hold)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		flinkDep := testingflinkdeployment.MakeFlinkDeployment("sample-streaming-app", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			JobManagerResource(new(1.0), "1024m", "").
			TaskManagerResource(new(1.0), "2048m", "").
			Parallelism(4).
			FlinkConfiguration(map[string]string{
				"taskmanager.numberOfTaskSlots": "2",
			}).
			Obj()

		ginkgo.By("Creating an unsuspended FlinkDeployment; mutating webhook suspends it on admission")
		behavioral.MustCreate(ctx, k8sClient, flinkDep)
		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlLookupKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}

		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		gomega.Expect(created.Spec.Job).NotTo(gomega.BeNil())
		gomega.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))

		wl := &kueue.Workload{}
		ginkgo.By("Verifying unadmitted Workload has 2 PodSets with BinarySI memory and derived TaskManager count")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlLookupKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
			g.Expect(wl.Spec.PodSets).To(gomega.HaveLen(2))
			g.Expect(wl.Spec.PodSets[0].Name).To(gomega.Equal(kueue.NewPodSetReference("job-manager")))
			g.Expect(wl.Spec.PodSets[0].Count).To(gomega.Equal(int32(1)))
			g.Expect(wl.Spec.PodSets[0].Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]).
				To(gomega.Equal(resource.MustParse("1Gi")))
			g.Expect(wl.Spec.PodSets[1].Name).To(gomega.Equal(kueue.NewPodSetReference("task-manager")))
			g.Expect(wl.Spec.PodSets[1].Count).To(gomega.Equal(int32(2)))
			g.Expect(wl.Spec.PodSets[1].Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]).
				To(gomega.Equal(resource.MustParse("2Gi")))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Unholding the ClusterQueue and waiting for Kueue scheduler to admit Workload and set state=running")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.StopPolicy = new(kueue.None)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlLookupKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.JobManager.PodTemplate).NotTo(gomega.BeNil())
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
			g.Expect(created.Spec.TaskManager.PodTemplate).NotTo(gomega.BeNil())
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Simulating Flink Operator reporting the spec DEPLOYED, JobManager READY, and JobStatus RUNNING")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
			created.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusReady
			created.Status.JobStatus = flinkv1beta1.JobStatus{
				State: flinkv1beta1.JobStatusStateRunning,
			}
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Verifying Workload PodsReady condition transitions to True")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlLookupKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadPodsReady))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Simulating Flink bounded job completion (FINISHED)")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Verifying Workload finishes cleanly")
		behavioral.ExpectWorkloadToFinish(ctx, k8sClient, wlLookupKey)

		ginkgo.By("Rejecting running-state PodSet mutations on the finished FlinkDeployment while allowing metadata updates")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		scaleAttempt := created.DeepCopy()
		scaleAttempt.Spec.TaskManager.Replicas = new(int64(4))
		scaleErr := k8sClient.Update(ctx, scaleAttempt)
		gomega.Expect(scaleErr).To(gomega.HaveOccurred())
		gomega.Expect(scaleErr).To(utiltesting.BeForbiddenError())
		gomega.Expect(scaleErr.Error()).To(gomega.ContainSubstring("when FlinkDeployment is not suspended"))

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			if created.Annotations == nil {
				created.Annotations = make(map[string]string)
			}
			created.Annotations["example.com/post-finish-note"] = "recorded"
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should allow a queue-labeled child FlinkDeployment controlled by a Kueue-managed ancestor to be suspended and unsuspended without wedging", func() {
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.StopPolicy = new(kueue.Hold)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		parent := testingflinkdeployment.MakeFlinkDeployment("parent-app", ns.Name).
			Queue(localQueue.Name).
			TaskManagerReplicas(1).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, parent)

		child := testingflinkdeployment.MakeFlinkDeployment("child-app", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			TaskManagerReplicas(1).
			Obj()
		child.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
				Kind:       "FlinkDeployment",
				Name:       parent.Name,
				UID:        parent.UID,
				Controller: new(true),
			},
		}
		behavioral.MustCreate(ctx, k8sClient, child)

		childKey := client.ObjectKeyFromObject(child)
		createdChild := &flinkv1beta1.FlinkDeployment{}
		ginkgo.By("Waiting for JobReconciler to suspend the child while the ancestor Workload is unadmitted")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, childKey, createdChild)).To(gomega.Succeed())
			g.Expect(createdChild.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		parentWlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(parent.Name, parent.UID),
		}
		ginkgo.By("Unholding ClusterQueue so the ancestor Workload is admitted, then unsuspending and updating the child")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.StopPolicy = new(kueue.None)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, parentWlKey)

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, childKey, createdChild)).To(gomega.Succeed())
			createdChild.Spec.Job.State = flinkv1beta1.JobStateRunning
			g.Expect(k8sClient.Update(ctx, createdChild)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, childKey, createdChild)).To(gomega.Succeed())
			g.Expect(createdChild.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())
	})

	ginkgo.It("Should preserve upgradeMode on preemption and hold quota until savepoint suspension completes", func() {
		lowPrio := utiltestingapi.MakeWorkloadPriorityClass("flink-low").PriorityValue(100).Obj()
		highPrio := utiltestingapi.MakeWorkloadPriorityClass("flink-high").PriorityValue(1000).Obj()
		behavioral.MustCreate(ctx, k8sClient, lowPrio)
		behavioral.MustCreate(ctx, k8sClient, highPrio)
		ginkgo.DeferCleanup(func() {
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, lowPrio, true)
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, highPrio, true)
		})

		lowJob := testingflinkdeployment.MakeFlinkDeployment("streaming-savepoint", ns.Name).
			Queue(localQueue.Name).
			WorkloadPriorityClass(lowPrio.Name).
			Suspend(false).
			UpgradeMode(flinkv1beta1.UpgradeModeSavepoint).
			JobManagerResource(new(1.0), "1024m", "").
			TaskManagerReplicas(2).
			TaskManagerResource(new(1.0), "2048m", "").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, lowJob)

		lowJobKey := client.ObjectKeyFromObject(lowJob)
		lowWlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(lowJob.Name, lowJob.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, lowWlKey)

		createdLow := &flinkv1beta1.FlinkDeployment{}
		ginkgo.By("Simulating low-priority FlinkDeployment actively running")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lowJobKey, createdLow)).To(gomega.Succeed())
			g.Expect(createdLow.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			createdLow.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
			createdLow.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			createdLow.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusReady
			createdLow.Status.JobStatus = flinkv1beta1.JobStatus{State: flinkv1beta1.JobStatusStateRunning}
			createdLow.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 2}
			g.Expect(k8sClient.Status().Update(ctx, createdLow)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Submitting a high-priority FlinkDeployment that requires lowJob's quota (3 CPU / 5Gi)")
		highJob := testingflinkdeployment.MakeFlinkDeployment("urgent-batch", ns.Name).
			Queue(localQueue.Name).
			WorkloadPriorityClass(highPrio.Name).
			Suspend(false).
			UpgradeMode(flinkv1beta1.UpgradeModeStateless).
			JobManagerResource(new(1.0), "1024m", "").
			TaskManagerReplicas(2).
			TaskManagerResource(new(1.0), "2048m", "").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, highJob)
		highWlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(highJob.Name, highJob.UID),
		}

		ginkgo.By("Verifying lowJob is suspended WITHOUT overwriting upgradeMode=savepoint")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lowJobKey, createdLow)).To(gomega.Succeed())
			g.Expect(createdLow.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			g.Expect(createdLow.Spec.Job.UpgradeMode).To(gomega.Equal(flinkv1beta1.UpgradeModeSavepoint))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Simulating Flink taking a savepoint (LifecycleState=UPGRADING with JobManager and TaskManager pods still present): quota remains reserved and highJob waits")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lowJobKey, createdLow)).To(gomega.Succeed())
			createdLow.Status.LifecycleState = flinkv1beta1.LifecycleStateUpgrading
			createdLow.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateUpgrading
			g.Expect(k8sClient.Status().Update(ctx, createdLow)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		lowWl := &kueue.Workload{}
		highWl := &kueue.Workload{}
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lowWlKey, lowWl)).To(gomega.Succeed())
			g.Expect(lowWl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadEvicted))
			g.Expect(lowWl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadQuotaReserved))
			g.Expect(k8sClient.Get(ctx, highWlKey, highWl)).To(gomega.Succeed())
			g.Expect(highWl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())

		ginkgo.By("Simulating Flink completing savepoint and tearing down pods (SUSPENDED + MISSING + zero TaskManager replicas + FINISHED jobStatus)")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lowJobKey, createdLow)).To(gomega.Succeed())
			createdLow.Status.LifecycleState = flinkv1beta1.LifecycleStateSuspended
			createdLow.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			createdLow.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusMissing
			createdLow.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 0}
			createdLow.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
			g.Expect(k8sClient.Status().Update(ctx, createdLow)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Verifying lowJob releases quota without being marked Finished, and highJob is admitted")
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, highWlKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lowWlKey, lowWl)).To(gomega.Succeed())
			g.Expect(lowWl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadQuotaReserved))
			g.Expect(lowWl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadFinished))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should keep an admitted Workload through the operator's pre-deploy UPGRADING status, revert a manual suspend, and clear admission on deactivation without pods", func() {
		flinkDep := testingflinkdeployment.MakeFlinkDeployment("upgrading-app", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			JobManagerResource(new(1.0), "1024m", "").
			TaskManagerReplicas(1).
			TaskManagerResource(new(1.0), "2048m", "").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlKey)

		created := &flinkv1beta1.FlinkDeployment{}
		ginkgo.By("Simulating the operator's persisted pre-deploy status after un-suspend: spec running, UPGRADING, JobManager MISSING, jobStatus FINISHED")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateUpgrading
			created.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateUpgrading
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusMissing
			created.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 0}
			created.Status.JobStatus = flinkv1beta1.JobStatus{State: flinkv1beta1.JobStatusStateFinished}
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		wl := &kueue.Workload{}
		ginkgo.By("Verifying the Workload stays admitted and is not marked Finished while the operator is mid-transition")
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
			g.Expect(wl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadFinished))
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())

		ginkgo.By("Setting spec.job.state to suspended manually while keeping admitted PodSet overlays and verifying Kueue re-sets it to running")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.Job.State = flinkv1beta1.JobStateSuspended
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Deactivating the Workload while the operator stays parked in UPGRADING with no JobManager or TaskManager pods, and verifying admission is cleared")
		behavioral.DeactivateWorkload(ctx, k8sClient, wlKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			g.Expect(created.Status.LifecycleState).To(gomega.Equal(flinkv1beta1.LifecycleStateUpgrading))
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadEvicted))
			g.Expect(wl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadQuotaReserved))
			g.Expect(wl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadFinished))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should admit FlinkDeployment with root podTemplate sidecars and restore cleanly on deactivation", func() {
		flinkDep := testingflinkdeployment.MakeFlinkDeployment("root-podtemplate-app", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"workload-tier": "streaming"},
					Tolerations: []corev1.Toleration{
						{Key: "streaming", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
					},
					Containers: []corev1.Container{
						{
							Name:  "metrics-sidecar",
							Image: "prom/exporter:v1",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("250m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
							},
						},
					},
				},
			}).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlKey)

		created := &flinkv1beta1.FlinkDeployment{}
		wl := &kueue.Workload{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.JobManager.PodTemplate).NotTo(gomega.BeNil())
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
			g.Expect(created.Spec.TaskManager.PodTemplate).NotTo(gomega.BeNil())
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Ensuring running FlinkDeployment stays admitted without EquivalentToWorkload eviction loop")
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
			g.Expect(wl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadEvicted))
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())

		ginkgo.By("Deactivating Workload and verifying inherited role podTemplates are pruned back to nil without leaking root fields or metadata")
		behavioral.DeactivateWorkload(ctx, k8sClient, wlKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			g.Expect(created.Spec.JobManager.PodTemplate).To(gomega.BeNil())
			g.Expect(created.Spec.TaskManager.PodTemplate).To(gomega.BeNil())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should hold quota while post-deployment LifecycleStateFailed is still active and finish once pods are gone", func() {
		runningFailedDep := testingflinkdeployment.MakeFlinkDeployment("post-deploy-failed-running", ns.Name).
			Queue(localQueue.Name).
			TaskManagerReplicas(1).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, runningFailedDep)

		runningKey := client.ObjectKeyFromObject(runningFailedDep)
		runningWlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(runningFailedDep.Name, runningFailedDep.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, runningWlKey)

		created := &flinkv1beta1.FlinkDeployment{}
		wl := &kueue.Workload{}
		ginkgo.By("Holding quota when LifecycleState=FAILED still has JobManagerDeploymentStatus=ERROR")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, runningKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateFailed
			created.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusError
			created.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 1}
			created.Status.JobStatus.State = flinkv1beta1.JobStatusStateReconciling
			created.Status.Error = "JobManager deployment is in ERROR state."
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, runningWlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
			g.Expect(wl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadFinished))
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())

		ginkgo.By("Finishing native-mode Workload once JobManagerDeploymentStatus becomes MISSING")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, runningKey, created)).To(gomega.Succeed())
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusMissing
			created.Status.Error = "JobManager deployment is missing and HA data is not available."
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, runningWlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadFinished))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Holding quota in standalone mode when JM is MISSING under LifecycleState=FAILED but TaskManagers remain")
		standaloneDep := testingflinkdeployment.MakeFlinkDeployment("post-deploy-failed-standalone", ns.Name).
			Queue(localQueue.Name).
			Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
			JobManagerResource(new(1.0), "1024m", "").
			TaskManagerReplicas(1).
			TaskManagerResource(new(1.0), "2048m", "").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, standaloneDep)

		standaloneKey := client.ObjectKeyFromObject(standaloneDep)
		standaloneWlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(standaloneDep.Name, standaloneDep.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, standaloneWlKey)

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, standaloneKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateFailed
			created.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusMissing
			created.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 1}
			created.Status.JobStatus.State = flinkv1beta1.JobStatusStateReconciling
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, standaloneWlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
			g.Expect(wl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadFinished))
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, standaloneKey, created)).To(gomega.Succeed())
			created.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 0}
			created.Status.JobStatus.State = flinkv1beta1.JobStatusStateFailed
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		behavioral.ExpectWorkloadToFinish(ctx, k8sClient, standaloneWlKey)

		ginkgo.By("Admitting another FlinkDeployment, deactivating its Workload while active, and releasing quota on native MISSING JM")
		evictedFailedDep := testingflinkdeployment.MakeFlinkDeployment("post-deploy-failed-evicted", ns.Name).
			Queue(localQueue.Name).
			TaskManagerReplicas(1).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, evictedFailedDep)

		evictedKey := client.ObjectKeyFromObject(evictedFailedDep)
		evictedWlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(evictedFailedDep.Name, evictedFailedDep.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, evictedWlKey)

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, evictedKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
			created.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusReady
			created.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 1}
			created.Status.JobStatus.State = flinkv1beta1.JobStatusStateRunning
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		behavioral.DeactivateWorkload(ctx, k8sClient, evictedWlKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, evictedKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateFailed
			created.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusMissing
			created.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 1}
			created.Status.JobStatus.State = flinkv1beta1.JobStatusStateReconciling
			created.Status.Error = "Manual restore required."
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, evictedWlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusFalse(kueue.WorkloadQuotaReserved))
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusFalse(kueue.WorkloadAdmitted))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should preserve partial podTemplate overrides in etcd, handle 4.03 CPU without ULP overshoot, and evict cleanly with empty pointer strings", func() {
		ginkgo.By("Sizing ClusterQueue CPU nominal quota to exact 5030m (1000m JM + 4030m TM)")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.ResourceGroups[0].Flavors[0].Resources[0].NominalQuota = resource.MustParse("5030m")
			clusterQueue.Spec.StopPolicy = new(kueue.Hold)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		rawApp := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "flink.apache.org/v1beta1",
				"kind":       "FlinkDeployment",
				"metadata": map[string]any{
					"name":      "partial-podtemplate-403",
					"namespace": ns.Name,
					"labels": map[string]any{
						controllerconstants.QueueLabel: localQueue.Name,
					},
				},
				"spec": map[string]any{
					"image":        "flink:1.20",
					"flinkVersion": "v1_20",
					"podTemplate": map[string]any{
						"spec": map[string]any{
							"volumes": []any{
								map[string]any{
									"name": "local-dir",
									"hostPath": map[string]any{
										"path": "/tmp/flink",
										"type": "",
									},
								},
							},
							"containers": []any{
								map[string]any{
									"name": "flink-main-container",
									"ports": []any{
										map[string]any{"name": "metrics", "containerPort": int64(8080)},
									},
									"env": []any{
										map[string]any{
											"name": "MEM",
											"valueFrom": map[string]any{
												"resourceFieldRef": map[string]any{
													"resource": "limits.memory",
													"divisor":  "1Mi",
												},
											},
										},
									},
									"livenessProbe": map[string]any{
										"httpGet": map[string]any{"path": "/live", "port": int64(8081)},
									},
									"readinessProbe": map[string]any{
										"grpc": map[string]any{"port": int64(9090), "service": ""},
									},
								},
							},
							"affinity": map[string]any{
								"podAntiAffinity": map[string]any{
									"preferredDuringSchedulingIgnoredDuringExecution": []any{
										map[string]any{
											"weight": int64(50),
											"podAffinityTerm": map[string]any{
												"topologyKey": "topology.kubernetes.io/zone",
											},
										},
									},
								},
							},
							"topologySpreadConstraints": []any{
								map[string]any{
									"maxSkew":           int64(1),
									"topologyKey":       "topology.kubernetes.io/zone",
									"whenUnsatisfiable": "DoNotSchedule",
								},
							},
						},
					},
					"jobManager": map[string]any{
						"resource": map[string]any{"cpu": 1.0, "memory": "1024m"},
					},
					"taskManager": map[string]any{
						"resource": map[string]any{"cpu": 4.03, "memory": "2048m"},
						"podTemplate": map[string]any{
							"spec": map[string]any{
								"containers": []any{
									map[string]any{
										"ports": []any{map[string]any{"protocol": "TCP"}},
										"env": []any{
											map[string]any{
												"name": "MEM",
												"valueFrom": map[string]any{
													"resourceFieldRef": map[string]any{
														"resource": "requests.memory",
													},
												},
											},
										},
										"livenessProbe": map[string]any{
											"httpGet": map[string]any{"path": "/healthz"},
										},
									},
								},
								"affinity": map[string]any{
									"podAntiAffinity": map[string]any{
										"preferredDuringSchedulingIgnoredDuringExecution": []any{
											map[string]any{
												"podAffinityTerm": map[string]any{
													"topologyKey": "kubernetes.io/hostname",
												},
											},
										},
									},
								},
								"topologySpreadConstraints": []any{
									map[string]any{"whenUnsatisfiable": "ScheduleAnyway"},
								},
							},
						},
					},
					"job": map[string]any{
						"jarURI":      "local:///opt/flink/examples/streaming/StateMachineExample.jar",
						"parallelism": int64(1),
						"state":       "running",
					},
				},
			},
		}
		behavioral.MustCreate(ctx, k8sClient, rawApp)

		lookupKey := types.NamespacedName{Name: rawApp.GetName(), Namespace: ns.Name}
		wlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(rawApp.GetName(), rawApp.GetUID()),
		}

		assertNoZeroPollutionInEtcd := func(g gomega.Gomega) {
			persisted := &unstructured.Unstructured{}
			persisted.SetAPIVersion("flink.apache.org/v1beta1")
			persisted.SetKind("FlinkDeployment")
			g.Expect(k8sClient.Get(ctx, lookupKey, persisted)).To(gomega.Succeed())
			containers, found, err := unstructured.NestedSlice(persisted.Object, "spec", "taskManager", "podTemplate", "spec", "containers")
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(found).To(gomega.BeTrue())
			g.Expect(containers).To(gomega.HaveLen(1))
			c0 := containers[0].(map[string]any)
			g.Expect(c0).NotTo(gomega.HaveKey("name"))
			g.Expect(c0).NotTo(gomega.HaveKey("resources"))
			ports := c0["ports"].([]any)
			g.Expect(ports[0].(map[string]any)).NotTo(gomega.HaveKey("containerPort"))
			envs := c0["env"].([]any)
			rfr, _, err := unstructured.NestedMap(envs[0].(map[string]any), "valueFrom", "resourceFieldRef")
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(rfr).NotTo(gomega.HaveKey("divisor"))
			httpGet, _, err := unstructured.NestedMap(c0, "livenessProbe", "httpGet")
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(httpGet).NotTo(gomega.HaveKey("port"))
			terms, _, err := unstructured.NestedSlice(
				persisted.Object,
				"spec", "taskManager", "podTemplate", "spec", "affinity", "podAntiAffinity", "preferredDuringSchedulingIgnoredDuringExecution",
			)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(terms[0].(map[string]any)).NotTo(gomega.HaveKey("weight"))
			tsc, _, err := unstructured.NestedSlice(persisted.Object, "spec", "taskManager", "podTemplate", "spec", "topologySpreadConstraints")
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(tsc[0].(map[string]any)).NotTo(gomega.HaveKey("maxSkew"))
			g.Expect(tsc[0].(map[string]any)).NotTo(gomega.HaveKey("topologyKey"))
		}

		ginkgo.By("Verifying Default() suspended the job without injecting Go zero values into partial taskManager.podTemplate")
		assertNoZeroPollutionInEtcd(gomega.Default)

		ginkgo.By("Unholding ClusterQueue and verifying exact 5030m Workload admits and stays admitted without EquivalentToWorkload mismatch")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.StopPolicy = new(kueue.None)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlKey)

		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.JobManager.PodTemplate).NotTo(gomega.BeNil())
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
			g.Expect(created.Spec.TaskManager.PodTemplate).NotTo(gomega.BeNil())
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		wl := &kueue.Workload{}
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
			g.Expect(wl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadEvicted))
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			assertNoZeroPollutionInEtcd(g)
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())

		ginkgo.By("Deactivating Workload and verifying ValidateUpdate permits eviction while keeping partial overrides clean")
		behavioral.DeactivateWorkload(ctx, k8sClient, wlKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			assertNoZeroPollutionInEtcd(g)
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should re-suspend premature running updates while queued and cleanly evict when overlay fields were edited while queued", func() {
		ginkgo.By("Holding the ClusterQueue so the FlinkDeployment stays queued and suspended")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.StopPolicy = new(kueue.Hold)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		flinkDep := testingflinkdeployment.MakeFlinkDeployment("overlay-drift-app", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			TaskManagerReplicas(1).
			JobManagerPodTemplate(&corev1.PodTemplateSpec{
				Labels:      map[string]string{"env": "staging"},
				Annotations: map[string]string{"owner": "team-a"},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"workload-tier": "initial-jm"},
				},
			}).
			TaskManagerPodTemplate(&corev1.PodTemplateSpec{
				Labels:      map[string]string{"env": "staging"},
				Annotations: map[string]string{"owner": "team-a"},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"workload-tier": "initial-tm"},
				},
			}).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}

		wl := &kueue.Workload{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Spec.PodSets).To(gomega.HaveLen(2))
			g.Expect(wl.Spec.PodSets[0].Template.Spec.NodeSelector).To(gomega.HaveKeyWithValue("workload-tier", "initial-jm"))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		created := &flinkv1beta1.FlinkDeployment{}
		ginkgo.By("Re-suspending a premature user update that sets spec.job.state=running while the Workload is still queued")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.Job.State = flinkv1beta1.JobStateRunning
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Editing Labels, Annotations, and NodeSelector while still queued and suspended")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.JobManager.PodTemplate.Labels = map[string]string{"env": "production"}
			created.Spec.JobManager.PodTemplate.Annotations = map[string]string{"owner": "team-b"}
			created.Spec.JobManager.PodTemplate.Spec.NodeSelector = map[string]string{"workload-tier": "updated-jm"}
			created.Spec.TaskManager.PodTemplate.Labels = map[string]string{"env": "production"}
			created.Spec.TaskManager.PodTemplate.Annotations = map[string]string{"owner": "team-b"}
			created.Spec.TaskManager.PodTemplate.Spec.NodeSelector = map[string]string{"workload-tier": "updated-tm"}
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Unholding the ClusterQueue so startJob transitions the FlinkDeployment to running")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.StopPolicy = new(kueue.None)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlKey)

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue("workload-tier", "updated-jm"))
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Rejecting a running->suspended update that mutates queue-name")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		queueMutateAttempt := created.DeepCopy()
		queueMutateAttempt.Spec.Job.State = flinkv1beta1.JobStateSuspended
		delete(queueMutateAttempt.Labels, controllerconstants.QueueLabel)
		err := k8sClient.Update(ctx, queueMutateAttempt)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		ginkgo.By("Deactivating the Workload and verifying stopJob suspends and releases quota without 403 Forbidden")
		behavioral.DeactivateWorkload(ctx, k8sClient, wlKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusFalse(kueue.WorkloadQuotaReserved))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should allow repairing a suspended FlinkDeployment in place after queued podTemplate metadata and nodeSelector edits cause FailedToStart", func() {
		ginkgo.By("Holding the ClusterQueue so the FlinkDeployment stays queued and suspended")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.StopPolicy = new(kueue.Hold)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		flinkDep := testingflinkdeployment.MakeFlinkDeployment("failed-to-start-repair-app", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			TaskManagerReplicas(1).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}

		wl := &kueue.Workload{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Spec.PodSets).To(gomega.HaveLen(2))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		initialWlUID := wl.UID

		created := &flinkv1beta1.FlinkDeployment{}
		ginkgo.By("Editing podTemplate labels, annotations, schedulingGates, and a conflicting nodeSelector while queued")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.PodTemplate = testingflinkdeployment.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
				Labels:      map[string]string{"team": "streaming"},
				Annotations: map[string]string{"owner": "analytics"},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{instanceKey: "conflicting-pool"},
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "example.com/precheck"},
					},
				},
			})
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Unholding the ClusterQueue so admission onto spot-pool triggers permanent ErrInvalidPodSetUpdate and marks Workload FailedToStart")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.StopPolicy = new(kueue.None)
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrueAndReason(
				kueue.WorkloadFinished, jobframework.FailedToStartFinishedReason,
			))
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Repairing the conflicting podTemplate nodeSelector in place without hitting finished-Workload immutability")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.PodTemplate.Spec.NodeSelector = map[string]string{"workload-tier": "repaired"}
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Updating taskManager.replicas in place so Kueue replaces the FailedToStart Workload and starts the repaired FlinkDeployment")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.TaskManager.Replicas = new(int64(2))
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.UID).NotTo(gomega.Equal(initialWlUID))
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.PodTemplate.Labels).To(gomega.HaveKeyWithValue("team", "streaming"))
			g.Expect(created.Spec.PodTemplate.Annotations).To(gomega.HaveKeyWithValue("owner", "analytics"))
			g.Expect(created.Spec.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue("workload-tier", "repaired"))
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should hold quota during savepoint eviction drain on deactivation and allow detaching once cleared", func() {
		flinkDep := testingflinkdeployment.MakeFlinkDeployment("savepoint-drain-immutable", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			UpgradeMode(flinkv1beta1.UpgradeModeSavepoint).
			JobManagerResource(new(1.0), "1024m", "").
			TaskManagerReplicas(2).
			TaskManagerResource(new(1.0), "2048m", "").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlKey)

		created := &flinkv1beta1.FlinkDeployment{}
		ginkgo.By("Simulating running FlinkDeployment with active JobManager and TaskManager pods")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
			created.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusReady
			created.Status.JobStatus = flinkv1beta1.JobStatus{State: flinkv1beta1.JobStatusStateRunning}
			created.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 2}
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Rejecting queue-name removal and TaskManager replica scaling while FlinkDeployment is running")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		detachAttempt := created.DeepCopy()
		delete(detachAttempt.Labels, controllerconstants.QueueLabel)
		err := k8sClient.Update(ctx, detachAttempt)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		scaleAttempt := created.DeepCopy()
		scaleAttempt.Spec.TaskManager.Replicas = new(int64(3))
		err = k8sClient.Update(ctx, scaleAttempt)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("when FlinkDeployment is not suspended"))

		ginkgo.By("Deactivating Workload so stopJob suspends FlinkDeployment while QuotaReserved stays True during savepoint drain")
		behavioral.DeactivateWorkload(ctx, k8sClient, wlKey)
		wl := &kueue.Workload{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadEvicted))
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadQuotaReserved))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Simulating savepoint completion and pod teardown so QuotaReserved becomes False")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateSuspended
			created.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusMissing
			created.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 0}
			created.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusFalse(kueue.WorkloadQuotaReserved))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Allowing spec edits and queue-name removal once suspended and quota reservation is cleared")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.TaskManager.Replicas = new(int64(3))
			delete(created.Labels, controllerconstants.QueueLabel)
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should suspend via NoMatchingWorkload when scaled to 0 via /scale subresource and allow fixing replicas while suspended", func() {
		flinkDep := testingflinkdeployment.MakeFlinkDeployment("scale-to-zero-app", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			JobManagerResource(new(1.0), "1024m", "").
			TaskManagerReplicas(2).
			TaskManagerResource(new(1.0), "2048m", "").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlKey)

		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Scaling TaskManager replicas to 0 via /scale subresource")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			scaleObj := &autoscalingv1.Scale{}
			g.Expect(k8sClient.SubResource("scale").Get(ctx, created, scaleObj)).To(gomega.Succeed())
			scaleObj.Spec.Replicas = 0
			g.Expect(k8sClient.SubResource("scale").Update(ctx, created, client.WithSubResourceBody(scaleObj))).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Verifying JobReconciler suspends the FlinkDeployment on NoMatchingWorkload")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			g.Expect(created.Spec.TaskManager.Replicas).To(gomega.HaveValue(gomega.Equal(int64(0))))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Restoring TaskManager replicas to 2 while suspended and verifying re-admission to running")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.TaskManager.Replicas = new(int64(2))
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.TaskManager.Replicas).To(gomega.HaveValue(gomega.Equal(int64(2))))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should strip tainted flavor overlays via Stop for cross-flavor re-admission and not recreate a deleted finished Workload", func() {
		extraFlavor = utiltestingapi.MakeResourceFlavor("tainted-spot-flavor").
			NodeLabel(instanceKey, "tainted-spot-pool").
			Toleration(corev1.Toleration{
				Key:      "spot",
				Operator: corev1.TolerationOpEqual,
				Value:    "true",
				Effect:   corev1.TaintEffectNoSchedule,
			}).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, extraFlavor)

		taintedQuotas := *utiltestingapi.MakeFlavorQuotas(extraFlavor.Name).
			Resource(corev1.ResourceCPU, "4").
			Resource(corev1.ResourceMemory, "8Gi").
			Obj()
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.ResourceGroups[0].Flavors = []kueue.FlavorQuotas{
				taintedQuotas,
				clusterQueue.Spec.ResourceGroups[0].Flavors[0],
			}
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		behavioral.ExpectClusterQueuesToBeActive(ctx, k8sClient, clusterQueue)

		flinkDep := testingflinkdeployment.MakeFlinkDeployment("cross-flavor-app", ns.Name).
			Queue(localQueue.Name).
			TaskManagerReplicas(1).
			UpgradeMode(flinkv1beta1.UpgradeModeSavepoint).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlKey)

		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.TaskManager.PodTemplate).NotTo(gomega.BeNil())
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "tainted-spot-pool"))
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.Tolerations).To(gomega.ContainElement(corev1.Toleration{
				Key:      "spot",
				Operator: corev1.TolerationOpEqual,
				Value:    "true",
				Effect:   corev1.TaintEffectNoSchedule,
			}))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Manually suspending the running FlinkDeployment before deactivating Workload so Stop exercises suspended -> suspended RestorePodSetsInfo")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.Job.State = flinkv1beta1.JobStateSuspended
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Deactivating Workload and verifying Stop restores PodSet overlays without baking taint tolerations into Workload.spec.podSets")
		behavioral.DeactivateWorkload(ctx, k8sClient, wlKey)
		wl := &kueue.Workload{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			g.Expect(created.Spec.Job.UpgradeMode).To(gomega.Equal(flinkv1beta1.UpgradeModeSavepoint))
			g.Expect(created.Spec.JobManager.PodTemplate).To(gomega.BeNil())
			g.Expect(created.Spec.TaskManager.PodTemplate).To(gomega.BeNil())
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusFalse(kueue.WorkloadQuotaReserved))
			g.Expect(wl.Spec.PodSets[0].Template.Spec.Tolerations).To(gomega.BeEmpty())
			g.Expect(wl.Spec.PodSets[1].Template.Spec.Tolerations).To(gomega.BeEmpty())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Zeroing tainted flavor quota, reactivating Workload, and verifying clean re-admission onto taintless default-flavor")
		zeroTaintedQuotas := *utiltestingapi.MakeFlavorQuotas(extraFlavor.Name).
			Resource(corev1.ResourceCPU, "0").
			Resource(corev1.ResourceMemory, "0").
			Obj()
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), clusterQueue)).To(gomega.Succeed())
			clusterQueue.Spec.ResourceGroups[0].Flavors[0] = zeroTaintedQuotas
			g.Expect(k8sClient.Update(ctx, clusterQueue)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
			wl.Spec.Active = new(true)
			g.Expect(k8sClient.Update(ctx, wl)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.TaskManager.PodTemplate).NotTo(gomega.BeNil())
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.NodeSelector).To(gomega.HaveKeyWithValue(instanceKey, "spot-pool"))
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.Tolerations).To(gomega.BeEmpty())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Marking the FlinkDeployment finished so Kueue finishes the Workload")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
			created.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusReady
			created.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		behavioral.ExpectWorkloadToFinish(ctx, k8sClient, wlKey)

		ginkgo.By("Rejecting running-state PodSet mutations on the finished FlinkDeployment while allowing metadata updates")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		resurrectAttempt := created.DeepCopy()
		resurrectAttempt.Spec.TaskManager.Replicas = new(int64(3))
		resurrectErr := k8sClient.Update(ctx, resurrectAttempt)
		gomega.Expect(resurrectErr).To(gomega.HaveOccurred())
		gomega.Expect(resurrectErr).To(utiltesting.BeForbiddenError())
		gomega.Expect(resurrectErr.Error()).To(gomega.ContainSubstring("when FlinkDeployment is not suspended"))

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			if created.Annotations == nil {
				created.Annotations = make(map[string]string)
			}
			created.Annotations["example.com/audit"] = "archived"
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Deleting the finished Workload and verifying no new Workload is recreated")
		gomega.Expect(k8sClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Delete(ctx, wl)).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlKey, &kueue.Workload{})).To(utiltesting.BeNotFoundError())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		wlList := &kueue.WorkloadList{}
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.List(ctx, wlList, client.InNamespace(ns.Name))).To(gomega.Succeed())
			g.Expect(wlList.Items).To(gomega.BeEmpty())
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())
	})
})

var _ = ginkgo.Describe("FlinkDeployment controller with TopologyAwareScheduling", ginkgo.Label("job:flinkdeployment", "area:jobs", "feature:tas"), ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	const (
		nodeGroupLabel = "node-group"
	)

	var (
		ns           *corev1.Namespace
		nodes        []corev1.Node
		topology     *kueue.Topology
		tasFlavor    *kueue.ResourceFlavor
		clusterQueue *kueue.ClusterQueue
		localQueue   *kueue.LocalQueue
	)

	ginkgo.BeforeAll(func() {
		fwk.StartManager(ctx, cfg, managerAndSchedulerSetup(true))
	})

	ginkgo.AfterAll(func() {
		fwk.StopManager(ctx)
	})

	ginkgo.BeforeEach(func() {
		ns = behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "tas-flink-")

		nodes = []corev1.Node{
			*testingnode.MakeNode("b1r1").
				Label(nodeGroupLabel, "tas").
				Label(utiltesting.DefaultBlockTopologyLevel, "b1").
				Label(utiltesting.DefaultRackTopologyLevel, "r1").
				StatusAllocatable(corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("8"),
					corev1.ResourceMemory: resource.MustParse("16Gi"),
					corev1.ResourcePods:   resource.MustParse("10"),
				}).
				Ready().
				Obj(),
		}
		integration.CreateNodesWithStatus(ctx, k8sClient, nodes)

		topology = utiltestingapi.MakeDefaultTwoLevelTopology("default")
		behavioral.MustCreate(ctx, k8sClient, topology)

		tasFlavor = utiltestingapi.MakeResourceFlavor("tas-flavor").
			NodeLabel(nodeGroupLabel, "tas").
			TopologyName("default").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, tasFlavor)

		clusterQueue = utiltestingapi.MakeClusterQueue("tas-cluster-queue").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(tasFlavor.Name).
				Resource(corev1.ResourceCPU, "8").
				Resource(corev1.ResourceMemory, "16Gi").
				Obj()).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, clusterQueue)
		behavioral.ExpectClusterQueuesToBeActive(ctx, k8sClient, clusterQueue)

		localQueue = utiltestingapi.MakeLocalQueue("tas-local-queue", ns.Name).
			ClusterQueue(clusterQueue.Name).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, localQueue)
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(behavioral.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, clusterQueue, true)
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, tasFlavor, true)
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, topology, true)
		for _, node := range nodes {
			behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, &node, true)
		}
	})

	ginkgo.It("Should admit a FlinkDeployment with TAS topology annotations and inject TopologySchedulingGate cleanly", func() {
		flinkDep := testingflinkdeployment.MakeFlinkDeployment("tas-flink-app", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			JobManagerPodTemplate(&corev1.PodTemplateSpec{
				Annotations: map[string]string{
					kueue.PodSetRequiredTopologyAnnotation: utiltesting.DefaultRackTopologyLevel,
				},
			}).
			TaskManagerPodTemplate(&corev1.PodTemplateSpec{
				Annotations: map[string]string{
					kueue.PodSetPreferredTopologyAnnotation: utiltesting.DefaultBlockTopologyLevel,
				},
			}).
			TaskManagerReplicas(1).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlLookupKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}

		wl := &kueue.Workload{}
		ginkgo.By("Verifying Workload PodSets carry structured TopologyRequest")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlLookupKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Spec.PodSets).To(gomega.BeComparableTo([]kueue.PodSet{
				{
					Name:  kueue.NewPodSetReference("job-manager"),
					Count: 1,
					TopologyRequest: &kueue.PodSetTopologyRequest{
						Required: new(utiltesting.DefaultRackTopologyLevel),
					},
				},
				{
					Name:  kueue.NewPodSetReference("task-manager"),
					Count: 1,
					TopologyRequest: &kueue.PodSetTopologyRequest{
						Preferred: new(utiltesting.DefaultBlockTopologyLevel),
					},
				},
			}, cmpopts.IgnoreFields(kueue.PodSet{}, "Template")))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Verifying Workload is admitted with TopologyAssignment and FlinkDeployment has TopologySchedulingGate")
		behavioral.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, wl)

		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlLookupKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Admission).NotTo(gomega.BeNil())
			g.Expect(wl.Status.Admission.PodSetAssignments).To(gomega.HaveLen(2))
			g.Expect(wl.Status.Admission.PodSetAssignments[0].TopologyAssignment).To(gomega.BeComparableTo(
				tas.V1Beta2From(&tas.TopologyAssignment{
					Levels:  []string{utiltesting.DefaultBlockTopologyLevel, utiltesting.DefaultRackTopologyLevel},
					Domains: []tas.TopologyDomainAssignment{{Count: 1, Values: []string{"b1", "r1"}}},
				}),
			))
			g.Expect(wl.Status.Admission.PodSetAssignments[1].TopologyAssignment).To(gomega.BeComparableTo(
				tas.V1Beta2From(&tas.TopologyAssignment{
					Levels:  []string{utiltesting.DefaultBlockTopologyLevel, utiltesting.DefaultRackTopologyLevel},
					Domains: []tas.TopologyDomainAssignment{{Count: 1, Values: []string{"b1", "r1"}}},
				}),
			))

			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.SchedulingGates).To(
				gomega.ContainElement(corev1.PodSchedulingGate{Name: kueue.TopologySchedulingGate}))
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.SchedulingGates).To(
				gomega.ContainElement(corev1.PodSchedulingGate{Name: kueue.TopologySchedulingGate}))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Confirming TAS TopologySchedulingGate does not cause EquivalentToWorkload eviction")
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlLookupKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
			g.Expect(wl.Status.Conditions).NotTo(utiltesting.HaveConditionStatusTrue(kueue.WorkloadEvicted))
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())

		ginkgo.By("Rejecting updates that strip TopologySchedulingGate on the running TAS-admitted FlinkDeployment")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		gateStripped := created.DeepCopy()
		gateStripped.Spec.TaskManager.PodTemplate.Spec.SchedulingGates = nil
		err := k8sClient.Update(ctx, gateStripped)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
	})

	ginkgo.It("Should suspend via Stop without wedging when an admitted HA TAS-grouped FlinkDeployment is scaled to 0 or 4 via /scale", func() {
		groupAnnotations := map[string]string{
			kueue.PodSetGroupName:                  "ha-flink-group",
			kueue.PodSetRequiredTopologyAnnotation: utiltesting.DefaultRackTopologyLevel,
		}
		flinkDep := testingflinkdeployment.MakeFlinkDeployment("tas-ha-group-scale", ns.Name).
			Queue(localQueue.Name).
			Suspend(false).
			JobManagerReplicas(2).
			TaskManagerReplicas(1).
			JobManagerPodTemplate(&corev1.PodTemplateSpec{Annotations: groupAnnotations}).
			TaskManagerPodTemplate(&corev1.PodTemplateSpec{Annotations: groupAnnotations}).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		wlLookupKey := types.NamespacedName{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, flinkDep.UID),
		}
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlLookupKey)

		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.SchedulingGates).To(
				gomega.ContainElement(corev1.PodSchedulingGate{Name: kueue.TopologySchedulingGate}))
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.SchedulingGates).To(
				gomega.ContainElement(corev1.PodSchedulingGate{Name: kueue.TopologySchedulingGate}))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		scaleTaskManagers := func(replicas int32) {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
				scaleObj := &autoscalingv1.Scale{}
				g.Expect(k8sClient.SubResource("scale").Get(ctx, created, scaleObj)).To(gomega.Succeed())
				scaleObj.Spec.Replicas = replicas
				g.Expect(k8sClient.SubResource("scale").Update(ctx, created, client.WithSubResourceBody(scaleObj))).To(gomega.Succeed())
			}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		}

		ginkgo.By("Scaling HA TAS-grouped FlinkDeployment to 0 via /scale and verifying Stop suspends and strips TAS gates")
		scaleTaskManagers(0)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			g.Expect(created.Spec.TaskManager.Replicas).To(gomega.HaveValue(gomega.Equal(int64(0))))
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.SchedulingGates).To(gomega.BeEmpty())
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.SchedulingGates).To(gomega.BeEmpty())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Rejecting suspended update that changes TaskManager replicas from 0 to 3 (another invalid group count)")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		invalidGroupAttempt := created.DeepCopy()
		invalidGroupAttempt.Spec.TaskManager.Replicas = new(int64(3))
		invalidErr := k8sClient.Update(ctx, invalidGroupAttempt)
		gomega.Expect(invalidErr).To(gomega.HaveOccurred())
		gomega.Expect(invalidErr).To(utiltesting.BeForbiddenError())
		gomega.Expect(invalidErr.Error()).To(gomega.ContainSubstring("can only define groups where at least one pod set has only 1 replica"))

		ginkgo.By("Repairing TaskManager replicas back to 1 while suspended and waiting for re-admission")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.TaskManager.Replicas = new(int64(1))
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlLookupKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Scaling HA TAS-grouped FlinkDeployment to 4 via /scale and verifying Stop suspends and deletes the stale Workload")
		var preScaleFourWl kueue.Workload
		gomega.Expect(k8sClient.Get(ctx, wlLookupKey, &preScaleFourWl)).To(gomega.Succeed())
		preScaleFourUID := preScaleFourWl.UID
		scaleTaskManagers(4)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
			g.Expect(created.Spec.TaskManager.Replicas).To(gomega.HaveValue(gomega.Equal(int64(4))))
			g.Expect(created.Spec.JobManager.PodTemplate.Spec.SchedulingGates).To(gomega.BeEmpty())
			g.Expect(created.Spec.TaskManager.PodTemplate.Spec.SchedulingGates).To(gomega.BeEmpty())
			var curWl kueue.Workload
			err := k8sClient.Get(ctx, wlLookupKey, &curWl)
			g.Expect(err != nil || curWl.UID != preScaleFourUID).To(gomega.BeTrue())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Rejecting suspended update that renames podset-group-name while counts remain (2, 4)")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		renameGroupAttempt := created.DeepCopy()
		renameGroupAttempt.Spec.JobManager.PodTemplate.Annotations[kueue.PodSetGroupName] = "ha-flink-group-renamed"
		renameGroupAttempt.Spec.TaskManager.PodTemplate.Annotations[kueue.PodSetGroupName] = "ha-flink-group-renamed"
		renameErr := k8sClient.Update(ctx, renameGroupAttempt)
		gomega.Expect(renameErr).To(gomega.HaveOccurred())
		gomega.Expect(renameErr).To(utiltesting.BeForbiddenError())
		gomega.Expect(renameErr.Error()).To(gomega.ContainSubstring("can only define groups where at least one pod set has only 1 replica"))

		ginkgo.By("Repairing TaskManager replicas from 4 back to 1 and verifying re-admission to running")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.TaskManager.Replicas = new(int64(1))
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
		behavioral.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sClient, wlLookupKey)
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
			g.Expect(created.Spec.TaskManager.Replicas).To(gomega.HaveValue(gomega.Equal(int64(1))))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})
})

var _ = ginkgo.Describe("FlinkDeployment controller with legacy queue-labeled Session cluster", ginkgo.Label("job:flinkdeployment", "area:jobs"), ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeAll(func() {
		fwk.StartManager(ctx, cfg, managerSetup(
			jobframework.WithNoopWebhook(true),
		))
	})

	ginkgo.AfterAll(func() {
		fwk.StopManager(ctx)
	})

	ginkgo.BeforeEach(func() {
		ns = behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "flink-legacy-session-")
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(behavioral.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
	})

	ginkgo.It("Should skip reconciliation of a pre-existing queue-labeled Session cluster without creating a Workload or emitting Stopped events", func() {
		session := testingflinkdeployment.MakeFlinkDeployment("legacy-labeled-session", ns.Name).
			SessionCluster().
			Queue("legacy-queue").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, session)

		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(session), created)).To(gomega.Succeed())
			created.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusReady
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		wlList := &kueue.WorkloadList{}
		evV1List := &eventsv1.EventList{}
		coreEvList := &corev1.EventList{}
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(k8sClient.List(ctx, wlList, client.InNamespace(ns.Name))).To(gomega.Succeed())
			g.Expect(wlList.Items).To(gomega.BeEmpty())

			g.Expect(k8sClient.List(ctx, evV1List, client.InNamespace(ns.Name))).To(gomega.Succeed())
			for _, ev := range evV1List.Items {
				g.Expect(ev.Reason).NotTo(gomega.Equal("Stopped"))
			}

			g.Expect(k8sClient.List(ctx, coreEvList, client.InNamespace(ns.Name))).To(gomega.Succeed())
			for _, ev := range coreEvList.Items {
				g.Expect(ev.Reason).NotTo(gomega.Equal("Stopped"))
			}
		}, behavioral.ConsistentDuration, behavioral.ShortInterval).Should(gomega.Succeed())

		ginkgo.By("Allowing CRD-valid unmanaged objects with 2g memory in podTemplate and non-object flinkConfiguration under NoopWebhook and cluster-wide informer ListAndWatch")
		rawNoopSession := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "flink.apache.org/v1beta1",
				"kind":       "FlinkDeployment",
				"metadata": map[string]any{
					"name":      "noop-2g-podtemplate-session",
					"namespace": ns.Name,
				},
				"spec": map[string]any{
					"image":              "flink:1.20",
					"flinkVersion":       "v1_20",
					"flinkConfiguration": []any{"non", "object"},
					"podTemplate": map[string]any{
						"spec": map[string]any{
							"containers": []any{
								map[string]any{
									"name": "sidecar",
									"resources": map[string]any{
										"requests": map[string]any{"memory": "2g"},
									},
								},
							},
						},
					},
				},
			},
		}
		behavioral.MustCreate(ctx, k8sClient, rawNoopSession)

		typedList := &flinkv1beta1.FlinkDeploymentList{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.List(ctx, typedList, client.InNamespace(ns.Name))).To(gomega.Succeed())
			g.Expect(typedList.Items).To(gomega.HaveLen(2))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})
})
