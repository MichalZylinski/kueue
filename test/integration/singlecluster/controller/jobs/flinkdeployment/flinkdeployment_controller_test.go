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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	workloadflinkdeployment "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/test/util/behavioral"
)

var _ = ginkgo.Describe("FlinkDeployment controller", ginkgo.Label("job:flinkdeployment", "area:jobs"), ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		ns           *corev1.Namespace
		clusterQueue *kueue.ClusterQueue
		localQueue   *kueue.LocalQueue
	)

	ginkgo.BeforeAll(func() {
		fwk.StartManager(ctx, cfg, managerAndSchedulerSetup(
			jobframework.WithWaitForPodsReady(&config.WaitForPodsReady{}),
		))
	})

	ginkgo.AfterAll(func() {
		fwk.StopManager(ctx)
	})

	ginkgo.BeforeEach(func() {
		ns = behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "flink-")

		flavor := utiltestingapi.MakeResourceFlavor("default-flavor").Obj()
		behavioral.MustCreate(ctx, k8sClient, flavor)

		clusterQueue = utiltestingapi.MakeClusterQueue("cluster-queue").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default-flavor").
				Resource(corev1.ResourceCPU, "10").
				Resource(corev1.ResourceMemory, "20Gi").
				Obj()).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, clusterQueue)

		localQueue = utiltestingapi.MakeLocalQueue("local-queue", ns.Name).
			ClusterQueue("cluster-queue").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, localQueue)
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(behavioral.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		behavioral.ExpectObjectToBeDeleted(ctx, k8sClient, clusterQueue, true)
	})

	ginkgo.It("Should reconcile, suspend, admit, and track FlinkDeployment lifecycle", func() {
		flinkDep := &flinkv1beta1.FlinkDeployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample-streaming-app",
				Namespace: ns.Name,
				Labels: map[string]string{
					constants.QueueLabel: "local-queue",
				},
			},
			Spec: flinkv1beta1.FlinkDeploymentSpec{
				Image: "flink:1.18",
				JobManager: &flinkv1beta1.JobManagerSpec{
					Replicas: ptr.To[int32](1),
					Resource: &flinkv1beta1.Resource{
						CPU:    ptr.To(1.0),
						Memory: "2Gi",
					},
				},
				TaskManager: &flinkv1beta1.TaskManagerSpec{
					Replicas: ptr.To[int32](2),
					Resource: &flinkv1beta1.Resource{
						CPU:    ptr.To(1.0),
						Memory: "2Gi",
					},
				},
				Job: &flinkv1beta1.JobSpec{
					State:       flinkv1beta1.JobStateSuspended,
					UpgradeMode: flinkv1beta1.UpgradeModeStateless,
				},
			},
		}

		ginkgo.By("Creating the FlinkDeployment, webhook should suspend it")
		behavioral.MustCreate(ctx, k8sClient, flinkDep)

		lookupKey := client.ObjectKeyFromObject(flinkDep)
		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job).NotTo(gomega.BeNil())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Verifying that a Workload was created with 2 PodSets")
		wlLookupKey := client.ObjectKey{
			Namespace: ns.Name,
			Name:      workloadflinkdeployment.GetWorkloadNameForFlinkDeployment(flinkDep.Name, created.UID),
		}
		wl := &kueue.Workload{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlLookupKey, wl)).To(gomega.Succeed())
			g.Expect(wl.Spec.PodSets).To(gomega.HaveLen(2))
			g.Expect(wl.Spec.PodSets[0].Name).To(gomega.Equal(kueue.NewPodSetReference("jobmanager")))
			g.Expect(wl.Spec.PodSets[0].Count).To(gomega.Equal(int32(1)))
			g.Expect(wl.Spec.PodSets[1].Name).To(gomega.Equal(kueue.NewPodSetReference("taskmanager")))
			g.Expect(wl.Spec.PodSets[1].Count).To(gomega.Equal(int32(2)))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Waiting for Kueue scheduler to admit the Workload and unsuspend the FlinkDeployment")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			g.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateRunning))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Simulating Flink Operator setting JobManager READY and JobStatus RUNNING")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Status.JobManagerDeploymentStatus = flinkv1beta1.JobManagerDeploymentStatusReady
			created.Status.JobStatus = flinkv1beta1.JobStatus{
				State: "RUNNING",
			}
			g.Expect(k8sClient.Status().Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("Verifying that Workload PodsReady condition transitions to True")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, wlLookupKey, wl)).To(gomega.Succeed())
			cond := apimeta.FindStatusCondition(wl.Status.Conditions, string(kueue.WorkloadPodsReady))
			g.Expect(cond).NotTo(gomega.BeNil())
			g.Expect(cond.Status).To(gomega.Equal(metav1.ConditionTrue))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})
})
