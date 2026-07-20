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
	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingsparkapplication "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadfinish "sigs.k8s.io/kueue/pkg/workload/finish"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
	"sigs.k8s.io/kueue/test/integration/framework"
	"sigs.k8s.io/kueue/test/util"
)

// makeExecutorPod builds a standalone pod carrying the labels the
// spark-operator sets on real executor pods. Elastic demand observation
// (SparkApplication.numExecutors) matches on these labels alone, not
// ownership, so this is sufficient to simulate the driver creating an
// executor without needing a real driver pod or spark-operator in envtest.
func makeExecutorPod(app *sparkv1beta2.SparkApplication, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: app.Namespace,
			Labels: map[string]string{
				sparkcommon.LabelSparkAppName: app.Name,
				sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "executor",
				Image: "spark:test",
			}},
		},
	}
}

var _ = ginkgo.Describe("SparkApplication controller with elastic scaling", ginkgo.Label("job:sparkapplication", "area:jobs", "feature:elastic"), ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		ns             *corev1.Namespace
		resourceFlavor *kueue.ResourceFlavor
		clusterQueue   *kueue.ClusterQueue
		localQueue     *kueue.LocalQueue
	)

	ginkgo.BeforeAll(func() {
		gomega.Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{string(features.ElasticJobsViaWorkloadSlices): true})).Should(gomega.Succeed())
		fwk.StartManager(ctx, cfg, managerAndSchedulerSetup(false))
	})
	ginkgo.AfterAll(func() {
		fwk.StopManager(ctx)
	})

	ginkgo.BeforeEach(func() {
		ns = util.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "elastic-spark-")

		resourceFlavor = utiltestingapi.MakeResourceFlavor("default").Obj()
		util.MustCreate(ctx, k8sClient, resourceFlavor)

		clusterQueue = utiltestingapi.MakeClusterQueue("elastic-cq").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(resourceFlavor.Name).
				Resource(corev1.ResourceCPU, "10").
				Resource(corev1.ResourceMemory, "10Gi").
				Obj()).
			Obj()
		util.MustCreate(ctx, k8sClient, clusterQueue)

		localQueue = utiltestingapi.MakeLocalQueue("elastic-lq", ns.Name).ClusterQueue(clusterQueue.Name).Obj()
		util.MustCreate(ctx, k8sClient, localQueue)
	})
	ginkgo.AfterEach(func() {
		gomega.Expect(util.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		util.ExpectObjectToBeDeleted(ctx, k8sClient, clusterQueue, true)
		util.ExpectObjectToBeDeleted(ctx, k8sClient, localQueue, true)
		util.ExpectObjectToBeDeleted(ctx, k8sClient, resourceFlavor, true)
	})

	ginkgo.It("Should scale up and down via observed executor pod demand", framework.SlowSpec, func() {
		sparkApplication := testingsparkapplication.MakeSparkApplication("elastic-app", ns.Name).
			Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
			Queue(localQueue.Name).
			DriverCoreRequest("1").
			ExecutorCoreRequest("1").
			ExecutorInstances(2).
			DynamicAllocation(&sparkv1beta2.DynamicAllocation{
				Enabled:      true,
				MinExecutors: ptr.To[int32](1),
				MaxExecutors: ptr.To[int32](5),
			}).
			Obj()

		var initialWorkload *kueue.Workload

		ginkgo.By("creating the elastic SparkApplication", func() {
			util.MustCreate(ctx, k8sClient, sparkApplication)
		})

		ginkgo.By("the initial workload is admitted with the starting executor count", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				workloads := &kueue.WorkloadList{}
				g.Expect(k8sClient.List(ctx, workloads, client.InNamespace(ns.Name))).Should(gomega.Succeed())
				g.Expect(workloads.Items).Should(gomega.HaveLen(1))
				initialWorkload = &workloads.Items[0]
				g.Expect(initialWorkload.Spec.PodSets).Should(gomega.HaveLen(2))
				g.Expect(initialWorkload.Spec.PodSets[0].Count).Should(gomega.Equal(int32(1))) // driver
				g.Expect(initialWorkload.Spec.PodSets[1].Count).Should(gomega.Equal(int32(2))) // executor
				g.Expect(workload.IsAdmitted(initialWorkload)).Should(gomega.BeTrue())
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("ClusterQueue usage reflects the driver and initial executors (1+2=3 CPU)", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				cq := &kueue.ClusterQueue{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), cq)).Should(gomega.Succeed())
				g.Expect(cq.Status.FlavorsUsage).Should(gomega.HaveLen(1))
				g.Expect(cq.Status.FlavorsUsage[0].Resources[0].Total).Should(gomega.BeEquivalentTo(resource.MustParse("3")))
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("marking the SparkApplication as running, as the operator would", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sparkApplication), sparkApplication)).Should(gomega.Succeed())
				sparkApplication.Status.AppState.State = sparkv1beta2.ApplicationStateRunning
				g.Expect(k8sClient.Status().Update(ctx, sparkApplication)).Should(gomega.Succeed())
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		executorPodNames := []string{"exec-0", "exec-1", "exec-2", "exec-3", "exec-4"}
		ginkgo.By("simulating the driver ramping up to 5 executor pods (scale-up)", func() {
			for _, name := range executorPodNames {
				util.MustCreate(ctx, k8sClient, makeExecutorPod(sparkApplication, name))
			}
		})

		var scaledUpWorkload *kueue.Workload
		ginkgo.By("a new workload slice is created and admitted with the scaled-up executor count, and the old slice is finished", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				workloads := &kueue.WorkloadList{}
				g.Expect(k8sClient.List(ctx, workloads, client.InNamespace(ns.Name))).Should(gomega.Succeed())
				g.Expect(workloads.Items).Should(gomega.HaveLen(2))

				scaledUpWorkload = nil
				for i := range workloads.Items {
					if workloadfinish.IsFinished(&workloads.Items[i]) {
						g.Expect(workloads.Items[i].Name).Should(gomega.Equal(initialWorkload.Name))
					} else {
						g.Expect(workloads.Items[i].Name).ShouldNot(gomega.Equal(initialWorkload.Name))
						scaledUpWorkload = &workloads.Items[i]
					}
				}
				g.Expect(scaledUpWorkload).ShouldNot(gomega.BeNil())
				g.Expect(workload.IsAdmitted(scaledUpWorkload)).Should(gomega.BeTrue())
				g.Expect(scaledUpWorkload.Spec.PodSets).Should(gomega.HaveLen(2))
				g.Expect(scaledUpWorkload.Spec.PodSets[0].Count).Should(gomega.Equal(int32(1))) // driver
				g.Expect(scaledUpWorkload.Spec.PodSets[1].Count).Should(gomega.Equal(int32(5))) // executor
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("ClusterQueue usage reflects the scaled-up executor count (1+5=6 CPU)", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				cq := &kueue.ClusterQueue{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), cq)).Should(gomega.Succeed())
				g.Expect(cq.Status.FlavorsUsage[0].Resources[0].Total).Should(gomega.BeEquivalentTo(resource.MustParse("6")))
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("simulating the driver deleting 3 idle executor pods (scale-down)", func() {
			for _, name := range executorPodNames[:3] {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns.Name}}
				gomega.Expect(k8sClient.Delete(ctx, pod)).Should(gomega.Succeed())
			}
		})

		ginkgo.By("the admitted workload is updated in place with the scaled-down executor count, without creating a new slice", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				updated := &kueue.Workload{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(scaledUpWorkload), updated)).Should(gomega.Succeed())
				g.Expect(updated.Spec.PodSets[1].Count).Should(gomega.Equal(int32(2)))
				g.Expect(workload.IsAdmitted(updated)).Should(gomega.BeTrue())

				workloads := &kueue.WorkloadList{}
				g.Expect(k8sClient.List(ctx, workloads, client.InNamespace(ns.Name))).Should(gomega.Succeed())
				g.Expect(workloads.Items).Should(gomega.HaveLen(2)) // the finished initial slice, plus this one; no third slice
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("ClusterQueue usage reflects the scaled-down executor count (1+2=3 CPU)", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				cq := &kueue.ClusterQueue{}
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterQueue), cq)).Should(gomega.Succeed())
				g.Expect(cq.Status.FlavorsUsage[0].Resources[0].Total).Should(gomega.BeEquivalentTo(resource.MustParse("3")))
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
		})

		ginkgo.By("cleaning up the remaining executor pods", func() {
			for _, name := range executorPodNames[3:] {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns.Name}}
				gomega.Expect(k8sClient.Delete(ctx, pod)).Should(gomega.Succeed())
			}
		})
	})
})
