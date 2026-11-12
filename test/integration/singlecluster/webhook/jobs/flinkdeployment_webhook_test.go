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

package jobs

import (
	"math"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueconstants "sigs.k8s.io/kueue/pkg/constants"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingflinkdeployment "sigs.k8s.io/kueue/pkg/util/testingjobs/flinkdeployment"
	"sigs.k8s.io/kueue/pkg/webhooks"
	"sigs.k8s.io/kueue/test/util/behavioral"
)

var _ = ginkgo.Describe("FlinkDeployment Webhook", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.FlinkDeploymentIntegration, true)
		fwk.StartManager(ctx, cfg, managerSetup(func(mgr ctrl.Manager, opts ...jobframework.Option) error {
			options := jobframework.ProcessOptions(opts...)
			if options.IntegrationManager != nil {
				options.IntegrationManager.EnableIntegration(flinkdeployment.FrameworkName)
			}
			if err := indexer.Setup(ctx, mgr.GetFieldIndexer()); err != nil {
				return err
			}
			if err := flinkdeployment.SetupIndexes(ctx, mgr.GetFieldIndexer()); err != nil {
				return err
			}
			if _, err := webhooks.Setup(mgr, nil); err != nil {
				return err
			}
			return flinkdeployment.SetupWebhook(mgr, opts...)
		}))
		ns = behavioral.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "flinkdeployment-")
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(behavioral.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		flavors := &kueue.ResourceFlavorList{}
		gomega.Expect(k8sClient.List(ctx, flavors)).To(gomega.Succeed())
		for i := range flavors.Items {
			rf := &flavors.Items[i]
			if len(rf.Finalizers) > 0 {
				rf.Finalizers = nil
				gomega.Expect(k8sClient.Update(ctx, rf)).To(gomega.Succeed())
			}
			gomega.Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, rf))).To(gomega.Succeed())
		}
		fwk.StopManager(ctx)
	})

	ginkgo.It("should suspend a Kueue-managed Application-mode FlinkDeployment on create without mutating nested flinkConfiguration, 64-bit nonces, or status", func() {
		const highPrecisionNonce int64 = 1794223319123456789

		rawObj := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "flink.apache.org/v1beta1",
				"kind":       "FlinkDeployment",
				"metadata": map[string]any{
					"name":      "flink-app",
					"namespace": ns.Name,
					"labels": map[string]any{
						controllerconstants.QueueLabel: "local-queue",
					},
				},
				"spec": map[string]any{
					"image":        "flink:1.20",
					"flinkVersion": "v1_20",
					"restartNonce": highPrecisionNonce,
					"flinkConfiguration": map[string]any{
						"taskmanager": map[string]any{
							"numberOfTaskSlots": int64(2),
						},
						"pipeline.serialization-config": []any{
							map[string]any{
								"org.example.Event": map[string]any{"type": "kryo"},
							},
						},
					},
					"podTemplate": map[string]any{
						"apiVersion": "v1",
						"kind":       "Pod",
						"spec": map[string]any{
							"containers": []any{
								map[string]any{"name": "sidecar", "image": "busybox:1.36"},
							},
							"volumes": []any{
								map[string]any{"name": "proj", "projected": map[string]any{"defaultMode": int64(420)}},
								map[string]any{"name": "rbd", "rbd": map[string]any{"image": "img", "monitors": []any{"10.0.0.1:6789"}}},
								map[string]any{"name": "ceph", "cephfs": map[string]any{"path": "/mnt", "monitors": []any{"10.0.0.1:6789"}}},
							},
							"affinity": map[string]any{
								"nodeAffinity": map[string]any{
									"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{
										"nodeSelectorTerms": []any{
											map[string]any{
												"matchExpressions": []any{
													map[string]any{
														"key":      "kubernetes.io/os",
														"operator": "In",
														"values":   []any{"linux"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
					"jobManager": map[string]any{
						"resource": map[string]any{
							"memory": "1024m",
						},
					},
					"taskManager": map[string]any{
						"resource": map[string]any{
							"memory": "2Gi",
						},
					},
					"job": map[string]any{
						"jarURI":                 "local:///opt/flink/examples/streaming/StateMachineExample.jar",
						"parallelism":            int64(2),
						"state":                  "running",
						"savepointRedeployNonce": highPrecisionNonce,
					},
				},
			},
		}
		behavioral.MustCreate(ctx, k8sClient, rawObj)

		lookupKey := types.NamespacedName{Name: "flink-app", Namespace: ns.Name}
		persistedRaw := &unstructured.Unstructured{}
		persistedRaw.SetAPIVersion("flink.apache.org/v1beta1")
		persistedRaw.SetKind("FlinkDeployment")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, persistedRaw)).To(gomega.Succeed())

		// Verify mutating webhook did not flatten/duplicate nested keys or pollute /status.
		_, hasStatus, err := unstructured.NestedMap(persistedRaw.Object, "status")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(hasStatus).To(gomega.BeFalse())

		flinkCfgMap, found, err := unstructured.NestedMap(persistedRaw.Object, "spec", "flinkConfiguration")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(found).To(gomega.BeTrue())
		gomega.Expect(flinkCfgMap).NotTo(gomega.HaveKey("taskmanager.numberOfTaskSlots"))
		gomega.Expect(flinkCfgMap).To(gomega.HaveKey("taskmanager"))
		gomega.Expect(flinkCfgMap["pipeline.serialization-config"]).NotTo(gomega.Equal(""))

		ptAPIVersion, found, err := unstructured.NestedString(persistedRaw.Object, "spec", "podTemplate", "apiVersion")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(found).To(gomega.BeTrue())
		gomega.Expect(ptAPIVersion).To(gomega.Equal("v1"))
		ptKind, found, err := unstructured.NestedString(persistedRaw.Object, "spec", "podTemplate", "kind")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(found).To(gomega.BeTrue())
		gomega.Expect(ptKind).To(gomega.Equal("Pod"))

		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		gomega.Expect(created.Spec.Job).NotTo(gomega.BeNil())
		gomega.Expect(created.Spec.Job.State).To(gomega.Equal(flinkv1beta1.JobStateSuspended))
		gomega.Expect(created.Spec.RestartNonce).To(gomega.HaveValue(gomega.Equal(highPrecisionNonce)))
		gomega.Expect(created.Spec.Job.SavepointRedeployNonce).To(gomega.HaveValue(gomega.Equal(highPrecisionNonce)))

		ginkgo.By("preserving omitted monitors and empty requiredDuringSchedulingIgnoredDuringExecution without null injection on an unmanaged FlinkDeployment")
		unmanagedNullFreeRaw := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "flink.apache.org/v1beta1",
				"kind":       "FlinkDeployment",
				"metadata": map[string]any{
					"name":      "unmanaged-null-free-app",
					"namespace": ns.Name,
				},
				"spec": map[string]any{
					"image":        "flink:1.20",
					"flinkVersion": "v1_20",
					"podTemplate": map[string]any{
						"spec": map[string]any{
							"volumes": []any{
								map[string]any{"name": "proj", "projected": map[string]any{"defaultMode": int64(420)}},
								map[string]any{"name": "rbd", "rbd": map[string]any{"image": "img"}},
								map[string]any{"name": "ceph", "cephfs": map[string]any{"path": "/mnt"}},
							},
							"affinity": map[string]any{
								"nodeAffinity": map[string]any{
									"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{},
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
		behavioral.MustCreate(ctx, k8sClient, unmanagedNullFreeRaw)
		persistedUnmanaged := &unstructured.Unstructured{}
		persistedUnmanaged.SetAPIVersion("flink.apache.org/v1beta1")
		persistedUnmanaged.SetKind("FlinkDeployment")
		gomega.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "unmanaged-null-free-app", Namespace: ns.Name}, persistedUnmanaged)).To(gomega.Succeed())
		reqAffinity, found, err := unstructured.NestedMap(
			persistedUnmanaged.Object,
			"spec", "podTemplate", "spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution",
		)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(found).To(gomega.BeTrue())
		gomega.Expect(reqAffinity).NotTo(gomega.HaveKey("nodeSelectorTerms"))
		vols, found, err := unstructured.NestedSlice(persistedUnmanaged.Object, "spec", "podTemplate", "spec", "volumes")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(found).To(gomega.BeTrue())
		gomega.Expect(vols).To(gomega.HaveLen(3))
		gomega.Expect(vols[1].(map[string]any)["rbd"]).NotTo(gomega.HaveKey("monitors"))
		gomega.Expect(vols[2].(map[string]any)["cephfs"]).NotTo(gomega.HaveKey("monitors"))

		ginkgo.By("admitting an unmanaged FlinkDeployment with 64-bit CRD integers > math.MaxInt32 and listing FlinkDeploymentList cleanly while rejecting managed overflow")
		const aboveInt32 = int64(math.MaxInt32) + 1
		unmanaged64Bit := testingflinkdeployment.MakeFlinkDeployment("unmanaged-64bit-app", ns.Name).
			JobManagerReplicas(aboveInt32).
			Parallelism(aboveInt32).
			Suspend(false).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, unmanaged64Bit)
		list := &flinkv1beta1.FlinkDeploymentList{}
		gomega.Expect(k8sClient.List(ctx, list, client.InNamespace(ns.Name))).To(gomega.Succeed())
		gomega.Expect(list.Items).NotTo(gomega.BeEmpty())

		managed64Bit := testingflinkdeployment.MakeFlinkDeployment("managed-64bit-app", ns.Name).
			Queue("local-queue").
			JobManagerReplicas(aboveInt32).
			Parallelism(aboveInt32).
			Obj()
		overflowErr := k8sClient.Create(ctx, managed64Bit)
		gomega.Expect(overflowErr).To(gomega.HaveOccurred())
		gomega.Expect(overflowErr).To(utiltesting.BeForbiddenError())
		gomega.Expect(overflowErr.Error()).To(gomega.ContainSubstring("must be less than or equal to 2147483647"))
	})

	ginkgo.It("should reject a Kueue-managed Session cluster FlinkDeployment (spec.job == nil) while allowing unmanaged Session clusters", func() {
		managedSession := testingflinkdeployment.MakeFlinkDeployment("managed-session", ns.Name).
			Queue("local-queue").
			SessionCluster().
			Obj()
		err := k8sClient.Create(ctx, managedSession)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("only Application mode (spec.job != nil) is supported"))

		unmanagedSession := testingflinkdeployment.MakeFlinkDeployment("unmanaged-session", ns.Name).
			SessionCluster().
			Obj()
		behavioral.MustCreate(ctx, k8sClient, unmanagedSession)
	})

	ginkgo.It("should accept Kubernetes BinarySI memory units and reject missing or invalid memory units", func() {
		validBinarySI := testingflinkdeployment.MakeFlinkDeployment("valid-2gi", ns.Name).
			Queue("local-queue").
			JobManagerResource(nil, "2Gi", "").
			TaskManagerResource(nil, "512Mi", "").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, validBinarySI)

		invalidMem := testingflinkdeployment.MakeFlinkDeployment("bad-mem", ns.Name).
			Queue("local-queue").
			JobManagerResource(new(1.0), "2xyz", "").
			Obj()
		err := k8sClient.Create(ctx, invalidMem)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("spec.jobManager.resource.memory"))

		missingMem := testingflinkdeployment.MakeFlinkDeployment("missing-mem", ns.Name).
			Queue("local-queue").
			JobManagerResource(new(1.0), "", "").
			Obj()
		err = k8sClient.Create(ctx, missingMem)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("spec.jobManager.resource.memory"))
	})

	ginkgo.It("should enforce Workload-backed admission transitions and running-state immutability", func() {
		flavor := utiltestingapi.MakeResourceFlavor("webhook-spot-flavor").
			NodeLabel("tier", "spot").
			Obj()
		behavioral.MustCreate(ctx, k8sClient, flavor)

		app := testingflinkdeployment.MakeFlinkDeployment("running-app", ns.Name).
			Queue("local-queue").
			TaskManagerPodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"tenant": "team-a"},
				},
			}).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, app)

		lookupKey := types.NamespacedName{Name: app.Name, Namespace: app.Namespace}
		created := &flinkv1beta1.FlinkDeployment{}
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())

		ginkgo.By("rejecting suspended -> running before an owned Workload exists")
		unadmittedAttempt := created.DeepCopy()
		unadmittedAttempt.Spec.Job.State = flinkv1beta1.JobStateRunning
		err := k8sClient.Update(ctx, unadmittedAttempt)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		gvk := flinkv1beta1.SchemeGroupVersion.WithKind("FlinkDeployment")
		wlName := flinkdeployment.GetWorkloadNameForFlinkDeployment(created.Name, created.UID)
		wl := utiltestingapi.MakeWorkload(wlName, ns.Name).
			Queue("local-queue").
			PodSets(
				*utiltestingapi.MakePodSet("job-manager", 1).
					Containers(corev1.Container{
						Name:  "flink-main-container",
						Image: "flink:1.20",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							},
						},
					}).
					Obj(),
				*utiltestingapi.MakePodSet("task-manager", 1).
					NodeSelector(map[string]string{"tenant": "team-a"}).
					Containers(corev1.Container{
						Name:  "flink-main-container",
						Image: "flink:1.20",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("2Gi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("2Gi"),
							},
						},
					}).
					Obj(),
			).
			ControllerReference(gvk, created.Name, string(created.UID)).
			Obj()
		behavioral.MustCreate(ctx, k8sClient, wl)

		ginkgo.By("rejecting suspended -> running while the owned Workload is still queued")
		queuedAttempt := created.DeepCopy()
		queuedAttempt.Spec.Job.State = flinkv1beta1.JobStateRunning
		err = k8sClient.Update(ctx, queuedAttempt)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		ginkgo.By("admitting the owned Workload")
		now := time.Now()
		admittedStatusWl := utiltestingapi.MakeWorkload(wlName, ns.Name).
			ReserveQuotaAt(
				utiltestingapi.MakeAdmission("cluster-queue").
					PodSets(
						utiltestingapi.MakePodSetAssignment("job-manager").
							Assignment(corev1.ResourceCPU, kueue.ResourceFlavorReference(flavor.Name), "1").
							Obj(),
						utiltestingapi.MakePodSetAssignment("task-manager").
							Assignment(corev1.ResourceCPU, kueue.ResourceFlavorReference(flavor.Name), "1").
							Obj(),
					).
					Obj(),
				now,
			).
			AdmittedAt(true, now).
			Obj()
		gomega.Eventually(func(g gomega.Gomega) {
			liveWl := &kueue.Workload{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wl), liveWl)).To(gomega.Succeed())
			liveWl.Status = admittedStatusWl.Status
			g.Expect(k8sClient.Status().Update(ctx, liveWl)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("rejecting suspended -> running with hijacked ResourceFlavor nodeSelector")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			hijacked := created.DeepCopy()
			hijacked.Spec.Job.State = flinkv1beta1.JobStateRunning
			hijacked.Spec.JobManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"tier": "on-demand"},
				},
			})
			updateErr := k8sClient.Update(ctx, hijacked)
			g.Expect(updateErr).To(gomega.HaveOccurred())
			g.Expect(updateErr).To(utiltesting.BeForbiddenError())
			g.Expect(updateErr.Error()).To(gomega.ContainSubstring("field is immutable when FlinkDeployment is not suspended"))
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("rejecting podTemplate mutation while suspended once Workload is admitted")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		suspendedMutate := created.DeepCopy()
		suspendedMutate.Spec.TaskManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"tenant": "hijacked"},
			},
		})
		err = k8sClient.Update(ctx, suspendedMutate)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		ginkgo.By("allowing suspended -> running when PodSet overlays match the admitted Workload")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.Job.State = flinkv1beta1.JobStateRunning
			created.Spec.JobManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"tier": "spot"},
				},
			})
			created.Spec.JobManager.PodTemplate.Labels = map[string]string{
				kueueconstants.PodSetLabel:       "job-manager",
				kueueconstants.LocalQueueLabel:   "local-queue",
				kueueconstants.ClusterQueueLabel: "cluster-queue",
			}
			created.Spec.JobManager.PodTemplate.Annotations = map[string]string{
				kueue.WorkloadAnnotation: wlName,
			}
			created.Spec.TaskManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{
						"tenant": "team-a",
						"tier":   "spot",
					},
				},
			})
			created.Spec.TaskManager.PodTemplate.Labels = map[string]string{
				kueueconstants.PodSetLabel:       "task-manager",
				kueueconstants.LocalQueueLabel:   "local-queue",
				kueueconstants.ClusterQueueLabel: "cluster-queue",
			}
			created.Spec.TaskManager.PodTemplate.Annotations = map[string]string{
				kueue.WorkloadAnnotation: wlName,
			}
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("rejecting scaling spec.taskManager.replicas while running")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		created.Spec.TaskManager.Replicas = new(int64(4))
		err = k8sClient.Update(ctx, created)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		ginkgo.By("rejecting spec.serviceAccount mutation while running")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		created.Spec.ServiceAccount = "cluster-admin-sa"
		err = k8sClient.Update(ctx, created)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		ginkgo.By("rejecting removing kueue.x-k8s.io/queue-name while running")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		delete(created.Labels, controllerconstants.QueueLabel)
		err = k8sClient.Update(ctx, created)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		ginkgo.By("rejecting simultaneous spec.job.state=suspended and scaling replicas on a running FlinkDeployment")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		created.Spec.Job.State = flinkv1beta1.JobStateSuspended
		created.Spec.TaskManager.Replicas = new(int64(4))
		err = k8sClient.Update(ctx, created)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		ginkgo.By("rejecting simultaneous spec.job.state=suspended and wiping tenant nodeSelector")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		created.Spec.Job.State = flinkv1beta1.JobStateSuspended
		created.Spec.JobManager.PodTemplate = nil
		created.Spec.TaskManager.PodTemplate = nil
		err = k8sClient.Update(ctx, created)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		ginkgo.By("rejecting simultaneous spec.job.state=suspended and removing queue-name on a running FlinkDeployment")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		created.Spec.Job.State = flinkv1beta1.JobStateSuspended
		delete(created.Labels, controllerconstants.QueueLabel)
		err = k8sClient.Update(ctx, created)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())

		ginkgo.By("allowing manual spec.job.state=suspended followed by Stop's suspended -> suspended RestorePodSetsInfo once Workload is evicted")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.Job.State = flinkv1beta1.JobStateSuspended
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			liveWl := &kueue.Workload{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wl), liveWl)).To(gomega.Succeed())
			evictedWl := utiltestingapi.MakeWorkload(wlName, ns.Name).
				Condition(metav1.Condition{
					Type:               kueue.WorkloadEvicted,
					Status:             metav1.ConditionTrue,
					Reason:             kueue.WorkloadDeactivated,
					LastTransitionTime: metav1.Now(),
				}).
				Obj()
			liveWl.Status.Conditions = append(liveWl.Status.Conditions, evictedWl.Status.Conditions...)
			g.Expect(k8sClient.Status().Update(ctx, liveWl)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
			created.Spec.JobManager.PodTemplate = nil
			created.Spec.TaskManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"tenant": "team-a"},
				},
			})
			g.Expect(k8sClient.Update(ctx, created)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("rejecting invalid max-exec-time-seconds on an already-restored evicted FlinkDeployment once quota reservation is cleared")
		gomega.Eventually(func(g gomega.Gomega) {
			liveWl := &kueue.Workload{}
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(wl), liveWl)).To(gomega.Succeed())
			liveWl.Status.Admission = nil
			liveWl.Status.Conditions = []metav1.Condition{
				{
					Type:               kueue.WorkloadQuotaReserved,
					Status:             metav1.ConditionFalse,
					Reason:             kueue.WorkloadEvicted,
					LastTransitionTime: metav1.Now(),
				},
				{
					Type:               kueue.WorkloadEvicted,
					Status:             metav1.ConditionTrue,
					Reason:             kueue.WorkloadDeactivated,
					LastTransitionTime: metav1.Now(),
				},
			}
			g.Expect(k8sClient.Status().Update(ctx, liveWl)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Expect(k8sClient.Get(ctx, lookupKey, created)).To(gomega.Succeed())
		invalidMaxExec := created.DeepCopy()
		invalidMaxExec.Labels[controllerconstants.MaxExecTimeSecondsLabel] = "30m"
		err = k8sClient.Update(ctx, invalidMaxExec)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("strconv.Atoi"))
	})

	ginkgo.It("should preserve raw integer byte memory quantities on create and reject fractional BinarySI memory on update while suspended", func() {
		rawObj := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "flink.apache.org/v1beta1",
				"kind":       "FlinkDeployment",
				"metadata": map[string]any{
					"name":      "raw-bytes-app",
					"namespace": ns.Name,
					"labels": map[string]any{
						controllerconstants.QueueLabel: "local-queue",
					},
				},
				"spec": map[string]any{
					"image":        "flink:1.20",
					"flinkVersion": "v1_20",
					"jobManager": map[string]any{
						"resources": map[string]any{
							"requests": map[string]any{
								"cpu":    "1",
								"memory": int64(1000000000),
							},
						},
					},
					"taskManager": map[string]any{
						"resources": map[string]any{
							"requests": map[string]any{
								"cpu":    "1",
								"memory": "1024000",
							},
						},
					},
					"job": map[string]any{
						"jarURI":      "local:///opt/flink/examples/streaming/StateMachineExample.jar",
						"parallelism": int64(2),
						"state":       "running",
					},
				},
			},
		}
		behavioral.MustCreate(ctx, k8sClient, rawObj)

		lookupKey := types.NamespacedName{Name: "raw-bytes-app", Namespace: ns.Name}
		persistedRaw := &unstructured.Unstructured{}
		persistedRaw.SetAPIVersion("flink.apache.org/v1beta1")
		persistedRaw.SetKind("FlinkDeployment")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, persistedRaw)).To(gomega.Succeed())

		jobState, found, err := unstructured.NestedString(persistedRaw.Object, "spec", "job", "state")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(found).To(gomega.BeTrue())
		gomega.Expect(jobState).To(gomega.Equal(string(flinkv1beta1.JobStateSuspended)))

		jmMem, found, err := unstructured.NestedFieldNoCopy(persistedRaw.Object, "spec", "jobManager", "resources", "requests", "memory")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(found).To(gomega.BeTrue())
		gomega.Expect(jmMem).To(gomega.Equal(int64(1000000000)))

		tmMem, found, err := unstructured.NestedFieldNoCopy(persistedRaw.Object, "spec", "taskManager", "resources", "requests", "memory")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(found).To(gomega.BeTrue())
		gomega.Expect(tmMem).To(gomega.Equal("1024000"))

		ginkgo.By("rejecting fractional BinarySI memory literal on UPDATE while suspended")
		gomega.Expect(unstructured.SetNestedField(
			persistedRaw.Object,
			"1.5Gi",
			"spec", "jobManager", "resources", "requests", "memory",
		)).To(gomega.Succeed())
		err = k8sClient.Update(ctx, persistedRaw)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("must not contain a decimal point"))
	})

	ginkgo.It("should allow unmanaged FlinkDeployments with Flink MemorySize 2g in component resources without breaking typed LIST while rejecting managed 2g", func() {
		unmanagedRaw := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "flink.apache.org/v1beta1",
				"kind":       "FlinkDeployment",
				"metadata": map[string]any{
					"name":      "unmanaged-2g-session",
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
					"jobManager": map[string]any{
						"resources": map[string]any{
							"requests": map[string]any{
								"cpu":    "1",
								"memory": "2g",
							},
						},
					},
					"taskManager": map[string]any{
						"resources": map[string]any{
							"requests": map[string]any{
								"cpu":    "1",
								"memory": "2GiB",
							},
						},
					},
				},
			},
		}
		behavioral.MustCreate(ctx, k8sClient, unmanagedRaw)

		lookupKey := types.NamespacedName{Name: "unmanaged-2g-session", Namespace: ns.Name}
		persistedRaw := &unstructured.Unstructured{}
		persistedRaw.SetAPIVersion("flink.apache.org/v1beta1")
		persistedRaw.SetKind("FlinkDeployment")
		gomega.Expect(k8sClient.Get(ctx, lookupKey, persistedRaw)).To(gomega.Succeed())
		jmMem, found, err := unstructured.NestedString(persistedRaw.Object, "spec", "jobManager", "resources", "requests", "memory")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(found).To(gomega.BeTrue())
		gomega.Expect(jmMem).To(gomega.Equal("2g"))

		ginkgo.By("updating the unmanaged FlinkDeployment with 4g memory without webhook decode failure")
		gomega.Expect(unstructured.SetNestedField(
			persistedRaw.Object,
			"4g",
			"spec", "jobManager", "resources", "requests", "memory",
		)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Update(ctx, persistedRaw)).To(gomega.Succeed())

		ginkgo.By("listing typed FlinkDeploymentList without failing JSON unmarshal on 4g / 2GiB")
		typedList := &flinkv1beta1.FlinkDeploymentList{}
		gomega.Expect(k8sClient.List(ctx, typedList, &client.ListOptions{Namespace: ns.Name})).To(gomega.Succeed())
		gomega.Expect(typedList.Items).NotTo(gomega.BeEmpty())

		ginkgo.By("rejecting 2g memory in component resources on a Kueue-managed FlinkDeployment")
		managedRaw := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "flink.apache.org/v1beta1",
				"kind":       "FlinkDeployment",
				"metadata": map[string]any{
					"name":      "managed-2g-app",
					"namespace": ns.Name,
					"labels": map[string]any{
						controllerconstants.QueueLabel: "local-queue",
					},
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
					"jobManager": map[string]any{
						"resources": map[string]any{
							"requests": map[string]any{
								"cpu":    "1",
								"memory": "2g",
							},
						},
					},
					"taskManager": map[string]any{
						"resources": map[string]any{
							"requests": map[string]any{
								"cpu":    "1",
								"memory": "2Gi",
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
		err = k8sClient.Create(ctx, managedRaw)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("must use a BinarySI suffix"))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("spec.flinkConfiguration"))

		ginkgo.By("rejecting 2g memory inside spec.podTemplate on a Kueue-managed FlinkDeployment")
		managedPodTemplate2g := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "flink.apache.org/v1beta1",
				"kind":       "FlinkDeployment",
				"metadata": map[string]any{
					"name":      "managed-pt-2g-app",
					"namespace": ns.Name,
					"labels": map[string]any{
						controllerconstants.QueueLabel: "local-queue",
					},
				},
				"spec": map[string]any{
					"image":        "flink:1.20",
					"flinkVersion": "v1_20",
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
					"jobManager": map[string]any{
						"resource": map[string]any{"memory": "1Gi"},
					},
					"taskManager": map[string]any{
						"resource": map[string]any{"memory": "2Gi"},
					},
					"job": map[string]any{
						"jarURI":      "local:///opt/flink/examples/streaming/StateMachineExample.jar",
						"parallelism": int64(1),
						"state":       "running",
					},
				},
			},
		}
		err = k8sClient.Create(ctx, managedPodTemplate2g)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("decoding base podTemplate"))

		ginkgo.By("rejecting whitespace-padded non-memory quantities and nested pipeline.jobvertex-parallelism-overrides in native mode")
		invalidNestedAndWSRaw := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "flink.apache.org/v1beta1",
				"kind":       "FlinkDeployment",
				"metadata": map[string]any{
					"name":      "managed-ws-and-subkey",
					"namespace": ns.Name,
					"labels": map[string]any{
						controllerconstants.QueueLabel: "local-queue",
					},
				},
				"spec": map[string]any{
					"image":        "flink:1.20",
					"flinkVersion": "v1_20",
					"flinkConfiguration": map[string]any{
						"pipeline": map[string]any{
							"jobvertex-parallelism-overrides": map[string]any{
								"vertex-a": "32",
							},
						},
					},
					"jobManager": map[string]any{
						"resource": map[string]any{
							"memory":           "1Gi",
							"ephemeralStorage": " 2Gi",
						},
					},
					"taskManager": map[string]any{
						"resources": map[string]any{
							"requests": map[string]any{
								"cpu":    " 500m",
								"memory": "2Gi",
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
		err = k8sClient.Create(ctx, invalidNestedAndWSRaw)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("pipeline.jobvertex-parallelism-overrides.vertex-a"))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("must not contain whitespace"))

		ginkgo.By("rejecting flinkConfiguration pod-identity, image, and hostNetwork decorator keys")
		invalidIdentityCfg := testingflinkdeployment.MakeFlinkDeployment("invalid-identity-cfg", ns.Name).
			Queue("local-queue").
			FlinkConfiguration(map[string]string{
				"kubernetes.service-account":     "cluster-admin-sa",
				"kubernetes.container.image.ref": "attacker/flink:latest",
				"kubernetes.hostnetwork.enabled": "true",
			}).
			Obj()
		err = k8sClient.Create(ctx, invalidIdentityCfg)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("kubernetes.service-account"))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("kubernetes.container.image.ref"))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("kubernetes.hostnetwork.enabled"))

		ginkgo.By("rejecting standalone missing spec.image, invalid imagePullPolicy, missing sidecar image, and invalid podTemplate metadata/placement")
		standaloneMissingImage := testingflinkdeployment.MakeFlinkDeployment("standalone-missing-img", ns.Name).
			Queue("local-queue").
			Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
			Image("").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "flink-main-container", Image: "flink:1.20"},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, standaloneMissingImage)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("spec.image"))

		invalidPullPolicy := testingflinkdeployment.MakeFlinkDeployment("invalid-pull-policy", ns.Name).
			Queue("local-queue").
			ImagePullPolicy("always").
			Obj()
		err = k8sClient.Create(ctx, invalidPullPolicy)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("spec.imagePullPolicy"))

		missingSidecarImg := testingflinkdeployment.MakeFlinkDeployment("missing-sidecar-img", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "sidecar"},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, missingSidecarImg)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(`container "sidecar": image must be specified`))

		invalidPodLabel := testingflinkdeployment.MakeFlinkDeployment("invalid-pod-label", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Labels: map[string]string{"bad-label": "not valid!"},
			}).
			Obj()
		err = k8sClient.Create(ctx, invalidPodLabel)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("pod labels"))

		invalidToleration := testingflinkdeployment.MakeFlinkDeployment("invalid-toleration", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Tolerations: []corev1.Toleration{
						{Key: "", Operator: corev1.TolerationOpEqual, Value: "gpu"},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, invalidToleration)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("operator must be Exists when key is empty"))

		ginkgo.By("rejecting unnamed containers and duplicate x-kubernetes-list-map-keys in merged podTemplates")
		unnamedContainer := testingflinkdeployment.MakeFlinkDeployment("unnamed-container", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Image: "busybox:1.36"},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, unnamedContainer)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring(`containers[0].name ""`))

		dupPortAfterDefault := testingflinkdeployment.MakeFlinkDeployment("dup-port-default", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "flink-main-container",
							Ports: []corev1.ContainerPort{{ContainerPort: 8080}, {ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
						},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, dupPortAfterDefault)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("has duplicate port 8080/TCP"))

		ginkgo.By("rejecting missing Workload-required volume and nodeAffinity fields on Kueue-managed FlinkDeployments")
		missingNodeSelectorTerms := testingflinkdeployment.MakeFlinkDeployment("missing-nst", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{
						NodeAffinity: &corev1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{},
						},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, missingNodeSelectorTerms)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("nodeSelectorTerms must not be empty"))

		missingRBDMonitors := testingflinkdeployment.MakeFlinkDeployment("missing-rbd-monitors", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{Name: "rbd", RBD: &corev1.RBDVolumeSource{RBDImage: "img"}},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, missingRBDMonitors)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("volumes[0].rbd.monitors must not be empty"))

		missingCephMonitors := testingflinkdeployment.MakeFlinkDeployment("missing-ceph-monitors", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{Name: "ceph", CephFS: &corev1.CephFSVolumeSource{Path: "/mnt"}},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, missingCephMonitors)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("volumes[0].cephfs.monitors must not be empty"))

		missingScaleIOSecret := testingflinkdeployment.MakeFlinkDeployment("missing-sio-secret", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{Name: "sio", ScaleIO: &corev1.ScaleIOVolumeSource{Gateway: "https://gw", System: "sys"}},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, missingScaleIOSecret)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("volumes[0].scaleIO.secretRef is required"))

		missingPodCertSigner := testingflinkdeployment.MakeFlinkDeployment("missing-podcert-signer", ns.Name).
			Queue("local-queue").
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "proj",
							Projected: &corev1.ProjectedVolumeSource{
								Sources: []corev1.VolumeProjection{
									{PodCertificate: &corev1.PodCertificateProjection{KeyType: "ECDSAP256"}},
								},
							},
						},
					},
				},
			}).
			Obj()
		err = k8sClient.Create(ctx, missingPodCertSigner)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("volumes[0].projected.sources[0].podCertificate.signerName must not be empty"))
	})

	ginkgo.It("should enforce spec and label immutability on terminating FlinkDeployment with SkipAncestorCheckForDeletedWorkloads enabled while allowing finalizer removal", func() {
		const finalizerName = "flinkdeployments.flink.apache.org/finalizer"
		terminating := testingflinkdeployment.MakeFlinkDeployment("terminating-app", ns.Name).
			Queue("local-queue").
			WorkloadPriorityClass("high-prio").
			Obj()
		terminating.Finalizers = []string{finalizerName}
		behavioral.MustCreate(ctx, k8sClient, terminating)

		lookupKey := types.NamespacedName{Name: terminating.Name, Namespace: terminating.Namespace}
		gomega.Expect(k8sClient.Delete(ctx, terminating)).To(gomega.Succeed())

		live := &flinkv1beta1.FlinkDeployment{}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, live)).To(gomega.Succeed())
			g.Expect(live.DeletionTimestamp).NotTo(gomega.BeNil())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		ginkgo.By("rejecting spec and priority-class label mutations while deletionTimestamp is set")
		mutated := live.DeepCopy()
		mutated.Spec.Job.State = flinkv1beta1.JobStateRunning
		mutated.Spec.TaskManager.Replicas = new(int64(4))
		delete(mutated.Labels, controllerconstants.WorkloadPriorityClassLabel)
		err := k8sClient.Update(ctx, mutated)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err).To(utiltesting.BeForbiddenError())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("WorkloadPriorityClass cannot be removed from a workload"))
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("field is immutable when FlinkDeployment is being deleted"))

		ginkgo.By("allowing finalizer removal so deletion completes")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, live)).To(gomega.Succeed())
			live.Finalizers = nil
			g.Expect(k8sClient.Update(ctx, live)).To(gomega.Succeed())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(k8sClient.Get(ctx, lookupKey, &flinkv1beta1.FlinkDeployment{})).To(utiltesting.BeNotFoundError())
		}, behavioral.Timeout, behavioral.Interval).Should(gomega.Succeed())
	})
})
