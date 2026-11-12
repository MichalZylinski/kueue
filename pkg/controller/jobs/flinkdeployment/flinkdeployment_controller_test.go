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
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	"sigs.k8s.io/kueue/pkg/util/equality"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingflinkdeployment "sigs.k8s.io/kueue/pkg/util/testingjobs/flinkdeployment"
)

func TestPodSets(t *testing.T) {
	jobWithRawFlinkConfig := func(rawCfg string) *flinkv1beta1.FlinkDeployment {
		fd := testingflinkdeployment.MakeFlinkDeployment("strict-cfg", "ns").Obj()
		if err := json.Unmarshal([]byte(rawCfg), &fd.Spec.FlinkConfiguration); err != nil {
			t.Fatalf("json.Unmarshal(%s) error: %v", rawCfg, err)
		}
		return fd
	}

	testCases := map[string]struct {
		job           *flinkv1beta1.FlinkDeployment
		enableTAS     bool
		wantPodSets   []kueue.PodSet
		wantErrSubstr string
	}{
		"default replicas and Flink MemorySize plus Kubernetes Quantity parsing": {
			job: testingflinkdeployment.MakeFlinkDeployment("flink-job", "ns").
				ServiceAccount("flink").
				JobManagerResource(new(1.0), "1024m", "2Gi").
				TaskManagerResource(new(2.0), "2Gi", "").
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "flink",
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:              resource.MustParse("1"),
											corev1.ResourceMemory:           resource.MustParse("1Gi"),
											corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:              resource.MustParse("1"),
											corev1.ResourceMemory:           resource.MustParse("1Gi"),
											corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
										},
									},
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "flink",
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("2Gi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("2Gi"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"omitted CPU defaults to 1 core on JobManager and numberOfTaskSlots cores on TaskManager in native mode": {
			job: testingflinkdeployment.MakeFlinkDeployment("flink-default-cpu", "ns").
				Parallelism(5).
				FlinkConfig("taskmanager.numberOfTaskSlots", "2").
				JobManagerResource(nil, "1024m", "").
				TaskManagerResource(nil, "4096m", "").
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 3,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("4Gi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("4Gi"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"standalone mode rejects omitted TaskManager CPU instead of fabricating a default": {
			job: testingflinkdeployment.MakeFlinkDeployment("flink-standalone-no-tm-cpu", "ns").
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				JobManagerResource(new(1.0), "1024m", "").
				TaskManagerResource(nil, "2048m", "").
				TaskManagerReplicas(2).
				Obj(),
			wantErrSubstr: "taskManager cpu must be specified via taskManager.resource.cpu or taskManager.resources when mode is standalone",
		},
		"standalone mode defaults omitted JobManager CPU to 1 core over podTemplate main-container CPU and uses explicit TaskManager CPU": {
			job: testingflinkdeployment.MakeFlinkDeployment("flink-standalone-cpu", "ns").
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				FlinkConfig("taskmanager.numberOfTaskSlots", "4").
				JobManagerResource(nil, "1024m", "").
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										corev1.ResourceCPU: resource.MustParse("500m"),
									},
								},
							},
						},
					},
				}).
				TaskManagerResource(new(2.0), "2048m", "").
				TaskManagerReplicas(2).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 2,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "default",
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.20",
									ImagePullPolicy: corev1.PullIfNotPresent,
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("2Gi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("2Gi"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"job.parallelism with numberOfTaskSlots, ImagePullPolicy, ServiceAccount, ephemeralStorage, initContainers, and PodSpec.Resources limits": {
			job: testingflinkdeployment.MakeFlinkDeployment("extra-res", "ns").
				ImagePullPolicy(corev1.PullIfNotPresent).
				ServiceAccount("flink").
				Parallelism(6).
				JobManagerResource(new(0.5), "1g", "10Gi").
				TaskManagerResource(new(1.5), "2g", "20Gi").
				FlinkConfig("taskmanager.numberOfTaskSlots", "2").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Resources: &corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("4"),
								corev1.ResourceMemory: resource.MustParse("8Gi"),
							},
						},
						InitContainers: []corev1.Container{
							{
								Name:  "fetch-artifacts",
								Image: "curlimages/curl:8.10.1",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										corev1.ResourceCPU: resource.MustParse("250m"),
									},
								},
							},
						},
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "flink",
							Resources: &corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("4"),
									corev1.ResourceMemory: resource.MustParse("8Gi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("4"),
									corev1.ResourceMemory: resource.MustParse("8Gi"),
								},
							},
							InitContainers: []corev1.Container{
								{
									Name:  "fetch-artifacts",
									Image: "curlimages/curl:8.10.1",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse("250m"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse("250m"),
										},
									},
								},
							},
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.20",
									ImagePullPolicy: corev1.PullIfNotPresent,
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:              resource.MustParse("500m"),
											corev1.ResourceMemory:           resource.MustParse("1Gi"),
											corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:              resource.MustParse("500m"),
											corev1.ResourceMemory:           resource.MustParse("1Gi"),
											corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
										},
									},
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 3,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "flink",
							Resources: &corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("4"),
									corev1.ResourceMemory: resource.MustParse("8Gi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("4"),
									corev1.ResourceMemory: resource.MustParse("8Gi"),
								},
							},
							InitContainers: []corev1.Container{
								{
									Name:  "fetch-artifacts",
									Image: "curlimages/curl:8.10.1",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse("250m"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU: resource.MustParse("250m"),
										},
									},
								},
							},
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.20",
									ImagePullPolicy: corev1.PullIfNotPresent,
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:              resource.MustParse("1500m"),
											corev1.ResourceMemory:           resource.MustParse("2Gi"),
											corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:              resource.MustParse("1500m"),
											corev1.ResourceMemory:           resource.MustParse("2Gi"),
											corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"by-name container merge when merge-arrays-by-name is true with distinct per-role TAS topology modes": {
			job: testingflinkdeployment.MakeFlinkDeployment("flink-by-name", "ns").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "true").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels: map[string]string{"env": "prod"},
					Spec: corev1.PodSpec{
						NodeSelector:   map[string]string{"disk": "nvme"},
						ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu-claim"}},
						Containers: []corev1.Container{
							{
								Name:  "metrics-sidecar",
								Image: "prom/exporter:v1",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("128Mi"),
									},
								},
							},
						},
					},
				}).
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetUnconstrainedTopologyAnnotation: "true",
					},
				}).
				TaskManagerReplicas(2).
				TaskManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("4"),
						corev1.ResourceMemory: resource.MustParse("8Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:                    resource.MustParse("4"),
						corev1.ResourceMemory:                 resource.MustParse("8Gi"),
						corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Labels: map[string]string{"role": "tm"},
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
						kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodSetSliceSizeAnnotation:             "2",
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Resources: corev1.ResourceRequirements{
									Claims: []corev1.ResourceClaim{{Name: "gpu-claim"}},
								},
							},
						},
					},
				}).
				Obj(),
			enableTAS: true,
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					TopologyRequest: &kueue.PodSetTopologyRequest{
						Unconstrained: new(true),
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels:      map[string]string{"env": "prod"},
							Annotations: map[string]string{kueue.PodSetUnconstrainedTopologyAnnotation: "true"},
						},
						Spec: corev1.PodSpec{
							NodeSelector:   map[string]string{"disk": "nvme"},
							ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu-claim"}},
							Containers: []corev1.Container{
								{
									Name:  "metrics-sidecar",
									Image: "prom/exporter:v1",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("128Mi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("128Mi"),
										},
									},
								},
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 2,
					TopologyRequest: &kueue.PodSetTopologyRequest{
						Required:                    new("cloud.provider.com/rack"),
						PodSetSliceRequiredTopology: new("cloud.provider.com/rack"),
						PodSetSliceSize:             new(int32(2)),
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"env": "prod", "role": "tm"},
							Annotations: map[string]string{
								kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
								kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
								kueue.PodSetSliceSizeAnnotation:             "2",
							},
						},
						Spec: corev1.PodSpec{
							NodeSelector:   map[string]string{"disk": "nvme"},
							ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu-claim"}},
							Containers: []corev1.Container{
								{
									Name:  "metrics-sidecar",
									Image: "prom/exporter:v1",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("128Mi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("100m"),
											corev1.ResourceMemory: resource.MustParse("128Mi"),
										},
									},
								},
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:                    resource.MustParse("4"),
											corev1.ResourceMemory:                 resource.MustParse("8Gi"),
											corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:                    resource.MustParse("4"),
											corev1.ResourceMemory:                 resource.MustParse("8Gi"),
											corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
										},
										Claims: []corev1.ResourceClaim{{Name: "gpu-claim"}},
									},
								},
							},
						},
					},
				},
			},
		},
		"positional container merge when merge-arrays-by-name is false": {
			job: testingflinkdeployment.MakeFlinkDeployment("flink-positional", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "sidecar-a",
								Image: "busybox:1",
							},
						},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "sidecar-b",
								Image: "busybox:2",
							},
						},
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "sidecar-a", Image: "busybox:1"},
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "sidecar-b", Image: "busybox:2"},
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
			},
		},
		"invalid memory format returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-mem", "ns").
				TaskManagerResource(new(1.0), "2xyz", "").
				Obj(),
			wantErrSubstr: "invalid memory quantity",
		},
		"invalid replicas returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-replicas", "ns").
				JobManagerReplicas(0).
				Obj(),
			wantErrSubstr: "jobManager.replicas must be between 1 and 2147483647, got 0",
		},
		"component resources requests replace inherited podTemplate limits and backfill limits from requests": {
			job: testingflinkdeployment.MakeFlinkDeployment("cross-layer-req-gt-lim", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										corev1.ResourceMemory: resource.MustParse("1Gi"),
									},
								},
							},
						},
					},
				}).
				TaskManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("1"),
											corev1.ResourceMemory: resource.MustParse("4Gi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("1"),
											corev1.ResourceMemory: resource.MustParse("4Gi"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"component resources limits replace inherited podTemplate requests and backfill requests from limits": {
			job: testingflinkdeployment.MakeFlinkDeployment("cross-layer-limits-only", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("1m"),
										corev1.ResourceMemory: resource.MustParse("1"),
									},
								},
							},
						},
					},
				}).
				TaskManagerResources(corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("16"),
						corev1.ResourceMemory: resource.MustParse("64Gi"),
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("16"),
											corev1.ResourceMemory: resource.MustParse("64Gi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("16"),
											corev1.ResourceMemory: resource.MustParse("64Gi"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"Flink MemorySize mebibyte suffix in component resources returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("flink-unit-resources", "ns").
				TaskManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("2048m"),
					},
				}).
				Obj(),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi)",
		},
		"fractional BinarySI memory literal in component resources returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("fractional-resources", "ns").
				TaskManagerRawResources(flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("1.5Gi"),
					},
				}).
				Obj(),
			wantErrSubstr: "must not contain a decimal point",
		},
		"plain integer byte literal in component resources returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("plain-bytes-resources", "ns").
				JobManagerRawResources(flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("1000000000"),
					},
				}).
				Obj(),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi)",
		},
		"BinarySI Mi and Pi in component resources succeed": {
			job: testingflinkdeployment.MakeFlinkDeployment("binarysi-mi-and-pi", "ns").
				JobManagerRawResources(flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceCPU:    testingflinkdeployment.MustParseQuantity("1"),
						corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("1024Mi"),
					},
				}).
				TaskManagerRawResources(flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceCPU:    testingflinkdeployment.MustParseQuantity("2"),
						corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("1Pi"),
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("1"),
											corev1.ResourceMemory: resource.MustParse("1024Mi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("1"),
											corev1.ResourceMemory: resource.MustParse("1024Mi"),
										},
									},
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("1Pi"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("2"),
											corev1.ResourceMemory: resource.MustParse("1Pi"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"DecimalSI G suffix in component resources limits returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("decimal-unit-resources", "ns").
				TaskManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("4G"),
					},
				}).
				Obj(),
			wantErrSubstr: "resource limit memory (4G): must use a BinarySI suffix",
		},
		"reserved pods resource in podTemplate container returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("reserved-pods", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "sidecar",
								Image: "busybox:latest",
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourcePods: resource.MustParse("1"),
									},
								},
							},
						},
					},
				}).
				Obj(),
			wantErrSubstr: `resource "pods" is reserved for internal Kueue use`,
		},
		"negative pod overhead returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("neg-overhead", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Overhead: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("-1Gi"),
						},
					},
				}).
				Obj(),
			wantErrSubstr: "overhead resource memory (-1Gi) must be non-negative",
		},
		"non-prefix role toleration from positional merge returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("corrupted-tol", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations: []corev1.Toleration{
							{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "gpu", Effect: corev1.TaintEffectNoSchedule},
						},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations: []corev1.Toleration{
							{Key: "spot", Operator: corev1.TolerationOpExists},
						},
					},
				}).
				Obj(),
			wantErrSubstr: "must include spec.podTemplate.spec.tolerations as a prefix",
		},
		"omitted job.parallelism without taskManager.replicas returns error": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("missing-parallelism", "ns").Obj()
				fd.Spec.Job.Parallelism = nil
				return fd
			}(),
			wantErrSubstr: "job.parallelism must be specified when taskManager.replicas is not set",
		},
		"zero CPU in component resources returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("zero-cpu", "ns").
				TaskManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("0"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				}).
				Obj(),
			wantErrSubstr: "cpu (0) must be greater than 0",
		},
		"sub-byte memory in component resources returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("sub-byte-mem", "ns").
				TaskManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1m"),
					},
				}).
				Obj(),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi)",
		},
		"top-level spec.image, spec.imagePullPolicy, and spec.serviceAccount overwrite podTemplate values": {
			job: testingflinkdeployment.MakeFlinkDeployment("image-and-sa-precedence", "ns").
				Image("flink:1.20-override").
				ImagePullPolicy(corev1.PullAlways).
				ServiceAccount("top-level-sa").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						DeprecatedServiceAccount: "legacy-podtemplate-sa",
						ServiceAccountName:       "podtemplate-sa",
						Containers: []corev1.Container{
							{
								Name:            flinkMainContainerName,
								Image:           "flink:1.19-base",
								ImagePullPolicy: corev1.PullNever,
							},
						},
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "top-level-sa",
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.20-override",
									ImagePullPolicy: corev1.PullAlways,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "top-level-sa",
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.20-override",
									ImagePullPolicy: corev1.PullAlways,
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
								},
							},
						},
					},
				},
			},
		},
		"omitted spec.serviceAccount falls back from podTemplate deprecated serviceAccount to serviceAccountName in native mode": {
			job: testingflinkdeployment.MakeFlinkDeployment("native-sa-fallback", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ServiceAccountName: "root-sa",
					},
				}).
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						DeprecatedServiceAccount: "jm-deprecated-sa",
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "jm-deprecated-sa",
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "root-sa",
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
			},
		},
		"standalone mode with omitted spec.serviceAccount honors podTemplate serviceAccount on JobManager and uses default on TaskManager": {
			job: testingflinkdeployment.MakeFlinkDeployment("standalone-sa-fallback", "ns").
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						DeprecatedServiceAccount: "legacy-sa",
						ServiceAccountName:       "podtemplate-sa",
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "legacy-sa",
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "default",
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.20",
									ImagePullPolicy: corev1.PullIfNotPresent,
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
								},
							},
						},
					},
				},
			},
		},
		"whitespace-only spec.image and spec.imagePullPolicy preserve podTemplate flink-main-container values": {
			job: testingflinkdeployment.MakeFlinkDeployment("blank-image-fallback", "ns").
				Image("   ").
				ImagePullPolicy("  ").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:            flinkMainContainerName,
								Image:           "flink:1.19-from-template",
								ImagePullPolicy: corev1.PullIfNotPresent,
							},
						},
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.19-from-template",
									ImagePullPolicy: corev1.PullIfNotPresent,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.19-from-template",
									ImagePullPolicy: corev1.PullIfNotPresent,
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
								},
							},
						},
					},
				},
			},
		},
		"merge-arrays-by-name falls back to positional merge when container override omits name and avoids phantom container": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("unnamed-container-override", "ns").
					FlinkConfig(flinkConfigPodTemplateMergeArraysByName, "true").
					PodTemplate(&corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							NodeSelector: map[string]string{
								"node-role.kubernetes.io/worker": "",
							},
							Volumes: []corev1.Volume{
								{
									Name:     "scratch",
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
							Containers: []corev1.Container{
								{
									Name: flinkMainContainerName,
									Ports: []corev1.ContainerPort{
										{Name: "metrics", ContainerPort: 9249},
									},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
										},
									},
								},
							},
						},
					}).
					Obj()
				if err := json.Unmarshal(
					[]byte(`{"spec":{"containers":[{"ports":[{"containerPort":9250}],"resources":{"requests":{"nvidia.com/gpu":"4"},"limits":{"nvidia.com/gpu":"4"}}}]}}`),
					&fd.Spec.TaskManager.PodTemplate,
				); err != nil {
					t.Fatalf("json.Unmarshal(tm podTemplate) error: %v", err)
				}
				return fd
			}(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							NodeSelector: map[string]string{
								"node-role.kubernetes.io/worker": "",
							},
							Volumes: []corev1.Volume{
								{
									Name:     "scratch",
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Ports: []corev1.ContainerPort{
										{Name: "metrics", ContainerPort: 9249, Protocol: corev1.ProtocolTCP},
									},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:                    resource.MustParse("1"),
											corev1.ResourceMemory:                 resource.MustParse("1Gi"),
											corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:                    resource.MustParse("1"),
											corev1.ResourceMemory:                 resource.MustParse("1Gi"),
											corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
										},
									},
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							NodeSelector: map[string]string{
								"node-role.kubernetes.io/worker": "",
							},
							Volumes: []corev1.Volume{
								{
									Name:     "scratch",
									EmptyDir: &corev1.EmptyDirVolumeSource{},
								},
							},
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									Ports: []corev1.ContainerPort{
										{Name: "metrics", ContainerPort: 9250, Protocol: corev1.ProtocolTCP},
									},
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:                    resource.MustParse("1"),
											corev1.ResourceMemory:                 resource.MustParse("2Gi"),
											corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("4"),
										},
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:                    resource.MustParse("1"),
											corev1.ResourceMemory:                 resource.MustParse("2Gi"),
											corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("4"),
										},
									},
								},
							},
						},
					},
				},
			},
		},
		"empty string taskmanager.numberOfTaskSlots returns error": {
			job:           jobWithRawFlinkConfig(`{"taskmanager.numberOfTaskSlots": ""}`),
			wantErrSubstr: `invalid taskmanager.numberOfTaskSlots ""`,
		},
		"whitespace taskmanager.numberOfTaskSlots returns error": {
			job:           jobWithRawFlinkConfig(`{"taskmanager.numberOfTaskSlots": " 4 "}`),
			wantErrSubstr: `invalid taskmanager.numberOfTaskSlots " 4 "`,
		},
		"null taskmanager.numberOfTaskSlots returns error": {
			job:           jobWithRawFlinkConfig(`{"taskmanager.numberOfTaskSlots": null}`),
			wantErrSubstr: `invalid taskmanager.numberOfTaskSlots "null"`,
		},
		"empty string merge-arrays-by-name returns error": {
			job:           jobWithRawFlinkConfig(`{"kubernetes.operator.pod-template.merge-arrays-by-name": ""}`),
			wantErrSubstr: `invalid boolean value ""`,
		},
		"whitespace merge-arrays-by-name returns error": {
			job:           jobWithRawFlinkConfig(`{"kubernetes.operator.pod-template.merge-arrays-by-name": " false "}`),
			wantErrSubstr: `invalid boolean value " false "`,
		},
		"null merge-arrays-by-name returns error": {
			job:           jobWithRawFlinkConfig(`{"kubernetes.operator.pod-template.merge-arrays-by-name": null}`),
			wantErrSubstr: `invalid boolean value "null"`,
		},
		"whitespace-padded ephemeralStorage in component resource returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("ws-ephemeral", "ns").
				JobManagerResource(new(1.0), "1024m", " 2Gi ").
				Obj(),
			wantErrSubstr: `ephemeralStorage " 2Gi " must not contain whitespace`,
		},
		"whitespace-only ephemeralStorage in component resource returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("blank-ephemeral", "ns").
				TaskManagerResource(new(1.0), "2048m", "   ").
				Obj(),
			wantErrSubstr: `ephemeralStorage "   " must not contain whitespace`,
		},
		"whitespace-padded CPU quantity in component resources returns error": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("ws-res-cpu", "ns").Obj()
				fd.Spec.TaskManager.Resource = nil
				if err := json.Unmarshal([]byte(`{"requests":{"cpu":" 500m","memory":"2Gi"}}`), &fd.Spec.TaskManager.Resources); err != nil {
					t.Fatalf("json.Unmarshal(resources) error: %v", err)
				}
				return fd
			}(),
			wantErrSubstr: `resource request cpu (" 500m") must not contain whitespace`,
		},
		"zero taskManager.replicas from /scale subresource produces Count=0 for NoMatchingWorkload detection": {
			job: testingflinkdeployment.MakeFlinkDeployment("scaled-zero", "ns").
				ServiceAccount("flink").
				TaskManagerReplicas(0).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "flink",
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 0,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "flink",
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
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
								},
							},
						},
					},
				},
			},
		},
		"negative taskManager.replicas returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("neg-replicas", "ns").
				TaskManagerReplicas(-1).
				Obj(),
			wantErrSubstr: "taskManager.replicas must be between 0 and 2147483647, got -1",
		},
		"jobManager.replicas above math.MaxInt32 returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("jm-replicas-overflow", "ns").
				JobManagerReplicas(int64(math.MaxInt32) + 1).
				Obj(),
			wantErrSubstr: "jobManager.replicas must be between 1 and 2147483647, got 2147483648",
		},
		"taskManager.replicas above math.MaxInt32 returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("tm-replicas-overflow", "ns").
				TaskManagerReplicas(int64(math.MaxInt32) + 1).
				Obj(),
			wantErrSubstr: "taskManager.replicas must be between 0 and 2147483647, got 2147483648",
		},
		"job.parallelism above math.MaxInt32 returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("parallelism-overflow", "ns").
				Parallelism(int64(math.MaxInt32) + 1).
				Obj(),
			wantErrSubstr: "job.parallelism must be between 1 and 2147483647, got 2147483648",
		},
		"deferred base podTemplate decode error (Flink 2g memory in container) returns error": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("bad-base-pt", "ns").Obj()
				if err := json.Unmarshal(
					[]byte(`{"spec":{"containers":[{"name":"sidecar","resources":{"requests":{"memory":"2g"}}}]}}`),
					&fd.Spec.PodTemplate,
				); err != nil {
					t.Fatalf("json.Unmarshal(base podTemplate) error: %v", err)
				}
				return fd
			}(),
			wantErrSubstr: "decoding base podTemplate",
		},
		"deferred component podTemplate decode error with nil base podTemplate returns error": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("bad-role-pt", "ns").Obj()
				if err := json.Unmarshal(
					[]byte(`{"spec":{"containers":[{"name":"sidecar","resources":{"requests":{"memory":"2g"}}}]}}`),
					&fd.Spec.TaskManager.PodTemplate,
				); err != nil {
					t.Fatalf("json.Unmarshal(role podTemplate) error: %v", err)
				}
				return fd
			}(),
			wantErrSubstr: "decoding component podTemplate",
		},
		"deferred non-object flinkConfiguration decode error returns error": {
			job:           jobWithRawFlinkConfig(`["not", "a", "map"]`),
			wantErrSubstr: "cannot unmarshal array into Go value of type map[string]",
		},
		"omitted jobManager memory returns error instead of 0-memory PodSet": {
			job: testingflinkdeployment.MakeFlinkDeployment("no-jm-mem", "ns").
				JobManagerResource(new(1.0), "", "").
				Obj(),
			wantErrSubstr: "spec.job and explicit jobManager and taskManager memory are required",
		},
		"omitted taskManager memory returns error instead of 0-memory PodSet": {
			job: testingflinkdeployment.MakeFlinkDeployment("no-tm-mem", "ns").
				TaskManagerResource(new(1.0), "", "").
				Obj(),
			wantErrSubstr: "spec.job and explicit jobManager and taskManager memory are required",
		},
		"forbidden flinkConfiguration memory sizing key returns error": {
			job:           jobWithRawFlinkConfig(`{"taskmanager.memory.process.size": "4096m"}`),
			wantErrSubstr: "configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
		},
		"forbidden flinkConfiguration autoscaler key returns error": {
			job:           jobWithRawFlinkConfig(`{"job.autoscaler.enabled": "true"}`),
			wantErrSubstr: "dynamic Flink autoscaling is not supported",
		},
		"partial JSON role podTemplate preserves empty HTTPHeader.Value and Sysctl.Value overrides": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("empty-header-sysctl", "ns").
					ServiceAccount("flink").
					PodTemplate(&corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							SecurityContext: &corev1.PodSecurityContext{
								Sysctls: []corev1.Sysctl{
									{Name: "kernel.shm_rmid_forced", Value: "1"},
									{Name: "net.core.somaxconn", Value: "1024"},
								},
							},
							Containers: []corev1.Container{
								{
									Name: flinkMainContainerName,
									LivenessProbe: &corev1.Probe{
										HTTPGet: &corev1.HTTPGetAction{
											Path: "/healthz",
											Port: intstr.FromInt32(8081),
											HTTPHeaders: []corev1.HTTPHeader{
												{Name: "X-Custom", Value: "from-base"},
												{Name: "X-Keep", Value: "inherited"},
											},
										},
									},
								},
							},
						},
					}).
					Obj()
				if err := json.Unmarshal(
					[]byte(
						`{"spec":{"securityContext":{"sysctls":[{"name":"kernel.shm_rmid_forced","value":""},{}]},"containers":[{"livenessProbe":{"httpGet":{"httpHeaders":[{"name":"X-Custom","value":""},{}]}}}]}}`,
					),
					&fd.Spec.TaskManager.PodTemplate,
				); err != nil {
					t.Fatalf("json.Unmarshal(tm podTemplate) error: %v", err)
				}
				return fd
			}(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "flink",
							SecurityContext: &corev1.PodSecurityContext{
								Sysctls: []corev1.Sysctl{
									{Name: "kernel.shm_rmid_forced", Value: "1"},
									{Name: "net.core.somaxconn", Value: "1024"},
								},
							},
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									LivenessProbe: &corev1.Probe{
										HTTPGet: &corev1.HTTPGetAction{
											Path: "/healthz",
											Port: intstr.FromInt32(8081),
											HTTPHeaders: []corev1.HTTPHeader{
												{Name: "X-Custom", Value: "from-base"},
												{Name: "X-Keep", Value: "inherited"},
											},
										},
									},
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "flink",
							SecurityContext: &corev1.PodSecurityContext{
								Sysctls: []corev1.Sysctl{
									{Name: "kernel.shm_rmid_forced", Value: ""},
									{Name: "net.core.somaxconn", Value: "1024"},
								},
							},
							Containers: []corev1.Container{
								{
									Name:  flinkMainContainerName,
									Image: "flink:1.20",
									LivenessProbe: &corev1.Probe{
										HTTPGet: &corev1.HTTPGetAction{
											Path: "/healthz",
											Port: intstr.FromInt32(8081),
											HTTPHeaders: []corev1.HTTPHeader{
												{Name: "X-Custom", Value: ""},
												{Name: "X-Keep", Value: "inherited"},
											},
										},
									},
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
								},
							},
						},
					},
				},
			},
		},
		"standalone mode with omitted spec.imagePullPolicy preserves podTemplate imagePullPolicy on JobManager and overwrites TaskManager with IfNotPresent": {
			job: testingflinkdeployment.MakeFlinkDeployment("standalone-pull-policy-override", "ns").
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:            flinkMainContainerName,
								Image:           "flink:1.19-ignored-on-tm",
								ImagePullPolicy: corev1.PullAlways,
							},
						},
					},
				}).
				Obj(),
			wantPodSets: []kueue.PodSet{
				{
					Name:  jobManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.20",
									ImagePullPolicy: corev1.PullAlways,
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
								},
							},
						},
					},
				},
				{
					Name:  taskManagerPodSetName,
					Count: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: "default",
							Containers: []corev1.Container{
								{
									Name:            flinkMainContainerName,
									Image:           "flink:1.20",
									ImagePullPolicy: corev1.PullIfNotPresent,
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
								},
							},
						},
					},
				},
			},
		},
		"standalone mode rejects omitted spec.image even when podTemplate sets flink-main-container image": {
			job: testingflinkdeployment.MakeFlinkDeployment("standalone-missing-image", "ns").
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				Image("   ").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: flinkMainContainerName, Image: "flink:1.19-from-template"},
						},
					},
				}).
				Obj(),
			wantErrSubstr: "spec.image must be specified when mode is standalone",
		},
		"native mode rejects omitted spec.image when podTemplate omits flink-main-container image": {
			job: testingflinkdeployment.MakeFlinkDeployment("native-missing-image", "ns").
				Image("   ").
				Obj(),
			wantErrSubstr: `container "flink-main-container": image must be specified`,
		},
		"sidecar container omitting image returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("sidecar-missing-image", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "metrics-sidecar"},
						},
					},
				}).
				Obj(),
			wantErrSubstr: `container "metrics-sidecar": image must be specified`,
		},
		"initContainer omitting image returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("init-missing-image", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						InitContainers: []corev1.Container{
							{Name: "init-artifacts", Image: "   "},
						},
					},
				}).
				Obj(),
			wantErrSubstr: `initContainer "init-artifacts": image must be specified`,
		},
		"unsupported spec.imagePullPolicy returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-spec-pull-policy", "ns").
				ImagePullPolicy("always").
				Obj(),
			wantErrSubstr: `unsupported spec.imagePullPolicy "always"`,
		},
		"unsupported container imagePullPolicy returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-sidecar-pull-policy", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "sidecar", Image: "busybox:1.36", ImagePullPolicy: " IfNotPresent"},
						},
					},
				}).
				Obj(),
			wantErrSubstr: `container "sidecar": unsupported imagePullPolicy " IfNotPresent"`,
		},
		"invalid podTemplate label, annotation, and nodeSelector return errors": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-pod-labels", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels: map[string]string{"invalid/key/too/many/slashes": "v"},
				}).
				Obj(),
			wantErrSubstr: "pod labels:",
		},
		"invalid podTemplate nodeSelector returns error": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-pod-nodeselector", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						NodeSelector: map[string]string{"pool": "bad value!"},
					},
				}).
				Obj(),
			wantErrSubstr: "pod nodeSelector:",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, tc.enableTAS)
			ctx, _ := utiltesting.ContextWithLog(t)

			got, err := fromObject(tc.job).PodSets(ctx, nil)
			wireBytes, marshalErr := json.Marshal(tc.job)
			if marshalErr != nil {
				t.Fatalf("json.Marshal(tc.job) error: %v", marshalErr)
			}
			var wireJob flinkv1beta1.FlinkDeployment
			if unmarshalErr := json.Unmarshal(wireBytes, &wireJob); unmarshalErr != nil {
				t.Fatalf("json.Unmarshal(wireBytes) error: %v\njson: %s", unmarshalErr, string(wireBytes))
			}
			wireGot, wireErr := fromObject(&wireJob).PodSets(ctx, nil)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("PodSets() error = %v, want substring %q", err, tc.wantErrSubstr)
				}
				if wireErr == nil || !strings.Contains(wireErr.Error(), tc.wantErrSubstr) {
					t.Fatalf("wire round-trip PodSets() error = %v, want substring %q", wireErr, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("PodSets() unexpected error: %v", err)
			}
			if wireErr != nil {
				t.Fatalf("wire round-trip PodSets() unexpected error: %v\njson: %s", wireErr, string(wireBytes))
			}
			if diff := cmp.Diff(tc.wantPodSets, got); diff != "" {
				t.Errorf("PodSets() mismatch (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantPodSets, wireGot); diff != "" {
				t.Errorf("wire round-trip PodSets() mismatch (-want,+got):\n%s\njson: %s", diff, string(wireBytes))
			}
		})
	}
}

func TestRunAndRestorePodSetsInfoRoundTrip(t *testing.T) {
	rootTol := corev1.Toleration{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "flink", Effect: corev1.TaintEffectNoSchedule}
	rootSecondEqualTol := corev1.Toleration{Key: "tier", Operator: corev1.TolerationOpEqual, Value: "batch", Effect: corev1.TaintEffectNoSchedule}
	roleTol := corev1.Toleration{Key: "gpu-pool", Operator: corev1.TolerationOpEqual, Value: "a100", Effect: corev1.TaintEffectNoSchedule}
	rootGate := corev1.PodSchedulingGate{Name: "example.com/precheck"}
	rootSecondGate := corev1.PodSchedulingGate{Name: "example.com/second-gate"}
	roleGate := corev1.PodSchedulingGate{Name: "example.com/role-gate"}

	taintlessNonTASAssignInfo := []podset.PodSetInfo{
		{
			Name:         jobManagerPodSetName,
			Count:        1,
			Annotations:  map[string]string{kueue.WorkloadAnnotation: "wl-flink"},
			Labels:       map[string]string{"flavor": "on-demand"},
			NodeSelector: map[string]string{"node-pool": "cpu"},
		},
		{
			Name:         taskManagerPodSetName,
			Count:        1,
			Annotations:  map[string]string{kueue.WorkloadAnnotation: "wl-flink"},
			Labels:       map[string]string{"flavor": "gpu"},
			NodeSelector: map[string]string{"node-pool": "gpu-t4"},
		},
	}

	testCases := map[string]struct {
		initial          *flinkv1beta1.FlinkDeployment
		editWhileQueued  func(*flinkv1beta1.FlinkDeployment)
		customAssignInfo []podset.PodSetInfo
		wantRunningTMGs  []corev1.PodSchedulingGate
	}{
		"root podTemplate with empty role podTemplates restores role podTemplates to nil": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-roundtrip", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "streaming"},
					Annotations: map[string]string{"base-ann": "1"},
					Spec: corev1.PodSpec{
						NodeSelector:    map[string]string{"disk": "ssd"},
						Tolerations:     []corev1.Toleration{rootTol},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate},
						Containers: []corev1.Container{
							{Name: "sidecar", Image: "busybox:latest"},
						},
					},
				}).
				UpgradeMode(flinkv1beta1.UpgradeModeSavepoint).
				Obj(),
		},
		"explicitly prefixed user role tolerations and scheduling gates are preserved intact on restore": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-prefixed-role-slices", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations:     []corev1.Toleration{rootTol},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations:     []corev1.Toleration{rootTol, roleTol},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, roleGate},
					},
				}).
				Obj(),
		},
		"non-prefix role schedulingGates with merge-arrays-by-name=true do not clone base schedulingGates into role podTemplate": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-partial-override-by-name", "ns").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "true").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations:     []corev1.Toleration{rootTol, rootSecondEqualTol},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, rootSecondGate},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations:     []corev1.Toleration{rootTol, rootSecondEqualTol, roleTol},
						SchedulingGates: []corev1.PodSchedulingGate{roleGate},
					},
				}).
				Obj(),
			wantRunningTMGs: []corev1.PodSchedulingGate{
				roleGate,
				{Name: kueue.TopologySchedulingGate},
			},
		},
		"explicit by-name [rootGate, roleGate] and matching role map entries are preserved intact on restore": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-prefixed-by-name-and-maps", "ns").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "true").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "streaming", "flavor": "on-demand"},
					Annotations: map[string]string{"base-ann": "1"},
					Spec: corev1.PodSpec{
						NodeSelector:    map[string]string{"disk": "ssd", "node-pool": "cpu"},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "streaming", "role": "tm", "flavor": "gpu"},
					Annotations: map[string]string{"base-ann": "1", "role-ann": "2"},
					Spec: corev1.PodSpec{
						NodeSelector:    map[string]string{"disk": "ssd", "tier": "gpu", "node-pool": "gpu-t4"},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, roleGate},
					},
				}).
				Obj(),
			wantRunningTMGs: []corev1.PodSchedulingGate{
				rootGate,
				roleGate,
				{Name: kueue.TopologySchedulingGate},
			},
		},
		"queued root podTemplate edits (positional) do not leak stale root maps or resurrect deleted schedulingGates on restore": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-root-edit-positional", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "old-team", "obsolete-lbl": "drop"},
					Annotations: map[string]string{"owner": "old-owner", "obsolete-ann": "drop"},
					Spec: corev1.PodSpec{
						NodeSelector:    map[string]string{"disk": "hdd", "obsolete-sel": "drop"},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, rootSecondGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate.Labels = map[string]string{"team": "new-team"}
				fd.Spec.PodTemplate.Annotations = map[string]string{"owner": "new-owner"}
				fd.Spec.PodTemplate.Spec.NodeSelector = map[string]string{"disk": "nvme"}
				fd.Spec.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{rootGate}
			},
		},
		"queued root podTemplate edits (merge-arrays-by-name=true) do not leak stale root maps or resurrect deleted schedulingGates on restore": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-root-edit-by-name", "ns").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "true").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "old-team"},
					Annotations: map[string]string{"owner": "old-owner"},
					Spec: corev1.PodSpec{
						NodeSelector:    map[string]string{"disk": "hdd"},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, rootSecondGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate.Labels = map[string]string{"team": "new-team"}
				fd.Spec.PodTemplate.Annotations = nil
				fd.Spec.PodTemplate.Spec.NodeSelector = nil
				fd.Spec.PodTemplate.Spec.SchedulingGates = nil
			},
		},
		"preserves user role podTemplate apiVersion and kind on RestorePodSetsInfo": {
			initial: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("flink-role-typemeta", "ns").Obj()
				fd.Spec.JobManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{
					APIVersion: "v1",
					Kind:       "Pod",
				}
				return fd
			}(),
		},
		"preserves explicit empty pointer strings, IntOrString(\"\"), and inherited ResourceFieldSelector.Divisor across admission and restore": {
			initial: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("flink-empty-ptrs-and-divisor", "ns").
					PodTemplate(&corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							DNSConfig: &corev1.PodDNSConfig{
								Options: []corev1.PodDNSConfigOption{
									{Name: "ndots", Value: new("")},
								},
							},
							Volumes: []corev1.Volume{
								{
									Name: "local-dir",
									HostPath: &corev1.HostPathVolumeSource{
										Path: "/tmp/flink",
										Type: new(corev1.HostPathUnset),
									},
								},
								{
									Name: "ephemeral-vol",
									Ephemeral: &corev1.EphemeralVolumeSource{
										VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
											Spec: corev1.PersistentVolumeClaimSpec{
												StorageClassName: new("fast-ssd"),
												DataSource: &corev1.TypedLocalObjectReference{
													APIGroup: new(""),
													Kind:     "PersistentVolumeClaim",
													Name:     "seed-pvc",
												},
											},
										},
									},
								},
							},
							Containers: []corev1.Container{
								{
									Name: flinkMainContainerName,
									Env: []corev1.EnvVar{
										{
											Name: "POD_MEM",
											ValueFrom: &corev1.EnvVarSource{
												ResourceFieldRef: &corev1.ResourceFieldSelector{
													Resource: "limits.memory",
													Divisor:  resource.MustParse("1Mi"),
												},
											},
										},
									},
									LivenessProbe: &corev1.Probe{
										GRPC: &corev1.GRPCAction{
											Port:    9090,
											Service: new(""),
										},
									},
									ReadinessProbe: &corev1.Probe{
										HTTPGet: &corev1.HTTPGetAction{
											Path: "/readyz",
											Port: intstr.FromString(""),
										},
									},
								},
							},
						},
					}).
					Obj()
				if err := json.Unmarshal(
					[]byte(
						`{"spec":{"volumes":[{"name":"local-dir"},{"name":"ephemeral-vol","ephemeral":{"volumeClaimTemplate":{"spec":{"storageClassName":""}}}}],"containers":[{"env":[{"valueFrom":{"resourceFieldRef":{"resource":"requests.memory"}}}]}]}}`,
					),
					&fd.Spec.TaskManager.PodTemplate,
				); err != nil {
					t.Fatalf("json.Unmarshal(tm podTemplate) error: %v", err)
				}
				return fd
			}(),
		},
		"raw-JSON zero-value component PodSpec override (hostNetwork: false overriding root hostNetwork: true) survives eviction": {
			initial: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("flink-raw-zero-override", "ns").
					PodTemplate(&corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							HostNetwork: true,
						},
					}).
					Obj()
				fd.Spec.TaskManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
				if err := json.Unmarshal([]byte(`{"spec":{"hostNetwork":false}}`), fd.Spec.TaskManager.PodTemplate); err != nil {
					t.Fatalf("json.Unmarshal(tm podTemplate) error: %v", err)
				}
				return fd
			}(),
		},
		"queued edit setting spec.podTemplate = nil (positional) does not materialize deleted maps or schedulingGates onto role podTemplates": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-nil-root-positional", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "old-team"},
					Annotations: map[string]string{"owner": "old-owner"},
					Spec: corev1.PodSpec{
						NodeSelector:    map[string]string{"disk": "hdd"},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, rootSecondGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = nil
			},
		},
		"queued edit setting spec.podTemplate = nil (merge-arrays-by-name=true) does not materialize deleted maps or schedulingGates onto role podTemplates": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-nil-root-by-name", "ns").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "true").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "old-team"},
					Annotations: map[string]string{"owner": "old-owner"},
					Spec: corev1.PodSpec{
						NodeSelector:    map[string]string{"disk": "hdd"},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, rootSecondGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = nil
			},
		},
		"queued edit clearing role podTemplate maps and schedulingGates while spec.podTemplate is nil does not resurrect deleted entries": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-role-clear-nil-root", "ns").
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "keep-team", "obsolete-lbl": "drop"},
					Annotations: map[string]string{"owner": "keep-owner", "obsolete-ann": "drop"},
					Spec: corev1.PodSpec{
						NodeSelector:    map[string]string{"disk": "nvme", "obsolete-sel": "drop"},
						SchedulingGates: []corev1.PodSchedulingGate{roleGate, rootSecondGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.TaskManager.PodTemplate.Labels = map[string]string{"team": "keep-team"}
				fd.Spec.TaskManager.PodTemplate.Annotations = nil
				fd.Spec.TaskManager.PodTemplate.Spec.NodeSelector = nil
				fd.Spec.TaskManager.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{roleGate}
			},
		},
		"taintless non-TAS admission preserves explicit user role tolerations and positional schedulingGates matching or extending spec.podTemplate": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-taintless-non-tas-matching-role", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations:     []corev1.Toleration{rootTol},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate},
					},
				}).
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations:     []corev1.Toleration{rootTol},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations:     []corev1.Toleration{rootTol, rootSecondEqualTol},
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, rootSecondGate},
					},
				}).
				Obj(),
			customAssignInfo: taintlessNonTASAssignInfo,
			wantRunningTMGs:  []corev1.PodSchedulingGate{rootGate, rootSecondGate},
		},
		"positional schedulingGates admission with info gate already present in baseGates[1] does not overwrite baseGates[0]": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-existing-gate-at-idx1", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, {Name: kueue.TopologySchedulingGate}},
					},
				}).
				Obj(),
		},
		"queued edit removing first schedulingGate on role podTemplate (positional) preserves surviving gate across TAS admission and restore": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-drop-first-role-gate-tas", "ns").
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{roleGate, rootSecondGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.TaskManager.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{rootSecondGate}
			},
			wantRunningTMGs: []corev1.PodSchedulingGate{
				rootSecondGate,
				{Name: kueue.TopologySchedulingGate},
			},
		},
		"queued edit removing first schedulingGate on role podTemplate (positional) preserves surviving gate across non-TAS admission and restore": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-drop-first-role-gate-non-tas", "ns").
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{roleGate, rootSecondGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.TaskManager.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{rootSecondGate}
			},
			customAssignInfo: taintlessNonTASAssignInfo,
			wantRunningTMGs:  []corev1.PodSchedulingGate{rootSecondGate},
		},
		"queued edit removing first schedulingGate on root podTemplate (positional) preserves surviving root gate and restores role podTemplates to nil": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-drop-first-root-gate", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, rootSecondGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{rootSecondGate}
			},
		},
		"queued positional schedulingGates prefix edit ([g1] + [g1, gRole] -> [g2] + [g2, gRole]) preserves updated prefix across TAS admission and restore": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-prefix-gate-tas", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{rootGate},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, roleGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{rootSecondGate}
				fd.Spec.TaskManager.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{rootSecondGate, roleGate}
			},
			wantRunningTMGs: []corev1.PodSchedulingGate{
				rootSecondGate,
				roleGate,
				{Name: kueue.TopologySchedulingGate},
			},
		},
		"queued positional schedulingGates prefix edit ([g1] + [g1, gRole] -> [g2] + [g2, gRole]) preserves updated prefix across non-TAS admission and restore": {
			initial: testingflinkdeployment.MakeFlinkDeployment("flink-queued-prefix-gate-non-tas", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{rootGate},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{rootGate, roleGate},
					},
				}).
				Obj(),
			editWhileQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{rootSecondGate}
				fd.Spec.TaskManager.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{rootSecondGate, roleGate}
			},
			customAssignInfo: taintlessNonTASAssignInfo,
			wantRunningTMGs:  []corev1.PodSchedulingGate{rootSecondGate, roleGate},
		},
	}

	assignInfo := []podset.PodSetInfo{
		{
			Name:            jobManagerPodSetName,
			Count:           1,
			Annotations:     map[string]string{kueue.PodSetUnconstrainedTopologyAnnotation: "true"},
			Labels:          map[string]string{"flavor": "on-demand"},
			NodeSelector:    map[string]string{"node-pool": "cpu"},
			Tolerations:     []corev1.Toleration{{Key: "spot", Operator: corev1.TolerationOpExists}},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}},
		},
		{
			Name:            taskManagerPodSetName,
			Count:           1,
			Annotations:     map[string]string{kueue.PodSetUnconstrainedTopologyAnnotation: "true"},
			Labels:          map[string]string{"flavor": "gpu"},
			NodeSelector:    map[string]string{"node-pool": "gpu-t4"},
			Tolerations:     []corev1.Toleration{{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists}},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, log := utiltesting.ContextWithLog(t)
			activeAssignInfo := assignInfo
			if tc.customAssignInfo != nil {
				activeAssignInfo = tc.customAssignInfo
			}

			wlSnapshotPodSets, err := fromObject(tc.initial.DeepCopy()).PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("wlSnapshotPodSets PodSets() error: %v", err)
			}
			restoreInfo := []podset.PodSetInfo{
				podset.FromPodSet(&wlSnapshotPodSets[0]),
				podset.FromPodSet(&wlSnapshotPodSets[1]),
			}

			targetInitial := tc.initial.DeepCopy()
			if tc.editWhileQueued != nil {
				tc.editWhileQueued(targetInitial)
			}

			job := fromObject(targetInitial.DeepCopy())
			initialPodSets, err := job.PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("initial PodSets() error: %v", err)
			}
			if !equality.ComparePodSetSlices(wlSnapshotPodSets, initialPodSets) {
				t.Fatal("expected queued overlay edit to preserve ComparePodSetSlices equivalence with Workload snapshot")
			}

			if err := job.RunWithPodSetsInfo(ctx, nil, activeAssignInfo); err != nil {
				t.Fatalf("RunWithPodSetsInfo() unexpected error: %v", err)
			}
			if job.IsSuspended() {
				t.Fatal("expected job to be running after RunWithPodSetsInfo")
			}
			if tc.wantRunningTMGs != nil {
				if diff := cmp.Diff(tc.wantRunningTMGs, job.Spec.TaskManager.PodTemplate.Spec.SchedulingGates); diff != "" {
					t.Errorf("running TaskManager role SchedulingGates mismatch (-want,+got):\n%s", diff)
				}
			}

			runningPodSets, err := job.PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("running PodSets() error: %v", err)
			}

			wantRunning := make([]kueue.PodSet, len(initialPodSets))
			for i := range initialPodSets {
				wantRunning[i] = *initialPodSets[i].DeepCopy()
				if err := podset.Merge(log, &wantRunning[i].Template.ObjectMeta, &wantRunning[i].Template.Spec, activeAssignInfo[i]); err != nil {
					t.Fatalf("podset.Merge(%d) error: %v", i, err)
				}
				if activeAssignInfo[i].Annotations[kueue.PodSetUnconstrainedTopologyAnnotation] == "true" {
					wantRunning[i].TopologyRequest = &kueue.PodSetTopologyRequest{Unconstrained: new(true)}
				}
			}
			if diff := cmp.Diff(wantRunning, runningPodSets); diff != "" {
				t.Errorf("running PodSets mismatch (-want,+got):\n%s", diff)
			}
			if !equality.ComparePodSetSlices(runningPodSets, wantRunning, equality.WithIgnoreTolerations()) {
				t.Error("expected ComparePodSetSlices(runningPodSets, wantRunning) == true after RunWithPodSetsInfo")
			}

			job.Suspend()
			if changed := job.RestorePodSetsInfo(ctx, restoreInfo); !changed {
				t.Error("expected RestorePodSetsInfo() to report changed=true")
			}
			if changedAgain := job.RestorePodSetsInfo(ctx, restoreInfo); changedAgain {
				t.Error("expected second RestorePodSetsInfo() call to be idempotent (changed=false)")
			}

			restoredPodSets, err := job.PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("restored PodSets() error: %v", err)
			}
			if diff := cmp.Diff(initialPodSets, restoredPodSets); diff != "" {
				t.Errorf("restored PodSets mismatch (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(targetInitial, job.Object()); diff != "" {
				t.Errorf("restored FlinkDeployment object mismatch (-want,+got):\n%s", diff)
			}
			if !apiequality.Semantic.DeepEqual(targetInitial.Spec, job.Spec) {
				t.Error("restored FlinkDeployment.Spec lost semantic equality")
			}

			// Verify identical PodSet and spec round-trip across kube-apiserver JSON serialization.
			wireWlBytes, err := json.Marshal(tc.initial)
			if err != nil {
				t.Fatalf("json.Marshal(tc.initial) error: %v", err)
			}
			var wireWlSource flinkv1beta1.FlinkDeployment
			if err := json.Unmarshal(wireWlBytes, &wireWlSource); err != nil {
				t.Fatalf("json.Unmarshal(wireWlBytes) error: %v", err)
			}
			wireWlPodSets, err := fromObject(&wireWlSource).PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("wireWlSource PodSets() error: %v", err)
			}
			wireRestoreInfo := []podset.PodSetInfo{
				podset.FromPodSet(&wireWlPodSets[0]),
				podset.FromPodSet(&wireWlPodSets[1]),
			}

			wireInitBytes, err := json.Marshal(targetInitial)
			if err != nil {
				t.Fatalf("json.Marshal(targetInitial) error: %v", err)
			}
			var wireInitial flinkv1beta1.FlinkDeployment
			if err := json.Unmarshal(wireInitBytes, &wireInitial); err != nil {
				t.Fatalf("json.Unmarshal(wireInitBytes) error: %v", err)
			}
			wireInitialPodSets, err := fromObject(&wireInitial).PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("wireInitial PodSets() error: %v\njson: %s", err, string(wireInitBytes))
			}
			if diff := cmp.Diff(initialPodSets, wireInitialPodSets); diff != "" {
				t.Errorf("wireInitial PodSets mismatch (-want,+got):\n%s\njson: %s", diff, string(wireInitBytes))
			}

			wireRunningInput := wireInitial.DeepCopy()
			if err := fromObject(wireRunningInput).RunWithPodSetsInfo(ctx, nil, activeAssignInfo); err != nil {
				t.Fatalf("wire RunWithPodSetsInfo() error: %v", err)
			}
			wireRunBytes, err := json.Marshal(wireRunningInput)
			if err != nil {
				t.Fatalf("json.Marshal(wireRunningInput) error: %v", err)
			}
			var wireRunning flinkv1beta1.FlinkDeployment
			if err := json.Unmarshal(wireRunBytes, &wireRunning); err != nil {
				t.Fatalf("json.Unmarshal(wireRunBytes) error: %v", err)
			}
			wireRunningPodSets, err := fromObject(&wireRunning).PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("wireRunning PodSets() error: %v", err)
			}
			if diff := cmp.Diff(wantRunning, wireRunningPodSets); diff != "" {
				t.Errorf("wireRunning PodSets mismatch (-want,+got):\n%s", diff)
			}

			fromObject(&wireRunning).Suspend()
			if changed := fromObject(&wireRunning).RestorePodSetsInfo(ctx, wireRestoreInfo); !changed {
				t.Error("expected wire RestorePodSetsInfo() to report changed=true")
			}
			wireRestoredBytes, err := json.Marshal(&wireRunning)
			if err != nil {
				t.Fatalf("json.Marshal(wireRestored) error: %v", err)
			}
			var wireRestored flinkv1beta1.FlinkDeployment
			if err := json.Unmarshal(wireRestoredBytes, &wireRestored); err != nil {
				t.Fatalf("json.Unmarshal(wireRestoredBytes) error: %v", err)
			}
			wireRestoredPodSets, err := fromObject(&wireRestored).PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("wireRestored PodSets() error: %v", err)
			}
			if diff := cmp.Diff(initialPodSets, wireRestoredPodSets); diff != "" {
				t.Errorf("wireRestored PodSets mismatch (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(&wireInitial, &wireRestored); diff != "" {
				t.Errorf("wireRestored FlinkDeployment mismatch (-want,+got):\n%s", diff)
			}
			if !apiequality.Semantic.DeepEqual(wireInitial.Spec, wireRestored.Spec) {
				t.Errorf("wireRestored FlinkDeployment.Spec lost semantic equality:\ninit: %s\nrestored: %s",
					string(wireInitBytes), string(wireRestoredBytes))
			}
		})
	}
}

func TestRestorePodSetsInfoNoOp(t *testing.T) {
	nilComponentJob := func() *flinkv1beta1.FlinkDeployment {
		fd := testingflinkdeployment.MakeFlinkDeployment("minimal", "ns").Obj()
		fd.Spec.JobManager = nil
		fd.Spec.TaskManager = nil
		return fd
	}

	testCases := map[string]struct {
		job         *flinkv1beta1.FlinkDeployment
		podSetsInfo []podset.PodSetInfo
	}{
		"wrong podSetsInfo slice length": {
			job:         nilComponentJob(),
			podSetsInfo: []podset.PodSetInfo{{Name: jobManagerPodSetName}},
		},
		"nil JobManager and TaskManager component specs": {
			job:         nilComponentJob(),
			podSetsInfo: []podset.PodSetInfo{{Name: jobManagerPodSetName}, {Name: taskManagerPodSetName}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			if got := fromObject(tc.job.DeepCopy()).RestorePodSetsInfo(ctx, tc.podSetsInfo); got {
				t.Errorf("RestorePodSetsInfo() = %v, want false", got)
			}
		})
	}
}

func TestRestorePodSetsInfoStripsFlavorTolerationsWhenShrunkWhileQueued(t *testing.T) {
	rootTol := corev1.Toleration{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "flink", Effect: corev1.TaintEffectNoSchedule}
	user1Tol := corev1.Toleration{Key: "user-1", Operator: corev1.TolerationOpEqual, Value: "a", Effect: corev1.TaintEffectNoSchedule}
	user2Tol := corev1.Toleration{Key: "user-2", Operator: corev1.TolerationOpEqual, Value: "b", Effect: corev1.TaintEffectNoSchedule}
	user3Tol := corev1.Toleration{Key: "user-3", Operator: corev1.TolerationOpEqual, Value: "c", Effect: corev1.TaintEffectNoSchedule}
	flavorTol := corev1.Toleration{Key: "reserved-gpu-flavor", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}

	testCases := map[string]struct {
		initial         *flinkv1beta1.FlinkDeployment
		shrinkQueued    func(*flinkv1beta1.FlinkDeployment)
		wantRestoredTMs []corev1.Toleration
	}{
		"role tolerations shrunk from 3 to 1 while queued then admitted with flavor taint (len(beforeTolerations) <= len(info.Tolerations))": {
			initial: testingflinkdeployment.MakeFlinkDeployment("shrunk-role-tols", "ns").
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations: []corev1.Toleration{user1Tol, user2Tol, user3Tol},
					},
				}).
				Obj(),
			shrinkQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.TaskManager.PodTemplate.Spec.Tolerations = []corev1.Toleration{user1Tol}
			},
			wantRestoredTMs: []corev1.Toleration{user1Tol, user2Tol, user3Tol},
		},
		"prefixed role tolerations shrunk while queued preserve prefix and strip flavor taint": {
			initial: testingflinkdeployment.MakeFlinkDeployment("shrunk-prefixed-role-tols", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations: []corev1.Toleration{rootTol},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations: []corev1.Toleration{rootTol, user2Tol, user3Tol},
					},
				}).
				Obj(),
			shrinkQueued: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.TaskManager.PodTemplate.Spec.Tolerations = []corev1.Toleration{rootTol}
			},
			wantRestoredTMs: []corev1.Toleration{rootTol, user2Tol, user3Tol},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)

			wlSnapshotPodSets, err := fromObject(tc.initial.DeepCopy()).PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("wlSnapshotPodSets PodSets() error: %v", err)
			}
			restoreInfo := []podset.PodSetInfo{
				podset.FromPodSet(&wlSnapshotPodSets[0]),
				podset.FromPodSet(&wlSnapshotPodSets[1]),
			}

			live := tc.initial.DeepCopy()
			tc.shrinkQueued(live)

			job := fromObject(live)
			assignInfo := []podset.PodSetInfo{
				{
					Name:         jobManagerPodSetName,
					Count:        1,
					Labels:       map[string]string{"flavor": "on-demand"},
					NodeSelector: map[string]string{"node-pool": "cpu"},
				},
				{
					Name:         taskManagerPodSetName,
					Count:        1,
					Labels:       map[string]string{"flavor": "gpu"},
					NodeSelector: map[string]string{"node-pool": "gpu-h100"},
					Tolerations:  []corev1.Toleration{flavorTol},
				},
			}
			if err := job.RunWithPodSetsInfo(ctx, nil, assignInfo); err != nil {
				t.Fatalf("RunWithPodSetsInfo() error: %v", err)
			}
			if gotLen, infoLen := len(job.Spec.TaskManager.PodTemplate.Spec.Tolerations), len(restoreInfo[1].Tolerations); gotLen > infoLen {
				t.Fatalf("expected len(beforeTolerations)=%d <= len(info.Tolerations)=%d", gotLen, infoLen)
			}

			job.Suspend()
			if changed := job.RestorePodSetsInfo(ctx, restoreInfo); !changed {
				t.Fatal("expected RestorePodSetsInfo() to report changed=true")
			}

			if diff := cmp.Diff(tc.wantRestoredTMs, job.Spec.TaskManager.PodTemplate.Spec.Tolerations); diff != "" {
				t.Errorf("restored TaskManager tolerations mismatch (-want,+got):\n%s", diff)
			}
			restoredPodSets, err := job.PodSets(ctx, nil)
			if err != nil {
				t.Fatalf("restored PodSets() error: %v", err)
			}
			if !equality.ComparePodSetSlices(restoredPodSets, wlSnapshotPodSets) {
				t.Errorf("expected restored PodSets to match Workload snapshot without leaking flavor tolerations;\ngot:  %+v\nwant: %+v",
					restoredPodSets[1].Template.Spec.Tolerations, wlSnapshotPodSets[1].Template.Spec.Tolerations)
			}
		})
	}
}

func TestRunWithPodSetsInfoErrors(t *testing.T) {
	testCases := map[string]struct {
		job         *flinkv1beta1.FlinkDeployment
		podSetsInfo []podset.PodSetInfo
		wantErr     error
	}{
		"wrong podSetsInfo length": {
			job:         testingflinkdeployment.MakeFlinkDeployment("job", "ns").Obj(),
			podSetsInfo: []podset.PodSetInfo{{Name: jobManagerPodSetName}},
			wantErr:     podset.ErrInvalidPodsetInfo,
		},
		"session cluster nil spec.job": {
			job: testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName},
				{Name: taskManagerPodSetName},
			},
			wantErr: podset.ErrInvalidPodsetInfo,
		},
		"conflicting nodeSelector with root podTemplate": {
			job: testingflinkdeployment.MakeFlinkDeployment("conflict", "ns").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{NodeSelector: map[string]string{"pool": "a100"}},
				}).
				Obj(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName, Count: 1, NodeSelector: map[string]string{"pool": "t4"}},
				{Name: taskManagerPodSetName, Count: 1},
			},
			wantErr: podset.ErrInvalidPodSetUpdate,
		},
		"zero taskManager.replicas from /scale subresource": {
			job: testingflinkdeployment.MakeFlinkDeployment("scaled-zero", "ns").
				TaskManagerReplicas(0).
				Obj(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName, Count: 1},
				{Name: taskManagerPodSetName, Count: 1},
			},
			wantErr: podset.ErrInvalidPodsetInfo,
		},
		"jobManager.replicas above math.MaxInt32": {
			job: testingflinkdeployment.MakeFlinkDeployment("jm-overflow", "ns").
				JobManagerReplicas(int64(math.MaxInt32) + 1).
				Obj(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName, Count: 1},
				{Name: taskManagerPodSetName, Count: 1},
			},
			wantErr: podset.ErrInvalidPodsetInfo,
		},
		"taskManager.replicas above math.MaxInt32": {
			job: testingflinkdeployment.MakeFlinkDeployment("tm-overflow", "ns").
				TaskManagerReplicas(int64(math.MaxInt32) + 1).
				Obj(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName, Count: 1},
				{Name: taskManagerPodSetName, Count: 1},
			},
			wantErr: podset.ErrInvalidPodsetInfo,
		},
		"job.parallelism above math.MaxInt32": {
			job: testingflinkdeployment.MakeFlinkDeployment("parallelism-overflow", "ns").
				Parallelism(int64(math.MaxInt32) + 1).
				Obj(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName, Count: 1},
				{Name: taskManagerPodSetName, Count: 1},
			},
			wantErr: podset.ErrInvalidPodsetInfo,
		},
		"zero task-manager podSet count": {
			job: testingflinkdeployment.MakeFlinkDeployment("zero-count", "ns").Obj(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName, Count: 1},
				{Name: taskManagerPodSetName, Count: 0},
			},
			wantErr: podset.ErrInvalidPodsetInfo,
		},
		"deferred podTemplate decode error": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("bad-pt", "ns").Obj()
				_ = json.Unmarshal(
					[]byte(`{"spec":{"containers":[{"name":"sidecar","resources":{"requests":{"memory":"2g"}}}]}}`),
					&fd.Spec.PodTemplate,
				)
				return fd
			}(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName, Count: 1},
				{Name: taskManagerPodSetName, Count: 1},
			},
			wantErr: podset.ErrInvalidPodsetInfo,
		},
		"duplicate podSet name": {
			job: testingflinkdeployment.MakeFlinkDeployment("dup", "ns").Obj(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName},
				{Name: jobManagerPodSetName},
			},
			wantErr: podset.ErrInvalidPodsetInfo,
		},
		"unknown podSet name": {
			job: testingflinkdeployment.MakeFlinkDeployment("unknown", "ns").Obj(),
			podSetsInfo: []podset.PodSetInfo{
				{Name: jobManagerPodSetName},
				{Name: "worker"},
			},
			wantErr: podset.ErrInvalidPodsetInfo,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			err := fromObject(tc.job.DeepCopy()).RunWithPodSetsInfo(ctx, nil, tc.podSetsInfo)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("RunWithPodSetsInfo() error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestSkip(t *testing.T) {
	now := metav1.NewTime(time.Now())
	testCases := map[string]struct {
		job  *flinkv1beta1.FlinkDeployment
		want bool
	}{
		"unlabeled live Session cluster is skipped": {
			job:  testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
			want: true,
		},
		"labeled Session cluster is skipped": {
			job:  testingflinkdeployment.MakeFlinkDeployment("managed-session", "ns").Queue("lq").SessionCluster().Obj(),
			want: true,
		},
		"deleting Session cluster is not skipped": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("deleting-session", "ns").SessionCluster().Obj()
				fd.DeletionTimestamp = &now
				return fd
			}(),
			want: false,
		},
		"zero-valued NotFound struct is not skipped so orphaned Workloads still finalize": {
			job:  &flinkv1beta1.FlinkDeployment{},
			want: false,
		},
		"Application-mode FlinkDeployment is not skipped": {
			job:  testingflinkdeployment.MakeFlinkDeployment("app", "ns").Obj(),
			want: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			if got := fromObject(tc.job).Skip(ctx); got != tc.want {
				t.Errorf("Skip() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSuspendPreservesUpgradeMode(t *testing.T) {
	testCases := map[string]struct {
		job             *flinkv1beta1.FlinkDeployment
		wantSuspended   bool
		wantNilJob      bool
		wantUpgradeMode flinkv1beta1.UpgradeMode
	}{
		"stateless": {
			job: testingflinkdeployment.MakeFlinkDeployment("job", "ns").
				Suspend(false).
				UpgradeMode(flinkv1beta1.UpgradeModeStateless).
				Obj(),
			wantSuspended:   true,
			wantUpgradeMode: flinkv1beta1.UpgradeModeStateless,
		},
		"last-state": {
			job: testingflinkdeployment.MakeFlinkDeployment("job", "ns").
				Suspend(false).
				UpgradeMode(flinkv1beta1.UpgradeModeLastState).
				Obj(),
			wantSuspended:   true,
			wantUpgradeMode: flinkv1beta1.UpgradeModeLastState,
		},
		"savepoint": {
			job: testingflinkdeployment.MakeFlinkDeployment("job", "ns").
				Suspend(false).
				UpgradeMode(flinkv1beta1.UpgradeModeSavepoint).
				Obj(),
			wantSuspended:   true,
			wantUpgradeMode: flinkv1beta1.UpgradeModeSavepoint,
		},
		"session cluster nil spec.job does not fabricate JobSpec": {
			job:           testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
			wantSuspended: false,
			wantNilJob:    true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			fd := tc.job.DeepCopy()
			job := fromObject(fd)
			if job.IsSuspended() {
				t.Fatal("expected initial job to report IsSuspended()=false")
			}
			job.Suspend()
			if got := job.IsSuspended(); got != tc.wantSuspended {
				t.Fatalf("IsSuspended() after Suspend() = %v, want %v", got, tc.wantSuspended)
			}
			if tc.wantNilJob {
				if fd.Spec.Job != nil {
					t.Fatalf("Suspend() fabricated non-nil Spec.Job: %+v", fd.Spec.Job)
				}
				return
			}
			if fd.Spec.Job.UpgradeMode != tc.wantUpgradeMode {
				t.Errorf("UpgradeMode = %q, want %q preserved", fd.Spec.Job.UpgradeMode, tc.wantUpgradeMode)
			}
		})
	}
}

func TestStop(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)

	preAdmission := testingflinkdeployment.MakeFlinkDeployment("stop-app", "ns").
		Queue("lq").
		Suspend(true).
		UpgradeMode(flinkv1beta1.UpgradeModeSavepoint).
		TaskManagerPodTemplate(&corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"tenant": "team-a"},
			},
		}).
		Obj()

	origPodSets, err := fromObject(preAdmission).PodSets(ctx, nil)
	if err != nil {
		t.Fatalf("PodSets() error: %v", err)
	}
	restoreInfo := make([]podset.PodSetInfo, len(origPodSets))
	for i := range origPodSets {
		restoreInfo[i] = podset.FromPodSet(&origPodSets[i])
	}

	admittedInfo := []podset.PodSetInfo{
		{
			Name:        jobManagerPodSetName,
			Count:       1,
			Labels:      map[string]string{"kueue.x-k8s.io/podset": "job-manager", "kueue.x-k8s.io/cluster-queue-name": "cq"},
			Annotations: map[string]string{"kueue.x-k8s.io/workload": "wl-stop"},
			NodeSelector: map[string]string{
				"flavor": "spot",
			},
			Tolerations: []corev1.Toleration{{
				Key:      "spot",
				Operator: corev1.TolerationOpEqual,
				Value:    "true",
				Effect:   corev1.TaintEffectNoSchedule,
			}},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}},
		},
		{
			Name:        taskManagerPodSetName,
			Count:       1,
			Labels:      map[string]string{"kueue.x-k8s.io/podset": "task-manager", "kueue.x-k8s.io/cluster-queue-name": "cq"},
			Annotations: map[string]string{"kueue.x-k8s.io/workload": "wl-stop"},
			NodeSelector: map[string]string{
				"tenant": "team-a",
				"flavor": "spot",
			},
			Tolerations: []corev1.Toleration{{
				Key:      "spot",
				Operator: corev1.TolerationOpEqual,
				Value:    "true",
				Effect:   corev1.TaintEffectNoSchedule,
			}},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}},
		},
	}

	admittedRunning := preAdmission.DeepCopy()
	if err := fromObject(admittedRunning).RunWithPodSetsInfo(ctx, nil, admittedInfo); err != nil {
		t.Fatalf("RunWithPodSetsInfo() error: %v", err)
	}

	manuallySuspendedWithAdmittedOverlays := admittedRunning.DeepCopy()
	fromObject(manuallySuspendedWithAdmittedOverlays).Suspend()

	testCases := map[string]struct {
		initial        *flinkv1beta1.FlinkDeployment
		podSetsInfo    []podset.PodSetInfo
		patchErr       error
		wantStoppedNow bool
		wantPatches    int
		wantStored     *flinkv1beta1.FlinkDeployment
	}{
		"suspends running deployment, preserves savepoint upgradeMode, and restores PodSet overlays": {
			initial:        admittedRunning,
			podSetsInfo:    restoreInfo,
			wantStoppedNow: true,
			wantPatches:    1,
			wantStored:     preAdmission,
		},
		"returns stoppedNow=false when Patch fails on running deployment": {
			initial:        admittedRunning,
			podSetsInfo:    restoreInfo,
			patchErr:       errors.New("simulated patch conflict"),
			wantStoppedNow: false,
			wantPatches:    1,
			wantStored:     admittedRunning,
		},
		"restores PodSet overlays on manually suspended deployment without emitting Stopped again": {
			initial:        manuallySuspendedWithAdmittedOverlays,
			podSetsInfo:    restoreInfo,
			wantStoppedNow: false,
			wantPatches:    1,
			wantStored:     preAdmission,
		},
		"performs zero API patches on repeat reconcile when already suspended and already restored": {
			initial:        preAdmission,
			podSetsInfo:    restoreInfo,
			wantStoppedNow: false,
			wantPatches:    0,
			wantStored:     preAdmission,
		},
		"suspends running deployment when podSetsInfo is nil": {
			initial:        admittedRunning,
			podSetsInfo:    nil,
			wantStoppedNow: true,
			wantPatches:    1,
			wantStored:     manuallySuspendedWithAdmittedOverlays,
		},
		"performs zero API patches on Session cluster with nil spec.job": {
			initial:        testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
			podSetsInfo:    nil,
			wantStoppedNow: false,
			wantPatches:    0,
			wantStored:     testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			runCtx, _ := utiltesting.ContextWithLog(t)
			var patches int
			cl := utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme).
				WithObjects(tc.initial.DeepCopy()).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						patches++
						if tc.patchErr != nil {
							return tc.patchErr
						}
						return c.Patch(ctx, obj, patch, opts...)
					},
				}).
				Build()

			var live flinkv1beta1.FlinkDeployment
			if err := cl.Get(runCtx, client.ObjectKeyFromObject(tc.initial), &live); err != nil {
				t.Fatalf("cl.Get(initial) error: %v", err)
			}

			stoppedNow, err := fromObject(&live).Stop(runCtx, cl, tc.podSetsInfo, jobframework.StopReasonWorkloadEvicted, "evicted")
			if tc.patchErr != nil {
				if !errors.Is(err, tc.patchErr) {
					t.Fatalf("Stop() error = %v, want %v", err, tc.patchErr)
				}
			} else if err != nil {
				t.Fatalf("Stop() unexpected error: %v", err)
			}
			if stoppedNow != tc.wantStoppedNow {
				t.Errorf("Stop() stoppedNow = %v, want %v", stoppedNow, tc.wantStoppedNow)
			}
			if patches != tc.wantPatches {
				t.Errorf("Stop() client Patch count = %d, want %d", patches, tc.wantPatches)
			}

			var stored flinkv1beta1.FlinkDeployment
			if err := cl.Get(runCtx, client.ObjectKeyFromObject(tc.initial), &stored); err != nil {
				t.Fatalf("cl.Get(stored) error: %v", err)
			}
			if diff := cmp.Diff(tc.wantStored.Spec, stored.Spec, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("stored Spec mismatch after Stop() (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestIsActive(t *testing.T) {
	testCases := map[string]struct {
		mutate func(*testingflinkdeployment.FlinkDeploymentWrapper)
		want   bool
	}{
		"empty status": {
			mutate: func(*testingflinkdeployment.FlinkDeploymentWrapper) {},
			want:   false,
		},
		"suspended lifecycle and missing JM": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.LifecycleState(flinkv1beta1.LifecycleStateSuspended).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					JobStatusState(flinkv1beta1.JobStatusStateSuspended)
			},
			want: false,
		},
		"JM DEPLOYED_NOT_READY is active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusDeployedNotReady)
			},
			want: true,
		},
		"JM DEPLOYING is active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusDeploying)
			},
			want: true,
		},
		"LifecycleStateUpgrading during savepoint with JM READY is active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.LifecycleState(flinkv1beta1.LifecycleStateUpgrading).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusReady)
			},
			want: true,
		},
		"JM ERROR still has a JobManager Deployment and is active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusError)
			},
			want: true,
		},
		"UPGRADING parked with MISSING JM and zero TM replicas is not active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.LifecycleState(flinkv1beta1.LifecycleStateUpgrading).
					ReconciliationState(flinkv1beta1.ReconciliationStateUpgrading).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					TaskManagerStatusReplicas(0).
					JobStatusState(flinkv1beta1.JobStatusStateFinished)
			},
			want: false,
		},
		"STABLE lifecycle without pod evidence is not active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.LifecycleState(flinkv1beta1.LifecycleStateStable).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing)
			},
			want: false,
		},
		"non-zero TaskManager status replicas is active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Status.TaskManager = &flinkv1beta1.TaskManagerStatusInfo{Replicas: 2}
			},
			want: true,
		},
		"JobStatusStateCancelling is active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.JobStatusState(flinkv1beta1.JobStatusStateCancelling)
			},
			want: true,
		},
		"post-deployment native LifecycleStateFailed with MISSING JM and stale TM replicas / RECONCILING jobStatus is not active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					TaskManagerStatusReplicas(2).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling).
					StatusError("Manual restore required.")
			},
			want: false,
		},
		"rolled-back native LifecycleStateFailed with MISSING JM and stale TM replicas is not active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateRolledBack).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					TaskManagerStatusReplicas(2).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling).
					StatusError("HA metadata is not available")
			},
			want: false,
		},
		"post-deployment standalone LifecycleStateFailed with MISSING JM and non-zero TM replicas remains active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
					LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					TaskManagerStatusReplicas(2).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling)
			},
			want: true,
		},
		"post-deployment native LifecycleStateFailed with ERROR JM remains active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusError).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling)
			},
			want: true,
		},
		"pre-first-deployment LifecycleStateFailed with MISSING JM and RECONCILING jobStatus remains active": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateUpgrading).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling)
			},
			want: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			w := testingflinkdeployment.MakeFlinkDeployment("job", "ns")
			tc.mutate(w)
			if got := fromObject(w.Obj()).IsActive(); got != tc.want {
				t.Errorf("IsActive() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFinished(t *testing.T) {
	testCases := map[string]struct {
		mutate       func(*testingflinkdeployment.FlinkDeploymentWrapper)
		wantMsg      string
		wantSuccess  bool
		wantFinished bool
	}{
		"running job is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).JobStatusState(flinkv1beta1.JobStatusStateRunning)
			},
			wantFinished: false,
		},
		"suspended job with FINISHED jobStatus (from Flink suspension) is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(true).
					LifecycleState(flinkv1beta1.LifecycleStateSuspended).
					JobStatusState(flinkv1beta1.JobStatusStateFinished)
			},
			wantFinished: false,
		},
		"suspended job with FAILED jobStatus (JM never started before suspension) is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(true).
					JobStatusState(flinkv1beta1.JobStatusStateFailed)
			},
			wantFinished: false,
		},
		"re-admitted job still reporting LifecycleStateSuspended before operator reconciles is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateSuspended).
					JobStatusState(flinkv1beta1.JobStatusStateFinished)
			},
			wantFinished: false,
		},
		"running bounded FINISHED job succeeds once the operator reports DEPLOYED": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateStable).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobStatusState(flinkv1beta1.JobStatusStateFinished)
			},
			wantMsg:      "FlinkDeployment job finished successfully",
			wantSuccess:  true,
			wantFinished: true,
		},
		"FINISHED job under ROLLED_BACK spec is finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateRolledBack).
					ReconciliationState(flinkv1beta1.ReconciliationStateRolledBack).
					JobStatusState(flinkv1beta1.JobStatusStateFinished)
			},
			wantMsg:      "FlinkDeployment job finished successfully",
			wantSuccess:  true,
			wantFinished: true,
		},
		"running FAILED job includes status error": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateDeployed).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobStatusState(flinkv1beta1.JobStatusStateFailed).
					StatusError("OutOfMemoryError in TaskManager")
			},
			wantMsg:      "FlinkDeployment job failed: OutOfMemoryError in TaskManager",
			wantSuccess:  false,
			wantFinished: true,
		},
		"running CANCELED job is finished with success=false": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateDeployed).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobStatusState(flinkv1beta1.JobStatusStateCanceled)
			},
			wantMsg:      "FlinkDeployment job was canceled",
			wantSuccess:  false,
			wantFinished: true,
		},
		"unsuspended pre-deploy UPGRADING status with FINISHED jobStatus after un-suspend is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateUpgrading).
					ReconciliationState(flinkv1beta1.ReconciliationStateUpgrading).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					JobStatusState(flinkv1beta1.JobStatusStateFinished)
			},
			wantFinished: false,
		},
		"reconciliationStatus UPGRADING with FAILED jobStatus during in-place upgrade is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateDeployed).
					ReconciliationState(flinkv1beta1.ReconciliationStateUpgrading).
					JobStatusState(flinkv1beta1.JobStatusStateFailed)
			},
			wantFinished: false,
		},
		"ROLLING_BACK with CANCELED jobStatus is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateRollingBack).
					ReconciliationState(flinkv1beta1.ReconciliationStateRollingBack).
					JobStatusState(flinkv1beta1.JobStatusStateCanceled)
			},
			wantFinished: false,
		},
		"terminal jobStatus before first deployment (no reconciliation state) is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					JobStatusState(flinkv1beta1.JobStatusStateFailed).
					StatusError("JarURI not found")
			},
			wantFinished: false,
		},
		"LifecycleStateFailed from operator validation before first deployment is recoverable and not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateFailed).
					StatusError("Checkpoint directory[state.checkpoints.dir] must be defined for last-state and savepoint upgrade modes")
			},
			wantFinished: false,
		},
		"post-deployment DEPLOYED LifecycleStateFailed with RECONCILING jobStatus finishes as failed": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling).
					StatusError("JobManager deployment is missing and HA data is not available to make stateful upgrades. Manual restore required.")
			},
			wantMsg:      "FlinkDeployment failed: JobManager deployment is missing and HA data is not available to make stateful upgrades. Manual restore required.",
			wantSuccess:  false,
			wantFinished: true,
		},
		"post-deployment ROLLED_BACK LifecycleStateFailed with RECONCILING jobStatus and empty error finishes as failed": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateRolledBack).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling)
			},
			wantMsg:      "FlinkDeployment failed",
			wantSuccess:  false,
			wantFinished: true,
		},
		"suspended job with post-deployment LifecycleStateFailed and RECONCILING jobStatus is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(true).
					LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling).
					StatusError("Manual restore required.")
			},
			wantFinished: false,
		},
		"post-deployment native LifecycleStateFailed with ERROR JM remains active and is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusError).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling).
					StatusError("JobManager deployment failed")
			},
			wantFinished: false,
		},
		"post-deployment standalone LifecycleStateFailed with MISSING JM and active TaskManager Deployment is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
					LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateDeployed).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					TaskManagerStatusReplicas(2).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling).
					StatusError("Manual restore required.")
			},
			wantFinished: false,
		},
		"rolled-back standalone LifecycleStateFailed with MISSING JM and zero TM status replicas remains active via RECONCILING jobStatus and is not finished": {
			mutate: func(w *testingflinkdeployment.FlinkDeploymentWrapper) {
				w.Suspend(false).
					Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
					LifecycleState(flinkv1beta1.LifecycleStateFailed).
					ReconciliationState(flinkv1beta1.ReconciliationStateRolledBack).
					JobManagerDeploymentStatus(flinkv1beta1.JobManagerDeploymentStatusMissing).
					TaskManagerStatusReplicas(0).
					JobStatusState(flinkv1beta1.JobStatusStateReconciling)
			},
			wantFinished: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			w := testingflinkdeployment.MakeFlinkDeployment("job", "ns")
			tc.mutate(w)
			gotMsg, gotSuccess, gotFinished := fromObject(w.Obj()).Finished(ctx)
			if gotFinished != tc.wantFinished || gotSuccess != tc.wantSuccess || gotMsg != tc.wantMsg {
				t.Errorf("Finished() = (%q, %v, %v), want (%q, %v, %v)",
					gotMsg, gotSuccess, gotFinished, tc.wantMsg, tc.wantSuccess, tc.wantFinished)
			}
		})
	}
}

func TestPodsReady(t *testing.T) {
	testCases := map[string]struct {
		lifecycleState flinkv1beta1.LifecycleState
		jmStatus       flinkv1beta1.JobManagerDeploymentStatus
		jobState       flinkv1beta1.JobStatusState
		want           bool
	}{
		"STABLE lifecycle, JM ready, and job running": {
			lifecycleState: flinkv1beta1.LifecycleStateStable,
			jmStatus:       flinkv1beta1.JobManagerDeploymentStatusReady,
			jobState:       flinkv1beta1.JobStatusStateRunning,
			want:           true,
		},
		"DEPLOYED lifecycle before stability check completes": {
			lifecycleState: flinkv1beta1.LifecycleStateDeployed,
			jmStatus:       flinkv1beta1.JobManagerDeploymentStatusReady,
			jobState:       flinkv1beta1.JobStatusStateRunning,
			want:           false,
		},
		"UPGRADING lifecycle": {
			lifecycleState: flinkv1beta1.LifecycleStateUpgrading,
			jmStatus:       flinkv1beta1.JobManagerDeploymentStatusReady,
			jobState:       flinkv1beta1.JobStatusStateRunning,
			want:           false,
		},
		"JM not ready": {
			lifecycleState: flinkv1beta1.LifecycleStateStable,
			jmStatus:       flinkv1beta1.JobManagerDeploymentStatusDeployedNotReady,
			jobState:       flinkv1beta1.JobStatusStateRunning,
			want:           false,
		},
		"JM ready and STABLE but job initializing": {
			lifecycleState: flinkv1beta1.LifecycleStateStable,
			jmStatus:       flinkv1beta1.JobManagerDeploymentStatusReady,
			jobState:       flinkv1beta1.JobStatusStateInitializing,
			want:           false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			fd := testingflinkdeployment.MakeFlinkDeployment("job", "ns").
				LifecycleState(tc.lifecycleState).
				JobManagerDeploymentStatus(tc.jmStatus).
				JobStatusState(tc.jobState).
				Obj()
			if got := fromObject(fd).PodsReady(ctx, nil); got != tc.want {
				t.Errorf("PodsReady() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCanSupportIntegration(t *testing.T) {
	testCases := map[string]struct {
		gateEnabled   bool
		wantSupported bool
		wantErrSubstr string
	}{
		"disabled by default": {
			gateEnabled:   false,
			wantSupported: false,
			wantErrSubstr: "please enable the FlinkDeploymentIntegration feature gate",
		},
		"enabled when feature gate on": {
			gateEnabled:   true,
			wantSupported: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.FlinkDeploymentIntegration, tc.gateEnabled)
			ok, err := canSupportIntegration()
			if ok != tc.wantSupported {
				t.Errorf("canSupportIntegration() supported = %v, want %v", ok, tc.wantSupported)
			}
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("canSupportIntegration() error = %v, want substring %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("canSupportIntegration() unexpected error: %v", err)
			}
		})
	}
}

func TestGetWorkloadNameForFlinkDeployment(t *testing.T) {
	got := GetWorkloadNameForFlinkDeployment("my-flink-job", "uid-123")
	want := jobframework.GetWorkloadNameForOwnerWithGVK("my-flink-job", "uid-123", gvk)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("GetWorkloadNameForFlinkDeployment() mismatch (-want,+got):\n%s", diff)
	}
}

func TestReconcileLabeledSessionClusterSkipped(t *testing.T) {
	testCases := map[string]struct {
		jmStatus flinkv1beta1.JobManagerDeploymentStatus
	}{
		"active queue-labeled Session cluster emits no Stopped event and leaves CR untouched": {
			jmStatus: flinkv1beta1.JobManagerDeploymentStatusReady,
		},
		"inactive queue-labeled Session cluster creates no Workload and emits no event": {
			jmStatus: flinkv1beta1.JobManagerDeploymentStatusMissing,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			session := testingflinkdeployment.MakeFlinkDeployment("legacy-session", "ns").
				Queue("lq").
				SessionCluster().
				JobManagerDeploymentStatus(tc.jmStatus).
				Obj()

			cl := utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme).
				WithObjects(utiltesting.MakeNamespace("ns"), session).
				Build()
			var storedBefore flinkv1beta1.FlinkDeployment
			if err := cl.Get(ctx, client.ObjectKeyFromObject(session), &storedBefore); err != nil {
				t.Fatalf("cl.Get(before) error: %v", err)
			}

			recorder := events.NewFakeRecorder(10)
			reconciler, err := NewReconciler(
				ctx,
				cl,
				nil,
				recorder,
				jobframework.WithIntegrationManager(newTestIntegrationManager(t)),
				jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
			)
			if err != nil {
				t.Fatalf("NewReconciler() error: %v", err)
			}

			if _, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(session),
			}); err != nil {
				t.Fatalf("Reconcile() unexpected error: %v", err)
			}

			var workloads kueue.WorkloadList
			if err := cl.List(ctx, &workloads, client.InNamespace(session.Namespace)); err != nil {
				t.Fatalf("cl.List(Workloads) error: %v", err)
			}
			if len(workloads.Items) != 0 {
				t.Errorf("expected 0 Workloads for queue-labeled Session cluster, got %d", len(workloads.Items))
			}

			select {
			case ev := <-recorder.Events:
				t.Errorf("expected zero events for queue-labeled Session cluster, got %q", ev)
			default:
			}

			var storedAfter flinkv1beta1.FlinkDeployment
			if err := cl.Get(ctx, client.ObjectKeyFromObject(session), &storedAfter); err != nil {
				t.Fatalf("cl.Get(after) error: %v", err)
			}
			if diff := cmp.Diff(storedBefore, storedAfter); diff != "" {
				t.Errorf("queue-labeled Session cluster was mutated by Reconcile (-before,+after):\n%s", diff)
			}
		})
	}
}

func TestRawJSONPartialPodTemplateMergePatchPreservation(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)

	rawManifest := []byte(`{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind": "FlinkDeployment",
		"metadata": {
			"name": "partial-override-app",
			"namespace": "ns",
			"labels": {"kueue.x-k8s.io/queue-name": "lq"}
		},
		"spec": {
			"image": "flink:1.20",
			"flinkVersion": "v1_20",
			"podTemplate": {
				"spec": {
					"containers": [{
						"name": "flink-main-container",
						"ports": [{"name": "metrics", "containerPort": 8080}],
						"env": [{"name": "MEM", "valueFrom": {"resourceFieldRef": {"resource": "limits.memory", "divisor": "1Mi"}}}],
						"livenessProbe": {"httpGet": {"path": "/live", "port": 8081}}
					}],
					"affinity": {
						"podAntiAffinity": {
							"preferredDuringSchedulingIgnoredDuringExecution": [{
								"weight": 50,
								"podAffinityTerm": {"topologyKey": "topology.kubernetes.io/zone"}
							}]
						}
					},
					"topologySpreadConstraints": [{
						"maxSkew": 1,
						"topologyKey": "topology.kubernetes.io/zone",
						"whenUnsatisfiable": "DoNotSchedule"
					}]
				}
			},
			"jobManager": {"resource": {"cpu": 1, "memory": "1024m"}},
			"taskManager": {
				"resource": {"cpu": 4.03, "memory": "2048m"},
				"podTemplate": {
					"spec": {
						"containers": [{
							"ports": [{"protocol": "TCP"}],
							"env": [{"name": "MEM", "valueFrom": {"resourceFieldRef": {"resource": "requests.memory"}}}],
							"livenessProbe": {"httpGet": {"path": "/healthz"}}
						}],
						"affinity": {
							"podAntiAffinity": {
								"preferredDuringSchedulingIgnoredDuringExecution": [{
									"podAffinityTerm": {"topologyKey": "kubernetes.io/hostname"}
								}]
							}
						},
						"topologySpreadConstraints": [{"whenUnsatisfiable": "ScheduleAnyway"}]
					}
				}
			},
			"job": {
				"jarURI": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
				"parallelism": 1,
				"state": "suspended"
			}
		}
	}`)

	var fd flinkv1beta1.FlinkDeployment
	if err := json.Unmarshal(rawManifest, &fd); err != nil {
		t.Fatalf("json.Unmarshal(rawManifest) error: %v", err)
	}

	prePodSets, err := fromObject(&fd).PodSets(ctx, nil)
	if err != nil {
		t.Fatalf("prePodSets error: %v", err)
	}
	tmContainer := prePodSets[1].Template.Spec.Containers[0]
	if got := tmContainer.Resources.Requests[corev1.ResourceCPU]; !got.Equal(resource.MustParse("4030m")) {
		t.Errorf("TaskManager CPU = %s, want 4030m", got.String())
	}
	if tmContainer.Ports[0].ContainerPort != 8080 {
		t.Errorf("merged ContainerPort = %d, want inherited 8080", tmContainer.Ports[0].ContainerPort)
	}
	if gotDiv := tmContainer.Env[0].ValueFrom.ResourceFieldRef.Divisor; !gotDiv.Equal(resource.MustParse("1Mi")) {
		t.Errorf("merged ResourceFieldRef.Divisor = %s, want inherited 1Mi", gotDiv.String())
	}
	if tmContainer.LivenessProbe.HTTPGet.Port.IntVal != 8081 {
		t.Errorf("merged LivenessProbe.HTTPGet.Port = %v, want inherited 8081", tmContainer.LivenessProbe.HTTPGet.Port)
	}
	if gotWeight := prePodSets[1].Template.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].Weight; gotWeight != 50 {
		t.Errorf("merged PreferredSchedulingTerm.Weight = %d, want inherited 50", gotWeight)
	}
	if gotSkew := prePodSets[1].Template.Spec.TopologySpreadConstraints[0].MaxSkew; gotSkew != 1 {
		t.Errorf("merged TopologySpreadConstraint.MaxSkew = %d, want inherited 1", gotSkew)
	}

	origBeforeRun := fd.DeepCopy()
	if err := fromObject(&fd).RunWithPodSetsInfo(ctx, nil, []podset.PodSetInfo{
		{Name: jobManagerPodSetName, Count: 1, Labels: map[string]string{"kueue.x-k8s.io/workload": "wl"}, NodeSelector: map[string]string{"pool": "cpu"}},
		{Name: taskManagerPodSetName, Count: 1, Labels: map[string]string{"kueue.x-k8s.io/workload": "wl"}, NodeSelector: map[string]string{"pool": "gpu"}},
	}); err != nil {
		t.Fatalf("RunWithPodSetsInfo() error: %v", err)
	}
	admitPatch, err := client.MergeFrom(origBeforeRun).Data(&fd)
	if err != nil {
		t.Fatalf("MergeFrom(admit) error: %v", err)
	}
	for _, forbiddenZero := range []string{`"containerPort":0`, `"divisor":"0"`, `"port":0`, `"weight":0`, `"maxSkew":0`, `"topologyKey":""`} {
		if strings.Contains(string(admitPatch), forbiddenZero) {
			t.Errorf("admit MergeFrom patch injected zero value %s: %s", forbiddenZero, string(admitPatch))
		}
	}

	admittedWire, err := json.Marshal(&fd)
	if err != nil {
		t.Fatalf("json.Marshal(admitted) error: %v", err)
	}
	var admittedReloaded flinkv1beta1.FlinkDeployment
	if err := json.Unmarshal(admittedWire, &admittedReloaded); err != nil {
		t.Fatalf("json.Unmarshal(admittedWire) error: %v", err)
	}
	postPodSets, err := fromObject(&admittedReloaded).PodSets(ctx, nil)
	if err != nil {
		t.Fatalf("postPodSets error: %v", err)
	}
	if !equality.ComparePodSetSlices(prePodSets, postPodSets) {
		t.Error("expected ComparePodSetSlices(prePodSets, postPodSets) == true after reloading admitted JSON")
	}

	origBeforeRestore := admittedReloaded.DeepCopy()
	fromObject(&admittedReloaded).Suspend()
	fromObject(&admittedReloaded).RestorePodSetsInfo(ctx, []podset.PodSetInfo{
		podset.FromPodSet(&prePodSets[0]),
		podset.FromPodSet(&prePodSets[1]),
	})
	restorePatch, err := client.MergeFrom(origBeforeRestore).Data(&admittedReloaded)
	if err != nil {
		t.Fatalf("MergeFrom(restore) error: %v", err)
	}
	for _, forbiddenZero := range []string{`"containerPort":0`, `"divisor":"0"`, `"port":0`, `"weight":0`, `"maxSkew":0`, `"topologyKey":""`} {
		if strings.Contains(string(restorePatch), forbiddenZero) {
			t.Errorf("restore MergeFrom patch injected zero value %s: %s", forbiddenZero, string(restorePatch))
		}
	}
}

func TestReconcileQueuedPositionalGatesEditAndSuspendedBaseTolerationsRestore(t *testing.T) {
	t.Run("queued positional schedulingGates prefix edit followed by admission and eviction Stop clears QuotaReserved under ValidateUpdate", func(t *testing.T) {
		ctx, _ := utiltesting.ContextWithLog(t)
		g1 := corev1.PodSchedulingGate{Name: "example.com/g1"}
		g2 := corev1.PodSchedulingGate{Name: "example.com/g2"}
		gRole := corev1.PodSchedulingGate{Name: "example.com/g-role"}

		flavor := utiltestingapi.MakeResourceFlavor("spot-flavor").NodeLabel("pool", "spot").Obj()
		initial := testingflinkdeployment.MakeFlinkDeployment("queued-prefix-evict", "ns").
			UID("uid-queued-prefix-evict").
			Queue("lq").
			Suspend(true).
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{g1}},
			}).
			TaskManagerPodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{g1, gRole}},
			}).
			Obj()

		integrationMgr := newTestIntegrationManager(t)
		var wh *flinkDeploymentWebhook
		validateWrite := func(ctx context.Context, c client.WithWatch, newFD *flinkv1beta1.FlinkDeployment) error {
			var oldFD flinkv1beta1.FlinkDeployment
			if err := c.Get(ctx, client.ObjectKeyFromObject(newFD), &oldFD); err != nil {
				return err
			}
			_, err := wh.ValidateUpdate(ctx, &oldFD, newFD)
			return err
		}

		builder := utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme).
			WithObjects(utiltesting.MakeNamespace("ns"), flavor, initial.DeepCopy()).
			WithStatusSubresource(&kueue.Workload{}, &flinkv1beta1.FlinkDeployment{}).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if fd, ok := obj.(*flinkv1beta1.FlinkDeployment); ok {
						if err := validateWrite(ctx, c, fd); err != nil {
							return err
						}
					}
					return c.Update(ctx, obj, opts...)
				},
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if fd, ok := obj.(*flinkv1beta1.FlinkDeployment); ok {
						if err := validateWrite(ctx, c, fd.DeepCopy()); err != nil {
							return err
						}
					}
					return c.Patch(ctx, obj, patch, opts...)
				},
			})
		if err := SetupIndexes(ctx, utiltesting.AsIndexer(builder)); err != nil {
			t.Fatalf("SetupIndexes() error: %v", err)
		}
		cl := builder.Build()
		wh = &flinkDeploymentWebhook{
			integrationManager:           integrationMgr,
			client:                       cl,
			managedJobsNamespaceSelector: labels.Everything(),
		}

		reconciler, err := NewReconciler(
			ctx,
			cl,
			nil,
			events.NewFakeRecorder(20),
			jobframework.WithIntegrationManager(integrationMgr),
			jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
		)
		if err != nil {
			t.Fatalf("NewReconciler() error: %v", err)
		}
		req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(initial)}

		// Step 1: initial reconcile creates queued Workload capturing [g1, gRole].
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("initial Reconcile() error: %v", err)
		}
		wlKey := client.ObjectKey{Namespace: initial.Namespace, Name: GetWorkloadNameForFlinkDeployment(initial.Name, initial.UID)}
		var wl kueue.Workload
		if err := cl.Get(ctx, wlKey, &wl); err != nil {
			t.Fatalf("cl.Get(Workload) error: %v", err)
		}
		wlUID := wl.UID

		// Step 2: update queued CR prefix from [g1] -> [g2] and [g1, gRole] -> [g2, gRole].
		var live flinkv1beta1.FlinkDeployment
		if err := cl.Get(ctx, req.NamespacedName, &live); err != nil {
			t.Fatalf("cl.Get(live) error: %v", err)
		}
		live.Spec.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{g2}
		live.Spec.TaskManager.PodTemplate.Spec.SchedulingGates = []corev1.PodSchedulingGate{g2, gRole}
		if err := cl.Update(ctx, &live); err != nil {
			t.Fatalf("queued prefix cl.Update() rejected: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("post-edit Reconcile() error: %v", err)
		}
		if err := cl.Get(ctx, wlKey, &wl); err != nil {
			t.Fatalf("cl.Get(Workload after edit) error: %v", err)
		}
		if wl.UID != wlUID {
			t.Fatalf("expected EquivalentToWorkload to preserve queued Workload UID %s, got %s", wlUID, wl.UID)
		}

		// Step 3: admit Workload and reconcile so startJob sets FlinkDeployment running.
		admission := utiltestingapi.MakeAdmission("cq", jobManagerPodSetName, taskManagerPodSetName).
			PodSets(
				utiltestingapi.MakePodSetAssignment(jobManagerPodSetName).Assignment(corev1.ResourceCPU, "spot-flavor", "1").Count(1).Obj(),
				utiltestingapi.MakePodSetAssignment(taskManagerPodSetName).Assignment(corev1.ResourceCPU, "spot-flavor", "1").Count(1).Obj(),
			).
			Obj()
		now := metav1.Now()
		wl.Status.Admission = admission
		apimeta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{
			Type:               kueue.WorkloadQuotaReserved,
			Status:             metav1.ConditionTrue,
			Reason:             "AdmittedByTest",
			LastTransitionTime: now,
		})
		apimeta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{
			Type:               kueue.WorkloadAdmitted,
			Status:             metav1.ConditionTrue,
			Reason:             "AdmittedByTest",
			LastTransitionTime: now,
		})
		if err := cl.Status().Update(ctx, &wl); err != nil {
			t.Fatalf("cl.Status().Update(admit) error: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("admit Reconcile() error: %v", err)
		}
		if err := cl.Get(ctx, req.NamespacedName, &live); err != nil {
			t.Fatalf("cl.Get(running) error: %v", err)
		}
		if live.Spec.Job.State != flinkv1beta1.JobStateRunning {
			t.Fatalf("expected running state after admission, got %q", live.Spec.Job.State)
		}

		// Step 4: evict Workload and reconcile; Stop() must pass ValidateUpdate and clear QuotaReserved.
		if err := cl.Get(ctx, wlKey, &wl); err != nil {
			t.Fatalf("cl.Get(wl before evict) error: %v", err)
		}
		apimeta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{
			Type:               kueue.WorkloadEvicted,
			Status:             metav1.ConditionTrue,
			Reason:             kueue.WorkloadEvictedByPreemption,
			Message:            "preempted",
			LastTransitionTime: now,
		})
		if err := cl.Status().Update(ctx, &wl); err != nil {
			t.Fatalf("cl.Status().Update(evict) error: %v", err)
		}
		if _, err := reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("eviction Reconcile() failed (Stop rejected by ValidateUpdate): %v", err)
		}
		if err := cl.Get(ctx, req.NamespacedName, &live); err != nil {
			t.Fatalf("cl.Get(suspended) error: %v", err)
		}
		if live.Spec.Job.State != flinkv1beta1.JobStateSuspended {
			t.Errorf("Spec.Job.State = %q, want suspended", live.Spec.Job.State)
		}
		wantRoleGates := []corev1.PodSchedulingGate{g2, gRole}
		if diff := cmp.Diff(wantRoleGates, live.Spec.TaskManager.PodTemplate.Spec.SchedulingGates); diff != "" {
			t.Errorf("restored TaskManager SchedulingGates mismatch (-want,+got):\n%s", diff)
		}
		if err := cl.Get(ctx, wlKey, &wl); err != nil {
			t.Fatalf("cl.Get(wl after evict) error: %v", err)
		}
		if apimeta.IsStatusConditionTrue(wl.Status.Conditions, kueue.WorkloadQuotaReserved) {
			t.Fatal("QuotaReserved leaked True after eviction Reconcile()")
		}
	})

	t.Run("suspended spec.podTemplate.spec.tolerations replacement and nil clearing do not wedge Stop or resurrect deleted tolerations", func(t *testing.T) {
		ctx, _ := utiltesting.ContextWithLog(t)
		tol1 := corev1.Toleration{Key: "tier", Operator: corev1.TolerationOpEqual, Value: "v1", Effect: corev1.TaintEffectNoSchedule}
		tol2 := corev1.Toleration{Key: "tier", Operator: corev1.TolerationOpEqual, Value: "v2", Effect: corev1.TaintEffectNoSchedule}

		initial := testingflinkdeployment.MakeFlinkDeployment("suspended-base-tols", "ns").
			Queue("lq").
			Suspend(true).
			PodTemplate(&corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Tolerations: []corev1.Toleration{tol1}},
			}).
			Obj()
		wlSnapshotPodSets, err := fromObject(initial.DeepCopy()).PodSets(ctx, nil)
		if err != nil {
			t.Fatalf("PodSets() error: %v", err)
		}
		restoreInfo := []podset.PodSetInfo{
			podset.FromPodSet(&wlSnapshotPodSets[0]),
			podset.FromPodSet(&wlSnapshotPodSets[1]),
		}
		wh := &flinkDeploymentWebhook{
			integrationManager:           newTestIntegrationManager(t),
			client:                       utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme).WithObjects(utiltesting.MakeNamespace("ns")).Build(),
			managedJobsNamespaceSelector: labels.Everything(),
		}

		// Case 1: replacing spec.podTemplate.spec.tolerations from [tol1] to [tol2] while suspended.
		replaced := initial.DeepCopy()
		replaced.Spec.PodTemplate.Spec.Tolerations = []corev1.Toleration{tol2}
		replacedAfterRestore := replaced.DeepCopy()
		if changed := fromObject(replacedAfterRestore).RestorePodSetsInfo(ctx, restoreInfo); changed {
			t.Errorf("RestorePodSetsInfo() after base toleration replacement changed = true, want false")
		}
		if replacedAfterRestore.Spec.JobManager.PodTemplate != nil || replacedAfterRestore.Spec.TaskManager.PodTemplate != nil {
			t.Fatalf("RestorePodSetsInfo() unexpectedly materialized role podTemplates: jm=%+v tm=%+v",
				replacedAfterRestore.Spec.JobManager.PodTemplate, replacedAfterRestore.Spec.TaskManager.PodTemplate)
		}
		if _, err := wh.ValidateUpdate(ctx, replaced, replacedAfterRestore); err != nil {
			t.Errorf("ValidateUpdate() rejected RestorePodSetsInfo after base toleration replacement: %v", err)
		}

		// Case 2: clearing spec.podTemplate = nil while suspended must not resurrect [tol1] onto role podTemplates.
		cleared := initial.DeepCopy()
		cleared.Spec.PodTemplate = nil
		clearedAfterRestore := cleared.DeepCopy()
		if changed := fromObject(clearedAfterRestore).RestorePodSetsInfo(ctx, restoreInfo); changed {
			t.Errorf("RestorePodSetsInfo() after clearing spec.podTemplate changed = true, want false")
		}
		if clearedAfterRestore.Spec.JobManager.PodTemplate != nil || clearedAfterRestore.Spec.TaskManager.PodTemplate != nil {
			t.Fatalf("RestorePodSetsInfo() resurrected deleted tolerations onto role podTemplates: jm=%+v tm=%+v",
				clearedAfterRestore.Spec.JobManager.PodTemplate, clearedAfterRestore.Spec.TaskManager.PodTemplate)
		}
		clearedPodSets, err := fromObject(clearedAfterRestore).PodSets(ctx, nil)
		if err != nil {
			t.Fatalf("clearedAfterRestore PodSets() error: %v", err)
		}
		for i, ps := range clearedPodSets {
			if len(ps.Template.Spec.Tolerations) != 0 {
				t.Errorf("podSet[%d] resurrected tolerations = %+v, want empty", i, ps.Template.Spec.Tolerations)
			}
		}

		// Case 3: non-prefix slice sharing trailing element with baseSlice is not suffix-trimmed.
		g0 := corev1.PodSchedulingGate{Name: "example.com/g0"}
		g1 := corev1.PodSchedulingGate{Name: "example.com/g1"}
		g2 := corev1.PodSchedulingGate{Name: "example.com/g2"}
		gotNonPrefix := restoreRoleSlice(
			[]corev1.PodSchedulingGate{g0, g2},
			[]corev1.PodSchedulingGate{g0, g2},
			[]corev1.PodSchedulingGate{g1, g2},
			true,
		)
		if diff := cmp.Diff([]corev1.PodSchedulingGate{g0, g2}, gotNonPrefix); diff != "" {
			t.Errorf("restoreRoleSlice() corrupted non-prefix slice (-want,+got):\n%s", diff)
		}
	})
}
