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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"gomodules.xyz/jsonpatch/v2"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/yaml"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	kueueconstants "sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	"sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingflinkdeployment "sigs.k8s.io/kueue/pkg/util/testingjobs/flinkdeployment"
	"sigs.k8s.io/kueue/pkg/workload"
)

func newTestIntegrationManager(t *testing.T) *jobframework.IntegrationManager {
	t.Helper()
	mgr := &jobframework.IntegrationManager{}
	if err := RegisterIntegration(mgr); err != nil {
		t.Fatalf("RegisterIntegration() error: %v", err)
	}
	mgr.EnableIntegration(FrameworkName)
	return mgr
}

func TestDefault(t *testing.T) {
	ancestorParent := testingflinkdeployment.MakeFlinkDeployment("parent-app", "ns").
		Queue("parent-lq").
		UID("uid-parent").
		Suspend(true).
		Obj()
	makeAncestorOwnedChild := func() *flinkv1beta1.FlinkDeployment {
		child := testingflinkdeployment.MakeFlinkDeployment("child-app", "ns").
			Queue("parent-lq").
			UID("uid-child").
			Suspend(false).
			Obj()
		child.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
				Kind:       "FlinkDeployment",
				Name:       ancestorParent.Name,
				UID:        ancestorParent.UID,
				Controller: new(true),
			},
		}
		return child
	}

	testCases := map[string]struct {
		job                        *flinkv1beta1.FlinkDeployment
		extraObjects               []client.Object
		defaultQueue               *kueue.LocalQueue
		nilQueues                  bool
		nilIntegrationManager      bool
		manageJobsWithoutQueueName bool
		want                       *flinkv1beta1.FlinkDeployment
	}{
		"suspends running Application job with queue-name": {
			job: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("user-queue").
				Suspend(false).
				Obj(),
			want: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("user-queue").
				Suspend(true).
				Obj(),
		},
		"leaves unmanaged job running when queue-name absent": {
			job: testingflinkdeployment.MakeFlinkDeployment("unmanaged", "ns").
				Suspend(false).
				Obj(),
			want: testingflinkdeployment.MakeFlinkDeployment("unmanaged", "ns").
				Suspend(false).
				Obj(),
		},
		"injects default LocalQueue and suspends running Application job": {
			job: testingflinkdeployment.MakeFlinkDeployment("default-q", "ns").
				Suspend(false).
				Obj(),
			defaultQueue: utiltestingapi.MakeLocalQueue("default", "ns").Obj(),
			want: testingflinkdeployment.MakeFlinkDeployment("default-q", "ns").
				Queue("default").
				Suspend(true).
				Obj(),
		},
		"does not inject default LocalQueue onto unlabeled Session cluster": {
			job: testingflinkdeployment.MakeFlinkDeployment("unlabeled-session", "ns").
				SessionCluster().
				Obj(),
			defaultQueue: utiltestingapi.MakeLocalQueue("default", "ns").Obj(),
			want: testingflinkdeployment.MakeFlinkDeployment("unlabeled-session", "ns").
				SessionCluster().
				Obj(),
		},
		"safe when queues manager is nil": {
			job: testingflinkdeployment.MakeFlinkDeployment("nil-queues", "ns").
				Queue("user-queue").
				Suspend(false).
				Obj(),
			nilQueues: true,
			want: testingflinkdeployment.MakeFlinkDeployment("nil-queues", "ns").
				Queue("user-queue").
				Suspend(true).
				Obj(),
		},
		"does not fabricate empty JobSpec on labeled Session cluster": {
			job: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("user-queue").
				SessionCluster().
				Obj(),
			want: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("user-queue").
				SessionCluster().
				Obj(),
		},
		"leaves child FlinkDeployment running when controller owner ancestor is Kueue-managed": {
			job:          makeAncestorOwnedChild(),
			extraObjects: []client.Object{ancestorParent},
			want:         makeAncestorOwnedChild(),
		},
		"nil integrationManager does not panic on controller-owned Application FlinkDeployment with default LocalQueue": {
			job:                   makeAncestorOwnedChild(),
			defaultQueue:          utiltestingapi.MakeLocalQueue("default", "ns").Obj(),
			nilIntegrationManager: true,
			want: func() *flinkv1beta1.FlinkDeployment {
				expected := makeAncestorOwnedChild()
				expected.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return expected
			}(),
		},
		"nil integrationManager with manageJobsWithoutQueueName=true suspends unlabeled Application FlinkDeployment": {
			job: testingflinkdeployment.MakeFlinkDeployment("unlabeled-nil-mgr", "ns").
				Suspend(false).
				Obj(),
			nilIntegrationManager:      true,
			manageJobsWithoutQueueName: true,
			want: testingflinkdeployment.MakeFlinkDeployment("unlabeled-nil-mgr", "ns").
				Suspend(true).
				Obj(),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)

			builder := utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme).WithObjects(&corev1.Namespace{
				Name: "ns",
			})
			if len(tc.extraObjects) > 0 {
				builder = builder.WithObjects(tc.extraObjects...)
			}
			if tc.defaultQueue != nil {
				builder = builder.WithObjects(tc.defaultQueue)
			}
			cl := builder.Build()

			var queues *qcache.Manager
			if !tc.nilQueues {
				cache := schdcache.New(cl)
				queues = qcache.NewManagerForUnitTests(cl, cache)
				if tc.defaultQueue != nil {
					if err := queues.AddLocalQueue(ctx, tc.defaultQueue); err != nil {
						t.Fatalf("AddLocalQueue() error: %v", err)
					}
				}
			}

			var integrationMgr *jobframework.IntegrationManager
			if !tc.nilIntegrationManager {
				integrationMgr = newTestIntegrationManager(t)
			}
			wh := &flinkDeploymentWebhook{
				integrationManager:           integrationMgr,
				client:                       cl,
				queues:                       queues,
				manageJobsWithoutQueueName:   tc.manageJobsWithoutQueueName,
				managedJobsNamespaceSelector: labels.Everything(),
			}

			got := tc.job.DeepCopy()
			if err := wh.Default(ctx, got); err != nil {
				t.Fatalf("Default() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Default() mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func validateAgainstJSONSchemaProps(val any, schema *apiextensionsv1.JSONSchemaProps, path string, isRoot bool) error {
	if schema == nil {
		return nil
	}
	if val == nil {
		if schema.Nullable {
			return nil
		}
		return fmt.Errorf("%s: null value not allowed for non-nullable schema type %q", path, schema.Type)
	}
	if schema.XIntOrString {
		switch val.(type) {
		case string, json.Number, float64:
			return nil
		default:
			return fmt.Errorf("%s: expected int-or-string, got %T", path, val)
		}
	}
	switch schema.Type {
	case "object":
		obj, ok := val.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: expected object, got %T", path, val)
		}
		preserveUnknown := schema.XPreserveUnknownFields != nil && *schema.XPreserveUnknownFields
		for _, req := range schema.Required {
			if _, exists := obj[req]; !exists {
				return fmt.Errorf("%s: missing required field %q", path, req)
			}
		}
		for k, v := range obj {
			if isRoot && (k == "apiVersion" || k == "kind" || k == "metadata") {
				continue
			}
			propSchema, known := schema.Properties[k]
			if known {
				if err := validateAgainstJSONSchemaProps(v, &propSchema, path+"."+k, false); err != nil {
					return err
				}
				continue
			}
			if schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil {
				if err := validateAgainstJSONSchemaProps(v, schema.AdditionalProperties.Schema, path+"."+k, false); err != nil {
					return err
				}
				continue
			}
			if !preserveUnknown && (schema.AdditionalProperties == nil || !schema.AdditionalProperties.Allows) && len(schema.Properties) > 0 {
				return fmt.Errorf("%s: unknown field %q", path, k)
			}
		}
	case "array":
		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("%s: expected array, got %T", path, val)
		}
		if schema.Items != nil && schema.Items.Schema != nil {
			for i, elem := range arr {
				if err := validateAgainstJSONSchemaProps(elem, schema.Items.Schema, fmt.Sprintf("%s[%d]", path, i), false); err != nil {
					return err
				}
			}
		}
	case "string":
		if _, ok := val.(string); !ok {
			return fmt.Errorf("%s: expected string, got %T", path, val)
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("%s: expected boolean, got %T", path, val)
		}
	case "integer", "number":
		if _, ok := val.(json.Number); !ok {
			if _, okFloat := val.(float64); !okFloat {
				return fmt.Errorf("%s: expected %s, got %T", path, schema.Type, val)
			}
		}
	}
	return nil
}

func TestDefaultPatchResponseValidatesAgainstFlinkCRD(t *testing.T) {
	crdPath := filepath.Join("..", "..", "..", "..", "dep-crds", "flink-operator-crds", "flinkdeployments.flink.apache.org.yaml")
	crdBytes, err := os.ReadFile(crdPath)
	if err != nil {
		t.Skipf("skipping CRD schema check because %s is not present: %v", crdPath, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(crdBytes, &crd); err != nil {
		t.Fatalf("yaml.Unmarshal(CRD) error: %v", err)
	}
	rootSchema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema

	rawRequest := []byte(`{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind": "FlinkDeployment",
		"metadata": {
			"name": "flink-crd-check",
			"namespace": "ns",
			"labels": {"kueue.x-k8s.io/queue-name": "user-queue"}
		},
		"spec": {
			"image": "flink:1.20",
			"flinkVersion": "v1_20",
			"podTemplate": {
				"apiVersion": "v1",
				"kind": "Pod",
				"spec": {
					"containers": [{"name": "sidecar", "image": "busybox:1.36", "resources": {}}],
					"volumes": [
						{"name": "proj", "projected": {"defaultMode": 420}},
						{"name": "rbd", "rbd": {"image": "img"}},
						{"name": "ceph", "cephfs": {"path": "/mnt"}}
					],
					"affinity": {
						"nodeAffinity": {
							"requiredDuringSchedulingIgnoredDuringExecution": {}
						}
					}
				}
			},
			"jobManager": {
				"resources": {
					"requests": {"cpu": 1, "memory": 1000000000}
				},
				"podTemplate": {
					"apiVersion": "v1",
					"kind": "Pod"
				}
			},
			"taskManager": {
				"resources": {
					"requests": {"cpu": "2", "memory": "1000000000"}
				},
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
				"parallelism": 2,
				"state": "running"
			}
		}
	}`)

	ctx, _ := utiltesting.ContextWithLog(t)
	cl := utiltesting.NewClientBuilder().WithObjects(&corev1.Namespace{Name: "ns"}).Build()
	wh := &flinkDeploymentWebhook{
		integrationManager:           newTestIntegrationManager(t),
		client:                       cl,
		managedJobsNamespaceSelector: labels.Everything(),
	}

	var decoded flinkv1beta1.FlinkDeployment
	if err := json.Unmarshal(rawRequest, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(rawRequest) error: %v", err)
	}
	if err := wh.Default(ctx, &decoded); err != nil {
		t.Fatalf("Default() error: %v", err)
	}

	marshaled, err := json.Marshal(&decoded)
	if err != nil {
		t.Fatalf("json.Marshal(decoded) error: %v", err)
	}

	resp := admission.PatchResponseFromRaw(rawRequest, marshaled)
	wantPatches := []jsonpatch.JsonPatchOperation{
		{
			Operation: "replace",
			Path:      "/spec/job/state",
			Value:     "suspended",
		},
	}
	if diff := cmp.Diff(wantPatches, resp.Patches); diff != "" {
		t.Errorf("PatchResponseFromRaw() unexpected patches (-want,+got):\n%s", diff)
	}

	decodedMap, err := flinkv1beta1.DecodeJSONMapUseNumber(marshaled)
	if err != nil {
		t.Fatalf("DecodeJSONMapUseNumber() error: %v", err)
	}
	if err := validateAgainstJSONSchemaProps(decodedMap, rootSchema, "FlinkDeployment", true); err != nil {
		t.Errorf("Default() marshaled JSON failed FlinkDeployment CRD OpenAPI validation: %v\njson: %s", err, string(marshaled))
	}
}

func TestValidateCreate(t *testing.T) {
	ancestorParent := testingflinkdeployment.MakeFlinkDeployment("parent-app", "ns").
		Queue("parent-lq").
		UID("uid-parent").
		Suspend(true).
		Obj()

	testCases := map[string]struct {
		job                        *flinkv1beta1.FlinkDeployment
		extraObjects               []client.Object
		nilIntegrationManager      bool
		manageJobsWithoutQueueName bool
		enableTAS                  bool
		disablePodSetMetadataCheck bool
		wantErrs                   field.ErrorList
	}{
		"valid managed Application FlinkDeployment with Flink MemorySize": {
			job: testingflinkdeployment.MakeFlinkDeployment("valid", "ns").
				Queue("lq").
				Obj(),
		},
		"valid managed Application FlinkDeployment with Kubernetes 2Gi memory": {
			job: testingflinkdeployment.MakeFlinkDeployment("valid-2gi", "ns").
				Queue("lq").
				TaskManagerResource(new(1.0), "2Gi", "").
				Obj(),
		},
		"valid managed Application FlinkDeployment with omitted CPU and split-ratio memory fraction": {
			job: testingflinkdeployment.MakeFlinkDeployment("valid-default-cpu", "ns").
				Queue("lq").
				JobManagerResource(nil, "1024m", "").
				TaskManagerResource(nil, "4096m", "").
				FlinkConfig("taskmanager.memory.managed.fraction", "0.4").
				Obj(),
		},
		"allows pipeline.jobvertex-parallelism-overrides and AdaptiveBatchScheduler parallelism keys in standalone mode when taskManager.replicas is explicitly set": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("valid-vertex-override", "ns").
					Queue("lq").
					Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
					TaskManagerReplicas(3).
					Obj()
				rawCfg := `{
					"pipeline.jobvertex-parallelism-overrides": "v1:8",
					"pipeline": {"jobvertex-parallelism-overrides": {"v2": "16"}},
					"execution.batch.adaptive.auto-parallelism.max-parallelism": "16",
					"execution.batch.adaptive.auto-parallelism.min-parallelism": "2",
					"execution.batch.adaptive.auto-parallelism.default-source-parallelism": "4",
					"jobmanager.adaptive-batch-scheduler.max-parallelism": "16",
					"jobmanager.adaptive-batch-scheduler.min-parallelism": "2",
					"jobmanager.adaptive-batch-scheduler.default-source-parallelism": "4"
				}`
				if err := json.Unmarshal([]byte(rawCfg), &fd.Spec.FlinkConfiguration); err != nil {
					t.Fatalf("json.Unmarshal(rawCfg) error: %v", err)
				}
				return fd
			}(),
		},
		"allows unmanaged Application FlinkDeployment with whitespace-padded CPU quantity in component resources": {
			job: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				raw := `{"metadata":{"name":"unmanaged-ws-cpu","namespace":"ns"},"spec":{"image":"flink:1.20","jobManager":{"resources":{"requests":{"cpu":" 500m","memory":"2g"}}},"taskManager":{"resources":{"requests":{"cpu":"1 ","memory":"2Gi"}}},"job":{"jarURI":"local:///app.jar","parallelism":1,"state":"running"}}}`
				if err := json.Unmarshal([]byte(raw), &fd); err != nil {
					t.Fatalf("json.Unmarshal(unmanaged-ws-cpu) error: %v", err)
				}
				return &fd
			}(),
		},
		"allows 1Pi in spec.<role>.resource.memory": {
			job: testingflinkdeployment.MakeFlinkDeployment("valid-1pi-resource", "ns").
				Queue("lq").
				TaskManagerResource(new(1.0), "1Pi", "").
				Obj(),
		},
		"allows unmanaged Application FlinkDeployment with Flink MemorySize 2g in component resources": {
			job: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				raw := `{"metadata":{"name":"unmanaged-2g","namespace":"ns"},"spec":{"image":"flink:1.20","jobManager":{"resources":{"requests":{"cpu":"1","memory":"2g"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"2048mb"}}},"job":{"jarURI":"local:///app.jar","parallelism":2,"state":"running"}}}`
				if err := json.Unmarshal([]byte(raw), &fd); err != nil {
					t.Fatalf("json.Unmarshal(unmanaged-2g) error: %v", err)
				}
				return &fd
			}(),
		},
		"allows false on kubernetes.operator.deployment.rollback.enabled": {
			job: testingflinkdeployment.MakeFlinkDeployment("rollback-false", "ns").
				Queue("lq").
				FlinkConfig("kubernetes.operator.deployment.rollback.enabled", "false").
				Obj(),
		},
		"allows component memory specified via resources.limits.memory": {
			job: testingflinkdeployment.MakeFlinkDeployment("valid-limits-mem", "ns").
				Queue("lq").
				JobManagerResources(corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				}).
				TaskManagerResources(corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				}).
				Obj(),
		},
		"allows standalone mode when spec.taskManager CPU is explicitly specified even if spec.jobManager CPU is omitted": {
			job: testingflinkdeployment.MakeFlinkDeployment("valid-standalone", "ns").
				Queue("lq").
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				JobManagerResource(nil, "1024m", "").
				TaskManagerResource(new(2.0), "2048m", "").
				Obj(),
		},
		"allows omitted spec.job.parallelism when spec.taskManager.replicas is explicitly set": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("valid-explicit-replicas", "ns").
					Queue("lq").
					TaskManagerReplicas(2).
					Obj()
				fd.Spec.Job.Parallelism = nil
				return fd
			}(),
		},
		"allows false on autoscaler keys in flinkConfiguration": {
			job: testingflinkdeployment.MakeFlinkDeployment("autoscaler-false", "ns").
				Queue("lq").
				FlinkConfig("job.autoscaler.enabled", "false").
				FlinkConfig("kubernetes.operator.job.autoscaler.enabled", "FALSE").
				Obj(),
		},
		"allows distinct per-role TAS topology modes without conflicting root TAS mode annotations": {
			job: testingflinkdeployment.MakeFlinkDeployment("valid-tas-per-role", "ns").
				Queue("lq").
				TaskManagerReplicas(2).
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetUnconstrainedTopologyAnnotation: "true",
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
						kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodSetSliceSizeAnnotation:             "2",
					},
				}).
				Obj(),
			enableTAS: true,
		},
		"rejects conflicting root and role TAS mode annotations matching FlinkUtils.mergePodTemplates": {
			job: testingflinkdeployment.MakeFlinkDeployment("conflicting-tas-override", "ns").
				Queue("lq").
				TaskManagerReplicas(2).
				PodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.provider.com/rack",
					},
				}).
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetUnconstrainedTopologyAnnotation: "true",
					},
				}).
				Obj(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "podTemplate", "metadata", "annotations"),
					nil,
					`must not contain more than one topology annotation: ["kueue.x-k8s.io/podset-required-topology", `+
						`"kueue.x-k8s.io/podset-preferred-topology", "kueue.x-k8s.io/podset-unconstrained-topology"]`,
				),
			},
		},
		"rejects Session cluster (spec.job == nil) when managed": {
			job: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("lq").
				SessionCluster().
				Obj(),
			wantErrs: field.ErrorList{
				field.Required(field.NewPath("spec", "job"), "only Application mode (spec.job != nil) is supported for a Kueue-managed FlinkDeployment"),
			},
		},
		"rejects Session cluster with explicit empty kueue.x-k8s.io/queue-name label": {
			job: testingflinkdeployment.MakeFlinkDeployment("empty-queue-session", "ns").
				Queue("").
				SessionCluster().
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata", "labels").Key(constants.QueueLabel),
					"",
					`a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')`,
				),
			},
		},
		"allows unmanaged Session cluster when queue-name absent even with manageJobsWithoutQueueName=true and generic kueue metadata": {
			job: testingflinkdeployment.MakeFlinkDeployment("unmanaged-session", "ns").
				Label(constants.PrebuiltWorkloadLabel, "NOT_DNS_1123").
				Label(constants.MaxExecTimeSecondsLabel, "not-an-int").
				Annotation(constants.WaitForPodsReadyAnnotation, "invalid").
				SessionCluster().
				Obj(),
			manageJobsWithoutQueueName: true,
		},
		"validates unlabeled Application job as managed when manageJobsWithoutQueueName=true": {
			job: testingflinkdeployment.MakeFlinkDeployment("managed-no-queue", "ns").
				Parallelism(0).
				Obj(),
			manageJobsWithoutQueueName: true,
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job", "parallelism"), int64(0), "must be greater than or equal to 1"),
			},
		},
		"nil integrationManager with manageJobsWithoutQueueName=true validates unlabeled Application job as managed": {
			job: testingflinkdeployment.MakeFlinkDeployment("managed-no-queue-nil-mgr", "ns").
				Parallelism(0).
				Obj(),
			nilIntegrationManager:      true,
			manageJobsWithoutQueueName: true,
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job", "parallelism"), int64(0), "must be greater than or equal to 1"),
			},
		},
		"rejects missing JobManager and TaskManager memory on managed job": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("missing-mem", "ns").Queue("lq").Obj()
				fd.Spec.JobManager = nil
				fd.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{
					Resource: &flinkv1beta1.Resource{CPU: new(2.0)},
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Required(field.NewPath("spec", "jobManager", "resource", "memory"), "memory must be specified via spec.jobManager.resource.memory or spec.jobManager.resources"),
				field.Required(field.NewPath("spec", "taskManager", "resource", "memory"), "memory must be specified via spec.taskManager.resource.memory or spec.taskManager.resources"),
			},
		},
		"rejects podTemplate flink-main-container memory quota escape when component memory is omitted": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("podtemplate-mem-escape", "ns").
					Queue("lq").
					PodTemplate(&corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: flinkMainContainerName,
									Resources: corev1.ResourceRequirements{
										Requests: corev1.ResourceList{
											corev1.ResourceMemory: resource.MustParse("1Mi"),
										},
									},
								},
							},
						},
					}).
					Obj()
				fd.Spec.JobManager = nil
				fd.Spec.TaskManager = &flinkv1beta1.TaskManagerSpec{
					Resources: &flinkv1beta1.ResourceRequirements{
						Requests: flinkv1beta1.ResourceList{
							corev1.ResourceCPU: flinkv1beta1.MustParseQuantity("1"),
						},
					},
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Required(field.NewPath("spec", "jobManager", "resource", "memory"), "memory must be specified via spec.jobManager.resource.memory or spec.jobManager.resources"),
				field.Required(
					field.NewPath("spec", "taskManager", "resources", "requests").Key("memory"),
					"memory must be specified via spec.taskManager.resource.memory or spec.taskManager.resources",
				),
			},
		},
		"rejects standalone mode when spec.taskManager CPU is omitted": {
			job: testingflinkdeployment.MakeFlinkDeployment("standalone-missing-tm-cpu", "ns").
				Queue("lq").
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				JobManagerResource(nil, "1024m", "").
				TaskManagerResource(nil, "2048m", "").
				Obj(),
			wantErrs: field.ErrorList{
				field.Required(
					field.NewPath("spec", "taskManager", "resource", "cpu"),
					"cpu must be specified via spec.taskManager.resource.cpu or spec.taskManager.resources when spec.mode is standalone",
				),
			},
		},
		"rejects omitted spec.job.parallelism when spec.taskManager.replicas is omitted even if parallelism.default is set": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("missing-parallelism", "ns").
					Queue("lq").
					FlinkConfig("parallelism.default", "4").
					Obj()
				fd.Spec.Job.Parallelism = nil
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Required(field.NewPath("spec", "job", "parallelism"), "must be specified when spec.taskManager.replicas is not set"),
			},
		},
		"rejects DRA claims on component resources": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-claims", "ns").
				Queue("lq").
				TaskManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
					Claims: []corev1.ResourceClaim{{Name: "gpu-claim"}},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "taskManager", "resources", "claims"),
					"Flink Kubernetes Operator ignores claims in component resources; configure container resources.claims and pod spec.resourceClaims in spec.podTemplate or component podTemplate instead",
				),
			},
		},
		"rejects invalid memory unit": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-mem", "ns").
				Queue("lq").
				TaskManagerResource(new(1.0), "2xyz", "").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "resource", "memory"),
					"2xyz",
					`invalid memory quantity "2xyz": must use Flink MemorySize units or an integer Kubernetes suffix (Ki, Mi, Gi, Ti, Pi, K, M, G, T, P)`,
				),
			},
		},
		"rejects negative decimal exponent, Ei suffix, and 2Pi overflow in component resource.memory": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-exp-mem", "ns").
				Queue("lq").
				JobManagerResource(new(1.0), "2147483648000e-3", "").
				TaskManagerResource(new(1.0), "2Pi", "").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resource", "memory"),
					"2147483648000e-3",
					`invalid memory quantity "2147483648000e-3": must use Flink MemorySize units or an integer Kubernetes suffix (Ki, Mi, Gi, Ti, Pi, K, M, G, T, P)`,
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "resource", "memory"),
					"2Pi",
					`memory size "2Pi" exceeds maximum supported Flink memory size (2147483647 MiB)`,
				),
			},
		},
		"rejects 1Ei in component resource.memory and 2Pi in component resources": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-ei-and-2pi", "ns").
				Queue("lq").
				JobManagerResource(new(1.0), "1Ei", "").
				TaskManagerRawResources(flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceMemory: flinkv1beta1.MustParseQuantity("2Pi"),
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resource", "memory"),
					"1Ei",
					`invalid memory quantity "1Ei": must use Flink MemorySize units or an integer Kubernetes suffix (Ki, Mi, Gi, Ti, Pi, K, M, G, T, P)`,
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource request memory (2Pi): memory size "2Pi" exceeds maximum supported Flink memory size (2147483647 MiB)`,
				),
			},
		},
		"rejects non-positive CPU": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-cpu", "ns").
				Queue("lq").
				JobManagerResource(new(0.0), "1024m", "").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "jobManager", "resource", "cpu"), 0.0, "cpu value 0 must be greater than 0"),
			},
		},
		"rejects non-positive parallelism": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-p", "ns").
				Queue("lq").
				Parallelism(0).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job", "parallelism"), int64(0), "must be greater than or equal to 1"),
			},
		},
		"rejects replicas and parallelism exceeding math.MaxInt32 on managed job": {
			job: testingflinkdeployment.MakeFlinkDeployment("overflow-counts", "ns").
				Queue("lq").
				JobManagerReplicas(int64(math.MaxInt32) + 1).
				TaskManagerReplicas(int64(math.MaxInt32) + 1).
				Parallelism(int64(math.MaxInt32) + 1).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job", "parallelism"), int64(math.MaxInt32)+1, "must be less than or equal to 2147483647"),
				field.Invalid(field.NewPath("spec", "jobManager", "replicas"), int64(math.MaxInt32)+1, "must be less than or equal to 2147483647"),
				field.Invalid(field.NewPath("spec", "taskManager", "replicas"), int64(math.MaxInt32)+1, "must be less than or equal to 2147483647"),
			},
		},
		"allows unmanaged Application job with replicas and parallelism exceeding math.MaxInt32": {
			job: testingflinkdeployment.MakeFlinkDeployment("unmanaged-64bit", "ns").
				JobManagerReplicas(int64(math.MaxInt32) + 1).
				TaskManagerReplicas(int64(math.MaxInt32) + 1).
				Parallelism(int64(math.MaxInt32) + 1).
				Obj(),
		},
		"rejects dynamic Flink autoscaler in flinkConfiguration": {
			job: testingflinkdeployment.MakeFlinkDeployment("autoscaler", "ns").
				Queue("lq").
				FlinkConfig("job.autoscaler.enabled", "true").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("job.autoscaler.enabled"),
					"dynamic Flink autoscaling is not supported for a Kueue-managed FlinkDeployment",
				),
			},
		},
		"rejects flinkConfiguration process memory, fine-grained memory, deprecated network memory, CPU cores, request-factor, internal replicas, placement/metadata keys, external-resource GPU, and vertex parallelism overrides": {
			job: testingflinkdeployment.MakeFlinkDeployment("forbidden-conf", "ns").
				Queue("lq").
				FlinkConfig("external-resource.gpu.amount", "4").
				FlinkConfig("jobmanager.memory.heap.size", "1024m").
				FlinkConfig("kubernetes.internal.taskmanager.replicas", "50").
				FlinkConfig("kubernetes.jobmanager.annotations.kueue.x-k8s.io/podset-required-topology", "rack").
				FlinkConfig("kubernetes.jobmanager.labels", "tier:batch").
				FlinkConfig("kubernetes.taskmanager.cpu.request-factor", "0.5").
				FlinkConfig("kubernetes.taskmanager.node-selector.pool", "gpu").
				FlinkConfig("kubernetes.taskmanager.tolerations", "key:spot,operator:Exists").
				FlinkConfig("pipeline.jobvertex-parallelism-overrides", "v1:16").
				FlinkConfig("taskmanager.cpu.cores", "8").
				FlinkConfig("taskmanager.memory.managed.size", "2048m").
				FlinkConfig("taskmanager.memory.process.size", "8192m").
				FlinkConfig("taskmanager.network.memory.max", "1m").
				FlinkConfig("taskmanager.network.memory.min", "64g").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("external-resource.gpu.amount"),
					"configure extended resources such as GPUs in spec.jobManager.resources, spec.taskManager.resources, or podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("jobmanager.memory.heap.size"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.internal.taskmanager.replicas"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.jobmanager.annotations.kueue.x-k8s.io/podset-required-topology"),
					"configure pod templates, placement, tolerations, labels, and annotations in spec.podTemplate, spec.jobManager.podTemplate, or spec.taskManager.podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.jobmanager.labels"),
					"configure pod templates, placement, tolerations, labels, and annotations in spec.podTemplate, spec.jobManager.podTemplate, or spec.taskManager.podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.taskmanager.cpu.request-factor"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.taskmanager.node-selector.pool"),
					"configure pod templates, placement, tolerations, labels, and annotations in spec.podTemplate, spec.jobManager.podTemplate, or spec.taskManager.podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.taskmanager.tolerations"),
					"configure pod templates, placement, tolerations, labels, and annotations in spec.podTemplate, spec.jobManager.podTemplate, or spec.taskManager.podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("pipeline.jobvertex-parallelism-overrides"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when pipeline.jobvertex-parallelism-overrides is configured",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("taskmanager.cpu.cores"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("taskmanager.memory.managed.size"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("taskmanager.memory.process.size"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("taskmanager.network.memory.max"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("taskmanager.network.memory.min"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
			},
		},
		"rejects AdaptiveBatchScheduler auto-parallelism keys and deprecated aliases in native mode or without explicit taskManager.replicas": {
			job: testingflinkdeployment.MakeFlinkDeployment("adaptive-batch-native", "ns").
				Queue("lq").
				Mode(flinkv1beta1.KubernetesDeploymentModeNative).
				TaskManagerReplicas(2).
				FlinkConfig("execution.batch.adaptive.auto-parallelism.default-source-parallelism", "8").
				FlinkConfig("execution.batch.adaptive.auto-parallelism.max-parallelism", "16").
				FlinkConfig("execution.batch.adaptive.auto-parallelism.min-parallelism", "2").
				FlinkConfig("jobmanager.adaptive-batch-scheduler.default-source-parallelism", "8").
				FlinkConfig("jobmanager.adaptive-batch-scheduler.max-parallelism", "16").
				FlinkConfig("jobmanager.adaptive-batch-scheduler.min-parallelism", "2").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("execution.batch.adaptive.auto-parallelism.default-source-parallelism"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when execution.batch.adaptive.auto-parallelism.default-source-parallelism is configured",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("execution.batch.adaptive.auto-parallelism.max-parallelism"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when execution.batch.adaptive.auto-parallelism.max-parallelism is configured",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("execution.batch.adaptive.auto-parallelism.min-parallelism"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when execution.batch.adaptive.auto-parallelism.min-parallelism is configured",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("jobmanager.adaptive-batch-scheduler.default-source-parallelism"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when jobmanager.adaptive-batch-scheduler.default-source-parallelism is configured",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("jobmanager.adaptive-batch-scheduler.max-parallelism"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when jobmanager.adaptive-batch-scheduler.max-parallelism is configured",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("jobmanager.adaptive-batch-scheduler.min-parallelism"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when jobmanager.adaptive-batch-scheduler.min-parallelism is configured",
				),
			},
		},
		"rejects Go strconv.ParseBool shorthands on boolean flinkConfiguration keys": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-bools", "ns").
				Queue("lq").
				FlinkConfig("job.autoscaler.enabled", "1").
				FlinkConfig("kubernetes.operator.deployment.rollback.enabled", "1").
				FlinkConfig("kubernetes.operator.job.autoscaler.enabled", "t").
				FlinkConfig("kubernetes.operator.job.restart.failed", "yes").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "1").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("job.autoscaler.enabled"), "1", `invalid boolean value "1": must be "true" or "false"`),
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.deployment.rollback.enabled"), "1", `invalid boolean value "1": must be "true" or "false"`),
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.job.autoscaler.enabled"), "t", `invalid boolean value "t": must be "true" or "false"`),
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.job.restart.failed"), "yes", `invalid boolean value "yes": must be "true" or "false"`),
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.pod-template.merge-arrays-by-name"), "1", `invalid boolean value "1": must be "true" or "false"`),
			},
		},
		"rejects null, empty, and whitespace-padded values on parsed flinkConfiguration keys": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("null-empty-ws-conf", "ns").Queue("lq").Obj()
				rawCfg := `{
					"job.autoscaler.enabled": null,
					"kubernetes.operator.deployment.rollback.enabled": " false ",
					"kubernetes.operator.job.autoscaler.enabled": "",
					"kubernetes.operator.job.restart.failed": "   ",
					"kubernetes.operator.pod-template.merge-arrays-by-name": null,
					"taskmanager.numberOfTaskSlots": " 4 "
				}`
				if err := json.Unmarshal([]byte(rawCfg), &fd.Spec.FlinkConfiguration); err != nil {
					t.Fatalf("json.Unmarshal(rawCfg) error: %v", err)
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("job.autoscaler.enabled"), "null", `invalid boolean value "null": must be "true" or "false"`),
				field.Invalid(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.deployment.rollback.enabled"),
					" false ",
					`invalid boolean value " false ": must be "true" or "false"`,
				),
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.job.autoscaler.enabled"), "", `invalid boolean value "": must be "true" or "false"`),
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.job.restart.failed"), "   ", `invalid boolean value "   ": must be "true" or "false"`),
				field.Invalid(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.pod-template.merge-arrays-by-name"),
					"null",
					`invalid boolean value "null": must be "true" or "false"`,
				),
				field.Invalid(
					field.NewPath("spec", "flinkConfiguration").Key("taskmanager.numberOfTaskSlots"),
					" 4 ",
					`invalid taskmanager.numberOfTaskSlots " 4 ": strconv.ParseInt: parsing " 4 ": invalid syntax`,
				),
			},
		},
		"rejects kubernetes.operator.deployment.rollback.enabled=true because in-memory rollback restores stale placement and parallelism": {
			job: testingflinkdeployment.MakeFlinkDeployment("rollback-enabled", "ns").
				Queue("lq").
				FlinkConfig("kubernetes.operator.deployment.rollback.enabled", "true").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.deployment.rollback.enabled"),
					"automatic Flink Operator deployment rollback is not supported for a Kueue-managed FlinkDeployment because in-memory rollback restores lastStableSpec placement and parallelism without updating spec",
				),
			},
		},
		"rejects pipeline.jobvertex-parallelism-overrides in native mode even when taskManager.replicas is explicitly set": {
			job: testingflinkdeployment.MakeFlinkDeployment("native-vertex-override", "ns").
				Queue("lq").
				Mode(flinkv1beta1.KubernetesDeploymentModeNative).
				TaskManagerReplicas(2).
				FlinkConfig("pipeline.jobvertex-parallelism-overrides", "v1:16").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("pipeline.jobvertex-parallelism-overrides"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when pipeline.jobvertex-parallelism-overrides is configured",
				),
			},
		},
		"rejects flat and nested pipeline.jobvertex-parallelism-overrides.<vertexId> subkeys in native mode and in standalone mode without explicit taskManager.replicas": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("standalone-subkey-no-replicas", "ns").
					Queue("lq").
					Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
					TaskManagerResource(new(1.0), "2048m", "").
					Obj()
				rawCfg := `{
					"pipeline.jobvertex-parallelism-overrides.vertex-flat": "32",
					"pipeline": {"jobvertex-parallelism-overrides": {"vertex-nested": "64"}}
				}`
				if err := json.Unmarshal([]byte(rawCfg), &fd.Spec.FlinkConfiguration); err != nil {
					t.Fatalf("json.Unmarshal(rawCfg) error: %v", err)
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("pipeline.jobvertex-parallelism-overrides.vertex-flat"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when pipeline.jobvertex-parallelism-overrides.vertex-flat is configured",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("pipeline.jobvertex-parallelism-overrides.vertex-nested"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when pipeline.jobvertex-parallelism-overrides.vertex-nested is configured",
				),
			},
		},
		"rejects whitespace-padded non-memory quantities in component resources and resource.ephemeralStorage": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("ws-non-mem", "ns").
					Queue("lq").
					JobManagerResource(new(1.0), "1024m", " 2Gi").
					Obj()
				fd.Spec.TaskManager.Resource = nil
				if err := json.Unmarshal(
					[]byte(`{"requests":{"cpu":" 500m","memory":"2Gi"},"limits":{"ephemeral-storage":"4Gi "}}`),
					&fd.Spec.TaskManager.Resources,
				); err != nil {
					t.Fatalf("json.Unmarshal(resources) error: %v", err)
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resource", "ephemeralStorage"),
					" 2Gi",
					`ephemeralStorage " 2Gi" must not contain whitespace`,
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource request cpu (" 500m") must not contain whitespace`,
				),
			},
		},
		"rejects managed Application FlinkDeployment with Flink MemorySize 2g in component resources": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("managed-2g", "ns").Queue("lq").Obj()
				fd.Spec.JobManager.Resource = nil
				if err := json.Unmarshal([]byte(`{"requests":{"cpu":"1","memory":"2g"}}`), &fd.Spec.JobManager.Resources); err != nil {
					t.Fatalf("json.Unmarshal(resources) error: %v", err)
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resources"),
					nil,
					`resource request memory (2g): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes because Flink interprets "2g" incompatibly or rejects it`,
				),
			},
		},
		"rejects kubernetes.operator.job.restart.failed=true because the operator would resubmit a FAILED job after quota release": {
			job: testingflinkdeployment.MakeFlinkDeployment("restart-failed", "ns").
				Queue("lq").
				FlinkConfig("kubernetes.operator.job.restart.failed", "true").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.job.restart.failed"),
					"the Flink Operator would resubmit a FAILED job after Kueue has released its quota; rely on Kueue requeueing and backoff (for example via waitForPodsReady) instead",
				),
			},
		},
		"allows kubernetes.operator.job.restart.failed=false": {
			job: testingflinkdeployment.MakeFlinkDeployment("restart-failed-off", "ns").
				Queue("lq").
				FlinkConfig("kubernetes.operator.job.restart.failed", "false").
				Obj(),
		},
		"rejects zero CPU and sub-byte memory in component resources": {
			job: testingflinkdeployment.MakeFlinkDeployment("zero-cpu-subbyte-mem", "ns").
				Queue("lq").
				JobManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("0"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				}).
				TaskManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1m"),
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "jobManager", "resources"), nil, "resource request cpu (0) must be greater than 0"),
				field.Invalid(field.NewPath("spec", "taskManager", "resources"), nil, "resource request memory (1m) must be at least 1 byte"),
			},
		},
		"rejects whitespace-padded nested key segment in flinkConfiguration": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("ws-conf", "ns").Queue("lq").Obj()
				_ = json.Unmarshal([]byte(`{"jobmanager": {" memory": {"process": {"size": "64g"}}}}`), &fd.Spec.FlinkConfiguration)
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "flinkConfiguration"),
					field.OmitValueType{},
					`flinkConfiguration key " memory" must not contain leading or trailing whitespace`,
				),
			},
		},
		"rejects duplicate flat and nested flinkConfiguration keys": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("dup-conf", "ns").Queue("lq").Obj()
				_ = json.Unmarshal([]byte(`{"parallelism": {"default": 4}, "parallelism.default": "1"}`), &fd.Spec.FlinkConfiguration)
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "flinkConfiguration"),
					field.OmitValueType{},
					`duplicate flattened flinkConfiguration key "parallelism.default"`,
				),
			},
		},
		"rejects empty flinkConfiguration sizing key": {
			job: testingflinkdeployment.MakeFlinkDeployment("empty-mem-conf", "ns").
				Queue("lq").
				FlinkConfig("jobmanager.memory.process.size", "   ").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("jobmanager.memory.process.size"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
			},
		},
		"rejects specifying both resource and resources on the same component": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("both-res", "ns").
					Queue("lq").
					Obj()
				fd.Spec.TaskManager.Resources = &flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceCPU: flinkv1beta1.MustParseQuantity("2"),
					},
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Forbidden(field.NewPath("spec", "taskManager", "resources"), "may not be specified when resource is also specified"),
			},
		},
		"accepts plain integer byte literals, JSON number 1000000000, and 1Pi in component resources": {
			job: func() *flinkv1beta1.FlinkDeployment {
				var numQty flinkv1beta1.Quantity
				if err := json.Unmarshal([]byte(`1000000000`), &numQty); err != nil {
					t.Fatalf("json.Unmarshal(1000000000) error: %v", err)
				}
				return testingflinkdeployment.MakeFlinkDeployment("plain-int-bytes", "ns").
					Queue("lq").
					JobManagerRawResources(flinkv1beta1.ResourceRequirements{
						Requests: flinkv1beta1.ResourceList{
							corev1.ResourceCPU:    flinkv1beta1.MustParseQuantity("1"),
							corev1.ResourceMemory: flinkv1beta1.MustParseQuantity("1024000"),
						},
						Limits: flinkv1beta1.ResourceList{
							corev1.ResourceMemory: numQty,
						},
					}).
					TaskManagerRawResources(flinkv1beta1.ResourceRequirements{
						Requests: flinkv1beta1.ResourceList{
							corev1.ResourceCPU:    flinkv1beta1.MustParseQuantity("2"),
							corev1.ResourceMemory: flinkv1beta1.MustParseQuantity("1073741824"),
						},
						Limits: flinkv1beta1.ResourceList{
							corev1.ResourceMemory: flinkv1beta1.MustParseQuantity("1Pi"),
						},
					}).
					Obj()
			}(),
		},
		"rejects fractional BinarySI memory literals and Ei suffixes in component resources.requests": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-raw-mem-requests", "ns").
				Queue("lq").
				JobManagerRawResources(flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceMemory: flinkv1beta1.MustParseQuantity("1.5Gi"),
					},
				}).
				TaskManagerRawResources(flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceMemory: flinkv1beta1.MustParseQuantity("8Ei"),
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resources"),
					nil,
					`resource request memory (1.5Gi): memory size "1.5Gi" must not contain a decimal point; must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes`,
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource request memory (8Ei): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes because Flink interprets "8Ei" incompatibly or rejects it`,
				),
			},
		},
		"rejects scientific notation and leading plus sign in component resources.limits": {
			job: func() *flinkv1beta1.FlinkDeployment {
				var sciQty, plusQty flinkv1beta1.Quantity
				if err := json.Unmarshal([]byte(`"1e9"`), &sciQty); err != nil {
					t.Fatalf("json.Unmarshal(1e9) error: %v", err)
				}
				if err := json.Unmarshal([]byte(`"+1Gi"`), &plusQty); err != nil {
					t.Fatalf("json.Unmarshal(+1Gi) error: %v", err)
				}
				return testingflinkdeployment.MakeFlinkDeployment("bad-raw-mem-limits", "ns").
					Queue("lq").
					JobManagerRawResources(flinkv1beta1.ResourceRequirements{
						Requests: flinkv1beta1.ResourceList{
							corev1.ResourceMemory: flinkv1beta1.MustParseQuantity("512Mi"),
						},
						Limits: flinkv1beta1.ResourceList{
							corev1.ResourceMemory: sciQty,
						},
					}).
					TaskManagerRawResources(flinkv1beta1.ResourceRequirements{
						Requests: flinkv1beta1.ResourceList{
							corev1.ResourceMemory: flinkv1beta1.MustParseQuantity("512Mi"),
						},
						Limits: flinkv1beta1.ResourceList{
							corev1.ResourceMemory: plusQty,
						},
					}).
					Obj()
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resources"),
					nil,
					`resource limit memory (1e9): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes because Flink interprets "1e9" incompatibly or rejects it`,
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource limit memory (+1Gi): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes, got "+1Gi"`,
				),
			},
		},
		"accepts component resources request above an inherited podTemplate limit because Flink replaces both cpu and memory requests and limits": {
			job: testingflinkdeployment.MakeFlinkDeployment("cross-layer", "ns").
				Queue("lq").
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
		},
		"rejects Flink MemorySize and DecimalSI memory suffixes in component resources": {
			job: testingflinkdeployment.MakeFlinkDeployment("ambiguous-mem-units", "ns").
				Queue("lq").
				JobManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("2048m"),
					},
				}).
				TaskManagerResources(corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("2G"),
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resources"),
					nil,
					`resource request memory (2048m): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes because Flink interprets "2048m" incompatibly or rejects it`,
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource request memory (2G): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes because Flink interprets "2G" incompatibly or rejects it`,
				),
			},
		},
		"rejects slotmanager sizing keys and kubernetes.pod-template-file keys in flinkConfiguration": {
			job: testingflinkdeployment.MakeFlinkDeployment("slotmanager-conf", "ns").
				Queue("lq").
				FlinkConfig("kubernetes.pod-template-file", "/opt/flink/pod-template.yaml").
				FlinkConfig("kubernetes.pod-template-file.taskmanager", "/opt/flink/tm-pod-template.yaml").
				FlinkConfig("slotmanager.min-total-resource.cpu", "16").
				FlinkConfig("slotmanager.min-total-resource.memory", "64g").
				FlinkConfig("slotmanager.number-of-slots.min", "8").
				FlinkConfig("slotmanager.redundant-taskmanager-num", "2").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.pod-template-file"),
					"configure pod templates, placement, tolerations, labels, and annotations in spec.podTemplate, spec.jobManager.podTemplate, or spec.taskManager.podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.pod-template-file.taskmanager"),
					"configure pod templates, placement, tolerations, labels, and annotations in spec.podTemplate, spec.jobManager.podTemplate, or spec.taskManager.podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("slotmanager.min-total-resource.cpu"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("slotmanager.min-total-resource.memory"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("slotmanager.number-of-slots.min"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("slotmanager.redundant-taskmanager-num"),
					"configure component resources and replicas in spec.jobManager and spec.taskManager instead of spec.flinkConfiguration",
				),
			},
		},
		"rejects flinkConfiguration serviceAccount, container image, pull-policy, pull-secrets, and hostnetwork decorator keys": {
			job: testingflinkdeployment.MakeFlinkDeployment("forbidden-identity-image-hostnet", "ns").
				Queue("lq").
				FlinkConfig("kubernetes.container.image", "evil/flink:old-key").
				FlinkConfig("kubernetes.container.image.pull-policy", "Always").
				FlinkConfig("kubernetes.container.image.pull-secrets", "secret-a;secret-b").
				FlinkConfig("kubernetes.container.image.ref", "evil/flink:latest").
				FlinkConfig("kubernetes.hostnetwork.enabled", "true").
				FlinkConfig("kubernetes.jobmanager.service-account", "jm-admin").
				FlinkConfig("kubernetes.service-account", "cluster-admin").
				FlinkConfig("kubernetes.taskmanager.service-account", "tm-admin").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.container.image"),
					"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.container.image.pull-policy"),
					"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.container.image.pull-secrets"),
					"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.container.image.ref"),
					"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.hostnetwork.enabled"),
					"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.jobmanager.service-account"),
					"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.service-account"),
					"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.taskmanager.service-account"),
					"configure serviceAccount, image, imagePullPolicy, imagePullSecrets, and hostNetwork in spec.serviceAccount, spec.image, spec.imagePullPolicy, or podTemplate instead of spec.flinkConfiguration",
				),
			},
		},
		"attributes each PodSet's TAS errors to the template supplying its TAS annotations and deduplicates root-only errors": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-tas", "ns").
				Queue("lq").
				PodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetSliceSizeAnnotation: "2",
					},
				}).
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodIndexOffsetAnnotation:         "-1",
					},
				}).
				Obj(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				// job-manager inherits only root TAS annotations, so its errors point at spec.podTemplate.
				field.Forbidden(
					field.NewPath("spec", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceSizeAnnotation),
					"may not be set when 'kueue.x-k8s.io/podset-slice-required-topology' is not specified",
				),
				field.Invalid(
					field.NewPath("spec", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceSizeAnnotation),
					"2",
					"must not be greater than pod set count 1",
				),
				// task-manager overrides TAS annotations in its role template, so its effective
				// (merged) annotation set is attributed to spec.taskManager.podTemplate.
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodIndexOffsetAnnotation),
					"-1",
					"must be a non-negative integer",
				),
				field.Forbidden(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceSizeAnnotation),
					"may not be set when 'kueue.x-k8s.io/podset-slice-required-topology' is not specified",
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceSizeAnnotation),
					"2",
					"must not be greater than pod set count 1",
				),
			},
		},
		"emits structured TAS annotation error on malformed podset-unconstrained-topology instead of untargeted spec error": {
			job: testingflinkdeployment.MakeFlinkDeployment("malformed-unconstrained", "ns").
				Queue("lq").
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetUnconstrainedTopologyAnnotation: "not-a-bool",
					},
				}).
				Obj(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetUnconstrainedTopologyAnnotation),
					"not-a-bool",
					"must be a boolean value",
				),
			},
		},
		"rejects invalid TAS PodSet group of size 1": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-tas-group", "ns").
				Queue("lq").
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodSetGroupName:                  "single-member-group",
					},
				}).
				Obj(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetGroupName),
					"single-member-group",
					"can only define groups of exactly 2 pod sets, got: 1 pod set(s)",
				),
			},
		},
		"allows unmanaged Session cluster with Flink 2g memory in podTemplate and non-object flinkConfiguration": {
			job: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal([]byte(`{
					"apiVersion": "flink.apache.org/v1beta1",
					"kind": "FlinkDeployment",
					"metadata": {"name": "unmanaged-session-2g", "namespace": "ns"},
					"spec": {
						"image": "flink:1.20",
						"flinkConfiguration": ["not", "an", "object"],
						"podTemplate": {
							"spec": {
								"containers": [{"name": "sidecar", "resources": {"requests": {"memory": "2g"}}}]
							}
						}
					}
				}`), &fd); err != nil {
					t.Fatalf("json.Unmarshal() error: %v", err)
				}
				return &fd
			}(),
			manageJobsWithoutQueueName: true,
		},
		"allows unmanaged Application deployment with Flink 2g memory in podTemplate and non-object flinkConfiguration when manageJobsWithoutQueueName is false": {
			job: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal([]byte(`{
					"apiVersion": "flink.apache.org/v1beta1",
					"kind": "FlinkDeployment",
					"metadata": {"name": "unmanaged-app-2g", "namespace": "ns"},
					"spec": {
						"image": "flink:1.20",
						"flinkConfiguration": ["not", "an", "object"],
						"podTemplate": {
							"spec": {
								"containers": [{"name": "sidecar", "resources": {"requests": {"memory": "2g"}}}]
							}
						},
						"job": {"jarURI": "local:///job.jar", "parallelism": 1, "state": "running"}
					}
				}`), &fd); err != nil {
					t.Fatalf("json.Unmarshal() error: %v", err)
				}
				return &fd
			}(),
		},
		"rejects Kueue-managed FlinkDeployment with non-object flinkConfiguration": {
			job: func() *flinkv1beta1.FlinkDeployment {
				fd := testingflinkdeployment.MakeFlinkDeployment("managed-bad-cfg", "ns").Queue("lq").Obj()
				if err := json.Unmarshal([]byte(`["not", "an", "object"]`), &fd.Spec.FlinkConfiguration); err != nil {
					t.Fatalf("json.Unmarshal() error: %v", err)
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "flinkConfiguration"),
					field.OmitValueType{},
					"json: cannot unmarshal array into Go value of type map[string]jsontext.Value",
				),
			},
		},
		"rejects standalone mode when spec.image is whitespace-only even if podTemplate sets flink-main-container image": {
			job: testingflinkdeployment.MakeFlinkDeployment("standalone-no-image", "ns").
				Queue("lq").
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
			wantErrs: field.ErrorList{
				field.Required(field.NewPath("spec", "image"), "image must be specified when spec.mode is standalone"),
			},
		},
		"rejects native mode when spec.image is omitted and podTemplate omits flink-main-container image on TaskManager": {
			job: testingflinkdeployment.MakeFlinkDeployment("native-no-image", "ns").
				Queue("lq").
				Image("").
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: flinkMainContainerName, Image: "flink:1.19-jm-only"},
						},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Required(field.NewPath("spec", "image"), "image must be specified via spec.image or flink-main-container in podTemplate"),
			},
		},
		"allows native mode when spec.image is omitted and podTemplate sets flink-main-container image": {
			job: testingflinkdeployment.MakeFlinkDeployment("native-template-image", "ns").
				Queue("lq").
				Image("   ").
				ImagePullPolicy("   ").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: flinkMainContainerName, Image: "flink:1.19-from-template"},
						},
					},
				}).
				Obj(),
		},
		"rejects sidecar container in podTemplate without image": {
			job: testingflinkdeployment.MakeFlinkDeployment("sidecar-no-image", "ns").
				Queue("lq").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "metrics-sidecar"},
						},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					`job-manager podSet: container "metrics-sidecar": image must be specified`,
				),
			},
		},
		"rejects unsupported spec.imagePullPolicy": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-pull-policy", "ns").
				Queue("lq").
				ImagePullPolicy(" IfNotPresent").
				Obj(),
			wantErrs: field.ErrorList{
				field.NotSupported(
					field.NewPath("spec", "imagePullPolicy"),
					corev1.PullPolicy(" IfNotPresent"),
					[]string{"Always", "Never", "IfNotPresent"},
				),
			},
		},
		"rejects unsupported container imagePullPolicy in podTemplate": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-container-pull-policy", "ns").
				Queue("lq").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						InitContainers: []corev1.Container{
							{Name: "init-a", Image: "busybox:1.36", ImagePullPolicy: "always"},
						},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					`job-manager podSet: initContainer "init-a": unsupported imagePullPolicy "always": supported values: "Always", "Never", "IfNotPresent"`,
				),
			},
		},
		"rejects invalid podTemplate labels when WorkloadValidationForPodSetMetadata is enabled": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-pt-label", "ns").
				Queue("lq").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels: map[string]string{"bad-label": "not a valid value!"},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					`job-manager podSet: pod labels: metadata.labels: Invalid value: "not a valid value!": a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')`,
				),
			},
		},
		"skips podTemplate metadata check when WorkloadValidationForPodSetMetadata is disabled": {
			job: testingflinkdeployment.MakeFlinkDeployment("disabled-pt-meta-gate", "ns").
				Queue("lq").
				PodTemplate(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"bad-label": "not a valid value!"},
					Annotations: map[string]string{"INVALID_KEY/a/b": "v"},
				}).
				Obj(),
			disablePodSetMetadataCheck: true,
		},
		"rejects invalid podTemplate annotations when WorkloadValidationForPodSetMetadata is enabled": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-pt-annotation", "ns").
				Queue("lq").
				JobManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{"invalid/annotation/key": "v"},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					`job-manager podSet: pod annotations: metadata.annotations: Invalid value: "invalid/annotation/key": a valid label key must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]') with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyName')`,
				),
			},
		},
		"rejects invalid podTemplate nodeSelector": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-pt-nodeselector", "ns").
				Queue("lq").
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						NodeSelector: map[string]string{"invalid/key/slashes": "spot"},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					`task-manager podSet: pod nodeSelector: spec.nodeSelector: Invalid value: "invalid/key/slashes": a valid label key must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]') with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyName')`,
				),
			},
		},
		"rejects empty-key toleration without Exists operator in podTemplate": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-pt-toleration", "ns").
				Queue("lq").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations: []corev1.Toleration{
							{Key: "", Operator: corev1.TolerationOpEqual, Value: "gpu"},
						},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					`job-manager podSet: toleration[0]: tolerations[0].operator: Invalid value: "Equal": operator must be Exists when key is empty`,
				),
			},
		},
		"valid child FlinkDeployment owned by Kueue-managed ancestor": {
			job: func() *flinkv1beta1.FlinkDeployment {
				child := testingflinkdeployment.MakeFlinkDeployment("child-app", "ns").
					Queue("parent-lq").
					UID("uid-child").
					Suspend(false).
					Obj()
				child.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
						Kind:       "FlinkDeployment",
						Name:       ancestorParent.Name,
						UID:        ancestorParent.UID,
						Controller: new(true),
					},
				}
				return child
			}(),
			extraObjects: []client.Object{ancestorParent},
		},
		"nil integrationManager fallback validates queue-labeled top-level Application FlinkDeployment as managed": {
			job: testingflinkdeployment.MakeFlinkDeployment("top-level-nil-mgr", "ns").
				Queue("lq").
				JobManagerResource(new(1.0), "", "").
				Obj(),
			nilIntegrationManager: true,
			wantErrs: field.ErrorList{
				field.Required(
					field.NewPath("spec", "jobManager", "resource", "memory"),
					"memory must be specified via spec.jobManager.resource.memory or spec.jobManager.resources",
				),
			},
		},
		"rejects unnamed container in podTemplate when spec.image is set": {
			job: testingflinkdeployment.MakeFlinkDeployment("unnamed-container", "ns").
				Queue("lq").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Image: "busybox:1.36"}},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					`job-manager podSet: containers[0].name "": a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')`,
				),
			},
		},
		"rejects duplicate list-map volume keys after positional podTemplate merge": {
			job: testingflinkdeployment.MakeFlinkDeployment("dup-merged-volumes", "ns").
				Queue("lq").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{Name: "vol-a", EmptyDir: &corev1.EmptyDirVolumeSource{}},
							{Name: "vol-b", EmptyDir: &corev1.EmptyDirVolumeSource{}},
						},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{Name: "vol-b", EmptyDir: &corev1.EmptyDirVolumeSource{}},
						},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					`task-manager podSet: duplicate volumes name "vol-b"`,
				),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, tc.enableTAS)
			features.SetFeatureGateDuringTest(t, features.WorkloadValidationForPodSetMetadata, !tc.disablePodSetMetadataCheck)
			ctx, _ := utiltesting.ContextWithLog(t)

			builder := utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme).WithObjects(&corev1.Namespace{
				Name: "ns",
			})
			if len(tc.extraObjects) > 0 {
				builder = builder.WithObjects(tc.extraObjects...)
			}
			cl := builder.Build()

			var integrationMgr *jobframework.IntegrationManager
			if !tc.nilIntegrationManager {
				integrationMgr = newTestIntegrationManager(t)
			}
			wh := &flinkDeploymentWebhook{
				integrationManager:           integrationMgr,
				client:                       cl,
				manageJobsWithoutQueueName:   tc.manageJobsWithoutQueueName,
				managedJobsNamespaceSelector: labels.Everything(),
			}

			_, err := wh.ValidateCreate(ctx, tc.job)
			gotErrs := extractFieldErrors(t, err)
			if diff := cmp.Diff(tc.wantErrs, gotErrs, cmpopts.IgnoreFields(field.Error{}, "BadValue")); diff != "" {
				t.Errorf("ValidateCreate() error list mismatch (-want,+got):\n%s\nfull err: %v", diff, err)
			}
		})
	}
}

func extractFieldErrors(t *testing.T, err error) field.ErrorList {
	t.Helper()
	if err == nil {
		return nil
	}
	var agg utilerrors.Aggregate
	if !errors.As(err, &agg) {
		t.Fatalf("expected utilerrors.Aggregate, got %T: %v", err, err)
	}
	out := make(field.ErrorList, 0, len(agg.Errors()))
	for _, item := range agg.Errors() {
		var fe *field.Error
		if !errors.As(item, &fe) {
			t.Fatalf("expected *field.Error inside aggregate, got %T: %v", item, item)
		}
		out = append(out, fe)
	}
	return out
}

func TestValidateUpdate(t *testing.T) {
	now := metav1.NewTime(time.Now())

	baseSuspended := testingflinkdeployment.MakeFlinkDeployment("app", "ns").
		Queue("lq").
		UID("uid-app").
		Suspend(true).
		Obj()

	baseRunning := testingflinkdeployment.MakeFlinkDeployment("app", "ns").
		Queue("lq").
		UID("uid-app").
		Suspend(false).
		Obj()

	stopJobCtx, _ := utiltesting.ContextWithLog(t)
	tasCtx := stopJobCtx
	features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, true)

	cpuPoolFlavor := utiltestingapi.MakeResourceFlavor("cpu-pool-flavor").
		NodeLabel("pool", "cpu").
		Obj()
	baseSuspendedPodSets, err := fromObject(baseSuspended).PodSets(stopJobCtx, nil)
	if err != nil {
		t.Fatalf("baseSuspended PodSets() error: %v", err)
	}
	baseAdmittedWl := utiltestingapi.MakeWorkload(
		GetWorkloadNameForFlinkDeployment(baseSuspended.Name, baseSuspended.UID),
		baseSuspended.Namespace,
	).
		Queue("lq").
		PodSets(baseSuspendedPodSets...).
		ControllerReference(gvk, baseSuspended.Name, string(baseSuspended.UID)).
		ReserveQuotaAt(
			utiltestingapi.MakeAdmission("cq").
				PodSets(
					utiltestingapi.MakePodSetAssignment(jobManagerPodSetName).
						Assignment(corev1.ResourceCPU, "cpu-pool-flavor", "1").
						Obj(),
					utiltestingapi.MakePodSetAssignment(taskManagerPodSetName).
						Assignment(corev1.ResourceCPU, "cpu-pool-flavor", "1").
						Obj(),
				).
				Obj(),
			now.Time,
		).
		AdmittedAt(true, now.Time).
		Obj()
	baseQueuedWl := utiltestingapi.MakeWorkload(
		GetWorkloadNameForFlinkDeployment(baseSuspended.Name, baseSuspended.UID),
		baseSuspended.Namespace,
	).
		Queue("lq").
		PodSets(baseSuspendedPodSets...).
		ControllerReference(gvk, baseSuspended.Name, string(baseSuspended.UID)).
		Obj()
	baseEvictedDrainingWl := baseAdmittedWl.DeepCopy()
	baseEvictedDrainingWl.Status.Conditions = append(baseEvictedDrainingWl.Status.Conditions, metav1.Condition{
		Type:               kueue.WorkloadEvicted,
		Status:             metav1.ConditionTrue,
		Reason:             kueue.WorkloadEvictedByPreemption,
		LastTransitionTime: now,
	})
	baseEvictedDrainedWl := baseEvictedDrainingWl.DeepCopy()
	workload.UnsetQuotaReservationWithCondition(baseEvictedDrainedWl, "Evicted", "Savepoint drain completed", now.Time)
	workload.SyncAdmittedCondition(baseEvictedDrainedWl, now.Time)
	baseQuotaReservedOnlyWl := utiltestingapi.MakeWorkload(
		GetWorkloadNameForFlinkDeployment(baseSuspended.Name, baseSuspended.UID),
		baseSuspended.Namespace,
	).
		Queue("lq").
		PodSets(baseSuspendedPodSets...).
		ControllerReference(gvk, baseSuspended.Name, string(baseSuspended.UID)).
		ReserveQuotaAt(
			utiltestingapi.MakeAdmission("cq").
				PodSets(
					utiltestingapi.MakePodSetAssignment(jobManagerPodSetName).
						Assignment(corev1.ResourceCPU, "cpu-pool-flavor", "1").
						Obj(),
					utiltestingapi.MakePodSetAssignment(taskManagerPodSetName).
						Assignment(corev1.ResourceCPU, "cpu-pool-flavor", "1").
						Obj(),
				).
				Obj(),
			now.Time,
		).
		AdmittedAt(false, now.Time).
		Obj()

	setupFlavorClient := utiltesting.NewClientBuilder().WithObjects(cpuPoolFlavor).Build()
	baseAdmittedInfo, err := expectedAdmittedPodSetsInfo(stopJobCtx, setupFlavorClient, baseAdmittedWl)
	if err != nil {
		t.Fatalf("expectedAdmittedPodSetsInfo(baseAdmittedWl) error: %v", err)
	}
	baseAdmittedRunning := baseSuspended.DeepCopy()
	if err := fromObject(baseAdmittedRunning).RunWithPodSetsInfo(stopJobCtx, setupFlavorClient, baseAdmittedInfo); err != nil {
		t.Fatalf("RunWithPodSetsInfo(baseAdmittedRunning) error: %v", err)
	}

	preAdmissionTAS := testingflinkdeployment.MakeFlinkDeployment("app-tas", "ns").
		Queue("lq").
		UID("uid-app-tas").
		Suspend(true).
		PodTemplate(&corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"base-zone": "us-central1-a"},
			},
		}).
		TaskManagerPodTemplate(&corev1.PodTemplateSpec{
			Annotations: map[string]string{
				kueue.PodSetRequiredTopologyAnnotation: "cloud.provider.com/rack",
			},
			Spec: corev1.PodSpec{
				Tolerations: []corev1.Toleration{
					{Key: "user-gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
				},
			},
		}).
		Obj()
	preAdmissionTASPodSets, err := fromObject(preAdmissionTAS).PodSets(tasCtx, nil)
	if err != nil {
		t.Fatalf("preAdmissionTAS PodSets() error: %v", err)
	}
	tasOnDemandFlavor := utiltestingapi.MakeResourceFlavor("cpu-on-demand").
		NodeLabel("flavor-pool", "cpu-on-demand").
		TopologyName("default").
		Obj()
	tasSpotGPUFlavor := utiltestingapi.MakeResourceFlavor("gpu-h100").
		NodeLabel("flavor-pool", "gpu-h100").
		Toleration(corev1.Toleration{
			Key:      "spot",
			Operator: corev1.TolerationOpEqual,
			Value:    "true",
			Effect:   corev1.TaintEffectNoSchedule,
		}).
		TopologyName("default").
		Obj()
	sampleTASAssignment := tas.V1Beta2From(&tas.TopologyAssignment{
		Levels:  []string{"cloud.provider.com/rack"},
		Domains: []tas.TopologyDomainAssignment{{Count: 1, Values: []string{"r1"}}},
	})
	tasAdmittedWl := utiltestingapi.MakeWorkload(
		GetWorkloadNameForFlinkDeployment(preAdmissionTAS.Name, preAdmissionTAS.UID),
		preAdmissionTAS.Namespace,
	).
		Queue("lq").
		Label(kueue.MultiKueueOriginLabel, "manager-cluster").
		PodSets(preAdmissionTASPodSets...).
		ControllerReference(gvk, preAdmissionTAS.Name, string(preAdmissionTAS.UID)).
		ReserveQuotaAt(
			utiltestingapi.MakeAdmission("cq").
				PodSets(
					utiltestingapi.MakePodSetAssignment(jobManagerPodSetName).
						Assignment(corev1.ResourceCPU, "cpu-on-demand", "1").
						TopologyAssignment(sampleTASAssignment).
						Obj(),
					utiltestingapi.MakePodSetAssignment(taskManagerPodSetName).
						Assignment(corev1.ResourceCPU, "gpu-h100", "1").
						TopologyAssignment(sampleTASAssignment).
						Obj(),
				).
				Obj(),
			now.Time,
		).
		AdmissionChecks(kueue.AdmissionCheckState{
			Name:  "prov-check",
			State: kueue.CheckStateReady,
			PodSetUpdates: []kueue.PodSetUpdate{
				{
					Name:        taskManagerPodSetName,
					Annotations: map[string]string{"cluster-autoscaler.kubernetes.io/consume-provisioning-request": "pr-1"},
				},
			},
		}).
		AdmittedAt(true, now.Time).
		Obj()
	tasSetupClient := utiltesting.NewClientBuilder().WithObjects(tasOnDemandFlavor, tasSpotGPUFlavor).Build()
	tasAdmittedInfo, err := expectedAdmittedPodSetsInfo(tasCtx, tasSetupClient, tasAdmittedWl)
	if err != nil {
		t.Fatalf("expectedAdmittedPodSetsInfo(tasAdmittedWl) error: %v", err)
	}
	admittedRunning := preAdmissionTAS.DeepCopy()
	if err := fromObject(admittedRunning).RunWithPodSetsInfo(tasCtx, tasSetupClient, tasAdmittedInfo); err != nil {
		t.Fatalf("RunWithPodSetsInfo() setup error: %v", err)
	}
	restoredAfterStopJob := admittedRunning.DeepCopy()
	fromObject(restoredAfterStopJob).Suspend()
	fromObject(restoredAfterStopJob).RestorePodSetsInfo(tasCtx, jobframework.GetPodSetsInfoFromWorkload(tasAdmittedWl))

	baseWithEmptyPtrs := testingflinkdeployment.MakeFlinkDeployment("app-empty-ptrs", "ns").
		Queue("lq").
		UID("uid-empty-ptrs").
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
									StorageClassName: new(""),
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
						LivenessProbe: &corev1.Probe{
							GRPC: &corev1.GRPCAction{
								Port:    9090,
								Service: new(""),
							},
						},
					},
				},
			},
		}).
		Obj()
	preEmptyPtrsPodSets, err := fromObject(baseWithEmptyPtrs).PodSets(stopJobCtx, nil)
	if err != nil {
		t.Fatalf("preEmptyPtrsPodSets error: %v", err)
	}
	emptyPtrsWl := utiltestingapi.MakeWorkload(
		GetWorkloadNameForFlinkDeployment(baseWithEmptyPtrs.Name, baseWithEmptyPtrs.UID),
		baseWithEmptyPtrs.Namespace,
	).
		Queue("lq").
		PodSets(preEmptyPtrsPodSets...).
		ControllerReference(gvk, baseWithEmptyPtrs.Name, string(baseWithEmptyPtrs.UID)).
		Obj()
	admittedEmptyPtrs := baseWithEmptyPtrs.DeepCopy()
	if err := fromObject(admittedEmptyPtrs).RunWithPodSetsInfo(stopJobCtx, nil, []podset.PodSetInfo{
		{
			Name:  jobManagerPodSetName,
			Count: 1,
			Labels: map[string]string{
				kueueconstants.LocalQueueLabel: "lq",
				kueueconstants.PodSetLabel:     string(jobManagerPodSetName),
			},
			NodeSelector: map[string]string{"flavor": "on-demand"},
		},
		{
			Name:  taskManagerPodSetName,
			Count: 1,
			Labels: map[string]string{
				kueueconstants.LocalQueueLabel: "lq",
				kueueconstants.PodSetLabel:     string(taskManagerPodSetName),
			},
			NodeSelector: map[string]string{"flavor": "on-demand"},
		},
	}); err != nil {
		t.Fatalf("RunWithPodSetsInfo(admittedEmptyPtrs) error: %v", err)
	}
	evictedEmptyPtrs := admittedEmptyPtrs.DeepCopy()
	fromObject(evictedEmptyPtrs).Suspend()
	fromObject(evictedEmptyPtrs).RestorePodSetsInfo(stopJobCtx, jobframework.GetPodSetsInfoFromWorkload(emptyPtrsWl))

	// Simulate EquivalentToWorkload overlay drift: Workload was created from initialDriftedBase,
	// then the user updated Labels, Annotations, NodeSelector, and SchedulingGates while still
	// suspended (keeping EquivalentToWorkload == true), then Kueue admitted the job via
	// RunWithPodSetsInfo, and later JobReconciler.stopJob restores wl.Spec.PodSets.
	initialDriftedBase := testingflinkdeployment.MakeFlinkDeployment("app-drifted", "ns").
		Queue("lq").
		UID("uid-app-drifted").
		Suspend(true).
		JobManagerPodTemplate(&corev1.PodTemplateSpec{
			Labels:      map[string]string{"env": "staging"},
			Annotations: map[string]string{"owner": "team-a"},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"tier": "old-jm"},
				Tolerations:  []corev1.Toleration{{Key: "old-taint", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
				SchedulingGates: []corev1.PodSchedulingGate{
					{Name: "example.com/old-gate"},
					{Name: "example.com/kept-gate"},
				},
			},
		}).
		TaskManagerPodTemplate(&corev1.PodTemplateSpec{
			Labels:      map[string]string{"env": "staging"},
			Annotations: map[string]string{"owner": "team-a"},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"tier": "old-tm"},
			},
		}).
		Obj()
	initialDriftedPodSets, err := fromObject(initialDriftedBase).PodSets(stopJobCtx, nil)
	if err != nil {
		t.Fatalf("initialDriftedPodSets error: %v", err)
	}
	driftedControlledWorkload := utiltestingapi.MakeWorkload("wl-app-drifted", "ns").
		PodSets(initialDriftedPodSets...).
		ControllerReference(gvk, initialDriftedBase.Name, string(initialDriftedBase.UID)).
		Obj()
	driftedFinishedWorkload := driftedControlledWorkload.DeepCopy()
	driftedFinishedWorkload.Name = "wl-app-drifted-finished"
	driftedFinishedWorkload.Status.Conditions = []metav1.Condition{
		{
			Type:   kueue.WorkloadFinished,
			Status: metav1.ConditionTrue,
			Reason: kueue.WorkloadFinishedReasonSucceeded,
		},
	}
	driftedWrongOwnerWorkload := driftedControlledWorkload.DeepCopy()
	driftedWrongOwnerWorkload.Name = "wl-app-drifted-other-uid"
	driftedWrongOwnerWorkload.OwnerReferences[0].UID = "uid-different"

	editedWhileSuspended := initialDriftedBase.DeepCopy()
	editedWhileSuspended.Spec.JobManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{
		Labels:      map[string]string{"env": "prod"},
		Annotations: map[string]string{"owner": "team-b"},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"tier": "new-jm"},
			Tolerations:  []corev1.Toleration{{Key: "old-taint", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "example.com/kept-gate"},
				{Name: "example.com/new-gate"},
			},
		},
	}
	editedWhileSuspended.Spec.TaskManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{
		Labels:      map[string]string{"env": "prod"},
		Annotations: map[string]string{"owner": "team-b"},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"tier": "new-tm"},
		},
	}
	driftedRunning := editedWhileSuspended.DeepCopy()
	if err := fromObject(driftedRunning).RunWithPodSetsInfo(stopJobCtx, nil, []podset.PodSetInfo{
		{
			Name:         jobManagerPodSetName,
			Count:        1,
			Labels:       map[string]string{kueueconstants.LocalQueueLabel: "lq"},
			NodeSelector: map[string]string{"flavor-pool": "cpu-spot"},
		},
		{
			Name:         taskManagerPodSetName,
			Count:        1,
			Labels:       map[string]string{kueueconstants.LocalQueueLabel: "lq"},
			NodeSelector: map[string]string{"flavor-pool": "gpu-spot"},
		},
	}); err != nil {
		t.Fatalf("RunWithPodSetsInfo(driftedRunning) error: %v", err)
	}
	driftedStoppedFromWorkload := driftedRunning.DeepCopy()
	fromObject(driftedStoppedFromWorkload).Suspend()
	fromObject(driftedStoppedFromWorkload).RestorePodSetsInfo(
		stopJobCtx,
		jobframework.GetPodSetsInfoFromWorkload(driftedControlledWorkload),
	)
	if diff := cmp.Diff(
		[]corev1.PodSchedulingGate{{Name: "example.com/kept-gate"}},
		driftedStoppedFromWorkload.Spec.JobManager.PodTemplate.Spec.SchedulingGates,
	); diff != "" {
		t.Fatalf("driftedStoppedFromWorkload JobManager SchedulingGates mismatch (-want,+got):\n%s", diff)
	}

	ancestorParent := testingflinkdeployment.MakeFlinkDeployment("parent-app", "ns").
		Queue("parent-lq").
		UID("uid-parent").
		Suspend(true).
		Obj()
	makeAncestorOwnedChild := func(state flinkv1beta1.JobState, tmReplicas int64) *flinkv1beta1.FlinkDeployment {
		child := testingflinkdeployment.MakeFlinkDeployment("child-app", "ns").
			Queue("parent-lq").
			UID("uid-child").
			Suspend(state == flinkv1beta1.JobStateSuspended).
			TaskManagerReplicas(tmReplicas).
			Obj()
		child.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
				Kind:       "FlinkDeployment",
				Name:       ancestorParent.Name,
				UID:        ancestorParent.UID,
				Controller: new(true),
			},
		}
		return child
	}

	testCases := map[string]struct {
		oldJob                     *flinkv1beta1.FlinkDeployment
		newJob                     *flinkv1beta1.FlinkDeployment
		extraObjects               []client.Object
		nilIntegrationManager      bool
		disableSkipAncestorCheck   bool
		manageJobsWithoutQueueName bool
		enableTAS                  bool
		enableWorkloadLevelWFPR    bool
		maxTimeoutOnWorkload       *metav1.Duration
		defaultQueue               *kueue.LocalQueue
		flavors                    []*kueue.ResourceFlavor
		workloads                  []*kueue.Workload
		wantNonFieldErr            error
		wantErrs                   field.ErrorList
	}{
		"rejects spec.mode change while running even when derived PodSets are unchanged": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				ImagePullPolicy(corev1.PullIfNotPresent).
				ServiceAccount("flink").
				Suspend(false).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				ImagePullPolicy(corev1.PullIfNotPresent).
				ServiceAccount("flink").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "mode"),
					flinkv1beta1.KubernetesDeploymentModeStandalone,
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"allows spec.mode change while suspended": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(true).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(true).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
		},
		"allows a pre-existing labeled Session cluster to drop queue-name even with a default LocalQueue in the namespace": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("lq").
				SessionCluster().
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				SessionCluster().
				Obj(),
			defaultQueue: utiltestingapi.MakeLocalQueue("default", "ns").Obj(),
		},
		"skips unmanaged Session FlinkDeployment update": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged", "ns").
				SessionCluster().
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged", "ns").
				SessionCluster().
				TaskManagerReplicas(4).
				Obj(),
		},
		"allows finalizer removal on deleting FlinkDeployment even with missing owner or invalid spec": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				cp.Spec.JobManager.Resource.Memory = ""
				cp.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
					Kind:       "FlinkDeployment",
					Name:       "already-deleted-parent",
					UID:        types.UID("gone"),
					Controller: new(true),
				}}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Spec.JobManager.Resource.Memory = ""
				cp.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
					Kind:       "FlinkDeployment",
					Name:       "already-deleted-parent",
					UID:        types.UID("gone"),
					Controller: new(true),
				}}
				return cp
			}(),
			manageJobsWithoutQueueName: true,
		},
		"allows Kueue admission transition suspended -> running with injected nodeSelector": {
			oldJob:    baseSuspended,
			newJob:    baseAdmittedRunning,
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseAdmittedWl},
		},
		"allows Kueue startJob suspended -> running transition with ResourceFlavor placement, toleration, TAS gate, MultiKueue label, and AdmissionCheck PodSetUpdate": {
			oldJob:    preAdmissionTAS,
			newJob:    admittedRunning,
			enableTAS: true,
			flavors:   []*kueue.ResourceFlavor{tasOnDemandFlavor, tasSpotGPUFlavor},
			workloads: []*kueue.Workload{tasAdmittedWl},
		},
		"allows Kueue startJob re-unsuspending an implicit-TAS admitted FlinkDeployment after manual spec.job.state=suspended": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return cp
			}(),
			newJob:    admittedRunning,
			enableTAS: true,
			flavors:   []*kueue.ResourceFlavor{tasOnDemandFlavor, tasSpotGPUFlavor},
			workloads: []*kueue.Workload{tasAdmittedWl},
		},
		"rejects manual suspended -> running transition when no Workload exists": {
			oldJob: baseSuspended,
			newJob: baseRunning,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects manual suspended -> running transition when owned Workload is queued and not yet admitted": {
			oldJob:    baseSuspended,
			newJob:    baseRunning,
			workloads: []*kueue.Workload{baseQueuedWl},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects manual suspended -> running transition when owned Workload is evicted and draining": {
			oldJob:    baseSuspended,
			newJob:    baseAdmittedRunning,
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseEvictedDrainingWl},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects manual suspended -> running transition when owned Workload is evicted and drained": {
			oldJob:    baseSuspended,
			newJob:    baseAdmittedRunning,
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseEvictedDrainedWl},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects admitted suspended -> running transition with hijacked ResourceFlavor nodeSelector, extra taint toleration, or stripped TAS schedulingGate": {
			oldJob: preAdmissionTAS,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.TaskManager.PodTemplate.Spec.NodeSelector["flavor-pool"] = "reserved-b200"
				cp.Spec.TaskManager.PodTemplate.Spec.Tolerations = append(
					cp.Spec.TaskManager.PodTemplate.Spec.Tolerations,
					corev1.Toleration{Key: "reserved-b200", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
				)
				cp.Spec.TaskManager.PodTemplate.Spec.SchedulingGates = nil
				return cp
			}(),
			enableTAS: true,
			flavors:   []*kueue.ResourceFlavor{tasOnDemandFlavor, tasSpotGPUFlavor},
			workloads: []*kueue.Workload{tasAdmittedWl},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects mutating role podTemplate tolerations or nodeSelector while suspended when owned Workload is already admitted": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						NodeSelector: map[string]string{"pool": "reserved-h100"},
						Tolerations: []corev1.Toleration{
							{Key: "reserved-h100", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
						},
					},
				}
				return cp
			}(),
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseAdmittedWl},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s while Workload is admitted", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects stripping or changing queue-name while suspended when owned Workload is already admitted": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				delete(cp.Labels, constants.QueueLabel)
				return cp
			}(),
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseAdmittedWl},
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects mutating max-exec-time-seconds label while suspended when owned Workload is already admitted": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "120"
				return cp
			}(),
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseAdmittedWl},
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.MaxExecTimeSecondsLabel), "120", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects changing queue-name, adding priority-class, or scaling replicas while suspended when owned Workload has QuotaReserved=True pending AdmissionCheck": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Labels[constants.QueueLabel] = "other-lq"
				cp.Labels[constants.WorkloadPriorityClassLabel] = "high-prio"
				cp.Spec.TaskManager.Replicas = new(int64(4))
				return cp
			}(),
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseQuotaReservedOnlyWl},
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "other-lq", apivalidation.FieldImmutableErrorMsg),
				field.Invalid(
					field.NewPath("metadata", "labels").Key(constants.WorkloadPriorityClassLabel),
					"high-prio",
					"WorkloadPriorityClass cannot be added to a non-suspended workload",
				),
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s while Workload has quota reserved", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects suspended -> running when oldObj PodSets failed validation prior to webhook enablement": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("legacy-invalid", "ns").
				Queue("lq").
				Suspend(true).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerResource(nil, "2048m", "").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("legacy-invalid", "ns").
				Queue("lq").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects suspended -> running when oldObj drifted from admitted Workload via /scale subresource": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(8))
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(8))
				return cp
			}(),
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseAdmittedWl},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects mutating taskmanager.numberOfTaskSlots, merge-arrays-by-name, or spec.job.parallelism while running even when explicit replicas and CPU keep PodSets identical": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app-slots", "ns").
				Queue("lq").
				Suspend(false).
				Parallelism(4).
				TaskManagerReplicas(2).
				TaskManagerResource(new(2.0), "2048m", "").
				FlinkConfig("taskmanager.numberOfTaskSlots", "4").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "false").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app-slots", "ns").
				Queue("lq").
				Suspend(false).
				Parallelism(8).
				TaskManagerReplicas(2).
				TaskManagerResource(new(2.0), "2048m", "").
				FlinkConfig("taskmanager.numberOfTaskSlots", "1").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "true").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "job", "parallelism"),
					int64(8),
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
				field.Invalid(
					field.NewPath("spec", "flinkConfiguration").Key("taskmanager.numberOfTaskSlots"),
					"1",
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
				field.Invalid(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.pod-template.merge-arrays-by-name"),
					"true",
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects stripping queue-name, mutating spec.image, or scaling replicas while suspended during savepoint eviction drain (Evicted=True, QuotaReserved=True)": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				delete(cp.Labels, constants.QueueLabel)
				cp.Spec.Image = "flink:2.0"
				cp.Spec.TaskManager.Replicas = new(int64(4))
				return cp
			}(),
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseEvictedDrainingWl},
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "", apivalidation.FieldImmutableErrorMsg),
				field.Invalid(
					field.NewPath("spec", "image"),
					"flink:2.0",
					fmt.Sprintf("%s while Workload has quota reserved", apivalidation.FieldImmutableErrorMsg),
				),
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s while Workload has quota reserved", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"allows mutating role podTemplate, scaling replicas, or detaching queue-name while suspended once evicted Workload releases quota (Evicted=True, QuotaReserved=False)": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				delete(cp.Labels, constants.QueueLabel)
				cp.Spec.TaskManager.Replicas = new(int64(4))
				cp.Spec.TaskManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						NodeSelector: map[string]string{"tenant-tier": "batch"},
					},
				}
				return cp
			}(),
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseEvictedDrainedWl},
		},
		"allows stopJob pure suspension when running FlinkDeployment was scaled to 0 via /scale subresource": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(0))
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(0))
				fromObject(cp).Suspend()
				fromObject(cp).RestorePodSetsInfo(stopJobCtx, jobframework.GetPodSetsInfoFromWorkload(baseAdmittedWl))
				return cp
			}(),
			flavors:   []*kueue.ResourceFlavor{cpuPoolFlavor},
			workloads: []*kueue.Workload{baseAdmittedWl},
		},
		"allows fixing taskManager.replicas from 0 back to 2 while suspended once non-matching Workload is gone": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(0))
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(2))
				return cp
			}(),
		},
		"allows stopJob pure suspension when running TAS FlinkDeployment was scaled below podset-slice-size via /scale": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(2))
				cp.Spec.TaskManager.PodTemplate.Annotations[kueue.PodSetSliceRequiredTopologyAnnotation] = "cloud.provider.com/rack"
				cp.Spec.TaskManager.PodTemplate.Annotations[kueue.PodSetSliceSizeAnnotation] = "4"
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(2))
				cp.Spec.TaskManager.PodTemplate.Annotations[kueue.PodSetSliceRequiredTopologyAnnotation] = "cloud.provider.com/rack"
				cp.Spec.TaskManager.PodTemplate.Annotations[kueue.PodSetSliceSizeAnnotation] = "4"
				fromObject(cp).Suspend()
				fromObject(cp).RestorePodSetsInfo(tasCtx, jobframework.GetPodSetsInfoFromWorkload(tasAdmittedWl))
				return cp
			}(),
			enableTAS: true,
			flavors:   []*kueue.ResourceFlavor{tasOnDemandFlavor, tasSpotGPUFlavor},
			workloads: []*kueue.Workload{tasAdmittedWl},
		},
		"allows pure suspension of a pre-existing invalid running FlinkDeployment without Workload": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("legacy-running-missing-mem", "ns").
				Queue("lq").
				Suspend(false).
				JobManagerResource(new(1.0), "", "").
				TaskManagerResource(new(1.0), "1.5Gi", "").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("legacy-running-missing-mem", "ns").
				Queue("lq").
				Suspend(true).
				JobManagerResource(new(1.0), "", "").
				TaskManagerResource(new(1.0), "1.5Gi", "").
				Obj(),
		},
		"rejects dropping queue-name during suspension of a pre-existing invalid running FlinkDeployment": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("legacy-running-missing-mem", "ns").
				Queue("lq").
				Suspend(false).
				JobManagerResource(new(1.0), "", "").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("legacy-running-missing-mem", "ns").
				Suspend(true).
				JobManagerResource(new(1.0), "", "").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"allows manual savepointTriggerNonce update while running": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.Job.SavepointTriggerNonce = new(int64(1))
				return cp
			}(),
		},
		"rejects scaling TaskManager replicas while running": {
			oldJob: baseRunning,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(false).
				TaskManagerReplicas(4).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects changing queue-name while running": {
			oldJob: baseRunning,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("other-lq").
				Suspend(false).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "other-lq", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects stripping queue-name while running": {
			oldJob: baseRunning,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Suspend(false).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"skips unmanaged Application FlinkDeployment update with Flink MemorySize 2g in component resources": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal(
					[]byte(
						`{"metadata":{"name":"unmanaged-2g","namespace":"ns"},"spec":{"image":"flink:1.20","jobManager":{"resources":{"requests":{"cpu":"1","memory":"2g"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"2g"}}},"job":{"jarURI":"local:///app.jar","parallelism":2,"state":"running"}}}`,
					),
					&fd,
				); err != nil {
					t.Fatalf("json.Unmarshal(oldJob) error: %v", err)
				}
				return &fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal(
					[]byte(
						`{"metadata":{"name":"unmanaged-2g","namespace":"ns"},"spec":{"image":"flink:1.20","jobManager":{"resources":{"requests":{"cpu":"1","memory":"4g"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"4g"}}},"job":{"jarURI":"local:///app.jar","parallelism":4,"state":"running"}}}`,
					),
					&fd,
				); err != nil {
					t.Fatalf("json.Unmarshal(newJob) error: %v", err)
				}
				return &fd
			}(),
		},
		"allows Kueue stopJob transition running -> suspended with RestorePodSetsInfo removing injected flavor placement and implicit TAS": {
			oldJob:    admittedRunning,
			newJob:    restoredAfterStopJob,
			enableTAS: true,
			workloads: []*kueue.Workload{tasAdmittedWl},
		},
		"allows standalone spec.job.state=suspended patch on running FlinkDeployment": {
			oldJob: admittedRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return cp
			}(),
		},
		"rejects single-patch running -> suspended transition that wipes user nodeSelector or schedulingGates": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				return testingflinkdeployment.MakeFlinkDeployment("app-tenant-overlay", "ns").
					Queue("lq").
					UID("uid-tenant-overlay").
					Suspend(false).
					TaskManagerPodTemplate(&corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							NodeSelector:    map[string]string{"tenant": "team-a", "flavor": "spot"},
							SchedulingGates: []corev1.PodSchedulingGate{{Name: "example.com/custom-gate"}},
						},
					}).
					Obj()
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				return testingflinkdeployment.MakeFlinkDeployment("app-tenant-overlay", "ns").
					Queue("lq").
					UID("uid-tenant-overlay").
					Suspend(true).
					Obj()
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects simultaneous spec.job.state=suspended and TaskManagerReplicas mutation on running FlinkDeployment": {
			oldJob: baseRunning,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				UID("uid-app").
				Suspend(true).
				TaskManagerReplicas(4).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects simultaneous spec.job.state=suspended and spec.mode mutation on running FlinkDeployment": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				ImagePullPolicy(corev1.PullIfNotPresent).
				ServiceAccount("flink").
				Suspend(false).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				ImagePullPolicy(corev1.PullIfNotPresent).
				ServiceAccount("flink").
				Suspend(true).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "mode"),
					flinkv1beta1.KubernetesDeploymentModeStandalone,
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects simultaneous spec.job.state=suspended and root spec.podTemplate mutation on running FlinkDeployment": {
			oldJob: admittedRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				cp.Spec.PodTemplate.Spec.NodeSelector = map[string]string{"base-zone": "us-east1-b"}
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "podTemplate"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects simultaneous spec.job.state=suspended and stripping queue-name on running FlinkDeployment": {
			oldJob: baseRunning,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Suspend(true).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects simultaneous spec.job.state=suspended and changing queue-name on running FlinkDeployment": {
			oldJob: baseRunning,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("other-lq").
				Suspend(true).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "other-lq", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects simultaneous spec.job.state=suspended and adding WorkloadPriorityClass label on running FlinkDeployment": {
			oldJob: baseRunning,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				WorkloadPriorityClass("high-prio").
				Suspend(true).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata", "labels").Key(constants.WorkloadPriorityClassLabel),
					"high-prio",
					"WorkloadPriorityClass cannot be added to a non-suspended workload",
				),
			},
		},
		"rejects simultaneous spec.job.state=suspended and removing prebuilt-workload label on running FlinkDeployment": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Label(constants.PrebuiltWorkloadLabel, "prebuilt-wl").
				Suspend(false).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(true).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.PrebuiltWorkloadLabel), "", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects simultaneous spec.job.state=suspended and removing explicit user TAS annotation on running FlinkDeployment": {
			oldJob: admittedRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				delete(cp.Spec.TaskManager.PodTemplate.Annotations, kueue.PodSetRequiredTopologyAnnotation)
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects simultaneous spec.job.state=suspended and adding role podTemplate nodeSelector on running FlinkDeployment": {
			oldJob: admittedRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				cp.Spec.TaskManager.PodTemplate.Spec.NodeSelector["extra-key"] = "extra-val"
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects mutating top-level spec.image while running even when podTemplate flink-main-container specifies image": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(false).
				Image("flink:1.20.0").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: flinkMainContainerName, Image: "flink:1.19-base"},
						},
					},
				}).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(false).
				Image("flink:1.20.1").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: flinkMainContainerName, Image: "flink:1.19-base"},
						},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "image"),
					"flink:1.20.1",
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects clearing spec.serviceAccount, spec.image, or spec.imagePullPolicy while running even when podTemplate fallback keeps derived PodSets identical": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app-top-level-immutable", "ns").
				Queue("lq").
				Suspend(false).
				Image("flink:1.20").
				ImagePullPolicy(corev1.PullIfNotPresent).
				ServiceAccount("flink").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ServiceAccountName: "flink",
						Containers: []corev1.Container{
							{
								Name:            flinkMainContainerName,
								Image:           "flink:1.20",
								ImagePullPolicy: corev1.PullIfNotPresent,
							},
						},
					},
				}).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app-top-level-immutable", "ns").
				Queue("lq").
				Suspend(false).
				Image("").
				ImagePullPolicy("").
				ServiceAccount("").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ServiceAccountName: "flink",
						Containers: []corev1.Container{
							{
								Name:            flinkMainContainerName,
								Image:           "flink:1.20",
								ImagePullPolicy: corev1.PullIfNotPresent,
							},
						},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "image"),
					"",
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
				field.Invalid(
					field.NewPath("spec", "imagePullPolicy"),
					corev1.PullPolicy(""),
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
				field.Invalid(
					field.NewPath("spec", "serviceAccount"),
					"",
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects Application -> Session transition while running under manageJobsWithoutQueueName=true": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Suspend(false).
				TaskManagerReplicas(1).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				SessionCluster().
				TaskManagerReplicas(1).
				Obj(),
			manageJobsWithoutQueueName: true,
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"rejects Application -> Session transition while suspended keeping queue-name": {
			oldJob: baseSuspended,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				SessionCluster().
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"rejects Application -> Session transition while suspended and simultaneously removing queue-name": {
			oldJob: baseSuspended,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				SessionCluster().
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "", apivalidation.FieldImmutableErrorMsg),
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"rejects Session -> Application transition under manageJobsWithoutQueueName=true": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				SessionCluster().
				TaskManagerReplicas(1).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Suspend(false).
				TaskManagerReplicas(1).
				Obj(),
			manageJobsWithoutQueueName: true,
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"rejects Session -> Application transition when adding queue-name": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				SessionCluster().
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("lq").
				Suspend(false).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "lq", apivalidation.FieldImmutableErrorMsg),
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"rejects adding queue-name to an existing Session cluster": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				SessionCluster().
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("lq").
				SessionCluster().
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "lq", apivalidation.FieldImmutableErrorMsg),
				field.Required(field.NewPath("spec", "job"), "only Application mode (spec.job != nil) is supported for a Kueue-managed FlinkDeployment"),
			},
		},
		"allows detaching a suspended Application job from Kueue with a standalone-only spec": {
			oldJob: baseSuspended,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Suspend(true).
				FlinkConfig("job.autoscaler.enabled", "true").
				FlinkConfig("kubernetes.taskmanager.cpu.limit-factor", "1.5").
				Obj(),
		},
		"rejects standalone-only spec changes while still managed and suspended": {
			oldJob: baseSuspended,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(true).
				FlinkConfig("job.autoscaler.enabled", "true").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("job.autoscaler.enabled"),
					"dynamic Flink autoscaling is not supported for a Kueue-managed FlinkDeployment",
				),
			},
		},
		"rejects fractional BinarySI memory literal decoded from JSON on update while suspended": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.Resource = nil
				if err := json.Unmarshal([]byte(`{"requests":{"cpu":"2","memory":"1.5Gi"}}`), &cp.Spec.TaskManager.Resources); err != nil {
					t.Fatalf("json.Unmarshal(resources) error: %v", err)
				}
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource request memory (1.5Gi): memory size "1.5Gi" must not contain a decimal point; must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes`,
				),
			},
		},
		"allows a pre-existing labeled Session cluster to drop queue-name": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("lq").
				SessionCluster().
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				SessionCluster().
				TaskManagerReplicas(3).
				Obj(),
		},
		"allows a pre-existing labeled Session cluster to drop queue-name under manageJobsWithoutQueueName=true": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("lq").
				SessionCluster().
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				SessionCluster().
				Obj(),
			manageJobsWithoutQueueName: true,
		},
		"rejects spec changes on a pre-existing labeled Session cluster that keeps queue-name": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("lq").
				SessionCluster().
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").
				Queue("lq").
				SessionCluster().
				TaskManagerReplicas(3).
				Obj(),
			wantErrs: field.ErrorList{
				field.Required(field.NewPath("spec", "job"), "only Application mode (spec.job != nil) is supported for a Kueue-managed FlinkDeployment"),
			},
		},
		"allows Kueue eviction when root podTemplate has explicit empty pointer strings on gRPC probe, hostPath, ephemeral PVC, and dnsConfig": {
			oldJob:    admittedEmptyPtrs,
			newJob:    evictedEmptyPtrs,
			workloads: []*kueue.Workload{emptyPtrsWl},
		},
		"rejects simultaneous suspended -> running transition and scaling TaskManagerReplicas": {
			oldJob: baseSuspended,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(false).
				TaskManagerReplicas(4).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects simultaneous suspended -> running transition and spec.mode mutation": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				ServiceAccount("flink").
				Suspend(true).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				ServiceAccount("flink").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "mode"),
					flinkv1beta1.KubernetesDeploymentModeStandalone,
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects simultaneous suspended -> running transition and root spec.podTemplate mutation": {
			oldJob: preAdmissionTAS,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := preAdmissionTAS.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateRunning
				cp.Spec.PodTemplate.Spec.NodeSelector = map[string]string{"base-zone": "us-west1-a"}
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "podTemplate"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects simultaneous suspended -> running transition that modifies existing role nodeSelector or explicit TAS annotation": {
			oldJob: preAdmissionTAS,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := preAdmissionTAS.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateRunning
				cp.Spec.TaskManager.PodTemplate.Annotations[kueue.PodSetRequiredTopologyAnnotation] = "kubernetes.io/hostname"
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects simultaneous suspended -> running transition that drops existing role toleration": {
			oldJob: preAdmissionTAS,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := preAdmissionTAS.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateRunning
				cp.Spec.TaskManager.PodTemplate.Spec.Tolerations = nil
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"allows JobReconciler.stopJob running -> suspended restore when Workload overlay fields drifted while suspended": {
			oldJob:    driftedRunning,
			newJob:    driftedStoppedFromWorkload,
			workloads: []*kueue.Workload{driftedControlledWorkload},
		},
		"rejects running -> suspended drifted overlay restore when Workload is already Finished": {
			oldJob:    driftedRunning,
			newJob:    driftedStoppedFromWorkload,
			workloads: []*kueue.Workload{driftedFinishedWorkload},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects running -> suspended drifted overlay restore when Workload is owned by another UID": {
			oldJob:    driftedRunning,
			newJob:    driftedStoppedFromWorkload,
			workloads: []*kueue.Workload{driftedWrongOwnerWorkload},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects running -> suspended drifted overlay restore when also scaling TaskManagerReplicas": {
			oldJob: driftedRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := driftedStoppedFromWorkload.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(4))
				return cp
			}(),
			workloads: []*kueue.Workload{driftedControlledWorkload},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects live in-place scaling via flat and nested pipeline.jobvertex-parallelism-overrides.<vertexId> while running in native mode": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				rawCfg := `{
					"pipeline.jobvertex-parallelism-overrides.v1": "32",
					"pipeline": {"jobvertex-parallelism-overrides": {"v2": "64"}}
				}`
				if err := json.Unmarshal([]byte(rawCfg), &cp.Spec.FlinkConfiguration); err != nil {
					t.Fatalf("json.Unmarshal(rawCfg) error: %v", err)
				}
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("pipeline.jobvertex-parallelism-overrides.v1"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when pipeline.jobvertex-parallelism-overrides.v1 is configured",
				),
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("pipeline.jobvertex-parallelism-overrides.v2"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when pipeline.jobvertex-parallelism-overrides.v2 is configured",
				),
			},
		},
		"allows updating pipeline.jobvertex-parallelism-overrides.<vertexId> while running in standalone mode with explicit taskManager.replicas": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app-standalone", "ns").
				Queue("lq").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerReplicas(2).
				TaskManagerResource(new(1.0), "2048m", "").
				FlinkConfig("pipeline.jobvertex-parallelism-overrides.v1", "4").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app-standalone", "ns").
				Queue("lq").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerReplicas(2).
				TaskManagerResource(new(1.0), "2048m", "").
				FlinkConfig("pipeline.jobvertex-parallelism-overrides.v1", "8").
				Obj(),
		},
		"allows adding flink-main-container imagePullPolicy in taskManager.podTemplate while running in standalone mode with omitted spec.imagePullPolicy": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app-standalone-pull", "ns").
				Queue("lq").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerReplicas(2).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app-standalone-pull", "ns").
				Queue("lq").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerReplicas(2).
				TaskManagerResource(new(1.0), "2048m", "").
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:            flinkMainContainerName,
								ImagePullPolicy: corev1.PullAlways,
							},
						},
					},
				}).
				Obj(),
		},
		"allows removing flink-main-container imagePullPolicy in taskManager.podTemplate while running in standalone mode with omitted spec.imagePullPolicy": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app-standalone-pull-rm", "ns").
				Queue("lq").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerReplicas(2).
				TaskManagerResource(new(1.0), "2048m", "").
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:            flinkMainContainerName,
								ImagePullPolicy: corev1.PullAlways,
							},
						},
					},
				}).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app-standalone-pull-rm", "ns").
				Queue("lq").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerReplicas(2).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
		},
		"rejects reviving or replacing a running FlinkDeployment via restartNonce, jarURI, or args once its owned Workload is Finished": {
			oldJob: baseAdmittedRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.RestartNonce = new(int64(2))
				cp.Spec.Job.SavepointRedeployNonce = new(int64(2))
				cp.Spec.Job.JarURI = "local:///opt/flink/usrlib/new-job.jar"
				cp.Spec.Job.Args = []string{"--env", "prod"}
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             kueue.WorkloadFinishedReasonSucceeded,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects reviving a terminal running FlinkDeployment even before WorkloadFinished is patched or when Workload was deleted": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Status.LifecycleState = flinkv1beta1.LifecycleStateDeployed
				cp.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
				cp.Status.JobStatus.State = flinkv1beta1.JobStatusStateFailed
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Status.LifecycleState = flinkv1beta1.LifecycleStateDeployed
				cp.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
				cp.Status.JobStatus.State = flinkv1beta1.JobStatusStateFailed
				cp.Spec.RestartNonce = new(int64(1))
				return cp
			}(),
			workloads: []*kueue.Workload{baseAdmittedWl},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects suspending a running FlinkDeployment whose owned Workload is Finished": {
			oldJob: baseAdmittedRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             kueue.WorkloadFinishedReasonSucceeded,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects suspending a terminal running FlinkDeployment before WorkloadFinished is patched when Workload is not deleting": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
				cp.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
				cp.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
				cp.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
				cp.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return cp
			}(),
			workloads: []*kueue.Workload{baseAdmittedWl},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"allows Stop overlay-restoring suspension on a terminal running FlinkDeployment when non-finished Workload has deletionTimestamp": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
				cp.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
				cp.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := restoredAfterStopJob.DeepCopy()
				cp.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
				cp.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
				cp.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
				return cp
			}(),
			enableTAS: true,
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := tasAdmittedWl.DeepCopy()
					wl.DeletionTimestamp = &now
					wl.Finalizers = []string{kueue.ResourceInUseFinalizerName}
					return wl
				}(),
			},
		},
		"rejects plain suspension without overlay restore on a terminal running FlinkDeployment when non-finished Workload has deletionTimestamp": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
				cp.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
				cp.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Status.LifecycleState = flinkv1beta1.LifecycleStateStable
				cp.Status.ReconciliationStatus.State = flinkv1beta1.ReconciliationStateDeployed
				cp.Status.JobStatus.State = flinkv1beta1.JobStatusStateFinished
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return cp
			}(),
			enableTAS: true,
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := tasAdmittedWl.DeepCopy()
					wl.DeletionTimestamp = &now
					wl.Finalizers = []string{kueue.ResourceInUseFinalizerName}
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"allows metadata-only updates without touching spec on a running FlinkDeployment whose owned Workload is Finished": {
			oldJob: baseAdmittedRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				if cp.Annotations == nil {
					cp.Annotations = make(map[string]string)
				}
				cp.Annotations["example.com/audit"] = "archived"
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             kueue.WorkloadFinishedReasonSucceeded,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
		},
		"rejects running -> suspended transition that mutates component resources memory from 1536Mi to fractional 1.5Gi": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseRunning.DeepCopy()
				fd.Spec.TaskManager.Resource = nil
				if err := json.Unmarshal([]byte(`{"requests":{"cpu":"1","memory":"1536Mi"}}`), &fd.Spec.TaskManager.Resources); err != nil {
					t.Fatalf("json.Unmarshal(oldJob resources) error: %v", err)
				}
				return fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseRunning.DeepCopy()
				fd.Spec.Job.State = flinkv1beta1.JobStateSuspended
				fd.Spec.TaskManager.Resource = nil
				if err := json.Unmarshal([]byte(`{"requests":{"cpu":"1","memory":"1.5Gi"}}`), &fd.Spec.TaskManager.Resources); err != nil {
					t.Fatalf("json.Unmarshal(newJob resources) error: %v", err)
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					field.OmitValueType{},
					`resource request memory (1.5Gi): memory size "1.5Gi" must not contain a decimal point; must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes`,
				),
			},
		},
		"rejects running -> suspended transition that mutates component resources cpu from 1 to whitespace-padded \" 1 \"": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseRunning.DeepCopy()
				fd.Spec.TaskManager.Resource = nil
				if err := json.Unmarshal([]byte(`{"requests":{"cpu":"1","memory":"2Gi"}}`), &fd.Spec.TaskManager.Resources); err != nil {
					t.Fatalf("json.Unmarshal(oldJob resources) error: %v", err)
				}
				return fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseRunning.DeepCopy()
				fd.Spec.Job.State = flinkv1beta1.JobStateSuspended
				fd.Spec.TaskManager.Resource = nil
				if err := json.Unmarshal([]byte(`{"requests":{"cpu":" 1 ","memory":"2Gi"}}`), &fd.Spec.TaskManager.Resources); err != nil {
					t.Fatalf("json.Unmarshal(newJob resources) error: %v", err)
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					field.OmitValueType{},
					`resource request cpu (" 1 ") must not contain whitespace`,
				),
			},
		},
		"rejects running -> suspended transition that injects explicit empty container image override into component podTemplate": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseRunning.DeepCopy()
				fd.Spec.Image = ""
				fd.Spec.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
				if err := json.Unmarshal([]byte(`{"spec":{"containers":[{"name":"flink-main-container","image":"flink:1.20"}]}}`), fd.Spec.PodTemplate); err != nil {
					t.Fatalf("json.Unmarshal(base podTemplate) error: %v", err)
				}
				fd.Spec.TaskManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
				if err := json.Unmarshal([]byte(`{"spec":{"containers":[{"name":"flink-main-container","workingDir":"/work"}]}}`), fd.Spec.TaskManager.PodTemplate); err != nil {
					t.Fatalf("json.Unmarshal(old tm podTemplate) error: %v", err)
				}
				return fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseRunning.DeepCopy()
				fd.Spec.Image = ""
				fd.Spec.Job.State = flinkv1beta1.JobStateSuspended
				fd.Spec.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
				if err := json.Unmarshal([]byte(`{"spec":{"containers":[{"name":"flink-main-container","image":"flink:1.20"}]}}`), fd.Spec.PodTemplate); err != nil {
					t.Fatalf("json.Unmarshal(base podTemplate) error: %v", err)
				}
				fd.Spec.TaskManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
				if err := json.Unmarshal([]byte(`{"spec":{"containers":[{"name":"flink-main-container","workingDir":"/work","image":""}]}}`), fd.Spec.TaskManager.PodTemplate); err != nil {
					t.Fatalf("json.Unmarshal(new tm podTemplate) error: %v", err)
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Required(
					field.NewPath("spec", "image"),
					"image must be specified via spec.image or flink-main-container in podTemplate",
				),
			},
		},
		"rejects running -> suspended transition that injects explicit hostNetwork false override into component podTemplate": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseRunning.DeepCopy()
				fd.Spec.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
				if err := json.Unmarshal([]byte(`{"spec":{"hostNetwork":true}}`), fd.Spec.PodTemplate); err != nil {
					t.Fatalf("json.Unmarshal(base podTemplate) error: %v", err)
				}
				fd.Spec.TaskManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
				if err := json.Unmarshal([]byte(`{"metadata":{"labels":{"role":"tm"}}}`), fd.Spec.TaskManager.PodTemplate); err != nil {
					t.Fatalf("json.Unmarshal(old tm podTemplate) error: %v", err)
				}
				return fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseRunning.DeepCopy()
				fd.Spec.Job.State = flinkv1beta1.JobStateSuspended
				fd.Spec.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
				if err := json.Unmarshal([]byte(`{"spec":{"hostNetwork":true}}`), fd.Spec.PodTemplate); err != nil {
					t.Fatalf("json.Unmarshal(base podTemplate) error: %v", err)
				}
				fd.Spec.TaskManager.PodTemplate = &flinkv1beta1.PodTemplateSpec{}
				if err := json.Unmarshal([]byte(`{"metadata":{"labels":{"role":"tm"}},"spec":{"hostNetwork":false}}`), fd.Spec.TaskManager.PodTemplate); err != nil {
					t.Fatalf("json.Unmarshal(new tm podTemplate) error: %v", err)
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects post-finish update that mutates component resources memory from plain integer 1000000000 to 1G": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseAdmittedRunning.DeepCopy()
				fd.Spec.TaskManager.Resource = nil
				if err := json.Unmarshal([]byte(`{"requests":{"cpu":"1","memory":"1000000000"}}`), &fd.Spec.TaskManager.Resources); err != nil {
					t.Fatalf("json.Unmarshal(oldJob resources) error: %v", err)
				}
				return fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseAdmittedRunning.DeepCopy()
				fd.Spec.TaskManager.Resource = nil
				if err := json.Unmarshal([]byte(`{"requests":{"cpu":"1","memory":"1G"}}`), &fd.Spec.TaskManager.Resources); err != nil {
					t.Fatalf("json.Unmarshal(newJob resources) error: %v", err)
				}
				return fd
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             kueue.WorkloadFinishedReasonSucceeded,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects post-finish update that toggles flinkConfiguration JSON null to string \"null\"": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseAdmittedRunning.DeepCopy()
				if err := json.Unmarshal([]byte(`{"user.custom.opt":null}`), &fd.Spec.FlinkConfiguration); err != nil {
					t.Fatalf("json.Unmarshal(oldCfg) error: %v", err)
				}
				return fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				fd := baseAdmittedRunning.DeepCopy()
				if err := json.Unmarshal([]byte(`{"user.custom.opt":"null"}`), &fd.Spec.FlinkConfiguration); err != nil {
					t.Fatalf("json.Unmarshal(newCfg) error: %v", err)
				}
				return fd
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             kueue.WorkloadFinishedReasonSucceeded,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects two-step suspend-then-mutate spec and max-exec-time-seconds changes on a finished FlinkDeployment": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "3600"
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				cp.Spec.TaskManager.Replicas = new(int64(4))
				cp.Spec.Job.JarURI = "local:///opt/flink/usrlib/resurrect.jar"
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "7200"
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             kueue.WorkloadFinishedReasonSucceeded,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata", "labels").Key(constants.MaxExecTimeSecondsLabel),
					"7200",
					apivalidation.FieldImmutableErrorMsg,
				),
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"allows detaching queue-name while suspended on a finished FlinkDeployment": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				delete(cp.Labels, constants.QueueLabel)
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             kueue.WorkloadFinishedReasonSucceeded,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
		},
		"allows repairing suspended spec in place when Workload finished with FailedToStart before starting": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(2))
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             jobframework.FailedToStartFinishedReason,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
		},
		"allows repairing suspended spec in place after queued podTemplate labels, annotations, nodeSelector, and schedulingGates edits caused FailedToStart": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "streaming"},
					Annotations: map[string]string{"owner": "analytics"},
					Spec: corev1.PodSpec{
						NodeSelector: map[string]string{"gpu-pool": "conflicting-pool"},
						SchedulingGates: []corev1.PodSchedulingGate{
							{Name: "example.com/precheck"},
						},
					},
				})
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(2))
				cp.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Labels:      map[string]string{"team": "streaming"},
					Annotations: map[string]string{"owner": "analytics"},
					Spec: corev1.PodSpec{
						NodeSelector: map[string]string{"gpu-pool": "a100"},
						SchedulingGates: []corev1.PodSchedulingGate{
							{Name: "example.com/precheck"},
						},
					},
				})
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             jobframework.FailedToStartFinishedReason,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
		},
		"rejects mutating suspended FlinkDeployment carrying Kueue WorkloadAnnotation overlay without flavor tolerations when Workload finished with FailedToStart": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.JobManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Annotations: map[string]string{kueue.WorkloadAnnotation: baseAdmittedWl.Name},
					Labels: map[string]string{
						kueueconstants.PodSetLabel:     string(jobManagerPodSetName),
						kueueconstants.LocalQueueLabel: "lq",
					},
				})
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(2))
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             jobframework.FailedToStartFinishedReason,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"allows repairing suspended spec in place when Workload finished with FailedToStart and merged containers have duplicate env names": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Env: []corev1.EnvVar{
									{Name: "JAVA_OPTS", Value: "-Xmx1g"},
									{Name: "SHARED_ENV", Value: "from-base"},
								},
							},
						},
					},
				})
				cp.Spec.TaskManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Env: []corev1.EnvVar{
									{Name: "SHARED_ENV", Value: "from-role-override"},
								},
							},
						},
					},
				})
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(2))
				cp.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Env: []corev1.EnvVar{
									{Name: "JAVA_OPTS", Value: "-Xmx1g"},
									{Name: "SHARED_ENV", Value: "from-base"},
								},
							},
						},
					},
				})
				cp.Spec.TaskManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Env: []corev1.EnvVar{
									{Name: "SHARED_ENV", Value: "from-role-override"},
								},
							},
						},
					},
				})
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					ownerWithDupEnv := baseSuspended.DeepCopy()
					ownerWithDupEnv.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: flinkMainContainerName,
									Env: []corev1.EnvVar{
										{Name: "JAVA_OPTS", Value: "-Xmx1g"},
										{Name: "SHARED_ENV", Value: "from-base"},
									},
								},
							},
						},
					})
					ownerWithDupEnv.Spec.TaskManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: flinkMainContainerName,
									Env: []corev1.EnvVar{
										{Name: "SHARED_ENV", Value: "from-role-override"},
									},
								},
							},
						},
					})
					sanitizedPodSets, psErr := jobframework.JobPodSets(stopJobCtx, fromObject(ownerWithDupEnv), nil)
					if psErr != nil {
						t.Fatalf("JobPodSets() error: %v", psErr)
					}
					wl := baseAdmittedWl.DeepCopy()
					wl.Spec.PodSets = sanitizedPodSets
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             jobframework.FailedToStartFinishedReason,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
		},
		"rejects mutating suspended FlinkDeployment with stale admitted overlays even when Workload finished with FailedToStart": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseAdmittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				cp.Spec.TaskManager.Replicas = new(int64(2))
				return cp
			}(),
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := baseAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadFinished,
						Status:             metav1.ConditionTrue,
						Reason:             jobframework.FailedToStartFinishedReason,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"allows Stop suspended -> suspended RestorePodSetsInfo patch when Workload is evicted after manual suspension": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return cp
			}(),
			newJob:    restoredAfterStopJob,
			enableTAS: true,
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := tasAdmittedWl.DeepCopy()
					wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
						Type:               kueue.WorkloadEvicted,
						Status:             metav1.ConditionTrue,
						Reason:             kueue.WorkloadDeactivated,
						LastTransitionTime: now,
					})
					return wl
				}(),
			},
		},
		"allows Stop suspended -> suspended RestorePodSetsInfo patch when Workload has deletionTimestamp after manual suspension": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				return cp
			}(),
			newJob:    restoredAfterStopJob,
			enableTAS: true,
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := tasAdmittedWl.DeepCopy()
					wl.DeletionTimestamp = &now
					wl.Finalizers = []string{kueue.ResourceInUseFinalizerName}
					return wl
				}(),
			},
		},
		"rejects invalid max-exec-time-seconds on metadata-only update when Workload is evicted and already restored": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := restoredAfterStopJob.DeepCopy()
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "3600"
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := restoredAfterStopJob.DeepCopy()
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "30m"
				return cp
			}(),
			enableTAS: true,
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := tasAdmittedWl.DeepCopy()
					wl.Status.Admission = nil
					wl.Status.Conditions = []metav1.Condition{
						{
							Type:               kueue.WorkloadQuotaReserved,
							Status:             metav1.ConditionFalse,
							Reason:             kueue.WorkloadEvicted,
							LastTransitionTime: now,
						},
						{
							Type:               kueue.WorkloadEvicted,
							Status:             metav1.ConditionTrue,
							Reason:             kueue.WorkloadDeactivated,
							LastTransitionTime: now,
						},
					}
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata", "labels").Key(constants.MaxExecTimeSecondsLabel),
					"30m",
					`strconv.Atoi: parsing "30m": invalid syntax`,
				),
			},
		},
		"rejects overflowing max-exec-time-seconds combined with Stop RestorePodSetsInfo update when Workload is evicted": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := admittedRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "3600"
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := restoredAfterStopJob.DeepCopy()
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "3000000000"
				return cp
			}(),
			enableTAS: true,
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := tasAdmittedWl.DeepCopy()
					wl.Status.Admission = nil
					wl.Status.Conditions = []metav1.Condition{
						{
							Type:               kueue.WorkloadQuotaReserved,
							Status:             metav1.ConditionFalse,
							Reason:             kueue.WorkloadEvicted,
							LastTransitionTime: now,
						},
						{
							Type:               kueue.WorkloadEvicted,
							Status:             metav1.ConditionTrue,
							Reason:             kueue.WorkloadDeactivated,
							LastTransitionTime: now,
						},
					}
					return wl
				}(),
			},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata", "labels").Key(constants.MaxExecTimeSecondsLabel),
					3000000000,
					fmt.Sprintf("should be less than or equal to %d", math.MaxInt32),
				),
			},
		},
		"rejects non-positive max-exec-time-seconds on terminating suspended FlinkDeployment": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "0"
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata", "labels").Key(constants.MaxExecTimeSecondsLabel),
					0,
					"should be greater than 0",
				),
			},
		},
		"allows valid max-exec-time-seconds update when suspended and Workload is evicted without quota reservation": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := restoredAfterStopJob.DeepCopy()
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "3600"
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := restoredAfterStopJob.DeepCopy()
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "1800"
				return cp
			}(),
			enableTAS: true,
			workloads: []*kueue.Workload{
				func() *kueue.Workload {
					wl := tasAdmittedWl.DeepCopy()
					wl.Status.Admission = nil
					wl.Status.Conditions = []metav1.Condition{
						{
							Type:               kueue.WorkloadQuotaReserved,
							Status:             metav1.ConditionFalse,
							Reason:             kueue.WorkloadEvicted,
							LastTransitionTime: now,
						},
						{
							Type:               kueue.WorkloadEvicted,
							Status:             metav1.ConditionTrue,
							Reason:             kueue.WorkloadDeactivated,
							LastTransitionTime: now,
						},
					}
					return wl
				}(),
			},
		},
		"returns ErrWorkloadOwnerNotFound on terminating child FlinkDeployment with missing owner when SkipAncestorCheckForDeletedWorkloads is disabled": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				cp.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
					Kind:       "FlinkDeployment",
					Name:       "already-deleted-parent",
					UID:        types.UID("gone"),
					Controller: new(true),
				}}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
					Kind:       "FlinkDeployment",
					Name:       "already-deleted-parent",
					UID:        types.UID("gone"),
					Controller: new(true),
				}}
				return cp
			}(),
			disableSkipAncestorCheck:   true,
			manageJobsWithoutQueueName: true,
			wantNonFieldErr:            jobframework.ErrWorkloadOwnerNotFound,
		},
		"allows finalizer removal on terminating child FlinkDeployment with missing owner when SkipAncestorCheckForDeletedWorkloads is enabled": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				cp.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
					Kind:       "FlinkDeployment",
					Name:       "already-deleted-parent",
					UID:        types.UID("gone"),
					Controller: new(true),
				}}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
					Kind:       "FlinkDeployment",
					Name:       "already-deleted-parent",
					UID:        types.UID("gone"),
					Controller: new(true),
				}}
				return cp
			}(),
			manageJobsWithoutQueueName: true,
		},
		"allows finalizer removal on terminating top-level FlinkDeployment with pre-existing invalid spec under default SkipAncestorCheckForDeletedWorkloads": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				cp.Spec.JobManager.Resource.Memory = ""
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Spec.JobManager.Resource.Memory = ""
				return cp
			}(),
		},
		"allows finalizer removal on terminating pre-existing queue-labeled Session cluster under default SkipAncestorCheckForDeletedWorkloads": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := testingflinkdeployment.MakeFlinkDeployment("legacy-session", "ns").
					Queue("lq").
					SessionCluster().
					Obj()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := testingflinkdeployment.MakeFlinkDeployment("legacy-session", "ns").
					Queue("lq").
					SessionCluster().
					Obj()
				cp.DeletionTimestamp = &now
				return cp
			}(),
		},
		"rejects spec and label mutations on terminating FlinkDeployment under default SkipAncestorCheckForDeletedWorkloads": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				cp.Labels[constants.WorkloadPriorityClassLabel] = "high-prio"
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				cp.Spec.Job.State = flinkv1beta1.JobStateRunning
				cp.Spec.TaskManager.Replicas = new(int64(4))
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata", "labels").Key(constants.WorkloadPriorityClassLabel),
					"",
					"WorkloadPriorityClass cannot be removed from a workload",
				),
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is being deleted", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects spec mutation on terminating top-level FlinkDeployment even when newObj injects fake missing OwnerReference": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				cp.Spec.Job.State = flinkv1beta1.JobStateRunning
				cp.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
					Kind:       "FlinkDeployment",
					Name:       "forged-missing-parent",
					UID:        types.UID("forged"),
					Controller: new(true),
				}}
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is being deleted", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"rejects two-step unmanaged-to-managed update when pre-existing wait-for-pods-ready annotation exceeds maxTimeoutOnWorkload": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-app", "ns").
				Suspend(true).
				Annotation(constants.WaitForPodsReadyAnnotation, `{"timeoutSeconds":86400,"recoveryTimeoutSeconds":7200}`).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-app", "ns").
				Queue("lq").
				Suspend(true).
				Annotation(constants.WaitForPodsReadyAnnotation, `{"timeoutSeconds":86400,"recoveryTimeoutSeconds":7200}`).
				Obj(),
			enableWorkloadLevelWFPR: true,
			maxTimeoutOnWorkload:    &metav1.Duration{Duration: 5 * time.Minute},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata", "annotations").Key(constants.WaitForPodsReadyAnnotation),
					"",
					"timeoutSeconds must be less than or equal to 300 seconds",
				),
				field.Invalid(
					field.NewPath("metadata", "annotations").Key(constants.WaitForPodsReadyAnnotation),
					"",
					"recoveryTimeoutSeconds must be less than or equal to 300 seconds",
				),
			},
		},
		"deduplicates wait-for-pods-ready error on single-step unmanaged-to-managed update exceeding maxTimeoutOnWorkload": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-app", "ns").
				Suspend(true).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-app", "ns").
				Queue("lq").
				Suspend(true).
				Annotation(constants.WaitForPodsReadyAnnotation, `{"timeoutSeconds":86400}`).
				Obj(),
			enableWorkloadLevelWFPR: true,
			maxTimeoutOnWorkload:    &metav1.Duration{Duration: 5 * time.Minute},
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("metadata", "annotations").Key(constants.WaitForPodsReadyAnnotation),
					"",
					"timeoutSeconds must be less than or equal to 300 seconds",
				),
			},
		},
		"allows update on already-managed FlinkDeployment when unchanged wait-for-pods-ready exceeds lowered maxTimeoutOnWorkload": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Annotations = map[string]string{
					constants.WaitForPodsReadyAnnotation: `{"timeoutSeconds":86400}`,
				}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.Job.State = flinkv1beta1.JobStateSuspended
				cp.Annotations = map[string]string{
					constants.WaitForPodsReadyAnnotation: `{"timeoutSeconds":86400}`,
				}
				return cp
			}(),
			enableWorkloadLevelWFPR: true,
			maxTimeoutOnWorkload:    &metav1.Duration{Duration: 5 * time.Minute},
		},
		"allows running -> suspended transition on child FlinkDeployment owned by Kueue-managed ancestor": {
			oldJob:       makeAncestorOwnedChild(flinkv1beta1.JobStateRunning, 1),
			newJob:       makeAncestorOwnedChild(flinkv1beta1.JobStateSuspended, 1),
			extraObjects: []client.Object{ancestorParent},
		},
		"allows suspended -> running transition without child Workload on child FlinkDeployment owned by Kueue-managed ancestor": {
			oldJob:       makeAncestorOwnedChild(flinkv1beta1.JobStateSuspended, 1),
			newJob:       makeAncestorOwnedChild(flinkv1beta1.JobStateRunning, 1),
			extraObjects: []client.Object{ancestorParent},
		},
		"allows running -> running replica scaling on child FlinkDeployment owned by Kueue-managed ancestor": {
			oldJob:       makeAncestorOwnedChild(flinkv1beta1.JobStateRunning, 1),
			newJob:       makeAncestorOwnedChild(flinkv1beta1.JobStateRunning, 4),
			extraObjects: []client.Object{ancestorParent},
		},
		"nil integrationManager fallback enforces running-state immutability on queue-labeled top-level Application FlinkDeployment": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(4))
				return cp
			}(),
			nilIntegrationManager: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
		"nil integrationManager with manageJobsWithoutQueueName=true rejects direct unsuspend of unlabeled Application FlinkDeployment without admitted Workload": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("unlabeled-nil-mgr", "ns").
				Suspend(true).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("unlabeled-nil-mgr", "ns").
				Suspend(false).
				Obj(),
			nilIntegrationManager:      true,
			manageJobsWithoutQueueName: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg),
				),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, tc.enableTAS)
			features.SetFeatureGateDuringTest(t, features.SkipAncestorCheckForDeletedWorkloads, !tc.disableSkipAncestorCheck)
			features.SetFeatureGateDuringTest(t, features.WorkloadLevelWaitForPodsReady, tc.enableWorkloadLevelWFPR)
			ctx, _ := utiltesting.ContextWithLog(t)
			builder := utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme).WithObjects(&corev1.Namespace{
				Name: "ns",
			})
			if err := SetupIndexes(ctx, utiltesting.AsIndexer(builder)); err != nil {
				t.Fatalf("SetupIndexes() error: %v", err)
			}
			if len(tc.extraObjects) > 0 {
				builder = builder.WithObjects(tc.extraObjects...)
			}
			if tc.defaultQueue != nil {
				builder = builder.WithObjects(tc.defaultQueue)
			}
			for _, flv := range tc.flavors {
				builder = builder.WithObjects(flv)
			}
			for _, wl := range tc.workloads {
				builder = builder.WithObjects(wl)
			}
			cl := builder.Build()
			var queues *qcache.Manager
			if tc.defaultQueue != nil {
				queues = qcache.NewManagerForUnitTests(cl, schdcache.New(cl))
				if err := queues.AddLocalQueue(ctx, tc.defaultQueue); err != nil {
					t.Fatalf("AddLocalQueue() error: %v", err)
				}
			}
			var integrationMgr *jobframework.IntegrationManager
			if !tc.nilIntegrationManager {
				integrationMgr = newTestIntegrationManager(t)
			}
			wh := &flinkDeploymentWebhook{
				integrationManager:           integrationMgr,
				client:                       cl,
				queues:                       queues,
				manageJobsWithoutQueueName:   tc.manageJobsWithoutQueueName,
				managedJobsNamespaceSelector: labels.Everything(),
				maxTimeoutOnWorkload:         tc.maxTimeoutOnWorkload,
			}
			_, err := wh.ValidateUpdate(ctx, tc.oldJob, tc.newJob)
			if tc.wantNonFieldErr != nil {
				if !errors.Is(err, tc.wantNonFieldErr) {
					t.Fatalf("ValidateUpdate() error = %v, want %v", err, tc.wantNonFieldErr)
				}
				return
			}
			gotErrs := extractFieldErrors(t, err)
			if diff := cmp.Diff(tc.wantErrs, gotErrs, cmpopts.IgnoreFields(field.Error{}, "BadValue")); diff != "" {
				t.Errorf("ValidateUpdate() error list mismatch (-want,+got):\n%s\nfull err: %v", diff, err)
			}
		})
	}
}

func TestValidateUpdateWorkloadLookup(t *testing.T) {
	now := metav1.NewTime(time.Now())
	owner := testingflinkdeployment.MakeFlinkDeployment("target-app", "ns").
		Queue("lq").
		UID("uid-target").
		Suspend(true).
		Obj()
	ownerPodSets, err := fromObject(owner).PodSets(t.Context(), nil)
	if err != nil {
		t.Fatalf("owner PodSets() error: %v", err)
	}
	scaledUpdate := owner.DeepCopy()
	scaledUpdate.Spec.TaskManager.Replicas = new(int64(4))

	deterministicWlName := GetWorkloadNameForFlinkDeployment(owner.Name, owner.UID)
	makeQuotaReservedWorkload := func(name string, uid types.UID, finished bool) *kueue.Workload {
		wl := utiltestingapi.MakeWorkload(name, owner.Namespace).
			Queue("lq").
			PodSets(ownerPodSets...).
			ControllerReference(gvk, owner.Name, string(uid)).
			ReserveQuotaAt(
				utiltestingapi.MakeAdmission("cq").
					PodSets(
						utiltestingapi.MakePodSetAssignment(jobManagerPodSetName).Obj(),
						utiltestingapi.MakePodSetAssignment(taskManagerPodSetName).Obj(),
					).
					Obj(),
				now.Time,
			).
			AdmittedAt(false, now.Time).
			Obj()
		if finished {
			wl.Status.Conditions = append(wl.Status.Conditions, metav1.Condition{
				Type:               kueue.WorkloadFinished,
				Status:             metav1.ConditionTrue,
				Reason:             kueue.WorkloadFinishedReasonSucceeded,
				LastTransitionTime: now,
			})
		}
		return wl
	}

	unrelatedWl := utiltestingapi.MakeWorkload("unrelated-wl", owner.Namespace).
		Queue("lq").
		PodSets(ownerPodSets...).
		ControllerReference(gvk, "other-app", "uid-other").
		Obj()

	quotaReservedImmutableErrs := field.ErrorList{
		field.Invalid(
			field.NewPath("spec"),
			field.OmitValueType{},
			fmt.Sprintf("%s while Workload has quota reserved", apivalidation.FieldImmutableErrorMsg),
		),
	}
	finishedImmutableErrs := field.ErrorList{
		field.Invalid(
			field.NewPath("spec"),
			field.OmitValueType{},
			fmt.Sprintf("%s once Workload is finished", apivalidation.FieldImmutableErrorMsg),
		),
	}

	testCases := map[string]struct {
		workloads         []*kueue.Workload
		wantWorkloadGets  int
		wantWorkloadLists int
		wantIndexedItems  int
		wantErrs          field.ErrorList
	}{
		"deterministic Get finds active Workload without WorkloadList scan": {
			workloads: []*kueue.Workload{
				makeQuotaReservedWorkload(deterministicWlName, owner.UID, false),
				unrelatedWl,
			},
			wantWorkloadGets:  1,
			wantWorkloadLists: 0,
			wantErrs:          quotaReservedImmutableErrs,
		},
		"falls back to indexed zero-deepcopy List when deterministic Workload is not found": {
			workloads: []*kueue.Workload{
				makeQuotaReservedWorkload("custom-wl-name", owner.UID, false),
				unrelatedWl,
			},
			wantWorkloadGets:  1,
			wantWorkloadLists: 1,
			wantIndexedItems:  1,
			wantErrs:          quotaReservedImmutableErrs,
		},
		"falls through to indexed List when deterministic-name Workload is finished and resolves active replacement": {
			workloads: []*kueue.Workload{
				makeQuotaReservedWorkload(deterministicWlName, owner.UID, true),
				makeQuotaReservedWorkload("custom-wl-replacement", owner.UID, false),
				unrelatedWl,
			},
			wantWorkloadGets:  1,
			wantWorkloadLists: 1,
			wantIndexedItems:  2,
			wantErrs:          quotaReservedImmutableErrs,
		},
		"enforces post-finish spec immutability when finished deterministic-name Workload has no active replacement": {
			workloads: []*kueue.Workload{
				makeQuotaReservedWorkload(deterministicWlName, owner.UID, true),
				unrelatedWl,
			},
			wantWorkloadGets:  1,
			wantWorkloadLists: 1,
			wantIndexedItems:  1,
			wantErrs:          finishedImmutableErrs,
		},
		"falls through to indexed List when deterministic-name Workload has a different controller UID": {
			workloads: []*kueue.Workload{
				utiltestingapi.MakeWorkload(deterministicWlName, owner.Namespace).
					Queue("lq").
					PodSets(ownerPodSets...).
					ControllerReference(gvk, owner.Name, "uid-stale").
					Obj(),
				makeQuotaReservedWorkload("custom-wl-active", owner.UID, false),
				unrelatedWl,
			},
			wantWorkloadGets:  1,
			wantWorkloadLists: 1,
			wantIndexedItems:  2,
			wantErrs:          quotaReservedImmutableErrs,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)

			objs := make([]client.Object, 0, len(tc.workloads)+1)
			objs = append(objs, &corev1.Namespace{Name: owner.Namespace})
			for _, wl := range tc.workloads {
				objs = append(objs, wl.DeepCopy())
			}

			var workloadGets, workloadLists int
			var sawFieldSelector, sawDisableDeepCopy bool
			builder := utiltesting.NewClientBuilder().
				WithObjects(objs...).
				WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*kueue.Workload); ok {
							workloadGets++
						}
						return c.Get(ctx, key, obj, opts...)
					},
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						wlList, isWlList := list.(*kueue.WorkloadList)
						if isWlList {
							workloadLists++
							listOpts := &client.ListOptions{}
							listOpts.ApplyOptions(opts)
							if listOpts.FieldSelector != nil && listOpts.FieldSelector.String() != "" {
								sawFieldSelector = true
							}
							if listOpts.UnsafeDisableDeepCopy != nil && *listOpts.UnsafeDisableDeepCopy {
								sawDisableDeepCopy = true
							}
						}
						if err := c.List(ctx, list, opts...); err != nil {
							return err
						}
						if isWlList && len(wlList.Items) != tc.wantIndexedItems {
							t.Errorf("indexed WorkloadList returned %d items, want %d", len(wlList.Items), tc.wantIndexedItems)
						}
						return nil
					},
				})
			if err := SetupIndexes(ctx, utiltesting.AsIndexer(builder)); err != nil {
				t.Fatalf("SetupIndexes() error: %v", err)
			}

			wh := &flinkDeploymentWebhook{
				integrationManager:           newTestIntegrationManager(t),
				client:                       builder.Build(),
				managedJobsNamespaceSelector: labels.Everything(),
			}
			_, err := wh.ValidateUpdate(ctx, owner, scaledUpdate)
			gotErrs := extractFieldErrors(t, err)
			if diff := cmp.Diff(tc.wantErrs, gotErrs, cmpopts.IgnoreFields(field.Error{}, "BadValue")); diff != "" {
				t.Errorf("ValidateUpdate() error list mismatch (-want,+got):\n%s\nfull err: %v", diff, err)
			}
			if workloadGets != tc.wantWorkloadGets || workloadLists != tc.wantWorkloadLists {
				t.Errorf("Workload client calls = (get=%d, list=%d), want (get=%d, list=%d)",
					workloadGets, workloadLists, tc.wantWorkloadGets, tc.wantWorkloadLists)
			}
			if tc.wantWorkloadLists > 0 && (!sawFieldSelector || !sawDisableDeepCopy) {
				t.Errorf("fallback WorkloadList options = (fieldSelector=%v, disableDeepCopy=%v), want (true, true)",
					sawFieldSelector, sawDisableDeepCopy)
			}
		})
	}
}

func TestValidateDelete(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	wh := &flinkDeploymentWebhook{}
	obj := testingflinkdeployment.MakeFlinkDeployment("valid", "ns").Queue("lq").Obj()
	if _, err := wh.ValidateDelete(ctx, obj); err != nil {
		t.Errorf("ValidateDelete() unexpected error: %v", err)
	}
}
