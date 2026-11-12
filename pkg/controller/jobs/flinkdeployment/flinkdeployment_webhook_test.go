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
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingflinkdeployment "sigs.k8s.io/kueue/pkg/util/testingjobs/flinkdeployment"
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
							corev1.ResourceCPU: testingflinkdeployment.MustParseQuantity("1"),
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
						corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("2Pi"),
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
		"rejects kubernetes.operator.deployment.rollback.enabled=true": {
			job: testingflinkdeployment.MakeFlinkDeployment("rollback-enabled", "ns").
				Queue("lq").
				FlinkConfig("kubernetes.operator.deployment.rollback.enabled", "true").
				Obj(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.deployment.rollback.enabled"),
					"automatic Flink Operator deployment rollback is not supported for a Kueue-managed FlinkDeployment",
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
					`resource request memory (2g): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) because Flink interprets "2g" incompatibly or rejects it`,
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
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource request memory (1m): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) because Flink interprets "1m" incompatibly or rejects it`,
				),
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
						corev1.ResourceCPU: testingflinkdeployment.MustParseQuantity("2"),
					},
				}
				return fd
			}(),
			wantErrs: field.ErrorList{
				field.Forbidden(field.NewPath("spec", "taskManager", "resources"), "may not be specified when resource is also specified"),
			},
		},
		"rejects plain integer byte literals in component resources while accepting 1Pi BinarySI": {
			job: func() *flinkv1beta1.FlinkDeployment {
				var numQty flinkv1beta1.Quantity
				if err := json.Unmarshal([]byte(`1000000000`), &numQty); err != nil {
					t.Fatalf("json.Unmarshal(1000000000) error: %v", err)
				}
				return testingflinkdeployment.MakeFlinkDeployment("plain-int-bytes", "ns").
					Queue("lq").
					JobManagerRawResources(flinkv1beta1.ResourceRequirements{
						Requests: flinkv1beta1.ResourceList{
							corev1.ResourceCPU:    testingflinkdeployment.MustParseQuantity("1"),
							corev1.ResourceMemory: numQty,
						},
					}).
					TaskManagerRawResources(flinkv1beta1.ResourceRequirements{
						Requests: flinkv1beta1.ResourceList{
							corev1.ResourceCPU:    testingflinkdeployment.MustParseQuantity("2"),
							corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("1024Mi"),
						},
						Limits: flinkv1beta1.ResourceList{
							corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("1Pi"),
						},
					}).
					Obj()
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resources"),
					nil,
					`resource request memory (1000000000): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) because Flink interprets "1000000000" incompatibly or rejects it`,
				),
			},
		},
		"rejects fractional BinarySI memory literals and Ei suffixes in component resources.requests": {
			job: testingflinkdeployment.MakeFlinkDeployment("bad-raw-mem-requests", "ns").
				Queue("lq").
				JobManagerRawResources(flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("1.5Gi"),
					},
				}).
				TaskManagerRawResources(flinkv1beta1.ResourceRequirements{
					Requests: flinkv1beta1.ResourceList{
						corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("8Ei"),
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resources"),
					nil,
					`resource request memory (1.5Gi): memory size "1.5Gi" must not contain a decimal point; must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi)`,
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource request memory (8Ei): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) because Flink interprets "8Ei" incompatibly or rejects it`,
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
							corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("512Mi"),
						},
						Limits: flinkv1beta1.ResourceList{
							corev1.ResourceMemory: sciQty,
						},
					}).
					TaskManagerRawResources(flinkv1beta1.ResourceRequirements{
						Requests: flinkv1beta1.ResourceList{
							corev1.ResourceMemory: testingflinkdeployment.MustParseQuantity("512Mi"),
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
					`resource limit memory (1e9): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) because Flink interprets "1e9" incompatibly or rejects it`,
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource limit memory (+1Gi): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi), got "+1Gi"`,
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
					`resource request memory (2048m): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) because Flink interprets "2048m" incompatibly or rejects it`,
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource request memory (2G): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) because Flink interprets "2G" incompatibly or rejects it`,
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
					`job-manager podSet: initContainer "init-a": unsupported imagePullPolicy "always"`,
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
					`job-manager podSet: toleration[0]: invalid Equal toleration`,
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
		"rejects non-prefix component tolerations when root tolerations are set": {
			job: testingflinkdeployment.MakeFlinkDeployment("non-prefix-tolerations", "ns").
				Queue("lq").
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations: []corev1.Toleration{
							{Key: "base", Operator: corev1.TolerationOpExists},
						},
					},
				}).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Tolerations: []corev1.Toleration{
							{Key: "other", Operator: corev1.TolerationOpExists},
						},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec"),
					field.OmitValueType{},
					`task-manager podSet: component podTemplate.spec.tolerations must include spec.podTemplate.spec.tolerations as a prefix when both are specified`,
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
	stopJobCtx, _ := utiltesting.ContextWithLog(t)
	features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, true)

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

	basePodSets, err := fromObject(baseSuspended).PodSets(stopJobCtx, nil)
	if err != nil {
		t.Fatalf("baseSuspended PodSets() error: %v", err)
	}
	baseWl := utiltestingapi.MakeWorkload("wl-app", "ns").PodSets(basePodSets...).Obj()
	admittedRunning := baseSuspended.DeepCopy()
	if err := fromObject(admittedRunning).RunWithPodSetsInfo(stopJobCtx, nil, []podset.PodSetInfo{
		{
			Name:            jobManagerPodSetName,
			Count:           1,
			Labels:          map[string]string{kueueconstants.LocalQueueLabel: "lq", kueueconstants.PodSetLabel: string(jobManagerPodSetName)},
			Annotations:     map[string]string{kueue.PodSetUnconstrainedTopologyAnnotation: "true"},
			NodeSelector:    map[string]string{"pool": "cpu"},
			Tolerations:     []corev1.Toleration{{Key: "spot", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}},
		},
		{
			Name:            taskManagerPodSetName,
			Count:           1,
			Labels:          map[string]string{kueueconstants.LocalQueueLabel: "lq", kueueconstants.PodSetLabel: string(taskManagerPodSetName)},
			Annotations:     map[string]string{kueue.PodSetUnconstrainedTopologyAnnotation: "true"},
			NodeSelector:    map[string]string{"pool": "cpu"},
			Tolerations:     []corev1.Toleration{{Key: "spot", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}},
		},
	}); err != nil {
		t.Fatalf("RunWithPodSetsInfo(admittedRunning) error: %v", err)
	}
	stoppedFromAdmitted := admittedRunning.DeepCopy()
	fromObject(stoppedFromAdmitted).Suspend()
	fromObject(stoppedFromAdmitted).RestorePodSetsInfo(stopJobCtx, jobframework.GetPodSetsInfoFromWorkload(baseWl))

	// Root podTemplate with explicit empty pointer strings on gRPC probe, hostPath, ephemeral PVC, and dnsConfig.
	emptyPtrsBase := testingflinkdeployment.MakeFlinkDeployment("app-empty-ptrs", "ns").
		Queue("lq").
		UID("uid-empty-ptrs").
		Suspend(true).
		Obj()
	if err := json.Unmarshal(
		[]byte(
			`{"spec":{"containers":[{"name":"flink-main-container","image":"flink:1.20","livenessProbe":{"grpc":{"port":9090,"service":""}}}],"dnsConfig":{"searches":[]},"volumes":[{"name":"hp","hostPath":{"path":"/tmp","type":""}},{"name":"eph","ephemeral":{"volumeClaimTemplate":{"spec":{"accessModes":["ReadWriteOnce"],"dataSource":{"apiGroup":"","kind":"PersistentVolumeClaim","name":"src"},"resources":{"requests":{"storage":"1Gi"}}}}}}]}}`,
		),
		&emptyPtrsBase.Spec.PodTemplate,
	); err != nil {
		t.Fatalf("unmarshal emptyPtrsBase podTemplate: %v", err)
	}
	emptyPtrsPodSets, err := fromObject(emptyPtrsBase).PodSets(stopJobCtx, nil)
	if err != nil {
		t.Fatalf("emptyPtrsBase PodSets() error: %v", err)
	}
	emptyPtrsWl := utiltestingapi.MakeWorkload("wl-empty-ptrs", "ns").PodSets(emptyPtrsPodSets...).Obj()
	admittedEmptyPtrs := emptyPtrsBase.DeepCopy()
	if err := fromObject(admittedEmptyPtrs).RunWithPodSetsInfo(stopJobCtx, nil, []podset.PodSetInfo{
		{Name: jobManagerPodSetName, Count: 1, NodeSelector: map[string]string{"flavor": "on-demand"}},
		{Name: taskManagerPodSetName, Count: 1, NodeSelector: map[string]string{"flavor": "on-demand"}},
	}); err != nil {
		t.Fatalf("RunWithPodSetsInfo(admittedEmptyPtrs) error: %v", err)
	}
	evictedEmptyPtrs := admittedEmptyPtrs.DeepCopy()
	fromObject(evictedEmptyPtrs).Suspend()
	fromObject(evictedEmptyPtrs).RestorePodSetsInfo(stopJobCtx, jobframework.GetPodSetsInfoFromWorkload(emptyPtrsWl))

	// Simulate EquivalentToWorkload overlay drift while suspended followed by RunWithPodSetsInfo and stopJob restore.
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
	driftedWl := utiltestingapi.MakeWorkload("wl-app-drifted", "ns").PodSets(initialDriftedPodSets...).Obj()
	editedWhileSuspended := initialDriftedBase.DeepCopy()
	editedWhileSuspended.Spec.JobManager.PodTemplate = testingflinkdeployment.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
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
	})
	editedWhileSuspended.Spec.TaskManager.PodTemplate = testingflinkdeployment.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
		Labels:      map[string]string{"env": "prod"},
		Annotations: map[string]string{"owner": "team-b"},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"tier": "new-tm"},
		},
	})
	driftedRunning := editedWhileSuspended.DeepCopy()
	if err := fromObject(driftedRunning).RunWithPodSetsInfo(stopJobCtx, nil, []podset.PodSetInfo{
		{Name: jobManagerPodSetName, Count: 1, Labels: map[string]string{kueueconstants.LocalQueueLabel: "lq"}, NodeSelector: map[string]string{"flavor-pool": "cpu-spot"}},
		{Name: taskManagerPodSetName, Count: 1, Labels: map[string]string{kueueconstants.LocalQueueLabel: "lq"}, NodeSelector: map[string]string{"flavor-pool": "gpu-spot"}},
	}); err != nil {
		t.Fatalf("RunWithPodSetsInfo(driftedRunning) error: %v", err)
	}
	driftedStopped := driftedRunning.DeepCopy()
	fromObject(driftedStopped).Suspend()
	fromObject(driftedStopped).RestorePodSetsInfo(stopJobCtx, jobframework.GetPodSetsInfoFromWorkload(driftedWl))

	// TAS slice-size fixture shrunk out-of-band via /scale subresource before Stop().
	tasSliceSuspended := testingflinkdeployment.MakeFlinkDeployment("app-tas-slice", "ns").
		Queue("lq").
		UID("uid-tas-slice").
		Suspend(true).
		TaskManagerReplicas(4).
		TaskManagerPodTemplate(&corev1.PodTemplateSpec{
			Annotations: map[string]string{
				kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
				kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
				kueue.PodSetSliceSizeAnnotation:             "4",
			},
		}).
		Obj()
	tasSlicePodSets, err := fromObject(tasSliceSuspended).PodSets(stopJobCtx, nil)
	if err != nil {
		t.Fatalf("tasSliceSuspended PodSets() error: %v", err)
	}
	tasSliceWl := utiltestingapi.MakeWorkload("wl-tas-slice", "ns").PodSets(tasSlicePodSets...).Obj()
	tasSliceRunning := tasSliceSuspended.DeepCopy()
	if err := fromObject(tasSliceRunning).RunWithPodSetsInfo(stopJobCtx, nil, []podset.PodSetInfo{
		{Name: jobManagerPodSetName, Count: 1, NodeSelector: map[string]string{"flavor": "cpu"}},
		{Name: taskManagerPodSetName, Count: 4, NodeSelector: map[string]string{"flavor": "gpu"}, SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}}},
	}); err != nil {
		t.Fatalf("RunWithPodSetsInfo(tasSliceRunning) error: %v", err)
	}
	tasShrunkRunning := tasSliceRunning.DeepCopy()
	tasShrunkRunning.Spec.TaskManager.Replicas = new(int64(2))
	tasShrunkStopped := tasShrunkRunning.DeepCopy()
	fromObject(tasShrunkStopped).Suspend()
	fromObject(tasShrunkStopped).RestorePodSetsInfo(stopJobCtx, jobframework.GetPodSetsInfoFromWorkload(tasSliceWl))

	// HA TAS PodSet-grouped fixture scaled out-of-band to 0 or 4 via /scale subresource before Stop().
	tasGroupAnnotations := map[string]string{
		kueue.PodSetGroupName:                  "grp",
		kueue.PodSetRequiredTopologyAnnotation: "cloud.provider.com/rack",
	}
	tasGroupSuspended := testingflinkdeployment.MakeFlinkDeployment("app-tas-group", "ns").
		Queue("lq").
		UID("uid-tas-group").
		Suspend(true).
		JobManagerReplicas(2).
		TaskManagerReplicas(1).
		JobManagerPodTemplate(&corev1.PodTemplateSpec{Annotations: tasGroupAnnotations}).
		TaskManagerPodTemplate(&corev1.PodTemplateSpec{Annotations: tasGroupAnnotations}).
		Obj()
	tasGroupPodSets, err := fromObject(tasGroupSuspended).PodSets(stopJobCtx, nil)
	if err != nil {
		t.Fatalf("tasGroupSuspended PodSets() error: %v", err)
	}
	tasGroupWl := utiltestingapi.MakeWorkload("wl-tas-group", "ns").PodSets(tasGroupPodSets...).Obj()
	tasGroupRunning := tasGroupSuspended.DeepCopy()
	if err := fromObject(tasGroupRunning).RunWithPodSetsInfo(stopJobCtx, nil, []podset.PodSetInfo{
		{Name: jobManagerPodSetName, Count: 2, NodeSelector: map[string]string{"flavor": "cpu"}, SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}}},
		{Name: taskManagerPodSetName, Count: 1, NodeSelector: map[string]string{"flavor": "gpu"}, SchedulingGates: []corev1.PodSchedulingGate{{Name: kueue.TopologySchedulingGate}}},
	}); err != nil {
		t.Fatalf("RunWithPodSetsInfo(tasGroupRunning) error: %v", err)
	}
	tasGroupScaledZeroRunning := tasGroupRunning.DeepCopy()
	tasGroupScaledZeroRunning.Spec.TaskManager.Replicas = new(int64(0))
	tasGroupScaledZeroStopped := tasGroupScaledZeroRunning.DeepCopy()
	fromObject(tasGroupScaledZeroStopped).Suspend()
	fromObject(tasGroupScaledZeroStopped).RestorePodSetsInfo(stopJobCtx, jobframework.GetPodSetsInfoFromWorkload(tasGroupWl))

	tasGroupScaledFourRunning := tasGroupRunning.DeepCopy()
	tasGroupScaledFourRunning.Spec.TaskManager.Replicas = new(int64(4))
	tasGroupScaledFourStopped := tasGroupScaledFourRunning.DeepCopy()
	fromObject(tasGroupScaledFourStopped).Suspend()
	fromObject(tasGroupScaledFourStopped).RestorePodSetsInfo(stopJobCtx, jobframework.GetPodSetsInfoFromWorkload(tasGroupWl))

	scaledToZeroRunning := admittedRunning.DeepCopy()
	scaledToZeroRunning.Spec.TaskManager.Replicas = new(int64(0))
	scaledToZeroStopped := scaledToZeroRunning.DeepCopy()
	fromObject(scaledToZeroStopped).Suspend()
	fromObject(scaledToZeroStopped).RestorePodSetsInfo(stopJobCtx, jobframework.GetPodSetsInfoFromWorkload(baseWl))

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
		child.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: flinkv1beta1.SchemeGroupVersion.String(),
			Kind:       "FlinkDeployment",
			Name:       ancestorParent.Name,
			UID:        ancestorParent.UID,
			Controller: new(true),
		}}
		return child
	}

	immutableNotSuspendedMsg := fmt.Sprintf("%s when FlinkDeployment is not suspended", apivalidation.FieldImmutableErrorMsg)
	immutableDeletingMsg := fmt.Sprintf("%s when FlinkDeployment is being deleted", apivalidation.FieldImmutableErrorMsg)

	testCases := map[string]struct {
		oldJob                     *flinkv1beta1.FlinkDeployment
		newJob                     *flinkv1beta1.FlinkDeployment
		extraObjects               []client.Object
		nilIntegrationManager      bool
		disableSkipAncestorCheck   bool
		manageJobsWithoutQueueName bool
		enableTAS                  bool
		enableTASTopologySpreading bool
		enableWorkloadLevelWFPR    bool
		maxTimeoutOnWorkload       *metav1.Duration
		defaultQueue               *kueue.LocalQueue
		wantNonFieldErr            error
		wantErrs                   field.ErrorList
	}{
		"skips unmanaged Session FlinkDeployment update": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged", "ns").SessionCluster().Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged", "ns").SessionCluster().TaskManagerReplicas(5).Obj(),
		},
		"skips unmanaged Application FlinkDeployment update with Flink MemorySize 2g in component resources": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal(
					[]byte(
						`{"metadata":{"name":"unmanaged-app","namespace":"ns"},"spec":{"image":"flink:1.20","job":{"jarURI":"local:///opt/app.jar","parallelism":1,"state":"running"},"jobManager":{"resources":{"requests":{"cpu":"1","memory":"2g"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"2g"}}}}}`,
					),
					&fd,
				); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				return &fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal(
					[]byte(
						`{"metadata":{"name":"unmanaged-app","namespace":"ns"},"spec":{"image":"flink:1.20","job":{"jarURI":"local:///opt/app.jar","parallelism":4,"state":"running"},"jobManager":{"resources":{"requests":{"cpu":"1","memory":"2g"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"4g"}}}}}`,
					),
					&fd,
				); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				return &fd
			}(),
		},
		"allows a pre-existing labeled Session cluster to drop queue-name even with a default LocalQueue in the namespace": {
			oldJob:       testingflinkdeployment.MakeFlinkDeployment("session", "ns").Queue("lq").SessionCluster().Obj(),
			newJob:       testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
			defaultQueue: utiltestingapi.MakeLocalQueue("default", "ns").Obj(),
		},
		"allows a pre-existing labeled Session cluster to drop queue-name under manageJobsWithoutQueueName=true": {
			oldJob:                     testingflinkdeployment.MakeFlinkDeployment("session", "ns").Queue("lq").SessionCluster().Obj(),
			newJob:                     testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
			manageJobsWithoutQueueName: true,
		},
		"rejects spec changes on a pre-existing labeled Session cluster that keeps queue-name": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").Queue("lq").SessionCluster().Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").Queue("lq").SessionCluster().TaskManagerReplicas(4).Obj(),
			wantErrs: field.ErrorList{
				field.Required(field.NewPath("spec", "job"), "only Application mode (spec.job != nil) is supported for a Kueue-managed FlinkDeployment"),
			},
		},
		"rejects adding queue-name to an existing Session cluster": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").Queue("lq").SessionCluster().Obj(),
			wantErrs: field.ErrorList{
				field.Required(field.NewPath("spec", "job"), "only Application mode (spec.job != nil) is supported for a Kueue-managed FlinkDeployment"),
			},
		},
		"rejects Application -> Session transition while running under manageJobsWithoutQueueName=true": {
			oldJob:                     testingflinkdeployment.MakeFlinkDeployment("app", "ns").Suspend(false).Obj(),
			newJob:                     testingflinkdeployment.MakeFlinkDeployment("app", "ns").SessionCluster().Obj(),
			manageJobsWithoutQueueName: true,
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"rejects Application -> Session transition while suspended keeping queue-name": {
			oldJob: baseSuspended,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").Queue("lq").UID("uid-app").SessionCluster().Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"rejects Application -> Session transition while suspended and simultaneously removing queue-name": {
			oldJob: baseSuspended,
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").UID("uid-app").SessionCluster().Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"rejects Session -> Application transition under manageJobsWithoutQueueName=true": {
			oldJob:                     testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
			newJob:                     testingflinkdeployment.MakeFlinkDeployment("session", "ns").Suspend(true).Obj(),
			manageJobsWithoutQueueName: true,
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"rejects Session -> Application transition when adding queue-name": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").SessionCluster().Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("session", "ns").Queue("lq").Suspend(true).Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job"), field.OmitValueType{}, "cannot switch between Session and Application mode"),
			},
		},
		"allows finalizer removal on terminating top-level FlinkDeployment with pre-existing invalid spec": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				cp.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(map[string]string{"job.autoscaler.enabled": "true"})
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(map[string]string{"job.autoscaler.enabled": "true"})
				return cp
			}(),
		},
		"allows finalizer removal on terminating pre-existing queue-labeled Session cluster": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := testingflinkdeployment.MakeFlinkDeployment("session-del", "ns").Queue("lq").SessionCluster().Obj()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := testingflinkdeployment.MakeFlinkDeployment("session-del", "ns").Queue("lq").SessionCluster().Obj()
				cp.DeletionTimestamp = &now
				return cp
			}(),
		},
		"allows finalizer removal on terminating child FlinkDeployment with missing owner when SkipAncestorCheckForDeletedWorkloads is enabled": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := makeAncestorOwnedChild(flinkv1beta1.JobStateRunning, 1)
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := makeAncestorOwnedChild(flinkv1beta1.JobStateRunning, 1)
				cp.DeletionTimestamp = &now
				return cp
			}(),
		},
		"returns ErrWorkloadOwnerNotFound on terminating child FlinkDeployment with missing owner when SkipAncestorCheckForDeletedWorkloads is disabled": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := makeAncestorOwnedChild(flinkv1beta1.JobStateRunning, 1)
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := makeAncestorOwnedChild(flinkv1beta1.JobStateRunning, 1)
				cp.DeletionTimestamp = &now
				return cp
			}(),
			disableSkipAncestorCheck: true,
			wantNonFieldErr:          jobframework.ErrWorkloadOwnerNotFound,
		},
		"rejects spec and label mutations on terminating FlinkDeployment": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Finalizers = []string{"flinkdeployments.flink.apache.org/finalizer"}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.DeletionTimestamp = &now
				cp.Labels[constants.QueueLabel] = "other-lq"
				cp.Spec.TaskManager.Replicas = new(int64(4))
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "other-lq", apivalidation.FieldImmutableErrorMsg),
				field.Invalid(field.NewPath("spec"), field.OmitValueType{}, immutableDeletingMsg),
			},
		},
		"allows standalone spec.job.state=suspended patch on running FlinkDeployment": {
			oldJob: baseRunning,
			newJob: baseSuspended,
		},
		"allows pure suspension of a pre-existing invalid running FlinkDeployment": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(map[string]string{"job.autoscaler.enabled": "true"})
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(map[string]string{"job.autoscaler.enabled": "true"})
				return cp
			}(),
		},
		"rejects dropping queue-name during suspension of a pre-existing invalid running FlinkDeployment": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(map[string]string{"job.autoscaler.enabled": "true"})
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				delete(cp.Labels, constants.QueueLabel)
				cp.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(map[string]string{"job.autoscaler.enabled": "true"})
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"allows Kueue stopJob transition running -> suspended with RestorePodSetsInfo removing injected flavor placement and implicit TAS": {
			oldJob:    admittedRunning,
			newJob:    stoppedFromAdmitted,
			enableTAS: true,
		},
		"allows stopJob suspension when running FlinkDeployment was scaled to 0 via /scale subresource": {
			oldJob:    scaledToZeroRunning,
			newJob:    scaledToZeroStopped,
			enableTAS: true,
		},
		"allows stopJob suspension when running TAS FlinkDeployment was scaled below podset-slice-size via /scale": {
			oldJob:    tasShrunkRunning,
			newJob:    tasShrunkStopped,
			enableTAS: true,
		},
		"allows stopJob suspension when running HA TAS-grouped FlinkDeployment was scaled to 0 via /scale": {
			oldJob:    tasGroupScaledZeroRunning,
			newJob:    tasGroupScaledZeroStopped,
			enableTAS: true,
		},
		"allows stopJob suspension when running HA TAS-grouped FlinkDeployment was scaled to 4 via /scale": {
			oldJob:    tasGroupScaledFourRunning,
			newJob:    tasGroupScaledFourStopped,
			enableTAS: true,
		},
		"allows repairing TaskManagerReplicas from 4 back to 1 on suspended HA TAS-grouped FlinkDeployment": {
			oldJob: tasGroupScaledFourStopped,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := tasGroupScaledFourStopped.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(1))
				return cp
			}(),
			enableTAS: true,
		},
		"rejects changing TaskManagerReplicas on suspended HA TAS-grouped FlinkDeployment to another invalid group count": {
			oldJob: tasGroupScaledFourStopped,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := tasGroupScaledFourStopped.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(3))
				return cp
			}(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetGroupName),
					"grp",
					"can only define groups where at least one pod set has only 1 replica, got: 2 replica(s) and 3 replica(s) in the group",
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetGroupName),
					"grp",
					"can only define groups where at least one pod set has only 1 replica, got: 2 replica(s) and 3 replica(s) in the group",
				),
			},
		},
		"rejects changing podset-group-name on suspended HA TAS-grouped FlinkDeployment while counts remain invalid": {
			oldJob: tasGroupScaledFourStopped,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := tasGroupScaledFourStopped.DeepCopy()
				cp.Spec.JobManager.PodTemplate.Annotations[kueue.PodSetGroupName] = "grp-2"
				cp.Spec.TaskManager.PodTemplate.Annotations[kueue.PodSetGroupName] = "grp-2"
				return cp
			}(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetGroupName),
					"grp-2",
					"can only define groups where at least one pod set has only 1 replica, got: 2 replica(s) and 4 replica(s) in the group",
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetGroupName),
					"grp-2",
					"can only define groups where at least one pod set has only 1 replica, got: 2 replica(s) and 4 replica(s) in the group",
				),
			},
		},
		"rejects mismatched podset-topology-spreading edit on suspended HA TAS-grouped FlinkDeployment while counts remain invalid": {
			oldJob: tasGroupScaledFourStopped,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := tasGroupScaledFourStopped.DeepCopy()
				cp.Spec.TaskManager.PodTemplate.Annotations[kueue.PodSetTopologySpreadingAnnotation] =
					`{"workloadLabelSelectors":[{"key":"app","operator":"In","values":["main"]}],"rules":[{"topologyKey":"cloud.provider.com/rack","maxShareAllowingPlacement":"0.5"}]}`
				return cp
			}(),
			enableTAS:                  true,
			enableTASTopologySpreading: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetGroupName),
					"grp",
					"can only define groups where at least one pod set has only 1 replica, got: 2 replica(s) and 4 replica(s) in the group",
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetGroupName),
					"grp",
					"can only define groups where at least one pod set has only 1 replica, got: 2 replica(s) and 4 replica(s) in the group",
				),
				field.Invalid(
					field.NewPath("spec", "jobManager", "podTemplate", "metadata", "annotations"),
					field.OmitValueType{},
					"must specify the same 'kueue.x-k8s.io/podset-topology-spreading' annotation as 'spec.taskManager.podTemplate.metadata.annotations' in group 'grp', or neither pod set may specify it",
				),
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations"),
					field.OmitValueType{},
					"must specify the same 'kueue.x-k8s.io/podset-topology-spreading' annotation as 'spec.jobManager.podTemplate.metadata.annotations' in group 'grp', or neither pod set may specify it",
				),
			},
		},
		"rejects adding out-of-bounds podset-slice-size while suspended when Replicas is unchanged": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.PodTemplate = testingflinkdeployment.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
						kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodSetSliceSizeAnnotation:             "4",
					},
				})
				return cp
			}(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceSizeAnnotation),
					"4",
					"must not be greater than pod set count 1",
				),
			},
		},
		"rejects increasing podset-slice-size above count while suspended when Replicas is unchanged": {
			oldJob: tasSliceSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := tasSliceSuspended.DeepCopy()
				cp.Spec.TaskManager.PodTemplate.Annotations[kueue.PodSetSliceSizeAnnotation] = "8"
				return cp
			}(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceSizeAnnotation),
					"8",
					"must not be greater than pod set count 4",
				),
			},
		},
		"rejects non-divisible multi-layer podset-slice-required-topology-constraints while suspended when Replicas is unchanged": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app-tas-ml", "ns").
				Queue("lq").
				Suspend(true).
				TaskManagerReplicas(6).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app-tas-ml", "ns").
				Queue("lq").
				Suspend(true).
				TaskManagerReplicas(6).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation:                 "cloud.provider.com/block",
						kueue.PodSetSliceRequiredTopologyConstraintsAnnotation: `[{"topology":"cloud.provider.com/rack","size":4},{"topology":"kubernetes.io/hostname","size":2}]`,
					},
				}).
				Obj(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceRequiredTopologyConstraintsAnnotation),
					int32(4),
					"must evenly divide pod set count 6 when more than one layer is specified",
				),
			},
		},
		"rejects shrinking job.parallelism below podset-slice-size while suspended when taskManager.replicas is nil": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app-tas-par", "ns").
				Queue("lq").
				Suspend(true).
				Parallelism(8).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
						kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodSetSliceSizeAnnotation:             "4",
					},
				}).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app-tas-par", "ns").
				Queue("lq").
				Suspend(true).
				Parallelism(2).
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
						kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodSetSliceSizeAnnotation:             "4",
					},
				}).
				Obj(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceSizeAnnotation),
					"4",
					"must not be greater than pod set count 2",
				),
			},
		},
		"rejects raising taskmanager.numberOfTaskSlots when derived TaskManager count shrinks below podset-slice-size": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app-tas-slots", "ns").
				Queue("lq").
				Suspend(true).
				Parallelism(8).
				FlinkConfig(flinkConfigNumberOfTaskSlots, "1").
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
						kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodSetSliceSizeAnnotation:             "4",
					},
				}).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app-tas-slots", "ns").
				Queue("lq").
				Suspend(true).
				Parallelism(8).
				FlinkConfig(flinkConfigNumberOfTaskSlots, "4").
				TaskManagerPodTemplate(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
						kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodSetSliceSizeAnnotation:             "4",
					},
				}).
				Obj(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceSizeAnnotation),
					"4",
					"must not be greater than pod set count 2",
				),
			},
		},
		"rejects non-pure running -> suspended update that injects out-of-bounds podset-slice-size": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.PodTemplate = testingflinkdeployment.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Annotations: map[string]string{
						kueue.PodSetRequiredTopologyAnnotation:      "cloud.provider.com/rack",
						kueue.PodSetSliceRequiredTopologyAnnotation: "cloud.provider.com/rack",
						kueue.PodSetSliceSizeAnnotation:             "4",
					},
				})
				return cp
			}(),
			enableTAS: true,
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "podTemplate", "metadata", "annotations").Key(kueue.PodSetSliceSizeAnnotation),
					"4",
					"must not be greater than pod set count 1",
				),
			},
		},
		"rejects onboarding an unmanaged suspended FlinkDeployment with taskManager.replicas=0": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-zero", "ns").
				Suspend(true).
				TaskManagerReplicas(0).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-zero", "ns").
				Queue("lq").
				Suspend(true).
				TaskManagerReplicas(0).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "taskManager", "replicas"), int64(0), "must be greater than or equal to 1"),
			},
		},
		"allows single-step onboarding of an unmanaged running FlinkDeployment by suspending and adding queue-name and priority-class": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-running", "ns").
				Suspend(false).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-running", "ns").
				Queue("lq").
				Label(constants.WorkloadPriorityClassLabel, "high-priority").
				Suspend(true).
				Obj(),
		},
		"allows Kueue eviction when root podTemplate has explicit empty pointer strings on gRPC probe, hostPath, ephemeral PVC, and dnsConfig": {
			oldJob: admittedEmptyPtrs,
			newJob: evictedEmptyPtrs,
		},
		"allows JobReconciler.stopJob running -> suspended restore when Workload overlay fields drifted while suspended": {
			oldJob: driftedRunning,
			newJob: driftedStopped,
		},
		"rejects simultaneous spec.job.state=suspended and stripping queue-name on running FlinkDeployment": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				delete(cp.Labels, constants.QueueLabel)
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects simultaneous spec.job.state=suspended and changing queue-name on running FlinkDeployment": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Labels[constants.QueueLabel] = "other-lq"
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "other-lq", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects simultaneous spec.job.state=suspended and adding WorkloadPriorityClass label on running FlinkDeployment": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Labels[constants.WorkloadPriorityClassLabel] = "high-priority"
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.WorkloadPriorityClassLabel), "high-priority", "WorkloadPriorityClass cannot be added to a non-suspended workload"),
			},
		},
		"rejects simultaneous spec.job.state=suspended and removing prebuilt-workload label on running FlinkDeployment": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Labels[constants.PrebuiltWorkloadLabel] = "prebuilt-wl"
				return cp
			}(),
			newJob: baseSuspended,
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.PrebuiltWorkloadLabel), "", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects running -> suspended transition that mutates component resources memory from 1536Mi to fractional 1.5Gi": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal(
					[]byte(
						`{"metadata":{"name":"app","namespace":"ns","labels":{"kueue.x-k8s.io/queue-name":"lq"}},"spec":{"image":"flink:1.20","job":{"jarURI":"local:///opt/app.jar","parallelism":1,"state":"running"},"jobManager":{"resources":{"requests":{"cpu":"1","memory":"1536Mi"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"1536Mi"}}}}}`,
					),
					&fd,
				); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				return &fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal(
					[]byte(
						`{"metadata":{"name":"app","namespace":"ns","labels":{"kueue.x-k8s.io/queue-name":"lq"}},"spec":{"image":"flink:1.20","job":{"jarURI":"local:///opt/app.jar","parallelism":1,"state":"suspended"},"jobManager":{"resources":{"requests":{"cpu":"1","memory":"1.5Gi"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"1536Mi"}}}}}`,
					),
					&fd,
				); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				return &fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resources"),
					nil,
					`resource request memory (1.5Gi): memory size "1.5Gi" must not contain a decimal point; must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi)`,
				),
			},
		},
		`rejects running -> suspended transition that mutates component resources cpu from 1 to whitespace-padded " 1 "`: {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal(
					[]byte(
						`{"metadata":{"name":"app","namespace":"ns","labels":{"kueue.x-k8s.io/queue-name":"lq"}},"spec":{"image":"flink:1.20","job":{"jarURI":"local:///opt/app.jar","parallelism":1,"state":"running"},"jobManager":{"resources":{"requests":{"cpu":"1","memory":"1Gi"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"1Gi"}}}}}`,
					),
					&fd,
				); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				return &fd
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal(
					[]byte(
						`{"metadata":{"name":"app","namespace":"ns","labels":{"kueue.x-k8s.io/queue-name":"lq"}},"spec":{"image":"flink:1.20","job":{"jarURI":"local:///opt/app.jar","parallelism":1,"state":"suspended"},"jobManager":{"resources":{"requests":{"cpu":" 1 ","memory":"1Gi"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"1Gi"}}}}}`,
					),
					&fd,
				); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				return &fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "jobManager", "resources"),
					nil,
					`resource request cpu (" 1 ") must not contain whitespace`,
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
		"allows scaling TaskManagerReplicas and updating podTemplate while suspended": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(4))
				cp.Spec.PodTemplate = testingflinkdeployment.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{NodeSelector: map[string]string{"tier": "batch"}},
				})
				return cp
			}(),
		},
		"allows fixing taskManager.replicas from 0 back to 2 while suspended": {
			oldJob: scaledToZeroStopped,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := scaledToZeroStopped.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(2))
				return cp
			}(),
		},
		"allows detaching a suspended Application job from Kueue with a standalone-only spec": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				delete(cp.Labels, constants.QueueLabel)
				cp.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(map[string]string{"job.autoscaler.enabled": "true"})
				return cp
			}(),
		},
		"rejects standalone-only spec changes while still managed and suspended": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(map[string]string{"job.autoscaler.enabled": "true"})
				return cp
			}(),
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
				var fd flinkv1beta1.FlinkDeployment
				if err := json.Unmarshal(
					[]byte(
						`{"metadata":{"name":"app","namespace":"ns","uid":"uid-app","labels":{"kueue.x-k8s.io/queue-name":"lq"}},"spec":{"image":"flink:1.20","job":{"jarURI":"local:///opt/app.jar","parallelism":1,"state":"suspended"},"jobManager":{"resources":{"requests":{"cpu":"1","memory":"1Gi"}}},"taskManager":{"resources":{"requests":{"cpu":"1","memory":"1.5Gi"}}}}}`,
					),
					&fd,
				); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				return &fd
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(
					field.NewPath("spec", "taskManager", "resources"),
					nil,
					`resource request memory (1.5Gi): memory size "1.5Gi" must not contain a decimal point; must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi)`,
				),
			},
		},
		"allows valid max-exec-time-seconds update while suspended": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "3600"
				return cp
			}(),
		},
		"rejects non-positive max-exec-time-seconds update while suspended": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Labels[constants.MaxExecTimeSecondsLabel] = "0"
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.MaxExecTimeSecondsLabel), 0, "should be greater than 0"),
			},
		},
		"allows Kueue admission transition suspended -> running with ResourceFlavor placement, toleration, and TAS gate": {
			oldJob:    baseSuspended,
			newJob:    admittedRunning,
			enableTAS: true,
		},
		"rejects suspended -> running transition when newJob spec is invalid": {
			oldJob: baseSuspended,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.Job.Parallelism = new(int64(0))
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job", "parallelism"), int64(0), "must be greater than or equal to 1"),
			},
		},
		"allows manual savepointTriggerNonce update while running": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.Job.SavepointTriggerNonce = new(int64(42))
				return cp
			}(),
		},
		"allows updating pipeline.jobvertex-parallelism-overrides.<vertexId> while running in standalone mode with explicit taskManager.replicas": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app-standalone", "ns").
				Queue("lq").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerReplicas(2).
				TaskManagerResource(new(1.0), "2048m", "").
				FlinkConfig("pipeline.jobvertex-parallelism-overrides.v1", "2").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app-standalone", "ns").
				Queue("lq").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerReplicas(2).
				TaskManagerResource(new(1.0), "2048m", "").
				FlinkConfig("pipeline.jobvertex-parallelism-overrides.v1", "4").
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
						Containers: []corev1.Container{{
							Name:            flinkMainContainerName,
							ImagePullPolicy: corev1.PullIfNotPresent,
						}},
					},
				}).
				Obj(),
		},
		"rejects scaling TaskManager replicas while running": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.TaskManager.Replicas = new(int64(4))
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec"), field.OmitValueType{}, immutableNotSuspendedMsg),
			},
		},
		"rejects changing or stripping queue-name while running": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				delete(cp.Labels, constants.QueueLabel)
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "labels").Key(constants.QueueLabel), "", apivalidation.FieldImmutableErrorMsg),
			},
		},
		"rejects spec.mode change while running even when derived PodSets are unchanged": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				ImagePullPolicy(corev1.PullIfNotPresent).
				ServiceAccount("default").
				Suspend(false).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				ImagePullPolicy(corev1.PullIfNotPresent).
				ServiceAccount("default").
				Suspend(false).
				Mode(flinkv1beta1.KubernetesDeploymentModeStandalone).
				TaskManagerResource(new(1.0), "2048m", "").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "mode"), flinkv1beta1.KubernetesDeploymentModeStandalone, immutableNotSuspendedMsg),
			},
		},
		"rejects clearing spec.serviceAccount, spec.image, or spec.imagePullPolicy while running even when podTemplate fallback keeps derived PodSets identical": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Image("flink:1.20").
				ImagePullPolicy(corev1.PullAlways).
				ServiceAccount("custom-sa").
				Suspend(false).
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ServiceAccountName: "custom-sa",
						Containers: []corev1.Container{{
							Name:            flinkMainContainerName,
							Image:           "flink:1.20",
							ImagePullPolicy: corev1.PullAlways,
						}},
					},
				}).
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Image("").
				Suspend(false).
				PodTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ServiceAccountName: "custom-sa",
						Containers: []corev1.Container{{
							Name:            flinkMainContainerName,
							Image:           "flink:1.20",
							ImagePullPolicy: corev1.PullAlways,
						}},
					},
				}).
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "image"), "", immutableNotSuspendedMsg),
				field.Invalid(field.NewPath("spec", "imagePullPolicy"), corev1.PullPolicy(""), immutableNotSuspendedMsg),
				field.Invalid(field.NewPath("spec", "serviceAccount"), "", immutableNotSuspendedMsg),
			},
		},
		"rejects mutating root spec.podTemplate while running": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.PodTemplate = testingflinkdeployment.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{NodeSelector: map[string]string{"zone": "us-east1-b"}},
				})
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "podTemplate"), field.OmitValueType{}, immutableNotSuspendedMsg),
				field.Invalid(field.NewPath("spec"), field.OmitValueType{}, immutableNotSuspendedMsg),
			},
		},
		"rejects mutating taskmanager.numberOfTaskSlots, merge-arrays-by-name, or spec.job.parallelism while running even when explicit replicas and CPU keep PodSets identical": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(false).
				Parallelism(2).
				TaskManagerReplicas(2).
				TaskManagerResource(new(2.0), "2048m", "").
				FlinkConfig("taskmanager.numberOfTaskSlots", "2").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "false").
				Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("app", "ns").
				Queue("lq").
				Suspend(false).
				Parallelism(4).
				TaskManagerReplicas(2).
				TaskManagerResource(new(2.0), "2048m", "").
				FlinkConfig("taskmanager.numberOfTaskSlots", "4").
				FlinkConfig("kubernetes.operator.pod-template.merge-arrays-by-name", "true").
				Obj(),
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec", "job", "parallelism"), int64(4), immutableNotSuspendedMsg),
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("taskmanager.numberOfTaskSlots"), "4", immutableNotSuspendedMsg),
				field.Invalid(field.NewPath("spec", "flinkConfiguration").Key("kubernetes.operator.pod-template.merge-arrays-by-name"), "true", immutableNotSuspendedMsg),
			},
		},
		"rejects live in-place scaling via pipeline.jobvertex-parallelism-overrides.<vertexId> while running in native mode": {
			oldJob: baseRunning,
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Spec.FlinkConfiguration = flinkv1beta1.NewFlinkConfiguration(map[string]string{"pipeline.jobvertex-parallelism-overrides.v1": "8"})
				return cp
			}(),
			wantErrs: field.ErrorList{
				field.Forbidden(
					field.NewPath("spec", "flinkConfiguration").Key("pipeline.jobvertex-parallelism-overrides.v1"),
					"spec.mode must be standalone and spec.taskManager.replicas must be explicitly set when pipeline.jobvertex-parallelism-overrides.v1 is configured",
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
				field.Invalid(field.NewPath("metadata", "annotations").Key(constants.WaitForPodsReadyAnnotation), "", "timeoutSeconds must be less than or equal to 300 seconds"),
				field.Invalid(field.NewPath("metadata", "annotations").Key(constants.WaitForPodsReadyAnnotation), "", "recoveryTimeoutSeconds must be less than or equal to 300 seconds"),
			},
		},
		"deduplicates wait-for-pods-ready error on single-step unmanaged-to-managed update exceeding maxTimeoutOnWorkload": {
			oldJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-app", "ns").Suspend(true).Obj(),
			newJob: testingflinkdeployment.MakeFlinkDeployment("unmanaged-app", "ns").
				Queue("lq").
				Suspend(true).
				Annotation(constants.WaitForPodsReadyAnnotation, `{"timeoutSeconds":86400}`).
				Obj(),
			enableWorkloadLevelWFPR: true,
			maxTimeoutOnWorkload:    &metav1.Duration{Duration: 5 * time.Minute},
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("metadata", "annotations").Key(constants.WaitForPodsReadyAnnotation), "", "timeoutSeconds must be less than or equal to 300 seconds"),
			},
		},
		"allows update on already-managed FlinkDeployment when unchanged wait-for-pods-ready exceeds lowered maxTimeoutOnWorkload": {
			oldJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseRunning.DeepCopy()
				cp.Annotations = map[string]string{constants.WaitForPodsReadyAnnotation: `{"timeoutSeconds":86400}`}
				return cp
			}(),
			newJob: func() *flinkv1beta1.FlinkDeployment {
				cp := baseSuspended.DeepCopy()
				cp.Annotations = map[string]string{constants.WaitForPodsReadyAnnotation: `{"timeoutSeconds":86400}`}
				return cp
			}(),
			enableWorkloadLevelWFPR: true,
			maxTimeoutOnWorkload:    &metav1.Duration{Duration: 5 * time.Minute},
		},
		"allows state transitions and replica scaling on child FlinkDeployment owned by Kueue-managed ancestor": {
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
				field.Invalid(field.NewPath("spec"), field.OmitValueType{}, immutableNotSuspendedMsg),
			},
		},
		"nil integrationManager with manageJobsWithoutQueueName=true enforces running-state immutability on unlabeled Application FlinkDeployment": {
			oldJob:                     testingflinkdeployment.MakeFlinkDeployment("unlabeled-nil-mgr", "ns").Suspend(false).Obj(),
			newJob:                     testingflinkdeployment.MakeFlinkDeployment("unlabeled-nil-mgr", "ns").Suspend(false).TaskManagerReplicas(4).Obj(),
			nilIntegrationManager:      true,
			manageJobsWithoutQueueName: true,
			wantErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec"), field.OmitValueType{}, immutableNotSuspendedMsg),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, tc.enableTAS)
			if tc.enableTASTopologySpreading {
				features.SetFeatureGateDuringTest(t, features.TASTopologySpreading, true)
			}
			features.SetFeatureGateDuringTest(t, features.SkipAncestorCheckForDeletedWorkloads, !tc.disableSkipAncestorCheck)
			features.SetFeatureGateDuringTest(t, features.WorkloadLevelWaitForPodsReady, tc.enableWorkloadLevelWFPR)
			ctx, _ := utiltesting.ContextWithLog(t)
			builder := utiltesting.NewClientBuilder(flinkv1beta1.AddToScheme).WithObjects(&corev1.Namespace{Name: "ns"})
			if len(tc.extraObjects) > 0 {
				builder = builder.WithObjects(tc.extraObjects...)
			}
			if tc.defaultQueue != nil {
				builder = builder.WithObjects(tc.defaultQueue)
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

func TestValidateDelete(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	wh := &flinkDeploymentWebhook{}
	obj := testingflinkdeployment.MakeFlinkDeployment("valid", "ns").Queue("lq").Obj()
	if _, err := wh.ValidateDelete(ctx, obj); err != nil {
		t.Errorf("ValidateDelete() unexpected error: %v", err)
	}
}
