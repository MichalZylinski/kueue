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

package v1beta1

import (
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

var (
	// SchemeGroupVersion is the group version used to register FlinkDeployment objects.
	SchemeGroupVersion = schema.GroupVersion{Group: "flink.apache.org", Version: "v1beta1"}
	// SchemeBuilder registers the FlinkDeployment types with a runtime.Scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme adds the FlinkDeployment types to a runtime.Scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	// FlinkConfiguration, PodTemplateSpec, and Quantity carry unexported raw JSON fields;
	// apimachinery's semantic deep-equality panics on unexported struct fields unless custom
	// equalities are registered.
	utilruntime.Must(apiequality.Semantic.AddFunc(func(a, b FlinkConfiguration) bool {
		return a.Equal(b)
	}))
	utilruntime.Must(apiequality.Semantic.AddFunc(func(a, b PodTemplateSpec) bool {
		return a.Equal(b)
	}))
	utilruntime.Must(apiequality.Semantic.AddFunc(func(a, b Quantity) bool {
		return a.Equal(b)
	}))
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&FlinkDeployment{},
		&FlinkDeploymentList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
