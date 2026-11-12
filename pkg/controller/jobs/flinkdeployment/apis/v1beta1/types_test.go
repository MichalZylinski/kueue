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
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestFlinkConfigurationJSONAndFlatMap(t *testing.T) {
	testCases := map[string]struct {
		raw               string
		wantFlat          map[string]string
		wantFlatErrSubstr string
	}{
		"null": {
			raw:      `null`,
			wantFlat: nil,
		},
		"empty object": {
			raw:      `{}`,
			wantFlat: nil,
		},
		"flat strings, numbers, booleans, nulls, empty arrays, scalar arrays, list-of-maps, nested arrays, and nested maps": {
			raw: `{
				"taskmanager.numberOfTaskSlots": 4,
				"execution.checkpointing.unaligned.enabled": true,
				"empty.option": null,
				"empty.array": [],
				"metrics.reporters": ["prom", "slf4j"],
				"array.with.null": ["x", null, "y"],
				"pipeline.serialization-config": [
					{"org.example.Event": {"type": "kryo", "kryo-type": "registered"}}
				],
				"nested.matrix": [[1, true], ["a", null]],
				"state": {
					"backend": {"type": "rocksdb"},
					"savepoints": {"dir": "s3://bucket/savepoints"}
				}
			}`,
			wantFlat: map[string]string{
				"array.with.null": "x;null;y",
				"empty.option":    "null",
				"execution.checkpointing.unaligned.enabled": "true",
				"metrics.reporters":                         "prom;slf4j",
				"nested.matrix":                             ";",
				"pipeline.serialization-config":             "",
				"state.backend.type":                        "rocksdb",
				"state.savepoints.dir":                      "s3://bucket/savepoints",
				"taskmanager.numberOfTaskSlots":             "4",
			},
		},
		"rejects flat and nested key collision (nested first)": {
			raw:               `{"parallelism": {"default": 4}, "parallelism.default": "1"}`,
			wantFlatErrSubstr: `duplicate flattened flinkConfiguration key "parallelism.default"`,
		},
		"rejects flat and nested key collision (flat first)": {
			raw:               `{"parallelism.default": "1", "parallelism": {"default": 4}}`,
			wantFlatErrSubstr: `duplicate flattened flinkConfiguration key "parallelism.default"`,
		},
		"rejects flat and nested key collision with empty slice (nested scalar first)": {
			raw:               `{"parallelism": {"default": 4}, "parallelism.default": []}`,
			wantFlatErrSubstr: `duplicate flattened flinkConfiguration key "parallelism.default"`,
		},
		"rejects flat and nested key collision with empty slice (flat empty slice first in JSON)": {
			raw:               `{"parallelism.default": [], "parallelism": {"default": 4}}`,
			wantFlatErrSubstr: `duplicate flattened flinkConfiguration key "parallelism.default"`,
		},
		"rejects flat and nested key collision with empty slice (nested empty slice first)": {
			raw:               `{"parallelism": {"default": []}, "parallelism.default": 4}`,
			wantFlatErrSubstr: `duplicate flattened flinkConfiguration key "parallelism.default"`,
		},
		"rejects flat and nested key collision with empty map (nested scalar first)": {
			raw:               `{"parallelism": {"default": 4}, "parallelism.default": {}}`,
			wantFlatErrSubstr: `duplicate flattened flinkConfiguration key "parallelism.default"`,
		},
		"rejects flat and nested key collision with empty map (flat empty map first in JSON)": {
			raw:               `{"parallelism.default": {}, "parallelism": {"default": 4}}`,
			wantFlatErrSubstr: `duplicate flattened flinkConfiguration key "parallelism.default"`,
		},
		"rejects flat and nested key collision with empty map (nested empty map first)": {
			raw:               `{"parallelism": {"default": {}}, "parallelism.default": 4}`,
			wantFlatErrSubstr: `duplicate flattened flinkConfiguration key "parallelism.default"`,
		},
		"rejects flat and nested key collision between empty map and empty slice": {
			raw:               `{"parallelism": {"default": {}}, "parallelism.default": []}`,
			wantFlatErrSubstr: `duplicate flattened flinkConfiguration key "parallelism.default"`,
		},
		"standalone empty nested map is omitted from FlatMap without error": {
			raw:      `{"empty.map": {}, "parallelism.default": 4}`,
			wantFlat: map[string]string{"parallelism.default": "4"},
		},
		"rejects leading whitespace in flat key": {
			raw:               `{" taskmanager.numberOfTaskSlots": "8"}`,
			wantFlatErrSubstr: "must not contain leading or trailing whitespace",
		},
		"rejects whitespace-padded nested key segment": {
			raw:               `{"jobmanager": {" memory": {"process": {"size": "64g"}}}}`,
			wantFlatErrSubstr: `flinkConfiguration key " memory" must not contain leading or trailing whitespace`,
		},
		"rejects whitespace-padded segment inside flat dotted key": {
			raw:               `{"jobmanager. memory.process.size": "64g"}`,
			wantFlatErrSubstr: `must not contain leading or trailing whitespace in segment " memory"`,
		},
		"rejects empty key": {
			raw:               `{"": "value"}`,
			wantFlatErrSubstr: `flinkConfiguration key "" must not be empty`,
		},
		"defers invalid non-object JSON error to FlatMap while preserving raw bytes": {
			raw:               `["not", "a", "map"]`,
			wantFlatErrSubstr: "cannot unmarshal array into Go value of type map[string]",
		},
		"defers scalar string JSON error to FlatMap while preserving raw bytes": {
			raw:               `"not-an-object"`,
			wantFlatErrSubstr: "cannot unmarshal string into Go value of type map[string]",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var got FlinkConfiguration
			if err := json.Unmarshal([]byte(tc.raw), &got); err != nil {
				t.Fatalf("json.Unmarshal() unexpected error: %v", err)
			}

			// Verify raw JSON structure and key ordering round-trip verbatim (json.Marshal compacts whitespace).
			marshaled, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("json.Marshal() unexpected error: %v", err)
			}
			var compactWant bytes.Buffer
			if err := json.Compact(&compactWant, []byte(tc.raw)); err != nil {
				t.Fatalf("json.Compact() error: %v", err)
			}
			if string(marshaled) != compactWant.String() {
				t.Errorf("json.Marshal() did not preserve raw JSON structure verbatim:\ngot:  %s\nwant: %s", string(marshaled), compactWant.String())
			}

			flat, flatErr := got.FlatMap()
			if tc.wantFlatErrSubstr != "" {
				if flatErr == nil || !strings.Contains(flatErr.Error(), tc.wantFlatErrSubstr) {
					t.Fatalf("FlatMap() error = %v, want substring %q", flatErr, tc.wantFlatErrSubstr)
				}
				return
			}
			if flatErr != nil {
				t.Fatalf("FlatMap() unexpected error: %v", flatErr)
			}
			if diff := cmp.Diff(tc.wantFlat, flat); diff != "" {
				t.Errorf("FlatMap() mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestNewFlinkConfigurationFlatMap(t *testing.T) {
	testCases := map[string]struct {
		entries       map[string]string
		wantZero      bool
		wantFlat      map[string]string
		wantErrSubstr string
	}{
		"nil entries are zero": {
			entries:  nil,
			wantZero: true,
		},
		"empty entries are zero": {
			entries:  map[string]string{},
			wantZero: true,
		},
		"flat entries round-trip": {
			entries:  map[string]string{"parallelism.default": "2"},
			wantFlat: map[string]string{"parallelism.default": "2"},
		},
		"rejects whitespace-padded key": {
			entries:       map[string]string{" taskmanager.numberOfTaskSlots": "8"},
			wantErrSubstr: "must not contain leading or trailing whitespace",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cfg := NewFlinkConfiguration(tc.entries)
			if cfg.IsZero() != tc.wantZero {
				t.Errorf("IsZero() = %v, want %v", cfg.IsZero(), tc.wantZero)
			}
			flat, err := cfg.FlatMap()
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("FlatMap() error = %v, want substring %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FlatMap() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantFlat, flat); diff != "" {
				t.Errorf("FlatMap() mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func mustUnmarshalFlinkConfiguration(t *testing.T, raw string) FlinkConfiguration {
	t.Helper()
	var cfg FlinkConfiguration
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("json.Unmarshal(%s) error: %v", raw, err)
	}
	return cfg
}

func TestFlinkConfigurationEqualAndSemanticDeepEqual(t *testing.T) {
	testCases := map[string]struct {
		a         func(t *testing.T) FlinkConfiguration
		b         func(t *testing.T) FlinkConfiguration
		wantEqual bool
	}{
		"two zero values": {
			a:         func(*testing.T) FlinkConfiguration { return FlinkConfiguration{} },
			b:         func(*testing.T) FlinkConfiguration { return NewFlinkConfiguration(nil) },
			wantEqual: true,
		},
		"nested raw JSON equals equivalent flat Go map": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"parallelism": {"default": 2}}`)
			},
			b: func(*testing.T) FlinkConfiguration {
				return NewFlinkConfiguration(map[string]string{"parallelism.default": "2"})
			},
			wantEqual: true,
		},
		"same raw JSON with different key order": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"a": "1", "b": "2"}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"b": "2", "a": "1"}`)
			},
			wantEqual: true,
		},
		"different values": {
			a:         func(t *testing.T) FlinkConfiguration { return mustUnmarshalFlinkConfiguration(t, `{"a": "1"}`) },
			b:         func(*testing.T) FlinkConfiguration { return NewFlinkConfiguration(map[string]string{"a": "2"}) },
			wantEqual: false,
		},
		"zero versus populated": {
			a:         func(*testing.T) FlinkConfiguration { return FlinkConfiguration{} },
			b:         func(*testing.T) FlinkConfiguration { return NewFlinkConfiguration(map[string]string{"a": "1"}) },
			wantEqual: false,
		},
		"distinct colliding raw payloads that cannot flatten": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"a":{"b":1},"a.b":"2"}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"a":{"b":1},"a.b":"3"}`)
			},
			wantEqual: false,
		},
		"identical colliding raw payloads that cannot flatten": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"a":{"b":1},"a.b":"2"}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"a":{"b":1},"a.b":"2"}`)
			},
			wantEqual: true,
		},
		"distinct pipeline.serialization-config list-of-maps do not collide": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"pipeline.serialization-config":[{"org.example.EventA":{"type":"kryo"}}]}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"pipeline.serialization-config":[{"org.example.EventB":{"type":"pojo"}}]}`)
			},
			wantEqual: false,
		},
		"pipeline.serialization-config list-of-maps does not equal empty string": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"pipeline.serialization-config":[{"org.example.EventA":{"type":"kryo"}}]}`)
			},
			b: func(*testing.T) FlinkConfiguration {
				return NewFlinkConfiguration(map[string]string{"pipeline.serialization-config": ""})
			},
			wantEqual: false,
		},
		"identical pipeline.serialization-config with flat vs nested sibling scalar map": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"parallelism":{"default":2},"pipeline.serialization-config":[{"org.example.EventA":{"kryo-type":"registered","type":"kryo"}}]}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"parallelism.default":"2","pipeline.serialization-config":[{"org.example.EventA":{"type":"kryo","kryo-type":"registered"}}]}`)
			},
			wantEqual: true,
		},
		"distinct nested arrays do not collide": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"nested.matrix":[[1,2]]}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"nested.matrix":[[3,4]]}`)
			},
			wantEqual: false,
		},
		"semicolon-containing slice elements do not collide across shifted boundaries": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"list":["a;b","c"]}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"list":["a","b;c"]}`)
			},
			wantEqual: false,
		},
		"empty slice does not equal empty object": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"k":[]}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{}`)
			},
			wantEqual: false,
		},
		"empty nested map does not equal empty object or empty slice": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"k":{}}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"k":[]}`)
			},
			wantEqual: false,
		},
		"JSON null value does not equal string \"null\"": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"k":null}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"k":"null"}`)
			},
			wantEqual: false,
		},
		"JSON [null] slice does not equal [\"null\"] slice": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"k":[null]}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"k":["null"]}`)
			},
			wantEqual: false,
		},
		"JSON [null] slice does not equal scalar null or string \"null\"": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"k":[null]}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"k":null}`)
			},
			wantEqual: false,
		},
		"JSON null value equals identical JSON null across flat and nested maps": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"a":{"b":null}}`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"a.b":null}`)
			},
			wantEqual: true,
		},
		"non-object deferred decode error equals identical raw non-object": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `["x","y"]`)
			},
			b: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `["x","y"]`)
			},
			wantEqual: true,
		},
		"non-object deferred decode error does not equal distinct raw non-object or zero": {
			a: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `["x"]`)
			},
			b: func(*testing.T) FlinkConfiguration {
				return FlinkConfiguration{}
			},
			wantEqual: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			a, b := tc.a(t), tc.b(t)
			if got := a.Equal(b); got != tc.wantEqual {
				t.Errorf("Equal() = %v, want %v", got, tc.wantEqual)
			}
			if got := b.Equal(a); got != tc.wantEqual {
				t.Errorf("reverse Equal() = %v, want %v", got, tc.wantEqual)
			}

			// apiequality.Semantic.DeepEqual panics on unexported struct fields unless
			// FlinkConfiguration.Equal is registered in register.go.
			fdA := &FlinkDeployment{Spec: FlinkDeploymentSpec{Image: "flink:1.20", FlinkConfiguration: a}}
			fdB := &FlinkDeployment{Spec: FlinkDeploymentSpec{Image: "flink:1.20", FlinkConfiguration: b}}
			if got := apiequality.Semantic.DeepEqual(fdA, fdB); got != tc.wantEqual {
				t.Errorf("apiequality.Semantic.DeepEqual(*FlinkDeployment) = %v, want %v", got, tc.wantEqual)
			}
			if got := apiequality.Semantic.DeepEqual(fdA.Spec, fdB.Spec); got != tc.wantEqual {
				t.Errorf("apiequality.Semantic.DeepEqual(FlinkDeploymentSpec) = %v, want %v", got, tc.wantEqual)
			}
		})
	}
}

func TestFlinkConfigurationSetAndDeepCopy(t *testing.T) {
	testCases := map[string]struct {
		initial           func(t *testing.T) FlinkConfiguration
		setKey, setValue  string
		wantFlat          map[string]string
		wantFlatErrSubstr string
		wantMarshaled     string
		wantEqual         func(t *testing.T) FlinkConfiguration
		wantNotEqual      func(t *testing.T) FlinkConfiguration
	}{
		"Set on raw nested JSON materializes a flat map": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"parallelism": {"default": 2}}`)
			},
			setKey:        "taskmanager.numberOfTaskSlots",
			setValue:      "8",
			wantFlat:      map[string]string{"parallelism.default": "2", "taskmanager.numberOfTaskSlots": "8"},
			wantMarshaled: `{"parallelism.default":"2","taskmanager.numberOfTaskSlots":"8"}`,
		},
		"Set on zero value creates the map": {
			initial:       func(*testing.T) FlinkConfiguration { return FlinkConfiguration{} },
			setKey:        "a",
			setValue:      "1",
			wantFlat:      map[string]string{"a": "1"},
			wantMarshaled: `{"a":"1"}`,
		},
		"Set overrides an existing Go map entry": {
			initial:       func(*testing.T) FlinkConfiguration { return NewFlinkConfiguration(map[string]string{"a": "1"}) },
			setKey:        "a",
			setValue:      "2",
			wantFlat:      map[string]string{"a": "2"},
			wantMarshaled: `{"a":"2"}`,
		},
		"Set on structured YAML preserves list-of-maps and removes conflicting nested scalar path": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"parallelism":{"default":2,"max":8},"pipeline.serialization-config":[{"org.example.Event":{"type":"kryo"}}]}`)
			},
			setKey:   "parallelism.default",
			setValue: "4",
			wantFlat: map[string]string{
				"parallelism.default":           "4",
				"parallelism.max":               "8",
				"pipeline.serialization-config": "",
			},
			wantMarshaled: `{"parallelism":{"max":8},"parallelism.default":"4","pipeline.serialization-config":[{"org.example.Event":{"type":"kryo"}}]}`,
		},
		"Set preserves top-level sibling JSON null value and Equal semantics without rewriting to string \"null\"": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"nullable.opt":null,"taskmanager.numberOfTaskSlots":"2"}`)
			},
			setKey:   "taskmanager.numberOfTaskSlots",
			setValue: "4",
			wantFlat: map[string]string{
				"nullable.opt":                  "null",
				"taskmanager.numberOfTaskSlots": "4",
			},
			wantMarshaled: `{"nullable.opt":null,"taskmanager.numberOfTaskSlots":"4"}`,
			wantEqual: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"nullable.opt":null,"taskmanager.numberOfTaskSlots":"4"}`)
			},
			wantNotEqual: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"nullable.opt":"null","taskmanager.numberOfTaskSlots":"4"}`)
			},
		},
		"Set preserves nested sibling JSON null value without rewriting to string \"null\"": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"nested":{"nullable":null},"parallelism":{"default":2}}`)
			},
			setKey:   "parallelism.default",
			setValue: "4",
			wantFlat: map[string]string{
				"nested.nullable":     "null",
				"parallelism.default": "4",
			},
			wantMarshaled: `{"nested":{"nullable":null},"parallelism.default":"4"}`,
		},
		"Set on duplicate flat and nested key collision recovers by removing duplicate leaf paths": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"parallelism":{"default":2},"parallelism.default":"1","keep":"yes"}`)
			},
			setKey:        "parallelism.default",
			setValue:      "8",
			wantFlat:      map[string]string{"keep": "yes", "parallelism.default": "8"},
			wantMarshaled: `{"keep":"yes","parallelism.default":"8"}`,
		},
		"Set alongside whitespace-padded key preserves sibling entries in raw JSON": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{" bad ":"1","keep":"yes"}`)
			},
			setKey:            "added",
			setValue:          "2",
			wantFlatErrSubstr: `flinkConfiguration key " bad " must not contain leading or trailing whitespace`,
			wantMarshaled:     `{" bad ":"1","added":"2","keep":"yes"}`,
		},
		"Set suffix key X.Y alongside structured entry preserves scalar prefix key X": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"high-availability":"kubernetes","metrics.reporters":["prom"]}`)
			},
			setKey:   "high-availability.storageDir",
			setValue: "s3://bucket/ha",
			wantFlat: map[string]string{
				"high-availability":            "kubernetes",
				"high-availability.storageDir": "s3://bucket/ha",
				"metrics.reporters":            "prom",
			},
			wantMarshaled: `{"high-availability":"kubernetes","high-availability.storageDir":"s3://bucket/ha","metrics.reporters":["prom"]}`,
		},
		"Set suffix key X.Y alongside structured entry preserves empty-map prefix key X": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"empty.prefix":{},"metrics.reporters":["prom"]}`)
			},
			setKey:   "empty.prefix.child",
			setValue: "1",
			wantFlat: map[string]string{
				"empty.prefix.child": "1",
				"metrics.reporters":  "prom",
			},
			wantMarshaled: `{"empty.prefix":{},"empty.prefix.child":"1","metrics.reporters":["prom"]}`,
			wantNotEqual: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"empty.prefix.child":"1","metrics.reporters":["prom"]}`)
			},
		},
		"Set prefix key X alongside structured entry preserves flat suffix key X.Y": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"high-availability.storageDir":"s3://bucket/ha","metrics.reporters":["prom"]}`)
			},
			setKey:   "high-availability",
			setValue: "kubernetes",
			wantFlat: map[string]string{
				"high-availability":            "kubernetes",
				"high-availability.storageDir": "s3://bucket/ha",
				"metrics.reporters":            "prom",
			},
			wantMarshaled: `{"high-availability":"kubernetes","high-availability.storageDir":"s3://bucket/ha","metrics.reporters":["prom"]}`,
		},
		"Set prefix key X alongside structured entry lifts nested map at X so X.Y survives": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"high-availability":{"storageDir":"s3://bucket/ha"},"metrics.reporters":["prom"]}`)
			},
			setKey:   "high-availability",
			setValue: "kubernetes",
			wantFlat: map[string]string{
				"high-availability":            "kubernetes",
				"high-availability.storageDir": "s3://bucket/ha",
				"metrics.reporters":            "prom",
			},
			wantMarshaled: `{"high-availability":"kubernetes","high-availability.storageDir":"s3://bucket/ha","metrics.reporters":["prom"]}`,
		},
		"Set prefix key X lifts nested sub-maps without overwriting existing flat or nested siblings": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"a":{"b":"nested-val"},"a.b":{"c":"1"},"metrics.reporters":["prom"]}`)
			},
			setKey:   "a",
			setValue: "root-val",
			wantFlat: map[string]string{
				"a":                 "root-val",
				"a.b":               "nested-val",
				"a.b.c":             "1",
				"metrics.reporters": "prom",
			},
			wantMarshaled: `{"a":"root-val","a.b":"nested-val","a.b.c":"1","metrics.reporters":["prom"]}`,
		},
		"Set key X.Y replaces nested empty-map leaf at X.Y and prunes emptied parent map": {
			initial: func(t *testing.T) FlinkConfiguration {
				return mustUnmarshalFlinkConfiguration(t, `{"a":{"b":{}},"metrics.reporters":["prom"]}`)
			},
			setKey:   "a.b",
			setValue: "2",
			wantFlat: map[string]string{
				"a.b":               "2",
				"metrics.reporters": "prom",
			},
			wantMarshaled: `{"a.b":"2","metrics.reporters":["prom"]}`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			orig := &FlinkDeployment{Spec: FlinkDeploymentSpec{FlinkConfiguration: tc.initial(t)}}
			origSnapshot := orig.DeepCopy()
			origBytesBefore, err := json.Marshal(orig.Spec.FlinkConfiguration)
			if err != nil {
				t.Fatalf("json.Marshal(orig) before Set error: %v", err)
			}

			copied := orig.DeepCopy()
			copied.Spec.FlinkConfiguration.Set(tc.setKey, tc.setValue)

			origBytesAfter, err := json.Marshal(orig.Spec.FlinkConfiguration)
			if err != nil {
				t.Fatalf("json.Marshal(orig) after Set error: %v", err)
			}
			if string(origBytesBefore) != string(origBytesAfter) {
				t.Errorf("mutating DeepCopy() FlinkConfiguration leaked into original JSON: before=%s after=%s", origBytesBefore, origBytesAfter)
			}
			if !apiequality.Semantic.DeepEqual(orig, origSnapshot) {
				t.Error("mutating DeepCopy() FlinkConfiguration altered original semantic equality")
			}

			marshaled, err := json.Marshal(copied.Spec.FlinkConfiguration)
			if err != nil {
				t.Fatalf("json.Marshal(copied) error: %v", err)
			}
			if string(marshaled) != tc.wantMarshaled {
				t.Errorf("json.Marshal(copied) = %s, want %s", string(marshaled), tc.wantMarshaled)
			}
			if tc.wantEqual != nil && !copied.Spec.FlinkConfiguration.Equal(tc.wantEqual(t)) {
				t.Errorf("copied.Equal(wantEqual) = false, want true")
			}
			if tc.wantNotEqual != nil && copied.Spec.FlinkConfiguration.Equal(tc.wantNotEqual(t)) {
				t.Errorf("copied.Equal(wantNotEqual) = true, want false")
			}

			copiedFlat, err := copied.Spec.FlinkConfiguration.FlatMap()
			if tc.wantFlatErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantFlatErrSubstr) {
					t.Fatalf("copied FlatMap() error = %v, want substring %q", err, tc.wantFlatErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("copied FlatMap() error: %v", err)
			}
			if diff := cmp.Diff(tc.wantFlat, copiedFlat); diff != "" {
				t.Errorf("copied FlatMap() mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestFlinkDeploymentDeepCopyObject(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error: %v", err)
	}
	orig := &FlinkDeployment{
		Spec: FlinkDeploymentSpec{
			Image:              "flink:1.20",
			FlinkConfiguration: mustUnmarshalFlinkConfiguration(t, `{"parallelism": {"default": 2}}`),
			JobManager:         &JobManagerSpec{Replicas: new(int64(1))},
			TaskManager:        &TaskManagerSpec{Replicas: new(int64(2))},
			Job: &JobSpec{
				JarURI:      "local:///opt/flink/examples/streaming/StateMachineExample.jar",
				Parallelism: new(int64(4)),
				State:       JobStateSuspended,
				UpgradeMode: UpgradeModeSavepoint,
			},
		},
		Status: FlinkDeploymentStatus{
			TaskManager: &TaskManagerStatusInfo{Replicas: 2},
		},
	}
	objCopy, ok := orig.DeepCopyObject().(*FlinkDeployment)
	if !ok || objCopy == nil {
		t.Fatalf("FlinkDeployment.DeepCopyObject() = %T, want *FlinkDeployment", orig.DeepCopyObject())
	}
	if !apiequality.Semantic.DeepEqual(orig, objCopy) {
		t.Errorf("FlinkDeployment.DeepCopyObject() is not semantically equal to the original")
	}
	list := &FlinkDeploymentList{Items: []FlinkDeployment{*orig}}
	listCopy, ok := list.DeepCopyObject().(*FlinkDeploymentList)
	if !ok || listCopy == nil {
		t.Fatalf("FlinkDeploymentList.DeepCopyObject() = %T, want *FlinkDeploymentList", list.DeepCopyObject())
	}
	if !apiequality.Semantic.DeepEqual(list, listCopy) {
		t.Errorf("FlinkDeploymentList.DeepCopyObject() is not semantically equal to the original")
	}
}

func TestFlinkDeploymentList64BitCRDIntegersJSONRoundTrip(t *testing.T) {
	const aboveInt32 = int64(math.MaxInt32) + 1
	rawList := []byte(`{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind": "FlinkDeploymentList",
		"items": [{
			"apiVersion": "flink.apache.org/v1beta1",
			"kind": "FlinkDeployment",
			"metadata": {"name": "unmanaged-64bit", "namespace": "unmanaged-ns"},
			"spec": {
				"jobManager": {"replicas": 2147483648},
				"taskManager": {"replicas": 2147483648},
				"job": {"parallelism": 2147483648}
			},
			"status": {
				"taskManager": {"replicas": 2147483648}
			}
		}]
	}`)
	var list FlinkDeploymentList
	if err := json.Unmarshal(rawList, &list); err != nil {
		t.Fatalf("json.Unmarshal(FlinkDeploymentList) error: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("len(list.Items) = %d, want 1", len(list.Items))
	}
	item := list.Items[0]
	if item.Spec.JobManager == nil || item.Spec.JobManager.Replicas == nil || *item.Spec.JobManager.Replicas != aboveInt32 {
		t.Errorf("spec.jobManager.replicas = %v, want %d", item.Spec.JobManager, aboveInt32)
	}
	if item.Spec.TaskManager == nil || item.Spec.TaskManager.Replicas == nil || *item.Spec.TaskManager.Replicas != aboveInt32 {
		t.Errorf("spec.taskManager.replicas = %v, want %d", item.Spec.TaskManager, aboveInt32)
	}
	if item.Spec.Job == nil || item.Spec.Job.Parallelism == nil || *item.Spec.Job.Parallelism != aboveInt32 {
		t.Errorf("spec.job.parallelism = %v, want %d", item.Spec.Job, aboveInt32)
	}
	if item.Status.TaskManager == nil || item.Status.TaskManager.Replicas != aboveInt32 {
		t.Errorf("status.taskManager.replicas = %v, want %d", item.Status.TaskManager, aboveInt32)
	}
	cloned := list.DeepCopy()
	if !apiequality.Semantic.DeepEqual(&list, cloned) {
		t.Errorf("FlinkDeploymentList.DeepCopy() lost 64-bit integer equality")
	}
}

func TestPodTemplateSpecMarshalJSONStripsNullEntries(t *testing.T) {
	testCases := map[string]struct {
		template       PodTemplateSpec
		wantSubstrings []string
		wantAbsent     []string
	}{
		"fast path with no null entries keeps explicit containers": {
			template: PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "flink-main-container"}}},
			},
			wantSubstrings: []string{`"containers":[{"name":"flink-main-container"`},
			wantAbsent:     []string{`null`},
		},
		"nil containers with only nodeSelector": {
			template: PodTemplateSpec{
				Spec: corev1.PodSpec{NodeSelector: map[string]string{"pool": "shared"}},
			},
			wantSubstrings: []string{`"nodeSelector":{"pool":"shared"}`},
			wantAbsent:     []string{`"containers":null`},
		},
		"empty spec is dropped entirely while metadata is preserved": {
			template: PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"example.com/tas": "rack"}},
			},
			wantSubstrings: []string{`"annotations":{"example.com/tas":"rack"}`},
			wantAbsent:     []string{`"spec"`},
		},
		"podTemplate apiVersion and kind envelope is preserved": {
			template: PodTemplateSpec{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
				Spec:     corev1.PodSpec{NodeSelector: map[string]string{"pool": "shared"}},
			},
			wantSubstrings: []string{`"apiVersion":"v1"`, `"kind":"Pod"`},
			wantAbsent:     []string{`null`},
		},
		"volume projected sources, rbd monitors, cephfs monitors, and ephemeral dataSource apiGroup": {
			template: PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "sidecar"}},
					Volumes: []corev1.Volume{
						{Name: "projected-vol", Projected: &corev1.ProjectedVolumeSource{DefaultMode: new(int32(420))}},
						{Name: "rbd-vol", RBD: &corev1.RBDVolumeSource{RBDImage: "img"}},
						{Name: "cephfs-vol", CephFS: &corev1.CephFSVolumeSource{Path: "/mnt"}},
						{Name: "scaleio-vol", ScaleIO: &corev1.ScaleIOVolumeSource{Gateway: "https://gw", System: "sys"}},
						{
							Name: "ephemeral-vol",
							Ephemeral: &corev1.EphemeralVolumeSource{
								VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
									Spec: corev1.PersistentVolumeClaimSpec{
										DataSource:    &corev1.TypedLocalObjectReference{Kind: "PersistentVolumeClaim", Name: "src"},
										DataSourceRef: &corev1.TypedObjectReference{Kind: "PersistentVolumeClaim", Name: "src"},
									},
								},
							},
						},
					},
				},
			},
			wantSubstrings: []string{`"sidecar"`, `"defaultMode":420`, `"image":"img"`, `"path":"/mnt"`, `"gateway":"https://gw"`, `"kind":"PersistentVolumeClaim"`},
			wantAbsent:     []string{`"sources":null`, `"monitors":null`, `"secretRef":null`, `"apiGroup":null`, `null`},
		},
		"required nodeAffinity nodeSelectorTerms": {
			template: PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "sidecar"}},
					Affinity: &corev1.Affinity{
						NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{}},
					},
				},
			},
			wantSubstrings: []string{`"requiredDuringSchedulingIgnoredDuringExecution":{}`},
			wantAbsent:     []string{`"nodeSelectorTerms":null`},
		},
		"unnamed gRPC probes on containers, initContainers, and ephemeralContainers": {
			template: PodTemplateSpec{
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{{
						Name:         "init",
						StartupProbe: &corev1.Probe{GRPC: &corev1.GRPCAction{Port: 9090}},
					}},
					Containers: []corev1.Container{{
						Name:           "flink-main-container",
						LivenessProbe:  &corev1.Probe{GRPC: &corev1.GRPCAction{Port: 9090}},
						ReadinessProbe: &corev1.Probe{GRPC: &corev1.GRPCAction{Port: 9091}},
					}},
					EphemeralContainers: []corev1.EphemeralContainer{{
						Name:          "debug",
						LivenessProbe: &corev1.Probe{GRPC: &corev1.GRPCAction{Port: 9092}},
					}},
				},
			},
			wantSubstrings: []string{`"grpc":{"port":9090}`, `"grpc":{"port":9091}`, `"grpc":{"port":9092}`},
			wantAbsent:     []string{`"service":null`},
		},
		"named gRPC probe keeps its service": {
			template: PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:          "flink-main-container",
						LivenessProbe: &corev1.Probe{GRPC: &corev1.GRPCAction{Port: 9090, Service: new("health")}},
					}},
				},
			},
			wantSubstrings: []string{`"grpc":{"port":9090,"service":"health"}`},
			wantAbsent:     []string{`null`},
		},
		"zero PodStatus timestamps and extendedResourceClaimStatus requestMappings": {
			template: PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "flink-main-container"}}},
				Status: &corev1.PodStatus{
					Phase:      corev1.PodPending,
					Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}},
					ContainerStatuses: []corev1.ContainerStatus{{
						Name:                 "flink-main-container",
						State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}},
					}},
					InitContainerStatuses: []corev1.ContainerStatus{{
						Name:  "init",
						State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
					}},
					EphemeralContainerStatuses: []corev1.ContainerStatus{{
						Name:  "debug",
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
					}},
					ExtendedResourceClaimStatus: &corev1.PodExtendedResourceClaimStatus{ResourceClaimName: "claim-a"},
				},
			},
			wantSubstrings: []string{`"phase":"Pending"`, `"type":"Ready"`, `"exitCode":1`, `"resourceClaimName":"claim-a"`},
			wantAbsent:     []string{`"lastProbeTime":null`, `"lastTransitionTime":null`, `"startedAt":null`, `"finishedAt":null`, `"requestMappings":null`, `null`},
		},
		"non-zero PodStatus timestamps are preserved": {
			template: PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "flink-main-container"}}},
				Status: &corev1.PodStatus{
					Conditions: []corev1.PodCondition{{
						Type:               corev1.PodReady,
						Status:             corev1.ConditionTrue,
						LastTransitionTime: metav1.NewTime(time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)),
					}},
				},
			},
			wantSubstrings: []string{`"lastTransitionTime":"2026-01-02T03:04:05Z"`},
			wantAbsent:     []string{`"lastTransitionTime":null`},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(tc.template)
			if err != nil {
				t.Fatalf("json.Marshal() unexpected error: %v", err)
			}
			encoded := string(raw)
			for _, want := range tc.wantSubstrings {
				if !strings.Contains(encoded, want) {
					t.Errorf("expected marshaled JSON to contain %s, got: %s", want, encoded)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(encoded, absent) {
					t.Errorf("expected marshaled JSON not to contain %s, got: %s", absent, encoded)
				}
			}
			var roundTripped PodTemplateSpec
			if err := json.Unmarshal(raw, &roundTripped); err != nil {
				t.Fatalf("json.Unmarshal() unexpected error: %v", err)
			}
		})
	}
}

func TestFlinkDeploymentMarshalJSONPreserves64BitNoncesAndOmitsZeroStatus(t *testing.T) {
	// 1794223319123456789 > 2^53 (9007199254740992): standard float64 JSON unmarshaling
	// rounds it to 1794223319123456768 unless UseNumber() is used.
	const highPrecisionNonce int64 = 1794223319123456789

	testCases := map[string]struct {
		fd             *FlinkDeployment
		wantAbsent     []string
		wantSubstrings []string
		checkNonces    bool
	}{
		"64-bit nonces survive a podTemplate null-stripping round trip": {
			fd: &FlinkDeployment{
				APIVersion: SchemeGroupVersion.String(), Kind: "FlinkDeployment",
				Name: "nonce-check", Namespace: "default",
				Spec: FlinkDeploymentSpec{
					Image:        "flink:1.20",
					RestartNonce: new(highPrecisionNonce),
					PodTemplate: &PodTemplateSpec{
						APIVersion: "v1", Kind: "Pod",
						Spec: corev1.PodSpec{NodeSelector: map[string]string{"pool": "shared"}},
					},
					Job: &JobSpec{
						JarURI:                 "local:///opt/flink/examples/streaming/StateMachineExample.jar",
						State:                  JobStateSuspended,
						SavepointTriggerNonce:  new(highPrecisionNonce),
						CheckpointTriggerNonce: new(highPrecisionNonce + 1),
						SavepointRedeployNonce: new(highPrecisionNonce + 2),
						AutoscalerResetNonce:   new(highPrecisionNonce + 3),
					},
				},
			},
			wantSubstrings: []string{`"apiVersion":"v1"`, `"kind":"Pod"`},
			wantAbsent:     []string{`"containers":null`, `"flinkConfiguration"`},
			checkNonces:    true,
		},
		"zero jobStatus and reconciliationStatus are omitted from a partial status": {
			fd: &FlinkDeployment{
				Spec: FlinkDeploymentSpec{
					Image:       "flink:1.20",
					PodTemplate: &PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "flink-main-container"}}}},
				},
				Status: FlinkDeploymentStatus{LifecycleState: LifecycleStateCreated},
			},
			wantSubstrings: []string{`"lifecycleState":"CREATED"`},
			wantAbsent:     []string{`"jobStatus"`, `"reconciliationStatus"`, `null`},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			raw, err := json.Marshal(tc.fd)
			if err != nil {
				t.Fatalf("json.Marshal() unexpected error: %v", err)
			}
			encoded := string(raw)
			for _, want := range tc.wantSubstrings {
				if !strings.Contains(encoded, want) {
					t.Errorf("expected marshaled JSON to contain %s, got: %s", want, encoded)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(encoded, absent) {
					t.Errorf("expected marshaled JSON not to contain %s, got: %s", absent, encoded)
				}
			}

			var roundTripped FlinkDeployment
			if err := json.Unmarshal(raw, &roundTripped); err != nil {
				t.Fatalf("json.Unmarshal() unexpected error: %v", err)
			}
			if !tc.checkNonces {
				return
			}
			if roundTripped.Spec.PodTemplate == nil || roundTripped.Spec.PodTemplate.APIVersion != "v1" || roundTripped.Spec.PodTemplate.Kind != "Pod" {
				t.Errorf("roundTripped PodTemplate TypeMeta = %+v, want v1/Pod", roundTripped.Spec.PodTemplate)
			}
			wantNonces := map[string]struct {
				got  *int64
				want int64
			}{
				"spec.restartNonce":               {roundTripped.Spec.RestartNonce, highPrecisionNonce},
				"spec.job.savepointTriggerNonce":  {roundTripped.Spec.Job.SavepointTriggerNonce, highPrecisionNonce},
				"spec.job.checkpointTriggerNonce": {roundTripped.Spec.Job.CheckpointTriggerNonce, highPrecisionNonce + 1},
				"spec.job.savepointRedeployNonce": {roundTripped.Spec.Job.SavepointRedeployNonce, highPrecisionNonce + 2},
				"spec.job.autoscalerResetNonce":   {roundTripped.Spec.Job.AutoscalerResetNonce, highPrecisionNonce + 3},
			}
			for path, n := range wantNonces {
				if n.got == nil || *n.got != n.want {
					t.Errorf("%s = %v, want %d", path, n.got, n.want)
				}
			}
		})
	}
}

func mustUnmarshalQuantity(t *testing.T, raw string) Quantity {
	t.Helper()
	var q Quantity
	if err := json.Unmarshal([]byte(raw), &q); err != nil {
		t.Fatalf("json.Unmarshal(%s) error: %v", raw, err)
	}
	return q
}

func TestQuantityJSONAndDeepCopy(t *testing.T) {
	testCases := map[string]struct {
		rawInput             string
		fromGo               func() Quantity
		wantUnmarshalTypeErr bool
		wantParseErr         error
		wantLiteral          string
		wantMarshaled        string
		wantValueBytes       int64
	}{
		"preserves bare JSON integer 1000000000 without rewriting to 1G": {
			rawInput:       `1000000000`,
			wantLiteral:    "1000000000",
			wantMarshaled:  `1000000000`,
			wantValueBytes: 1000000000,
		},
		"preserves JSON string 1000000000 without rewriting to 1G": {
			rawInput:       `"1000000000"`,
			wantLiteral:    "1000000000",
			wantMarshaled:  `"1000000000"`,
			wantValueBytes: 1000000000,
		},
		"preserves JSON string 1024000 without rewriting to 1024k": {
			rawInput:       `"1024000"`,
			wantLiteral:    "1024000",
			wantMarshaled:  `"1024000"`,
			wantValueBytes: 1024000,
		},
		"preserves fractional BinarySI literal 1.5Gi for raw validation": {
			rawInput:       `"1.5Gi"`,
			wantLiteral:    "1.5Gi",
			wantMarshaled:  `"1.5Gi"`,
			wantValueBytes: 1610612736,
		},
		"QuantityFromResource falls back to canonical resource.Quantity string": {
			fromGo:         func() Quantity { return QuantityFromResource(resource.MustParse("1G")) },
			wantLiteral:    "1G",
			wantMarshaled:  `"1G"`,
			wantValueBytes: 1000000000,
		},
		"defers Flink MemorySize 2g parse error without failing UnmarshalJSON": {
			rawInput:      `"2g"`,
			wantParseErr:  resource.ErrFormatWrong,
			wantLiteral:   "2g",
			wantMarshaled: `"2g"`,
		},
		"defers Flink MemorySize 2048mb parse error without failing UnmarshalJSON": {
			rawInput:      `"2048mb"`,
			wantParseErr:  resource.ErrFormatWrong,
			wantLiteral:   "2048mb",
			wantMarshaled: `"2048mb"`,
		},
		"defers non-Kubernetes 2GiB suffix parse error without failing UnmarshalJSON": {
			rawInput:      `"2GiB"`,
			wantParseErr:  resource.ErrFormatWrong,
			wantLiteral:   "2GiB",
			wantMarshaled: `"2GiB"`,
		},
		"rejects boolean true token with JSON type error": {
			rawInput:             `true`,
			wantUnmarshalTypeErr: true,
		},
		"rejects boolean false token with JSON type error": {
			rawInput:             `false`,
			wantUnmarshalTypeErr: true,
		},
		"rejects JSON object token with JSON type error": {
			rawInput:             `{}`,
			wantUnmarshalTypeErr: true,
		},
		"rejects JSON array token with JSON type error": {
			rawInput:             `[]`,
			wantUnmarshalTypeErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var q Quantity
			if tc.fromGo != nil {
				q = tc.fromGo()
			} else {
				err := json.Unmarshal([]byte(tc.rawInput), &q)
				if tc.wantUnmarshalTypeErr {
					if _, ok := errors.AsType[*json.UnmarshalTypeError](err); !ok {
						t.Fatalf("json.Unmarshal(%s) error = %v, want *json.UnmarshalTypeError", tc.rawInput, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("json.Unmarshal(%s) unexpected error: %v", tc.rawInput, err)
				}
			}

			if tc.wantParseErr != nil {
				if !errors.Is(q.ParseError(), tc.wantParseErr) {
					t.Fatalf("ParseError() = %v, want %v", q.ParseError(), tc.wantParseErr)
				}
			} else {
				if q.ParseError() != nil {
					t.Fatalf("ParseError() = %v, want nil", q.ParseError())
				}
				if got := q.AsQuantity(); got.Value() != tc.wantValueBytes {
					t.Errorf("AsQuantity().Value() = %d, want %d", got.Value(), tc.wantValueBytes)
				}
			}
			if q.RawLiteral() != tc.wantLiteral {
				t.Errorf("RawLiteral() = %q, want %q", q.RawLiteral(), tc.wantLiteral)
			}

			marshaled, err := json.Marshal(q)
			if err != nil {
				t.Fatalf("json.Marshal() unexpected error: %v", err)
			}
			if string(marshaled) != tc.wantMarshaled {
				t.Errorf("json.Marshal() = %s, want %s", string(marshaled), tc.wantMarshaled)
			}

			copied := q.DeepCopy()
			if copied.RawLiteral() != tc.wantLiteral {
				t.Errorf("DeepCopy().RawLiteral() = %q, want %q", copied.RawLiteral(), tc.wantLiteral)
			}
			if tc.wantParseErr != nil && !errors.Is(copied.ParseError(), tc.wantParseErr) {
				t.Errorf("DeepCopy().ParseError() = %v, want %v", copied.ParseError(), tc.wantParseErr)
			}
			copiedMarshaled, err := json.Marshal(copied)
			if err != nil {
				t.Fatalf("json.Marshal(copied) error: %v", err)
			}
			if string(copiedMarshaled) != tc.wantMarshaled {
				t.Errorf("json.Marshal(copied) = %s, want %s", string(copiedMarshaled), tc.wantMarshaled)
			}
		})
	}
}

func TestQuantityEqualAndSemanticDeepEqual(t *testing.T) {
	testCases := map[string]struct {
		a         func(t *testing.T) Quantity
		b         func(t *testing.T) Quantity
		wantEqual bool
	}{
		"JSON integer 1000000000 equals JSON string 1000000000": {
			a:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `1000000000`) },
			b:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `"1000000000"`) },
			wantEqual: true,
		},
		"JSON integer 1000000000 equals QuantityFromResource 1G": {
			a:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `1000000000`) },
			b:         func(*testing.T) Quantity { return QuantityFromResource(resource.MustParse("1G")) },
			wantEqual: true,
		},
		"1000000000 bytes does not equal 1Gi": {
			a:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `1000000000`) },
			b:         func(*testing.T) Quantity { return MustParseQuantity("1Gi") },
			wantEqual: false,
		},
		"identical unparsed Flink MemorySize 2g quantities compare equal": {
			a:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `"2g"`) },
			b:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `"2g"`) },
			wantEqual: true,
		},
		"distinct unparsed Flink MemorySize 2g and 4g quantities do not compare equal": {
			a:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `"2g"`) },
			b:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `"4g"`) },
			wantEqual: false,
		},
		"unparsed Flink MemorySize 2g does not equal parsed 0": {
			a:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `"2g"`) },
			b:         func(t *testing.T) Quantity { return mustUnmarshalQuantity(t, `"0"`) },
			wantEqual: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			a, b := tc.a(t), tc.b(t)
			if got := a.Equal(b); got != tc.wantEqual {
				t.Errorf("Equal() = %v, want %v", got, tc.wantEqual)
			}
			if got := b.Equal(a); got != tc.wantEqual {
				t.Errorf("reverse Equal() = %v, want %v", got, tc.wantEqual)
			}

			fdA := &FlinkDeployment{
				Spec: FlinkDeploymentSpec{
					JobManager: &JobManagerSpec{
						Resources: &ResourceRequirements{Requests: ResourceList{corev1.ResourceMemory: a}},
					},
				},
			}
			fdB := &FlinkDeployment{
				Spec: FlinkDeploymentSpec{
					JobManager: &JobManagerSpec{
						Resources: &ResourceRequirements{Requests: ResourceList{corev1.ResourceMemory: b}},
					},
				},
			}
			if got := apiequality.Semantic.DeepEqual(fdA, fdB); got != tc.wantEqual {
				t.Errorf("apiequality.Semantic.DeepEqual(*FlinkDeployment) = %v, want %v", got, tc.wantEqual)
			}
		})
	}
}

func TestFlinkDeploymentListUnmarshalWithDeferredQuantityParseError(t *testing.T) {
	var list FlinkDeploymentList
	listJSON := `{"apiVersion":"flink.apache.org/v1beta1","kind":"FlinkDeploymentList","items":[{"metadata":{"name":"unmanaged-2g","namespace":"ns"},"spec":{"image":"flink:1.20","jobManager":{"resources":{"requests":{"cpu":"1","memory":"2g"}}}}}]}`
	if err := json.Unmarshal([]byte(listJSON), &list); err != nil {
		t.Fatalf("FlinkDeploymentList json.Unmarshal with \"2g\" memory failed: %v", err)
	}
	if len(list.Items) != 1 || list.Items[0].Spec.JobManager.Resources.Requests[corev1.ResourceMemory].RawLiteral() != "2g" {
		t.Errorf("unexpected decoded FlinkDeploymentList items: %+v", list.Items)
	}
}

func TestPodTemplateSpecRawJSONPreservationAndOverlayPatch(t *testing.T) {
	rawPartial := `{"spec":{"affinity":{"podAntiAffinity":{"preferredDuringSchedulingIgnoredDuringExecution":[{"podAffinityTerm":{"topologyKey":"kubernetes.io/hostname"}}]}},"containers":[{"env":[{"name":"MEM","valueFrom":{"resourceFieldRef":{"resource":"requests.memory"}}}],"livenessProbe":{"httpGet":{"path":"/healthz"}},"ports":[{"protocol":"TCP"}]}],"topologySpreadConstraints":[{"whenUnsatisfiable":"ScheduleAnyway"}]}}`

	var pt PodTemplateSpec
	if err := json.Unmarshal([]byte(rawPartial), &pt); err != nil {
		t.Fatalf("json.Unmarshal(rawPartial) error: %v", err)
	}
	if !pt.HasUnmodifiedRawSpec() {
		t.Fatal("expected HasUnmodifiedRawSpec() == true after UnmarshalJSON")
	}

	marshaledUnmutated, err := json.Marshal(pt)
	if err != nil {
		t.Fatalf("json.Marshal(unmutated) error: %v", err)
	}
	if string(marshaledUnmutated) != rawPartial {
		t.Errorf("unmutated MarshalJSON() = %s, want verbatim %s", string(marshaledUnmutated), rawPartial)
	}

	copied := pt.DeepCopy()
	if !apiequality.Semantic.DeepEqual(&pt, copied) {
		t.Error("expected semantic equality between original and DeepCopy PodTemplateSpec")
	}
	marshaledCopy, err := json.Marshal(copied)
	if err != nil {
		t.Fatalf("json.Marshal(copied) error: %v", err)
	}
	if string(marshaledCopy) != rawPartial {
		t.Errorf("DeepCopy MarshalJSON() = %s, want verbatim %s", string(marshaledCopy), rawPartial)
	}

	copied.Labels = map[string]string{"kueue.x-k8s.io/workload": "wl-1"}
	copied.Annotations = map[string]string{"kueue.x-k8s.io/podset-unconstrained-topology": "true"}
	copied.Spec.NodeSelector = map[string]string{"flavor": "on-demand"}
	copied.Spec.Tolerations = []corev1.Toleration{{Key: "spot", Operator: corev1.TolerationOpExists}}
	copied.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/topology"}}
	if !copied.HasUnmodifiedRawSpec() {
		t.Fatal("expected HasUnmodifiedRawSpec() == true when only metadata/placement overlays changed")
	}

	marshaledOverlay, err := json.Marshal(copied)
	if err != nil {
		t.Fatalf("json.Marshal(overlay) error: %v", err)
	}
	overlayStr := string(marshaledOverlay)
	for _, want := range []string{
		`"labels":{"kueue.x-k8s.io/workload":"wl-1"}`,
		`"nodeSelector":{"flavor":"on-demand"}`,
		`"tolerations":[{"key":"spot","operator":"Exists"}]`,
		`"schedulingGates":[{"name":"kueue.x-k8s.io/topology"}]`,
	} {
		if !strings.Contains(overlayStr, want) {
			t.Errorf("overlay MarshalJSON() missing %s in %s", want, overlayStr)
		}
	}
	for _, zeroPollution := range []string{
		`"containerPort":0`,
		`"divisor":"0"`,
		`"port":0`,
		`"weight":0`,
		`"maxSkew":0`,
		`"topologyKey":""`,
		`"resources":{}`,
		`"name":""`,
	} {
		if strings.Contains(overlayStr, zeroPollution) {
			t.Errorf("overlay MarshalJSON() injected Go zero value %s into %s", zeroPollution, overlayStr)
		}
	}

	copied.Labels = nil
	copied.Annotations = nil
	copied.Spec.NodeSelector = nil
	copied.Spec.Tolerations = nil
	copied.Spec.SchedulingGates = nil
	marshaledRestored, err := json.Marshal(copied)
	if err != nil {
		t.Fatalf("json.Marshal(restored) error: %v", err)
	}
	if string(marshaledRestored) != rawPartial {
		t.Errorf("restored MarshalJSON() = %s, want %s", string(marshaledRestored), rawPartial)
	}
}

func TestPodTemplateSpecMarshalOverlayPreservesEmptyStringTolerations(t *testing.T) {
	rawTolOverride := `{"spec":{"tolerations":[{"operator":"Exists","value":""},{"effect":""},{"key":"","operator":"Exists","value":""}]}}`
	var pt PodTemplateSpec
	if err := json.Unmarshal([]byte(rawTolOverride), &pt); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	admitted := pt.DeepCopy()
	admitted.Spec.Tolerations = append(admitted.Spec.Tolerations, corev1.Toleration{
		Key:      "gpu",
		Operator: corev1.TolerationOpEqual,
		Value:    "true",
		Effect:   corev1.TaintEffectNoSchedule,
	})
	admittedBytes, err := json.Marshal(admitted)
	if err != nil {
		t.Fatalf("json.Marshal(admitted) error: %v", err)
	}
	wantAdmitted := `{"spec":{"tolerations":[{"operator":"Exists","value":""},{"effect":""},{"key":"","operator":"Exists","value":""},{"effect":"NoSchedule","key":"gpu","operator":"Equal","value":"true"}]}}`
	if string(admittedBytes) != wantAdmitted {
		t.Errorf("admitted MarshalJSON() =\n%s\nwant:\n%s", string(admittedBytes), wantAdmitted)
	}

	rawMaps := admitted.RawSpecTolerations()
	if len(rawMaps) != 4 {
		t.Fatalf("len(RawSpecTolerations()) = %d, want 4", len(rawMaps))
	}
	if val, ok := rawMaps[0]["value"]; !ok || val != "" {
		t.Errorf("rawMaps[0][\"value\"] = %v, %v; want \"\", true", val, ok)
	}
	if eff, ok := rawMaps[1]["effect"]; !ok || eff != "" {
		t.Errorf("rawMaps[1][\"effect\"] = %v, %v; want \"\", true", eff, ok)
	}
	if k, ok := rawMaps[2]["key"]; !ok || k != "" {
		t.Errorf("rawMaps[2][\"key\"] = %v, %v; want \"\", true", k, ok)
	}

	var admittedReloaded PodTemplateSpec
	if err := json.Unmarshal(admittedBytes, &admittedReloaded); err != nil {
		t.Fatalf("json.Unmarshal(admittedBytes) error: %v", err)
	}
	admittedReloaded.Spec.Tolerations = admittedReloaded.Spec.Tolerations[:3]
	restoredBytes, err := json.Marshal(&admittedReloaded)
	if err != nil {
		t.Fatalf("json.Marshal(restored) error: %v", err)
	}
	if string(restoredBytes) != rawTolOverride {
		t.Errorf("restored MarshalJSON() =\n%s\nwant:\n%s", string(restoredBytes), rawTolOverride)
	}
}

func TestGoConstructedPodTemplateSpecMarshalJSONPrunesEmptyPodSpecFields(t *testing.T) {
	pt := FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				Sysctls: []corev1.Sysctl{
					{Name: "kernel.shm_rmid_forced", Value: ""},
					{},
				},
			},
			Volumes: []corev1.Volume{
				{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/tmp/flink",
						Type: new(corev1.HostPathUnset),
					},
				},
			},
			Containers: []corev1.Container{
				{
					Ports: []corev1.ContainerPort{
						{Protocol: corev1.ProtocolTCP},
					},
					Env: []corev1.EnvVar{
						{
							ValueFrom: &corev1.EnvVarSource{
								ResourceFieldRef: &corev1.ResourceFieldSelector{
									Resource: "requests.memory",
								},
							},
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{MountPath: "/mnt/data"},
					},
					LivenessProbe: &corev1.Probe{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/healthz",
							HTTPHeaders: []corev1.HTTPHeader{
								{Name: "X-Custom", Value: ""},
								{},
							},
						},
					},
					ReadinessProbe: &corev1.Probe{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/readyz",
							Port: intstr.FromString(""),
						},
					},
					StartupProbe: &corev1.Probe{
						GRPC: &corev1.GRPCAction{
							Port:    9090,
							Service: new(""),
						},
					},
				},
			},
			Affinity: &corev1.Affinity{
				PodAntiAffinity: &corev1.PodAntiAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
						{
							PodAffinityTerm: corev1.PodAffinityTerm{
								TopologyKey: "kubernetes.io/hostname",
							},
						},
					},
				},
			},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
				{WhenUnsatisfiable: corev1.ScheduleAnyway},
			},
		},
	})

	wire, err := json.Marshal(pt)
	if err != nil {
		t.Fatalf("json.Marshal(pt) error: %v", err)
	}
	wireStr := string(wire)

	for _, zeroPollution := range []string{
		`"name":""`,
		`"containerPort":0`,
		`"divisor":"0"`,
		`"port":0`,
		`"weight":0`,
		`"maxSkew":0`,
		`"topologyKey":""`,
		`"resources":{}`,
		`null`,
	} {
		if strings.Contains(wireStr, zeroPollution) {
			t.Errorf("Go-constructed PodTemplateSpec MarshalJSON() emitted zero field %s into %s", zeroPollution, wireStr)
		}
	}

	for _, wantPreserved := range []string{
		`"type":""`,
		`"service":""`,
		`"port":""`,
		`"protocol":"TCP"`,
		`"resource":"requests.memory"`,
		`"mountPath":"/mnt/data"`,
		`"whenUnsatisfiable":"ScheduleAnyway"`,
		`"httpHeaders":[{"name":"X-Custom","value":""},{}]`,
		`"sysctls":[{"name":"kernel.shm_rmid_forced","value":""},{}]`,
	} {
		if !strings.Contains(wireStr, wantPreserved) {
			t.Errorf("Go-constructed PodTemplateSpec MarshalJSON() dropped %s from %s", wantPreserved, wireStr)
		}
	}

	var decoded PodTemplateSpec
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(wire) error: %v", err)
	}
	if !decoded.HasUnmodifiedRawSpec() {
		t.Fatal("expected decoded.HasUnmodifiedRawSpec() == true")
	}
	remarshaled, err := json.Marshal(&decoded)
	if err != nil {
		t.Fatalf("json.Marshal(decoded) error: %v", err)
	}
	if string(remarshaled) != wireStr {
		t.Errorf("remarshaled JSON = %s, want %s", string(remarshaled), wireStr)
	}
}

func TestPodTemplateSpecDeferredDecodeError(t *testing.T) {
	testCases := map[string]struct {
		raw string
	}{
		"Flink MemorySize 2g inside container resources": {
			raw: `{"spec":{"containers":[{"name":"flink-main-container","resources":{"requests":{"memory":"2g"}}}]}}`,
		},
		"non-RFC3339 timestamp inside metadata": {
			raw: `{"metadata":{"creationTimestamp":"not-a-timestamp"},"spec":{"containers":[{"name":"flink-main-container"}]}}`,
		},
		"non-object podTemplate payload": {
			raw: `["invalid"]`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var pt PodTemplateSpec
			if err := json.Unmarshal([]byte(tc.raw), &pt); err != nil {
				t.Fatalf("json.Unmarshal(%s) unexpected error: %v", tc.raw, err)
			}
			if pt.DecodeError() == nil {
				t.Fatalf("DecodeError() = nil, want deferred decode error for %s", tc.raw)
			}
			if pt.HasUnmodifiedRawSpec() {
				t.Error("expected HasUnmodifiedRawSpec() == false when DecodeError() != nil")
			}

			marshaled, err := json.Marshal(pt)
			if err != nil {
				t.Fatalf("json.Marshal() unexpected error: %v", err)
			}
			if string(marshaled) != tc.raw {
				t.Errorf("json.Marshal() = %s, want verbatim %s", string(marshaled), tc.raw)
			}

			copied := pt.DeepCopy()
			if copied.DecodeError() == nil {
				t.Error("DeepCopy().DecodeError() = nil, want preserved decode error")
			}
			if !apiequality.Semantic.DeepEqual(&pt, copied) {
				t.Error("expected original and DeepCopy PodTemplateSpec with deferred decode error to compare equal")
			}
			if apiequality.Semantic.DeepEqual(&pt, &PodTemplateSpec{}) {
				t.Error("expected PodTemplateSpec with deferred decode error not to equal zero PodTemplateSpec")
			}
		})
	}
}
