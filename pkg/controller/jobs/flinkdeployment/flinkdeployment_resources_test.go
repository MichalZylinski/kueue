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
	"math"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	flinkv1beta1 "sigs.k8s.io/kueue/pkg/controller/jobs/flinkdeployment/apis/v1beta1"
	testingflinkdeployment "sigs.k8s.io/kueue/pkg/util/testingjobs/flinkdeployment"
)

func TestParseFlinkMemoryQuantity(t *testing.T) {
	testCases := map[string]struct {
		input         string
		wantBytes     int64
		wantFormat    resource.Format
		wantErrSubstr string
	}{
		"mebibytes shorthand m": {
			input:      "2048m",
			wantBytes:  2048 * (1 << 20),
			wantFormat: resource.BinarySI,
		},
		"mebibytes with whitespace and mb": {
			input:      "2048 mb",
			wantBytes:  2048 * (1 << 20),
			wantFormat: resource.BinarySI,
		},
		"mebibytes uppercase MB": {
			input:      "512MB",
			wantBytes:  512 * (1 << 20),
			wantFormat: resource.BinarySI,
		},
		"mebibytes full word": {
			input:      "128 mebibytes",
			wantBytes:  128 * (1 << 20),
			wantFormat: resource.BinarySI,
		},
		"gibibytes shorthand g": {
			input:      "2g",
			wantBytes:  2 * (1 << 30),
			wantFormat: resource.BinarySI,
		},
		"gibibytes gb": {
			input:      "4 gb",
			wantBytes:  4 * (1 << 30),
			wantFormat: resource.BinarySI,
		},
		"gibibytes full word": {
			input:      "1 gibibytes",
			wantBytes:  1 << 30,
			wantFormat: resource.BinarySI,
		},
		"kibibytes shorthand k": {
			input:      "1024k",
			wantBytes:  1024 * (1 << 10),
			wantFormat: resource.BinarySI,
		},
		"kibibytes kb": {
			input:      "64 kb",
			wantBytes:  64 * (1 << 10),
			wantFormat: resource.BinarySI,
		},
		"kibibytes full word": {
			input:      "64 kibibytes",
			wantBytes:  64 * (1 << 10),
			wantFormat: resource.BinarySI,
		},
		"tebibytes shorthand t": {
			input:      "1t",
			wantBytes:  1 << 40,
			wantFormat: resource.BinarySI,
		},
		"tebibytes full word": {
			input:      "1 tebibytes",
			wantBytes:  1 << 40,
			wantFormat: resource.BinarySI,
		},
		"bare integer kib-aligned bytes": {
			input:      "4096",
			wantBytes:  4096,
			wantFormat: resource.BinarySI,
		},
		"non-kib bare bytes": {
			input:      "500b",
			wantBytes:  500,
			wantFormat: resource.DecimalSI,
		},
		"bytes full word": {
			input:      "1000 bytes",
			wantBytes:  1000,
			wantFormat: resource.DecimalSI,
		},
		"kubernetes binary Gi suffix": {
			input:      "2Gi",
			wantBytes:  2 * (1 << 30),
			wantFormat: resource.BinarySI,
		},
		"kubernetes binary Mi suffix": {
			input:      "512Mi",
			wantBytes:  512 * (1 << 20),
			wantFormat: resource.BinarySI,
		},
		"normalized kib suffix": {
			input:      "64kib",
			wantBytes:  64 * (1 << 10),
			wantFormat: resource.BinarySI,
		},
		"normalized uppercase MIB suffix": {
			input:      "512MIB",
			wantBytes:  512 * (1 << 20),
			wantFormat: resource.BinarySI,
		},
		"normalized lowercase gib suffix": {
			input:      "2gib",
			wantBytes:  2 * (1 << 30),
			wantFormat: resource.BinarySI,
		},
		"normalized lowercase tib suffix": {
			input:      "1tib",
			wantBytes:  1 << 40,
			wantFormat: resource.BinarySI,
		},
		"Kubernetes 1Pi BinarySI": {
			input:      "1Pi",
			wantBytes:  1 << 50,
			wantFormat: resource.BinarySI,
		},
		"max supported 2147483647Mi": {
			input:      "2147483647Mi",
			wantBytes:  maxFlinkMemoryBytes,
			wantFormat: resource.BinarySI,
		},
		"Flink MemorySize bytes word": {
			input:      "2048 bytes",
			wantBytes:  2048,
			wantFormat: resource.BinarySI,
		},
		"Flink MemorySize gibibytes word": {
			input:      "2 gibibytes",
			wantBytes:  2 << 30,
			wantFormat: resource.BinarySI,
		},
		"reject fractional Kubernetes Gi quantity": {
			input:         "1.5Gi",
			wantErrSubstr: "must not contain a decimal point",
		},
		"reject fractional Kubernetes Gi quantity below one": {
			input:         "0.5Gi",
			wantErrSubstr: "must not contain a decimal point",
		},
		"reject fractional Flink MemorySize": {
			input:         "1.5M",
			wantErrSubstr: "must not contain a decimal point",
		},
		"reject empty": {
			input:         "   ",
			wantErrSubstr: "cannot be empty",
		},
		"reject zero Flink MemorySize": {
			input:         "0m",
			wantErrSubstr: "must be greater than zero",
		},
		"reject zero Kubernetes Quantity": {
			input:         "0Gi",
			wantErrSubstr: "must be greater than zero",
		},
		"reject sub-byte fractional Quantity": {
			input:         "0.5",
			wantErrSubstr: "must not contain a decimal point",
		},
		"reject negative": {
			input:         "-1024m",
			wantErrSubstr: "must be greater than zero",
		},
		"reject unknown unit": {
			input:         "100xyz",
			wantErrSubstr: "invalid memory quantity",
		},
		"reject negative decimal exponent 2147483648000e-3": {
			input:         "2147483648000e-3",
			wantErrSubstr: "invalid memory quantity",
		},
		"reject negative decimal exponent 1000e-1": {
			input:         "1000e-1",
			wantErrSubstr: "invalid memory quantity",
		},
		"reject positive scientific notation 1e16": {
			input:         "1e16",
			wantErrSubstr: "invalid memory quantity",
		},
		"reject leading plus sign +1Gi": {
			input:         "+1Gi",
			wantErrSubstr: "invalid memory quantity",
		},
		"reject Ei suffix 1Ei": {
			input:         "1Ei",
			wantErrSubstr: "invalid memory quantity",
		},
		"reject 2Pi exceeding Integer.MAX_VALUE MiB": {
			input:         "2Pi",
			wantErrSubstr: "exceeds maximum supported Flink memory size",
		},
		"reject 2048Ti exceeding Integer.MAX_VALUE MiB": {
			input:         "2048Ti",
			wantErrSubstr: "exceeds maximum supported Flink memory size",
		},
		"reject Flink MemorySize exceeding max supported size": {
			input:         "9000000t",
			wantErrSubstr: "exceeds maximum supported Flink memory size",
		},
		"reject Kubernetes Quantity Ei suffix": {
			input:         "99999999Ei",
			wantErrSubstr: "invalid memory quantity",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got, err := parseFlinkMemoryQuantity(tc.input)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("parseFlinkMemoryQuantity(%q) error = %v, want substring %q", tc.input, err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFlinkMemoryQuantity(%q) unexpected error: %v", tc.input, err)
			}
			if got.Value() != tc.wantBytes {
				t.Errorf("parseFlinkMemoryQuantity(%q).Value() = %d, want %d (string=%s)", tc.input, got.Value(), tc.wantBytes, got.String())
			}
			if got.Format != tc.wantFormat {
				t.Errorf("parseFlinkMemoryQuantity(%q).Format = %q, want %q", tc.input, got.Format, tc.wantFormat)
			}
		})
	}
}

func TestParseFlinkCPUQuantity(t *testing.T) {
	testCases := map[string]struct {
		input         float64
		want          resource.Quantity
		wantErrSubstr string
	}{
		"whole core": {
			input: 2.0,
			want:  resource.MustParse("2"),
		},
		"fractional core": {
			input: 0.5,
			want:  resource.MustParse("500m"),
		},
		"sub-millicore rounds up": {
			input: 0.0002,
			want:  resource.MustParse("1m"),
		},
		"sub-millicore 1.2m rounds up to 2m": {
			input: 0.0012,
			want:  resource.MustParse("2m"),
		},
		"exact decimal 4.03 cores avoids IEEE-754 1m overshoot": {
			input: 4.03,
			want:  resource.MustParse("4030m"),
		},
		"exact decimal 4.07 cores avoids IEEE-754 1m overshoot": {
			input: 4.07,
			want:  resource.MustParse("4070m"),
		},
		"exact decimal 8.06 cores avoids IEEE-754 1m overshoot": {
			input: 8.06,
			want:  resource.MustParse("8060m"),
		},
		"exact decimal 8.14 cores avoids IEEE-754 1m overshoot": {
			input: 8.14,
			want:  resource.MustParse("8140m"),
		},
		"reject zero": {
			input:         0,
			wantErrSubstr: "must be greater than 0",
		},
		"reject negative": {
			input:         -1.0,
			wantErrSubstr: "must be greater than 0",
		},
		"reject NaN": {
			input:         math.NaN(),
			wantErrSubstr: "must be a finite number",
		},
		"reject Inf": {
			input:         math.Inf(1),
			wantErrSubstr: "must be a finite number",
		},
		"reject overflow": {
			input:         1e19,
			wantErrSubstr: "exceeds maximum supported cores",
		},
		"reject IEEE-754 float64 rounding boundary math.MaxInt64/1000": {
			input:         float64(math.MaxInt64 / 1000),
			wantErrSubstr: "exceeds maximum supported cores",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got, err := parseFlinkCPUQuantity(tc.input)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("parseFlinkCPUQuantity(%v) error = %v, want substring %q", tc.input, err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFlinkCPUQuantity(%v) unexpected error: %v", tc.input, err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("parseFlinkCPUQuantity(%v) = %s, want %s", tc.input, got.String(), tc.want.String())
			}
		})
	}
}

func TestParseFlinkBool(t *testing.T) {
	testCases := map[string]struct {
		input         string
		want          bool
		wantErrSubstr string
	}{
		"lowercase true": {
			input: "true",
			want:  true,
		},
		"uppercase TRUE": {
			input: "TRUE",
			want:  true,
		},
		"mixed-case False": {
			input: "False",
			want:  false,
		},
		"rejects whitespace-padded TRUE": {
			input:         "  TRUE  ",
			wantErrSubstr: `invalid boolean value "  TRUE  "`,
		},
		"rejects whitespace-padded false": {
			input:         " false ",
			wantErrSubstr: `invalid boolean value " false "`,
		},
		"rejects Go ParseBool numeric 1": {
			input:         "1",
			wantErrSubstr: `invalid boolean value "1"`,
		},
		"rejects Go ParseBool numeric 0": {
			input:         "0",
			wantErrSubstr: `invalid boolean value "0"`,
		},
		"rejects Go ParseBool shorthand t": {
			input:         "t",
			wantErrSubstr: `invalid boolean value "t"`,
		},
		"rejects Go ParseBool shorthand f": {
			input:         "f",
			wantErrSubstr: `invalid boolean value "f"`,
		},
		"rejects yes": {
			input:         "yes",
			wantErrSubstr: `invalid boolean value "yes"`,
		},
		"rejects empty": {
			input:         "",
			wantErrSubstr: `invalid boolean value ""`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got, err := parseFlinkBool(tc.input)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("parseFlinkBool(%q) error = %v, want substring %q", tc.input, err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFlinkBool(%q) unexpected error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("parseFlinkBool(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestValidateComponentMemoryQuantity(t *testing.T) {
	fromJSON := func(raw string) func(*testing.T) flinkv1beta1.Quantity {
		return func(t *testing.T) flinkv1beta1.Quantity {
			t.Helper()
			var q flinkv1beta1.Quantity
			if err := json.Unmarshal([]byte(raw), &q); err != nil {
				t.Fatalf("json.Unmarshal(%s) error: %v", raw, err)
			}
			return q
		}
	}

	testCases := map[string]struct {
		qty           func(*testing.T) flinkv1beta1.Quantity
		wantErrSubstr string
	}{
		"accepts plain integer bytes string 1000000000": {
			qty: fromJSON(`"1000000000"`),
		},
		"accepts plain integer bytes JSON number 1000000000": {
			qty: fromJSON(`1000000000`),
		},
		"accepts plain integer bytes string 1024000": {
			qty: fromJSON(`"1024000"`),
		},
		"accepts plain integer bytes string 2147483648": {
			qty: fromJSON(`"2147483648"`),
		},
		"accepts plain integer bytes string 1073741824": {
			qty: fromJSON(`"1073741824"`),
		},
		"accepts Ki BinarySI": {
			qty: fromJSON(`"64Ki"`),
		},
		"accepts Mi BinarySI": {
			qty: fromJSON(`"512Mi"`),
		},
		"accepts Gi BinarySI": {
			qty: fromJSON(`"2Gi"`),
		},
		"accepts Ti BinarySI": {
			qty: fromJSON(`"1Ti"`),
		},
		"accepts Pi BinarySI": {
			qty: fromJSON(`"1Pi"`),
		},
		"accepts 2147483647Mi max boundary": {
			qty: fromJSON(`"2147483647Mi"`),
		},
		"accepts QuantityFromResource BinarySI fallback": {
			qty: func(*testing.T) flinkv1beta1.Quantity {
				return flinkv1beta1.QuantityFromResource(resource.MustParse("2Gi"))
			},
		},
		"rejects fractional BinarySI 1.5Gi": {
			qty:           fromJSON(`"1.5Gi"`),
			wantErrSubstr: "must not contain a decimal point",
		},
		"rejects fractional BinarySI 0.5Gi": {
			qty:           fromJSON(`"0.5Gi"`),
			wantErrSubstr: "must not contain a decimal point",
		},
		"rejects fractional BinarySI 2.5Mi": {
			qty:           fromJSON(`"2.5Mi"`),
			wantErrSubstr: "must not contain a decimal point",
		},
		"rejects Ei suffix 1Ei": {
			qty:           fromJSON(`"1Ei"`),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes",
		},
		"rejects overflowing 8Ei": {
			qty:           fromJSON(`"8Ei"`),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes",
		},
		"rejects 2Pi exceeding Integer.MAX_VALUE MiB": {
			qty:           fromJSON(`"2Pi"`),
			wantErrSubstr: "exceeds maximum supported Flink memory size",
		},
		"rejects 2048Ti exceeding Integer.MAX_VALUE MiB": {
			qty:           fromJSON(`"2048Ti"`),
			wantErrSubstr: "exceeds maximum supported Flink memory size",
		},
		"rejects overflowing 8192Pi": {
			qty:           fromJSON(`"8192Pi"`),
			wantErrSubstr: "exceeds maximum supported Flink memory size",
		},
		"rejects negative decimal exponent 2147483648000e-3": {
			qty:           fromJSON(`"2147483648000e-3"`),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes",
		},
		"rejects DecimalSI 2G": {
			qty:           fromJSON(`"2G"`),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes",
		},
		"rejects Flink MemorySize / milli-byte 2048m": {
			qty:           fromJSON(`"2048m"`),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes",
		},
		"rejects Flink MemorySize 2g": {
			qty:           fromJSON(`"2g"`),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes",
		},
		"rejects GiB suffix 2GiB": {
			qty:           fromJSON(`"2GiB"`),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes",
		},
		"rejects scientific notation 1e9": {
			qty:           fromJSON(`"1e9"`),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes",
		},
		"rejects leading plus sign +1Gi": {
			qty:           fromJSON(`"+1Gi"`),
			wantErrSubstr: "must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes",
		},
		"rejects zero 0": {
			qty:           fromJSON(`"0"`),
			wantErrSubstr: "must be at least 1 byte",
		},
		"rejects whitespace-padded literal": {
			qty:           fromJSON(`" 1Gi "`),
			wantErrSubstr: "must not contain whitespace",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateComponentMemoryQuantity(tc.qty(t))
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("validateComponentMemoryQuantity() error = %v, want substring %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateComponentMemoryQuantity() unexpected error: %v", err)
			}
		})
	}
}

func TestParseEphemeralStorageQuantity(t *testing.T) {
	testCases := map[string]struct {
		input         string
		want          resource.Quantity
		wantErrSubstr string
	}{
		"valid 2Gi": {
			input: "2Gi",
			want:  resource.MustParse("2Gi"),
		},
		"valid 500Mi": {
			input: "500Mi",
			want:  resource.MustParse("500Mi"),
		},
		"rejects leading whitespace": {
			input:         " 2Gi",
			wantErrSubstr: `ephemeralStorage " 2Gi" must not contain whitespace`,
		},
		"rejects trailing whitespace": {
			input:         "2Gi ",
			wantErrSubstr: `ephemeralStorage "2Gi " must not contain whitespace`,
		},
		"rejects whitespace-only string": {
			input:         "   ",
			wantErrSubstr: `ephemeralStorage "   " must not contain whitespace`,
		},
		"rejects zero": {
			input:         "0Gi",
			wantErrSubstr: `ephemeralStorage "0Gi" must be greater than zero`,
		},
		"rejects negative": {
			input:         "-1Gi",
			wantErrSubstr: `ephemeralStorage "-1Gi" must be greater than zero`,
		},
		"rejects invalid suffix": {
			input:         "10bad",
			wantErrSubstr: `invalid ephemeralStorage "10bad"`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got, err := parseEphemeralStorageQuantity(tc.input)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("parseEphemeralStorageQuantity(%q) error = %v, want substring %q", tc.input, err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseEphemeralStorageQuantity(%q) unexpected error: %v", tc.input, err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("parseEphemeralStorageQuantity(%q) = %s, want %s", tc.input, got.String(), tc.want.String())
			}
		})
	}
}

func TestValidateComponentResourceRequirementsDeferredParseErrors(t *testing.T) {
	testCases := map[string]struct {
		rawJSON       string
		wantErrSubstr string
	}{
		"Flink 2g memory in limits reports BinarySI guidance before request <= limit check": {
			rawJSON:       `{"requests":{"memory":"4Gi"},"limits":{"memory":"2g"}}`,
			wantErrSubstr: `resource limit memory (2g): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes`,
		},
		"GiB suffix in requests reports BinarySI guidance": {
			rawJSON:       `{"requests":{"memory":"2GiB"}}`,
			wantErrSubstr: `resource request memory (2GiB): must use a BinarySI suffix (Ki, Mi, Gi, Ti, Pi) or plain integer bytes`,
		},
		"invalid non-Kubernetes CPU quantity reports parse error": {
			rawJSON:       `{"requests":{"cpu":"bad-cpu","memory":"2Gi"}}`,
			wantErrSubstr: `invalid resource request cpu (bad-cpu)`,
		},
		"leading whitespace in CPU request is rejected": {
			rawJSON:       `{"requests":{"cpu":" 500m","memory":"2Gi"}}`,
			wantErrSubstr: `resource request cpu (" 500m") must not contain whitespace`,
		},
		"trailing whitespace in CPU limit is rejected": {
			rawJSON:       `{"requests":{"memory":"2Gi"},"limits":{"cpu":"500m "}}`,
			wantErrSubstr: `resource limit cpu ("500m ") must not contain whitespace`,
		},
		"whitespace in ephemeral-storage request is rejected": {
			rawJSON:       `{"requests":{"ephemeral-storage":" 2Gi ","memory":"2Gi"}}`,
			wantErrSubstr: `resource request ephemeral-storage (" 2Gi ") must not contain whitespace`,
		},
		"whitespace in extended GPU resource limit is rejected": {
			rawJSON:       `{"requests":{"memory":"2Gi","nvidia.com/gpu":"1"},"limits":{"nvidia.com/gpu":" 1 "}}`,
			wantErrSubstr: `resource limit nvidia.com/gpu (" 1 ") must not contain whitespace`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var reqs flinkv1beta1.ResourceRequirements
			if err := json.Unmarshal([]byte(tc.rawJSON), &reqs); err != nil {
				t.Fatalf("json.Unmarshal(%s) unexpected error: %v", tc.rawJSON, err)
			}
			err := validateComponentResourceRequirements(&reqs)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("validateComponentResourceRequirements(%s) error = %v, want substring %q", tc.rawJSON, err, tc.wantErrSubstr)
			}
		})
	}
}

func TestValidatePodTolerations(t *testing.T) {
	testCases := map[string]struct {
		tolerations   []corev1.Toleration
		wantErrSubstr string
	}{
		"valid Equal, default-Equal, empty-key Exists, and NoExecute with tolerationSeconds": {
			tolerations: []corev1.Toleration{
				{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "gpu", Effect: corev1.TaintEffectNoSchedule},
				{Key: "tier", Value: "batch", Effect: corev1.TaintEffectPreferNoSchedule},
				{Key: "", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
				{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: new(int64(60))},
			},
		},
		"rejects empty key when operator is empty": {
			tolerations: []corev1.Toleration{
				{Key: "", Value: "gpu", Effect: corev1.TaintEffectNoSchedule},
			},
			wantErrSubstr: "operator must be Exists when key is empty",
		},
		"rejects empty key when operator is Equal": {
			tolerations: []corev1.Toleration{
				{Key: "", Operator: corev1.TolerationOpEqual, Value: "gpu"},
			},
			wantErrSubstr: "operator must be Exists when key is empty",
		},
		"rejects unsupported operator": {
			tolerations: []corev1.Toleration{
				{Key: "dedicated", Operator: "In", Value: "gpu"},
			},
			wantErrSubstr: `supported values: "Equal", "Exists"`,
		},
		"rejects invalid label value when operator is Equal": {
			tolerations: []corev1.Toleration{
				{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "bad value!"},
			},
			wantErrSubstr: "tolerations[0].value",
		},
		"rejects non-empty value when operator is Exists": {
			tolerations: []corev1.Toleration{
				{Key: "dedicated", Operator: corev1.TolerationOpExists, Value: "gpu"},
			},
			wantErrSubstr: "value must be empty when operator is Exists",
		},
		"rejects unsupported taint effect": {
			tolerations: []corev1.Toleration{
				{Key: "dedicated", Operator: corev1.TolerationOpExists, Effect: "HardEvict"},
			},
			wantErrSubstr: `supported values: "NoSchedule", "PreferNoSchedule", "NoExecute"`,
		},
		"rejects tolerationSeconds when effect is not NoExecute": {
			tolerations: []corev1.Toleration{
				{Key: "dedicated", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule, TolerationSeconds: new(int64(30))},
			},
			wantErrSubstr: "effect must be NoExecute when tolerationSeconds is set",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validatePodTolerations(tc.tolerations)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("validatePodTolerations() error = %v, want substring %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validatePodTolerations() unexpected error: %v", err)
			}
		})
	}
}

func TestBuildPodSetsContainerNamesAndListMapValidation(t *testing.T) {
	makeBaseJob := func() *flinkv1beta1.FlinkDeployment {
		return testingflinkdeployment.MakeFlinkDeployment("app", "ns").
			Queue("lq").
			Obj()
	}

	testCases := map[string]struct {
		mutate        func(*flinkv1beta1.FlinkDeployment)
		wantErrSubstr string
	}{
		"accepts valid named containers, ports, mounts, devices, claims, ephemeralContainers, and PodSpec list-maps": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						InitContainers: []corev1.Container{
							{
								Name:  "init-setup",
								Image: "busybox:1.36",
								Ports: []corev1.ContainerPort{{ContainerPort: 9000}},
								Env: []corev1.EnvVar{
									{Name: "DUP_ALLOWED", Value: "first"},
									{Name: "DUP_ALLOWED", Value: "second"},
								},
							},
						},
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Ports: []corev1.ContainerPort{
									{ContainerPort: 8081},
									{ContainerPort: 8081, Protocol: corev1.ProtocolUDP},
									{ContainerPort: 8082, Protocol: corev1.ProtocolSCTP},
								},
								VolumeMounts: []corev1.VolumeMount{
									{Name: "scratch", MountPath: "/scratch", BindMountOptions: []string{"noexec", "nosuid"}},
								},
								VolumeDevices: []corev1.VolumeDevice{
									{Name: "raw-blk", DevicePath: "/dev/xvda"},
								},
								Resources: corev1.ResourceRequirements{
									Claims: []corev1.ResourceClaim{{Name: "gpu-claim"}},
								},
								Env: []corev1.EnvVar{
									{Name: "FOO", Value: "a"},
									{Name: "FOO", Value: "b"},
								},
								RestartPolicyRules: []corev1.ContainerRestartRule{
									{
										Action: corev1.ContainerRestartRuleActionRestart,
										ExitCodes: &corev1.ContainerRestartRuleOnExitCodes{
											Operator: corev1.ContainerRestartRuleOnExitCodesOpIn,
											Values:   []int32{1, 2},
										},
									},
								},
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{
							{
								Name:  "debugger",
								Image: "busybox:1.36",
								Ports: []corev1.ContainerPort{{ContainerPort: 40000}},
								Env:   []corev1.EnvVar{{Name: "DBG", Value: "1"}},
							},
						},
						Volumes: []corev1.Volume{
							{Name: "scratch", EmptyDir: &corev1.EmptyDirVolumeSource{}},
							{
								Name: "rbd-vol",
								RBD:  &corev1.RBDVolumeSource{RBDImage: "img", CephMonitors: []string{"10.0.0.1:6789"}},
							},
							{
								Name:   "ceph-vol",
								CephFS: &corev1.CephFSVolumeSource{Monitors: []string{"10.0.0.1:6789"}},
							},
							{
								Name: "sio-vol",
								ScaleIO: &corev1.ScaleIOVolumeSource{
									Gateway:   "https://gw",
									System:    "sys",
									SecretRef: &corev1.LocalObjectReference{Name: "sio-sec"},
								},
							},
							{
								Name: "cert-vol",
								Projected: &corev1.ProjectedVolumeSource{
									Sources: []corev1.VolumeProjection{
										{
											PodCertificate: &corev1.PodCertificateProjection{
												KeyType:    "ECDSAP256",
												SignerName: "example.com/workload-signer",
											},
										},
									},
								},
							},
						},
						Affinity: &corev1.Affinity{
							NodeAffinity: &corev1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
									NodeSelectorTerms: []corev1.NodeSelectorTerm{
										{
											MatchExpressions: []corev1.NodeSelectorRequirement{
												{Key: "kubernetes.io/os", Operator: corev1.NodeSelectorOpIn, Values: []string{"linux"}},
											},
										},
									},
								},
							},
						},
						SchedulingGates:  []corev1.PodSchedulingGate{{Name: "example.com/custom-gate"}},
						ResourceClaims:   []corev1.PodResourceClaim{{Name: "gpu-claim", ResourceClaimTemplateName: new("gpu-tpl")}},
						ImagePullSecrets: []corev1.LocalObjectReference{{Name: "regcred"}},
						EvictionResponders: []corev1.EvictionResponder{
							{Name: "acme.io/evictor", Priority: new(int32(10000))},
						},
						HostAliases: []corev1.HostAlias{{IP: "127.0.0.1", Hostnames: []string{"local.test"}}},
						TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
							{MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.DoNotSchedule},
							{MaxSkew: 2, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.ScheduleAnyway},
						},
					},
				})
			},
		},
		"rejects unnamed container when spec.image is set instead of appending phantom flink-main-container": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Image: "busybox:1.36"}},
					},
				})
			},
			wantErrSubstr: `containers[0].name ""`,
		},
		"rejects invalid DNS-1123 container name": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "BAD_NAME", Image: "busybox:1.36"}},
					},
				})
			},
			wantErrSubstr: `containers[0].name "BAD_NAME"`,
		},
		"rejects duplicate container name after positional merge": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "sidecar", Image: "busybox:1.36"},
							{Name: flinkMainContainerName, Image: "flink:1.20"},
						},
					},
				})
				fd.Spec.TaskManager.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: flinkMainContainerName, Image: "flink:1.20"},
						},
					},
				})
			},
			wantErrSubstr: `duplicate containers name "flink-main-container"`,
		},
		"rejects duplicate initContainer name": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						InitContainers: []corev1.Container{
							{Name: "init-a", Image: "busybox:1.36"},
							{Name: "init-a", Image: "busybox:1.36"},
						},
					},
				})
			},
			wantErrSubstr: `duplicate initContainers name "init-a"`,
		},
		"rejects duplicate container port when empty protocol defaults to TCP": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Ports: []corev1.ContainerPort{
									{ContainerPort: 8080},
									{ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
								},
							},
						},
					},
				})
			},
			wantErrSubstr: `container "flink-main-container" has duplicate port 8080/TCP`,
		},
		"rejects duplicate ephemeralContainer port when empty protocol defaults to TCP": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						EphemeralContainers: []corev1.EphemeralContainer{
							{
								Name: "dbg",
								Ports: []corev1.ContainerPort{
									{ContainerPort: 9090},
									{ContainerPort: 9090, Protocol: corev1.ProtocolTCP},
								},
							},
						},
					},
				})
			},
			wantErrSubstr: `ephemeralContainer "dbg" has duplicate port 9090/TCP`,
		},
		"rejects out-of-range containerPort": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  flinkMainContainerName,
								Ports: []corev1.ContainerPort{{ContainerPort: 0}},
							},
						},
					},
				})
			},
			wantErrSubstr: `ports[0].containerPort (0) must be between 1 and 65535`,
		},
		"rejects unsupported port protocol": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  flinkMainContainerName,
								Ports: []corev1.ContainerPort{{ContainerPort: 8080, Protocol: "HTTP"}},
							},
						},
					},
				})
			},
			wantErrSubstr: `ports[0].protocol "HTTP"`,
		},
		"rejects duplicate volumeMount mountPath": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								VolumeMounts: []corev1.VolumeMount{
									{Name: "v1", MountPath: "/data"},
									{Name: "v2", MountPath: "/data"},
								},
							},
						},
					},
				})
			},
			wantErrSubstr: `has duplicate volumeMount mountPath "/data"`,
		},
		"rejects duplicate volumeDevice devicePath": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								VolumeDevices: []corev1.VolumeDevice{
									{Name: "d1", DevicePath: "/dev/xvda"},
									{Name: "d2", DevicePath: "/dev/xvda"},
								},
							},
						},
					},
				})
			},
			wantErrSubstr: `has duplicate volumeDevice devicePath "/dev/xvda"`,
		},
		"rejects duplicate container resources.claims": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: flinkMainContainerName,
								Resources: corev1.ResourceRequirements{
									Claims: []corev1.ResourceClaim{{Name: "gpu"}, {Name: "gpu"}},
								},
							},
						},
					},
				})
			},
			wantErrSubstr: `duplicate container "flink-main-container" resources.claims name "gpu"`,
		},
		"rejects duplicate ephemeralContainer env name": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						EphemeralContainers: []corev1.EphemeralContainer{
							{
								Name: "dbg",
								Env: []corev1.EnvVar{
									{Name: "DUP", Value: "1"},
									{Name: "DUP", Value: "2"},
								},
							},
						},
					},
				})
			},
			wantErrSubstr: `duplicate ephemeralContainer "dbg" env name "DUP"`,
		},
		"rejects duplicate PodSpec volumes name": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{Name: "shared"}, {Name: "shared"}},
					},
				})
			},
			wantErrSubstr: `duplicate volumes name "shared"`,
		},
		"rejects duplicate PodSpec schedulingGates name": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{{Name: "g1"}, {Name: "g1"}},
					},
				})
			},
			wantErrSubstr: `duplicate schedulingGates name "g1"`,
		},
		"rejects duplicate PodSpec resourceClaims name": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ResourceClaims: []corev1.PodResourceClaim{{Name: "rc1"}, {Name: "rc1"}},
					},
				})
			},
			wantErrSubstr: `duplicate resourceClaims name "rc1"`,
		},
		"rejects duplicate PodSpec resources.claims name": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Resources: &corev1.ResourceRequirements{
							Claims: []corev1.ResourceClaim{{Name: "pod-claim"}, {Name: "pod-claim"}},
						},
					},
				})
			},
			wantErrSubstr: `duplicate resources.claims name "pod-claim"`,
		},
		"rejects duplicate PodSpec imagePullSecrets name": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ImagePullSecrets: []corev1.LocalObjectReference{{Name: "sec"}, {Name: "sec"}},
					},
				})
			},
			wantErrSubstr: `duplicate imagePullSecrets name "sec"`,
		},
		"rejects evictionResponder without priority or duplicate name": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						EvictionResponders: []corev1.EvictionResponder{{Name: "acme.io/r1"}},
					},
				})
			},
			wantErrSubstr: `evictionResponders[0].priority is required`,
		},
		"rejects duplicate PodSpec hostAliases ip": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						HostAliases: []corev1.HostAlias{{IP: "10.0.0.1"}, {IP: "10.0.0.1"}},
					},
				})
			},
			wantErrSubstr: `duplicate hostAliases ip "10.0.0.1"`,
		},
		"rejects duplicate topologySpreadConstraints (topologyKey, whenUnsatisfiable)": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
							{MaxSkew: 1, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.DoNotSchedule},
							{MaxSkew: 2, TopologyKey: "topology.kubernetes.io/zone", WhenUnsatisfiable: corev1.DoNotSchedule},
						},
					},
				})
			},
			wantErrSubstr: `duplicate topologySpreadConstraints entry for topologyKey "topology.kubernetes.io/zone" and whenUnsatisfiable "DoNotSchedule"`,
		},
		"rejects empty requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Affinity: &corev1.Affinity{
							NodeAffinity: &corev1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{},
							},
						},
					},
				})
			},
			wantErrSubstr: `affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms must not be empty`,
		},
		"rejects rbd volume without monitors": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{Name: "rbd", RBD: &corev1.RBDVolumeSource{RBDImage: "img"}},
						},
					},
				})
			},
			wantErrSubstr: `volumes[0].rbd.monitors must not be empty`,
		},
		"rejects cephfs volume without monitors": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{Name: "ceph", CephFS: &corev1.CephFSVolumeSource{Path: "/mnt"}},
						},
					},
				})
			},
			wantErrSubstr: `volumes[0].cephfs.monitors must not be empty`,
		},
		"rejects scaleIO volume without secretRef": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{Name: "sio", ScaleIO: &corev1.ScaleIOVolumeSource{Gateway: "https://gw", System: "sys"}},
						},
					},
				})
			},
			wantErrSubstr: `volumes[0].scaleIO.secretRef is required`,
		},
		"rejects projected podCertificate without keyType": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{
								Name: "proj",
								Projected: &corev1.ProjectedVolumeSource{
									Sources: []corev1.VolumeProjection{
										{PodCertificate: &corev1.PodCertificateProjection{SignerName: "example.com/signer"}},
									},
								},
							},
						},
					},
				})
			},
			wantErrSubstr: `volumes[0].projected.sources[0].podCertificate.keyType must not be empty`,
		},
		"rejects projected podCertificate without signerName": {
			mutate: func(fd *flinkv1beta1.FlinkDeployment) {
				fd.Spec.PodTemplate = flinkv1beta1.FromCorePodTemplateSpec(&corev1.PodTemplateSpec{
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
				})
			},
			wantErrSubstr: `volumes[0].projected.sources[0].podCertificate.signerName must not be empty`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			fd := makeBaseJob()
			tc.mutate(fd)
			_, err := fromObject(fd).buildPodSets()
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("buildPodSets() error = %v, want substring %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildPodSets() unexpected error: %v", err)
			}
		})
	}
}
