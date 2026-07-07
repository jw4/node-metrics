package registry

import "testing"

func TestExtractMaxCPUTemp(t *testing.T) {
	tests := []struct {
		name    string
		samples []Sample
		want    float64
		wantOK  bool
	}{
		{
			name: "picks max across coretemp sensors, ignores non-CPU chips",
			samples: []Sample{
				{ChipName: "platform_coretemp_0", Value: 71},
				{ChipName: "platform_coretemp_0", Value: 69},
				{ChipName: "platform_coretemp_0", Value: 65},
				{ChipName: "platform_applesmc_768", Value: 74.25}, // higher, but not a CPU chip
				{ChipName: "thermal_thermal_zone0", Value: 52.5},
			},
			want:   71,
			wantOK: true,
		},
		{
			name:    "no CPU chip present",
			samples: []Sample{{ChipName: "platform_applesmc_768", Value: 24.25}},
			want:    0,
			wantOK:  false,
		},
		{
			name: "k10temp chip recognized (AMD)",
			samples: []Sample{
				{ChipName: "k10temp", Value: 55},
				{ChipName: "k10temp", Value: 60},
			},
			want:   60,
			wantOK: true,
		},
		{
			name:    "empty input",
			samples: nil,
			want:    0,
			wantOK:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extractMaxCPUTemp(tt.samples)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Fatalf("value = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCPUTempPromQL(t *testing.T) {
	got := CPUTemp.PromQL("belfalas-w-jw4-us:9100")
	want := `node_hwmon_temp_celsius{instance="belfalas-w-jw4-us:9100"}`
	if got != want {
		t.Fatalf("PromQL = %q, want %q", got, want)
	}
}
