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

func TestExtractSingle(t *testing.T) {
	tests := []struct {
		name    string
		samples []Sample
		want    float64
		wantOK  bool
	}{
		{
			name:    "exactly one sample",
			samples: []Sample{{Value: 42.5}},
			want:    42.5,
			wantOK:  true,
		},
		{
			name:    "no samples",
			samples: nil,
			want:    0,
			wantOK:  false,
		},
		{
			name:    "more than one sample is ambiguous, not a guess",
			samples: []Sample{{Value: 1}, {Value: 2}},
			want:    0,
			wantOK:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extractSingle(tt.samples)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Fatalf("value = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewMetricsPromQL(t *testing.T) {
	const instance = "belfalas-w-jw4-us:9100"
	tests := []struct {
		metric Metric
		want   string
	}{
		{
			metric: CPUUsagePct,
			want:   `100 * (1 - avg(rate(node_cpu_seconds_total{instance="belfalas-w-jw4-us:9100",mode="idle"}[1m])))`,
		},
		{
			metric: MemoryUsedPct,
			want:   `100 * (1 - node_memory_MemAvailable_bytes{instance="belfalas-w-jw4-us:9100"} / node_memory_MemTotal_bytes{instance="belfalas-w-jw4-us:9100"})`,
		},
		{
			metric: Load1,
			want:   `node_load1{instance="belfalas-w-jw4-us:9100"}`,
		},
		{
			metric: DiskUsedPct,
			want:   `100 * (1 - node_filesystem_avail_bytes{instance="belfalas-w-jw4-us:9100",mountpoint="/"} / node_filesystem_size_bytes{instance="belfalas-w-jw4-us:9100",mountpoint="/"})`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.metric.Name, func(t *testing.T) {
			got := tt.metric.PromQL(instance)
			if got != tt.want {
				t.Fatalf("PromQL = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAllIncludesEveryMetric(t *testing.T) {
	want := []string{"cpu_temp", "cpu_usage_pct", "memory_used_pct", "load1", "disk_used_pct"}
	if len(All) != len(want) {
		t.Fatalf("len(All) = %d, want %d", len(All), len(want))
	}
	for i, m := range All {
		if m.Name != want[i] {
			t.Fatalf("All[%d].Name = %q, want %q", i, m.Name, want[i])
		}
	}
}
