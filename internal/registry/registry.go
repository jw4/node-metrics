package registry

import (
	"fmt"
	"strings"
)

// Sample is one (chip, value) reading returned by a Prometheus instant query
// for a hwmon-style metric.
type Sample struct {
	ChipName string
	Value    float64
}

// Metric describes one node_exporter metric this pipeline collects: how to
// query it for a given instance, and how to reduce the resulting samples to a
// single float64 per host. Adding a metric later is a new Metric value in All
// -- no other package needs to change.
type Metric struct {
	Name    string
	PromQL  func(instance string) string
	Extract func(samples []Sample) (float64, bool)
}

// cpuChipPrefixes lists the hwmon chip-name prefixes that identify a CPU
// package thermal sensor across the vendors/kernels this cluster runs.
// "platform_coretemp_0" (Intel, observed on belfalas this session) and bare
// "coretemp"/"k10temp"/"cpu_thermal" (older kernels / non-"platform_"-prefixed
// exporters) are both matched.
var cpuChipPrefixes = []string{
	"platform_coretemp",
	"platform_k10temp",
	"platform_cpu_thermal",
	"coretemp",
	"k10temp",
	"cpu_thermal",
}

func isCPUChip(chip string) bool {
	for _, p := range cpuChipPrefixes {
		if strings.HasPrefix(chip, p) {
			return true
		}
	}
	return false
}

func extractMaxCPUTemp(samples []Sample) (float64, bool) {
	var max float64
	found := false
	for _, s := range samples {
		if !isCPUChip(s.ChipName) {
			continue
		}
		if !found || s.Value > max {
			max = s.Value
			found = true
		}
	}
	return max, found
}

// CPUTemp is the first registry entry.
var CPUTemp = Metric{
	Name: "cpu_temp",
	PromQL: func(instance string) string {
		return fmt.Sprintf(`node_hwmon_temp_celsius{instance=%q}`, instance)
	},
	Extract: extractMaxCPUTemp,
}

// extractSingle returns the lone sample from a query already reduced to
// exactly one row by its own PromQL (an aggregation or an exact label match),
// unlike cpu_temp's multi-chip Extract. More or fewer than one row means the
// query didn't match this host as expected, so it's reported as absent rather
// than guessing which row to use.
func extractSingle(samples []Sample) (float64, bool) {
	if len(samples) != 1 {
		return 0, false
	}
	return samples[0].Value, true
}

// CPUUsagePct is percent-busy (100 - idle), averaged across cores over the
// last minute.
var CPUUsagePct = Metric{
	Name: "cpu_usage_pct",
	PromQL: func(instance string) string {
		return fmt.Sprintf(`100 * (1 - avg(rate(node_cpu_seconds_total{instance=%q,mode="idle"}[1m])))`, instance)
	},
	Extract: extractSingle,
}

// MemoryUsedPct is percent of total memory not available for new allocations.
var MemoryUsedPct = Metric{
	Name: "memory_used_pct",
	PromQL: func(instance string) string {
		return fmt.Sprintf(`100 * (1 - node_memory_MemAvailable_bytes{instance=%q} / node_memory_MemTotal_bytes{instance=%q})`, instance, instance)
	},
	Extract: extractSingle,
}

// Load1 is the 1-minute load average, already a single scalar per host.
var Load1 = Metric{
	Name: "load1",
	PromQL: func(instance string) string {
		return fmt.Sprintf(`node_load1{instance=%q}`, instance)
	},
	Extract: extractSingle,
}

// DiskUsedPct is percent used on the root ("/") filesystem.
var DiskUsedPct = Metric{
	Name: "disk_used_pct",
	PromQL: func(instance string) string {
		return fmt.Sprintf(`100 * (1 - node_filesystem_avail_bytes{instance=%q,mountpoint="/"} / node_filesystem_size_bytes{instance=%q,mountpoint="/"})`, instance, instance)
	},
	Extract: extractSingle,
}

// All is the full set of metrics the collector polls, in registration order.
var All = []Metric{CPUTemp, CPUUsagePct, MemoryUsedPct, Load1, DiskUsedPct}
