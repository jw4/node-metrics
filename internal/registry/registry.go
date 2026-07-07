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

// CPUTemp is the first (and currently only) registry entry.
var CPUTemp = Metric{
	Name: "cpu_temp",
	PromQL: func(instance string) string {
		return fmt.Sprintf(`node_hwmon_temp_celsius{instance=%q}`, instance)
	},
	Extract: extractMaxCPUTemp,
}

// All is the full set of metrics the collector polls, in registration order.
var All = []Metric{CPUTemp}
