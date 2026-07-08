// internal/tuiapp/model_test.go
package tuiapp

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/jw4/node-metrics/internal/tuinats"
)

// fakeStreamer lets tests drive Model without a real NATS connection.
type fakeStreamer struct {
	streams map[string]chan tuinats.Point // pre-populated per host by the test
	calls   []string                      // hosts StreamHost was called for, in order
	cancels []string                      // hosts whose cancel func was invoked, in order
}

func (f *fakeStreamer) StreamHost(_ context.Context, _, host string, _ time.Duration) (<-chan tuinats.Point, func(), error) {
	f.calls = append(f.calls, host)
	ch, ok := f.streams[host]
	if !ok {
		ch = make(chan tuinats.Point, 8)
		f.streams[host] = ch
	}
	cancel := func() { f.cancels = append(f.cancels, host) }
	return ch, cancel, nil
}

// runCmd executes a tea.Cmd synchronously and returns its Msg (or nil if the
// Cmd itself was nil) -- bubbletea Cmds are just func() tea.Msg, so tests
// don't need teatest or a running tea.Program to exercise them.
func runCmd(cmd tea.Cmd) tea.Msg {
	if cmd == nil {
		return nil
	}
	return cmd()
}

func TestModel_InitStartsStreamingFirstHost(t *testing.T) {
	fs := &fakeStreamer{streams: map[string]chan tuinats.Point{}}
	m := New(context.Background(), fs, "cpu_temp", []string{"belfalas", "r710"}, time.Hour)

	cmd := m.Init()
	if len(fs.calls) != 1 || fs.calls[0] != "belfalas" {
		t.Fatalf("expected StreamHost(belfalas) on Init, got calls=%v", fs.calls)
	}

	fs.streams["belfalas"] <- tuinats.Point{Value: 65}
	msg := runCmd(cmd)
	pm, ok := msg.(pointMsg)
	if !ok || pm.value != 65 || pm.host != "belfalas" {
		t.Fatalf("expected pointMsg{belfalas,65}, got %#v", msg)
	}
}

func TestModel_HostSwitchDebouncesThenCancelsPreviousStream(t *testing.T) {
	fs := &fakeStreamer{streams: map[string]chan tuinats.Point{}}
	m := New(context.Background(), fs, "cpu_temp", []string{"belfalas", "r710"}, time.Hour)
	m.Init()
	fs.calls = nil // reset; only interested in what happens after the switch

	_, tickCmd := m.Update(tea.KeyMsg{Type: tea.KeyRight})
	if len(fs.cancels) != 0 {
		t.Fatalf("expected no cancel before the debounce tick fires, got %v", fs.cancels)
	}

	tickMsg := runCmd(tickCmd)
	updated, switchCmd := m.Update(tickMsg)
	next := updated.(*Model)
	if next.activeIdx != 1 {
		t.Fatalf("activeIdx = %d, want 1", next.activeIdx)
	}
	if len(fs.cancels) != 1 || fs.cancels[0] != "belfalas" {
		t.Fatalf("expected belfalas's stream cancelled on switch, got %v", fs.cancels)
	}
	if len(fs.calls) != 1 || fs.calls[0] != "r710" {
		t.Fatalf("expected StreamHost(r710) started on switch, got %v", fs.calls)
	}
	if switchCmd == nil {
		t.Fatal("expected a Cmd to start listening on r710's stream")
	}
}

func TestModel_StaleDebounceTickIsIgnoredAfterASwitchAlreadyApplied(t *testing.T) {
	fs := &fakeStreamer{streams: map[string]chan tuinats.Point{}}
	m := New(context.Background(), fs, "cpu_temp", []string{"belfalas", "r710", "nkul"}, time.Hour)
	m.Init()

	_, tick1 := m.Update(tea.KeyMsg{Type: tea.KeyRight}) // schedules -> r710
	_, tick2 := m.Update(tea.KeyMsg{Type: tea.KeyRight}) // reschedules -> nkul, same debounceActive flag

	// First tick fires: applies the switch to nkul (the latest pending index).
	updated, _ := m.Update(runCmd(tick1))
	m = updated.(*Model)
	if m.activeIdx != 2 {
		t.Fatalf("activeIdx after first tick = %d, want 2 (nkul)", m.activeIdx)
	}

	// Second (stale) tick must be a no-op -- must not switch again or panic.
	updated, cmd := m.Update(runCmd(tick2))
	m = updated.(*Model)
	if m.activeIdx != 2 {
		t.Fatalf("stale tick changed activeIdx to %d, want unchanged 2", m.activeIdx)
	}
	if cmd != nil {
		t.Fatal("stale debounce tick should not issue another Cmd")
	}
}

func TestModel_NetZeroSwitchDoesNotReconnectSameHost(t *testing.T) {
	fs := &fakeStreamer{streams: map[string]chan tuinats.Point{}}
	m := New(context.Background(), fs, "cpu_temp", []string{"belfalas"}, time.Hour)
	m.Init()
	fs.calls = nil // reset; only interested in what happens after the switch

	_, tickCmd := m.Update(tea.KeyMsg{Type: tea.KeyRight}) // wraps back to index 0 (only one host)
	updated, cmd := m.Update(runCmd(tickCmd))
	next := updated.(*Model)
	if next.activeIdx != 0 {
		t.Fatalf("activeIdx = %d, want 0 (unchanged)", next.activeIdx)
	}
	if len(fs.calls) != 0 {
		t.Fatalf("expected no StreamHost call for a net-zero host switch, got %v", fs.calls)
	}
	if len(fs.cancels) != 0 {
		t.Fatalf("expected no cancel for a net-zero host switch, got %v", fs.cancels)
	}
	if cmd != nil {
		t.Fatal("expected no Cmd for a net-zero host switch")
	}
}

func TestModel_FullCycleNetZeroSwitchDoesNotReconnect(t *testing.T) {
	fs := &fakeStreamer{streams: map[string]chan tuinats.Point{}}
	hosts := []string{"belfalas", "r710", "nkul"}
	m := New(context.Background(), fs, "cpu_temp", hosts, time.Hour)
	m.Init()
	fs.calls = nil // reset; only interested in what happens after the switch

	var tickCmd tea.Cmd
	for range hosts {
		_, tickCmd = m.Update(tea.KeyMsg{Type: tea.KeyRight}) // each reschedules; final press lands back on start
	}
	updated, cmd := m.Update(runCmd(tickCmd))
	next := updated.(*Model)
	if next.activeIdx != 0 {
		t.Fatalf("activeIdx = %d, want 0 (unchanged after full cycle)", next.activeIdx)
	}
	if len(fs.calls) != 0 {
		t.Fatalf("expected no StreamHost call for a full-cycle net-zero switch, got %v", fs.calls)
	}
	if len(fs.cancels) != 0 {
		t.Fatalf("expected no cancel for a full-cycle net-zero switch, got %v", fs.cancels)
	}
	if cmd != nil {
		t.Fatal("expected no Cmd for a full-cycle net-zero switch")
	}
}

func TestModel_PointMsgAppendsAndReArmsListening(t *testing.T) {
	fs := &fakeStreamer{streams: map[string]chan tuinats.Point{}}
	m := New(context.Background(), fs, "cpu_temp", []string{"belfalas"}, time.Hour)
	m.Init()

	updated, rearmCmd := m.Update(pointMsg{host: "belfalas", value: 71})
	next := updated.(*Model)
	if got := next.points["belfalas"]; len(got) != 1 || got[0] != 71 {
		t.Fatalf("points[belfalas] = %v, want [71]", got)
	}
	if rearmCmd == nil {
		t.Fatal("expected Update to re-arm listening on the same channel")
	}
}

func TestModel_RendersLatestValueInTitle(t *testing.T) {
	fs := &fakeStreamer{streams: map[string]chan tuinats.Point{}}
	m := New(context.Background(), fs, "cpu_temp", []string{"belfalas"}, time.Hour)
	m.points["belfalas"] = []float64{65, 71}

	view := m.View()
	if !strings.Contains(view, "belfalas") || !strings.Contains(view, "71") {
		t.Fatalf("view missing host name or latest value: %q", view)
	}
}

func TestModel_RendersMetricNameInTitle(t *testing.T) {
	fs := &fakeStreamer{streams: map[string]chan tuinats.Point{}}
	m := New(context.Background(), fs, "disk_used_pct", []string{"belfalas"}, time.Hour)
	m.points["belfalas"] = []float64{24.66}

	view := m.View()
	if !strings.Contains(view, "disk_used_pct") {
		t.Fatalf("view missing metric name: %q", view)
	}
}

func TestModel_WindowSizeMsgUpdatesDimensions(t *testing.T) {
	fs := &fakeStreamer{streams: map[string]chan tuinats.Point{}}
	m := New(context.Background(), fs, "cpu_temp", []string{"belfalas"}, time.Hour)

	updated, cmd := m.Update(tea.WindowSizeMsg{Width: 40, Height: 8})
	next := updated.(*Model)
	if next.width != 40 || next.height != 8 {
		t.Fatalf("width/height = %d/%d, want 40/8", next.width, next.height)
	}
	if cmd != nil {
		t.Fatal("expected no Cmd from a WindowSizeMsg")
	}
}

func TestGraphHeight(t *testing.T) {
	tests := []struct {
		name       string
		paneHeight int
		want       int
	}{
		{"unknown (zero) falls back to default", 0, 10},
		{"tall pane uses height minus the header line and asciigraph's own +1 row", 24, 22},
		{"short pane clamps to the minimum", 3, 3},
		{"very short pane clamps to the minimum", 1, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := graphHeight(tt.paneHeight); got != tt.want {
				t.Fatalf("graphHeight(%d) = %d, want %d", tt.paneHeight, got, tt.want)
			}
		})
	}
}

func TestGraphWidth(t *testing.T) {
	tests := []struct {
		name      string
		paneWidth int
		want      int
	}{
		{"unknown (zero) means let asciigraph choose", 0, 0},
		{"wide pane uses width minus the label gutter", 80, 70},
		{"narrow pane clamps to the minimum", 20, 20},
		{"very narrow pane clamps to the minimum", 5, 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := graphWidth(tt.paneWidth); got != tt.want {
				t.Fatalf("graphWidth(%d) = %d, want %d", tt.paneWidth, got, tt.want)
			}
		})
	}
}
