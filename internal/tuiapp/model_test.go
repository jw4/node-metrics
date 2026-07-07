// internal/tuiapp/model_test.go
package tuiapp

import (
	"context"
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
	if !contains(view, "belfalas") || !contains(view, "71") {
		t.Fatalf("view missing host name or latest value: %q", view)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
