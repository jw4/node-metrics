// internal/tuiapp/model.go
// Package tuiapp is the bubbletea app: a tab per host, asciigraph rendering
// of the active host's points, a debounced host-switch so rapid tabbing
// fires one backfill cycle rather than one per keypress, and the Cmd-based
// wiring that keeps exactly one host's stream live at a time.
package tuiapp

import (
	"context"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/guptarohit/asciigraph"

	"github.com/jw4/node-metrics/internal/tuinats"
)

const (
	debounceDelay  = 250 * time.Millisecond
	maxPointsShown = 240 // oldest points scroll off past this to bound render width/memory
)

// hostStreamer is the subset of *tuinats.Client this package depends on --
// small enough that tests supply a fake instead of a real NATS connection.
type hostStreamer interface {
	StreamHost(ctx context.Context, metric, host string, window time.Duration) (<-chan tuinats.Point, func(), error)
}

type pointMsg struct {
	host  string
	value float64
}

type streamEndedMsg struct{ host string }

type debounceFiredMsg struct{}

type Model struct {
	ctx    context.Context
	client hostStreamer
	metric string
	window time.Duration

	hosts     []string
	activeIdx int
	points    map[string][]float64

	activeChan   <-chan tuinats.Point
	activeCancel func()

	pendingIdx     int
	debounceActive bool

	connected bool

	// width/height are the pane's real dimensions, from the most recent
	// tea.WindowSizeMsg (bubbletea delivers one automatically on start, and
	// again on every resize -- e.g. tmux rebuilding its layout as panes are
	// added). Zero until the first message arrives.
	width, height int
}

func New(ctx context.Context, client hostStreamer, metric string, hosts []string, window time.Duration) *Model {
	return &Model{
		ctx:       ctx,
		client:    client,
		metric:    metric,
		window:    window,
		hosts:     hosts,
		points:    map[string][]float64{},
		connected: true,
	}
}

// Init starts streaming the first host -- there is always at least one
// (cmd/tui/main.go checks len(hosts) > 0 before constructing the Model).
func (m *Model) Init() tea.Cmd {
	return m.activateHost(m.hosts[m.activeIdx])
}

// activateHost cancels the previously-active stream (if any), starts a new
// one for host, and returns a Cmd that begins listening on it. Exactly one
// host's stream is ever live -- this is what "tears down the old live
// subscription" (spec, Component: TUI, point 4) means in code.
func (m *Model) activateHost(host string) tea.Cmd {
	if m.activeCancel != nil {
		m.activeCancel()
	}
	ch, cancel, err := m.client.StreamHost(m.ctx, m.metric, host, m.window)
	if err != nil {
		m.connected = false
		return nil
	}
	m.connected = true
	m.activeChan = ch
	m.activeCancel = cancel
	return waitForPoint(host, ch)
}

// waitForPoint returns a Cmd that blocks on ch for exactly one message. The
// bubbletea idiom for a streaming channel is to re-issue this Cmd every time
// a pointMsg for the still-active host is handled (see Update) -- that's the
// "listen loop".
func waitForPoint(host string, ch <-chan tuinats.Point) tea.Cmd {
	return func() tea.Msg {
		p, ok := <-ch
		if !ok {
			return streamEndedMsg{host: host}
		}
		return pointMsg{host: host, value: p.Value}
	}
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyRight, tea.KeyTab:
			return m, m.scheduleSwitch(m.pendingBase() + 1)
		case tea.KeyLeft, tea.KeyShiftTab:
			return m, m.scheduleSwitch(m.pendingBase() - 1)
		case tea.KeyCtrlC:
			return m, tea.Quit
		}

	case debounceFiredMsg:
		if !m.debounceActive {
			return m, nil // stale tick from an earlier keypress -- ignore
		}
		m.debounceActive = false
		if m.pendingIdx == m.activeIdx {
			return m, nil // net-zero switch -- no-op, avoid reconnecting the already-active host
		}
		m.activeIdx = m.pendingIdx
		return m, m.activateHost(m.hosts[m.activeIdx])

	case pointMsg:
		if msg.host != m.currentHost() {
			return m, nil // point from a host we've since switched away from
		}
		series := append(m.points[msg.host], msg.value)
		if len(series) > maxPointsShown {
			series = series[len(series)-maxPointsShown:]
		}
		m.points[msg.host] = series
		return m, waitForPoint(msg.host, m.activeChan)

	case streamEndedMsg:
		if msg.host != m.currentHost() {
			return m, nil
		}
		m.connected = false
		return m, m.activateHost(msg.host)
	}
	return m, nil
}

// pendingBase returns the index a new keypress should advance from: the
// still-pending index if a debounce is already in flight (so repeated
// tabbing keeps advancing from where the pending switch would land), or the
// currently-active index otherwise.
func (m *Model) pendingBase() int {
	if m.debounceActive {
		return m.pendingIdx
	}
	return m.activeIdx
}

func (m *Model) currentHost() string {
	if len(m.hosts) == 0 {
		return ""
	}
	return m.hosts[m.activeIdx]
}

// scheduleSwitch records the pending host index and returns a Cmd that fires
// debounceFiredMsg after debounceDelay. Pressing right/left again before the
// tick fires just overwrites pendingIdx and issues another tick Cmd; only the
// first tick to actually arrive applies a switch (debounceActive guards it),
// so rapid tabbing collapses to exactly one activateHost call.
func (m *Model) scheduleSwitch(idx int) tea.Cmd {
	if len(m.hosts) == 0 {
		return nil
	}
	idx = ((idx % len(m.hosts)) + len(m.hosts)) % len(m.hosts)
	m.pendingIdx = idx
	m.debounceActive = true
	return tea.Tick(debounceDelay, func(time.Time) tea.Msg { return debounceFiredMsg{} })
}

func (m *Model) View() string {
	host := m.currentHost()
	if host == "" {
		return "no hosts reporting yet"
	}
	series := m.points[host]
	latest := "n/a"
	if len(series) > 0 {
		latest = fmt.Sprintf("%.1f", series[len(series)-1])
	}
	graph := ""
	if len(series) > 0 {
		opts := []asciigraph.Option{asciigraph.Height(graphHeight(m.height))}
		if w := graphWidth(m.width); w > 0 {
			opts = append(opts, asciigraph.Width(w))
		}
		graph = asciigraph.Plot(series, opts...)
	}
	status := "connected"
	if !m.connected {
		status = "reconnecting..."
	}
	return fmt.Sprintf("%s  |  %s  |  %s  |  latest: %s\n%s", m.metric, host, status, latest, graph)
}

// graphHeight sizes the plot to the pane's real height (reserving one line
// for the status header above it), so a short pane doesn't overflow into its
// tmux neighbors and a tall one isn't left with wasted empty space below a
// fixed-size graph. Falls back to a fixed default before the first
// tea.WindowSizeMsg arrives (only ever matters for the very first render).
func graphHeight(paneHeight int) int {
	const (
		defaultHeight = 10
		// asciigraph.Height(h) renders h+1 lines (confirmed against its
		// source: the plot grid is allocated rows+1 tall), so reserving
		// just 1 line for the header isn't enough -- it undercounts by
		// exactly that +1, which pushes the header off the top of a tight
		// pane instead of leaving it visible above the graph.
		reserved  = 2 // status header line + asciigraph's own extra row
		minHeight = 3
	)
	if paneHeight <= 0 {
		return defaultHeight
	}
	if h := paneHeight - reserved; h >= minHeight {
		return h
	}
	return minHeight
}

// graphWidth sizes the plot to the pane's real width, reserving room for
// asciigraph's y-axis label gutter, so the graph doesn't grow past
// maxPointsShown columns and wrap awkwardly in a narrow pane. Returns 0
// (asciigraph's own default: one column per data point, uncapped) before the
// first tea.WindowSizeMsg arrives.
func graphWidth(paneWidth int) int {
	const (
		gutter   = 10
		minWidth = 20
	)
	if paneWidth <= 0 {
		return 0
	}
	if w := paneWidth - gutter; w >= minWidth {
		return w
	}
	return minWidth
}
