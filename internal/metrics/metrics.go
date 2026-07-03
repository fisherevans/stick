// Package metrics ships stick's operational and usage metrics to Datadog over
// the agentless v2 series HTTP API. CT 105 runs no Datadog agent, so a Sink
// buffers points in memory and POSTs them on an interval (the same pattern the
// nottingham-cloud cluster-probe uses). A Sink is safe for concurrent use and is
// a no-op when unconfigured, so dev and stub runs need no Datadog key.
//
// Namespace is `stick.*`; every point carries `service:stick` and a `host:` tag.
// Per-turn points are additionally tagged by `consumer`, `model`, and `status`
// so usage and cost break down per consumer.
package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Datadog v2 series metric types.
const (
	typeCount = 1
	typeGauge = 3
)

// Sink buffers metric points and periodically flushes them to Datadog.
type Sink struct {
	apiKey  string
	site    string // e.g. https://api.us5.datadoghq.com
	hostTag string
	log     *slog.Logger
	client  *http.Client
	enabled bool

	mu     sync.Mutex
	buffer []series
}

type series struct {
	Metric    string     `json:"metric"`
	Type      int        `json:"type"`
	Points    []point    `json:"points"`
	Tags      []string   `json:"tags"`
	Resources []resource `json:"resources,omitempty"`
}

type point struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type resource struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

// New returns an enabled Sink. site defaults to Datadog US5 if empty; hostTag
// defaults to "stick".
func New(apiKey, site, hostTag string, log *slog.Logger) *Sink {
	if site == "" {
		site = "https://api.us5.datadoghq.com"
	}
	if hostTag == "" {
		hostTag = "stick"
	}
	return &Sink{
		apiKey: apiKey, site: site, hostTag: hostTag, log: log,
		client:  &http.Client{Timeout: 10 * time.Second},
		enabled: true,
	}
}

// NewDisabled returns a no-op Sink.
func NewDisabled() *Sink { return &Sink{} }

// Enabled reports whether the Sink ships anything.
func (s *Sink) Enabled() bool { return s != nil && s.enabled }

func (s *Sink) add(name string, typ int, v float64, tags []string) {
	if !s.Enabled() {
		return
	}
	all := make([]string, 0, len(tags)+2)
	all = append(all, "service:stick", "host:"+s.hostTag)
	all = append(all, tags...)
	s.mu.Lock()
	s.buffer = append(s.buffer, series{
		Metric:    name,
		Type:      typ,
		Points:    []point{{Timestamp: time.Now().Unix(), Value: v}},
		Tags:      all,
		Resources: []resource{{Type: "host", Name: s.hostTag}},
	})
	s.mu.Unlock()
}

// Count records a monotonic count delta.
func (s *Sink) Count(name string, v float64, tags ...string) { s.add(name, typeCount, v, tags) }

// Gauge records a point-in-time value.
func (s *Sink) Gauge(name string, v float64, tags ...string) { s.add(name, typeGauge, v, tags) }

// Run flushes on interval until ctx is cancelled, then flushes once more. sample
// runs before each flush so the caller can record point-in-time gauges (pool,
// sessions, host); it may be nil. On a disabled Sink, Run just blocks on ctx.
func (s *Sink) Run(ctx context.Context, interval time.Duration, sample func()) {
	if !s.Enabled() {
		<-ctx.Done()
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			if sample != nil {
				sample()
			}
			s.flush(context.Background())
			return
		case <-t.C:
			if sample != nil {
				sample()
			}
			s.flush(ctx)
		}
	}
}

func (s *Sink) flush(ctx context.Context) {
	s.mu.Lock()
	batch := s.buffer
	s.buffer = nil
	s.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	body, err := json.Marshal(map[string]any{"series": batch})
	if err != nil {
		s.log.Error("metrics marshal", "err", err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.site+"/api/v2/series", bytes.NewReader(body))
	if err != nil {
		s.log.Error("metrics request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", s.apiKey)
	resp, err := s.client.Do(req)
	if err != nil {
		// Drop the batch rather than retaining it unbounded; low-value metrics.
		s.log.Error("metrics submit", "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		s.log.Error("metrics submit status", "status", resp.StatusCode)
	}
}
