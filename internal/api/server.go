// Package api exposes the stick contract over HTTP: provisioned-secret auth, the
// SSE streaming turn endpoint, and session/pool management. See docs/contract.md.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/fisherevans/stick/internal/agent"
	"github.com/fisherevans/stick/internal/auth"
	"github.com/fisherevans/stick/internal/metrics"
	"github.com/fisherevans/stick/internal/semaphore"
	"github.com/fisherevans/stick/internal/session"
)

// Server wires the pool, session manager, and auth into an http.Handler.
type Server struct {
	pool     *semaphore.Pool
	sessions *session.Manager
	auth     *auth.Registry
	metrics  *metrics.Sink
}

func NewServer(pool *semaphore.Pool, sessions *session.Manager, registry *auth.Registry, sink *metrics.Sink) *Server {
	if sink == nil {
		sink = metrics.NewDisabled()
	}
	return &Server{pool: pool, sessions: sessions, auth: registry, metrics: sink}
}

// Handler returns the fully-routed handler. The /v1 API is authenticated; the
// health endpoint is open so probes don't need a secret.
func (s *Server) Handler() http.Handler {
	api := http.NewServeMux()
	api.HandleFunc("POST /v1/sessions", s.createSession)
	api.HandleFunc("POST /v1/sessions/{key}/turns", s.sendTurn)
	api.HandleFunc("GET /v1/sessions/{key}", s.getSession)
	api.HandleFunc("DELETE /v1/sessions/{key}", s.deleteSession)
	api.HandleFunc("GET /v1/pool", s.getPool)

	mux := http.NewServeMux()
	mux.Handle("/v1/", s.auth.Middleware(api))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return mux
}

// --- wire types (see docs/contract.md) ---

// sessionConfigReq is the caller-supplied session configuration shared by the
// create and turn requests. It is bound when a session is created and remembered
// across idle eviction (see session.Manager.Ensure).
type sessionConfigReq struct {
	Tools           []agent.Tool `json:"tools,omitempty"`
	SystemPrompt    string       `json:"system_prompt,omitempty"`
	Model           string       `json:"model,omitempty"`
	AllowedTools    []string     `json:"allowed_tools,omitempty"`
	DisallowedTools []string     `json:"disallowed_tools,omitempty"`
	Seed            string       `json:"seed,omitempty"`
}

func (r sessionConfigReq) toConfig() agent.SessionConfig {
	return agent.SessionConfig{
		Tools:        r.Tools,
		SystemPrompt: r.SystemPrompt,
		Model:        r.Model,
		AllowTools:   r.AllowedTools,
		DenyTools:    r.DisallowedTools,
		Seed:         r.Seed,
	}
}

type createReq struct {
	Key string `json:"key"`
	sessionConfigReq
}

type sessionResp struct {
	Key       string    `json:"key"`
	State     string    `json:"state"` // "active"
	CreatedAt time.Time `json:"created_at"`
}

type turnReq struct {
	Input string `json:"input"`
	sessionConfigReq
}

type turnStartedData struct {
	TurnID     string `json:"turn_id"`
	SessionKey string `json:"session_key"`
}

type queuedData struct {
	QueuePosition int `json:"queue_position"`
}

type poolResp struct {
	SticksTotal int `json:"sticks_total"`
	SticksInUse int `json:"sticks_in_use"`
	QueueDepth  int `json:"queue_depth"`
}

// --- handlers ---

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	consumer, _ := auth.ConsumerFrom(r.Context())
	var body createReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Key == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "missing or invalid session key")
		return
	}
	// Creating a session is cheap (no stick held until a turn runs), so this
	// returns immediately. Queue backpressure surfaces on the turn stream.
	sess, _, err := s.sessions.Ensure(r.Context(), consumer, body.Key, body.toConfig())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "could not create session")
		return
	}
	writeJSON(w, http.StatusOK, sessionResp{Key: sess.Key, State: "active", CreatedAt: sess.CreatedAt})
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	consumer, _ := auth.ConsumerFrom(r.Context())
	sess, ok := s.sessions.Get(consumer, r.PathValue("key"))
	if !ok {
		writeErr(w, http.StatusNotFound, "no_such_session", "no live session for that key")
		return
	}
	writeJSON(w, http.StatusOK, sessionResp{Key: sess.Key, State: "active", CreatedAt: sess.CreatedAt})
}

func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) {
	consumer, _ := auth.ConsumerFrom(r.Context())
	s.sessions.Delete(consumer, r.PathValue("key")) // idempotent
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getPool(w http.ResponseWriter, _ *http.Request) {
	st := s.pool.Stats()
	writeJSON(w, http.StatusOK, poolResp{SticksTotal: st.Total, SticksInUse: st.InUse, QueueDepth: st.QueueDepth})
}

func (s *Server) sendTurn(w http.ResponseWriter, r *http.Request) {
	consumer, _ := auth.ConsumerFrom(r.Context())
	key := r.PathValue("key")

	var body turnReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Input == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "missing or invalid turn input")
		return
	}

	// Get or create the warm session (cheap: no stick until a turn runs).
	sess, _, err := s.sessions.Ensure(r.Context(), consumer, key, body.toConfig())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "could not create session")
		return
	}

	// Reserve the (sequential) turn slot before committing to a stream, so a
	// busy session 409s cleanly rather than mid-stream.
	if err := sess.BeginTurn(); err != nil {
		if errors.Is(err, session.ErrTurnInProgress) {
			writeErr(w, http.StatusConflict, "turn_in_progress", "a turn is already streaming for this session")
			return
		}
		writeErr(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer sess.EndTurn()

	sse, ok := newSSE(w)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "internal", "streaming unsupported")
		return
	}

	// Acquire a stick for the duration of this turn, streaming `queued` frames
	// while all sticks are busy. Released as soon as the turn ends.
	qStart := time.Now()
	ticket, err := s.acquireStreaming(r.Context(), sse)
	if err != nil {
		return // ctx cancelled; consumer went away
	}
	defer ticket.Release()
	queueWait := time.Since(qStart)

	start := time.Now()
	status, usage := s.runTurn(r.Context(), sse, sess, body.Input)
	s.recordTurn(consumer, sess.Agent(), status, usage, queueWait, time.Since(start))
}

// runTurn emits turn_started then forwards agent events, with periodic
// heartbeats. It returns the terminal status ("ok" | "error" | "aborted") and
// the turn's usage (nil if none was reported), for metrics.
func (s *Server) runTurn(ctx context.Context, sse *sseWriter, sess *session.Session, input string) (string, *agent.Usage) {
	turnID := newID("t")
	if err := sse.event("turn_started", turnStartedData{TurnID: turnID, SessionKey: sess.Key}); err != nil {
		return "aborted", nil
	}
	ch := sess.Agent().RunTurn(ctx, turnID, input)
	hb := time.NewTicker(15 * time.Second)
	defer hb.Stop()
	status := "aborted"
	var usage *agent.Usage
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return status, usage
			}
			switch e.Kind {
			case agent.KindTurnCompleted:
				status = "ok"
				if d, ok := e.Data.(agent.TurnCompletedData); ok {
					usage = d.Usage
				}
			case agent.KindError:
				status = "error"
			}
			if err := sse.event(string(e.Kind), e.Data); err != nil {
				return status, usage
			}
		case <-hb.C:
			if err := sse.comment("ping"); err != nil {
				return status, usage
			}
		case <-ctx.Done():
			return status, usage
		}
	}
}

// recordTurn ships per-turn usage, cost, timing, and resource metrics, tagged by
// consumer and model so usage breaks down per consumer.
func (s *Server) recordTurn(consumer string, ag agent.Agent, status string, u *agent.Usage, queueWait, dur time.Duration) {
	if !s.metrics.Enabled() {
		return
	}
	model := "unknown"
	if u != nil && u.Model != "" {
		model = u.Model
	}
	cm := []string{"consumer:" + consumer, "model:" + model}
	s.metrics.Count("stick.turn.count", 1, append(cm, "status:"+status)...)
	s.metrics.Gauge("stick.turn.duration_ms", float64(dur.Milliseconds()), cm...)
	s.metrics.Gauge("stick.turn.queue_wait_ms", float64(queueWait.Milliseconds()), "consumer:"+consumer)
	if u != nil {
		s.metrics.Count("stick.turn.tokens.input", float64(u.InputTokens), cm...)
		s.metrics.Count("stick.turn.tokens.output", float64(u.OutputTokens), cm...)
		s.metrics.Count("stick.turn.tokens.cache_read", float64(u.CacheReadInputTokens), cm...)
		s.metrics.Count("stick.turn.tokens.cache_write", float64(u.CacheCreationInputTokens), cm...)
		s.metrics.Count("stick.turn.cost_usd", u.CostUSD, cm...)
	}
	if rp, ok := ag.(interface{ LastMaxRSSKB() int64 }); ok {
		if kb := rp.LastMaxRSSKB(); kb > 0 {
			s.metrics.Gauge("stick.turn.max_rss_kb", float64(kb), "consumer:"+consumer)
		}
	}
}

// acquireStreaming waits for a stick, emitting `queued` frames while all sticks
// are busy, and returns the granted Ticket. Errors only if ctx is cancelled.
func (s *Server) acquireStreaming(ctx context.Context, sse *sseWriter) (*semaphore.Ticket, error) {
	w := s.pool.Acquire(ctx)
	if pos := w.Position(); pos > 0 {
		_ = sse.event("queued", queuedData{QueuePosition: pos})
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case tk := <-w.Granted():
			return tk, nil
		case <-t.C:
			if pos := w.Position(); pos > 0 {
				_ = sse.event("queued", queuedData{QueuePosition: pos})
			}
		case <-ctx.Done():
			w.Cancel()
			return nil, ctx.Err()
		}
	}
}

func newID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"code": code, "message": msg})
}
