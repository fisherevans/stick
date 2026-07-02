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
	"github.com/fisherevans/stick/internal/semaphore"
	"github.com/fisherevans/stick/internal/session"
)

// Server wires the pool, session manager, and auth into an http.Handler.
type Server struct {
	pool     *semaphore.Pool
	sessions *session.Manager
	auth     *auth.Registry
}

func NewServer(pool *semaphore.Pool, sessions *session.Manager, registry *auth.Registry) *Server {
	return &Server{pool: pool, sessions: sessions, auth: registry}
}

// Handler returns the fully-routed, authenticated handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", s.createSession)
	mux.HandleFunc("POST /v1/sessions/{key}/turns", s.sendTurn)
	mux.HandleFunc("GET /v1/sessions/{key}", s.getSession)
	mux.HandleFunc("DELETE /v1/sessions/{key}", s.deleteSession)
	mux.HandleFunc("GET /v1/pool", s.getPool)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return s.auth.Middleware(mux)
}

// --- wire types (see docs/contract.md) ---

type createReq struct {
	Key                string `json:"key"`
	IdleTimeoutSeconds int    `json:"idle_timeout_seconds,omitempty"`
}

type sessionResp struct {
	Key       string    `json:"key"`
	State     string    `json:"state"` // "active"
	CreatedAt time.Time `json:"created_at"`
}

type turnReq struct {
	Input    string          `json:"input"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
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
	// Explicit create blocks until a stick is warm (returns active). Queue
	// backpressure is surfaced on the streaming turn endpoint, not here.
	sess, _, err := s.sessions.Ensure(r.Context(), consumer, body.Key, s.blockingAcquire(r.Context()))
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

	// Warm fast path: a live session can 409 before we commit to a stream.
	if sess, ok := s.sessions.Get(consumer, key); ok {
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
		s.runTurn(r.Context(), sse, sess, body.Input)
		return
	}

	// Create path: may queue, so stream from the start to carry `queued` frames.
	sse, ok := newSSE(w)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "internal", "streaming unsupported")
		return
	}
	sess, _, err := s.sessions.Ensure(r.Context(), consumer, key, s.streamingAcquire(r.Context(), sse))
	if err != nil {
		_ = sse.event(string(agent.KindError), agent.ErrorData{Code: "internal", Message: "could not acquire a session"})
		return
	}
	if err := sess.BeginTurn(); err != nil {
		_ = sse.event(string(agent.KindError), agent.ErrorData{Code: "turn_in_progress", Message: err.Error()})
		return
	}
	defer sess.EndTurn()
	s.runTurn(r.Context(), sse, sess, body.Input)
}

// runTurn emits turn_started then forwards agent events, with periodic heartbeats.
func (s *Server) runTurn(ctx context.Context, sse *sseWriter, sess *session.Session, input string) {
	turnID := newID("t")
	if err := sse.event("turn_started", turnStartedData{TurnID: turnID, SessionKey: sess.Key}); err != nil {
		return
	}
	ch := sess.Agent().RunTurn(ctx, turnID, input)
	hb := time.NewTicker(15 * time.Second)
	defer hb.Stop()
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return
			}
			if err := sse.event(string(e.Kind), e.Data); err != nil {
				return
			}
		case <-hb.C:
			if err := sse.comment("ping"); err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// blockingAcquire waits for a stick, no streaming (used by explicit create).
func (s *Server) blockingAcquire(_ context.Context) func(context.Context) (*semaphore.Ticket, error) {
	return func(ctx context.Context) (*semaphore.Ticket, error) {
		w := s.pool.Acquire(ctx)
		select {
		case t := <-w.Granted():
			return t, nil
		case <-ctx.Done():
			w.Cancel()
			return nil, ctx.Err()
		}
	}
}

// streamingAcquire waits for a stick, emitting `queued` frames while it waits.
func (s *Server) streamingAcquire(_ context.Context, sse *sseWriter) func(context.Context) (*semaphore.Ticket, error) {
	return func(ctx context.Context) (*semaphore.Ticket, error) {
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
