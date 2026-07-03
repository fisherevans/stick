// Package session manages warm Claude Code sessions keyed by (consumer,
// consumer-supplied key). A session is cheap while idle - the per-turn runtime
// only spawns a process during a turn - so a session does NOT hold a stick for
// its lifetime. The semaphore is acquired per turn by the API layer instead
// (a stick gates a concurrent turn, which is what consumes RAM), which lets many
// warm sessions coexist under a small pool. Sessions serve sequential turns and
// are evicted after an idle timeout or an explicit release.
//
// Creation is serialized per key so two concurrent first-turns don't each spin up
// an agent for the same key.
package session

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/fisherevans/stick/internal/agent"
)

// ErrTurnInProgress is returned when a turn is requested on a session that is
// already streaming one; turns are sequential per session.
var ErrTurnInProgress = errors.New("turn in progress")

// Session is one warm agent bound to a stick.
type Session struct {
	Consumer  string
	Key       string
	CreatedAt time.Time

	agent agent.Agent

	mu       sync.Mutex
	lastUsed time.Time
	turnOn   bool
	closed   bool
}

// Agent returns the session's agent.
func (s *Session) Agent() agent.Agent { return s.agent }

// BeginTurn marks a turn active, rejecting a concurrent one. Pair with EndTurn.
func (s *Session) BeginTurn() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("session closed")
	}
	if s.turnOn {
		return ErrTurnInProgress
	}
	s.turnOn = true
	s.lastUsed = time.Now()
	return nil
}

// EndTurn clears the active-turn flag and refreshes the idle clock.
func (s *Session) EndTurn() {
	s.mu.Lock()
	s.turnOn = false
	s.lastUsed = time.Now()
	s.mu.Unlock()
}

// idleSince reports whether the session has been idle since cutoff (never idle
// while a turn is active).
func (s *Session) idleSince(cutoff time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.turnOn && s.lastUsed.Before(cutoff)
}

// close tears down the agent. Idempotent.
func (s *Session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	_ = s.agent.Close()
}

// Manager owns the live session set and the idle sweeper.
type Manager struct {
	factory     agent.Factory
	idleTimeout time.Duration

	mu       sync.Mutex
	sessions map[string]*Session

	create keyedMutex

	stop chan struct{}
}

// NewManager builds a manager and starts its idle sweeper. Call Close to stop it.
func NewManager(factory agent.Factory, idleTimeout time.Duration) *Manager {
	m := &Manager{
		factory:     factory,
		idleTimeout: idleTimeout,
		sessions:    map[string]*Session{},
		stop:        make(chan struct{}),
	}
	go m.sweep()
	return m
}

func id(consumer, key string) string { return consumer + "\x00" + key }

// Get returns the live session for (consumer, key), if any.
func (m *Manager) Get(consumer, key string) (*Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id(consumer, key)]
	return s, ok
}

// Ensure returns the live session for (consumer, key), creating one if absent.
// Creating a session is cheap (no stick, no process); the stick is acquired
// per turn by the caller. tools are the consumer-declared output tools bound to
// the session at creation; they are ignored if the session already exists (tools
// are fixed for a warm session's life). The bool reports whether one was created.
func (m *Manager) Ensure(ctx context.Context, consumer, key string, tools []agent.Tool) (*Session, bool, error) {
	sid := id(consumer, key)

	if s, ok := m.Get(consumer, key); ok {
		return s, false, nil
	}

	// Serialize creators for this key.
	m.create.Lock(sid)
	defer m.create.Unlock(sid)

	if s, ok := m.Get(consumer, key); ok {
		return s, false, nil
	}

	ag, err := m.factory.NewAgent(ctx, consumer, key, tools)
	if err != nil {
		return nil, false, err
	}
	now := time.Now()
	s := &Session{
		Consumer: consumer, Key: key, CreatedAt: now,
		agent: ag, lastUsed: now,
	}
	m.mu.Lock()
	m.sessions[sid] = s
	m.mu.Unlock()
	return s, true, nil
}

// Delete evicts and closes the session for (consumer, key). Idempotent; reports
// whether a session was present.
func (m *Manager) Delete(consumer, key string) bool {
	sid := id(consumer, key)
	m.mu.Lock()
	s, ok := m.sessions[sid]
	if ok {
		delete(m.sessions, sid)
	}
	m.mu.Unlock()
	if ok {
		s.close()
	}
	return ok
}

// Count returns the number of live sessions.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// Close stops the sweeper and closes all sessions.
func (m *Manager) Close() {
	close(m.stop)
	m.mu.Lock()
	all := make([]*Session, 0, len(m.sessions))
	for sid, s := range m.sessions {
		all = append(all, s)
		delete(m.sessions, sid)
	}
	m.mu.Unlock()
	for _, s := range all {
		s.close()
	}
}

func (m *Manager) sweep() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			cutoff := time.Now().Add(-m.idleTimeout)
			m.mu.Lock()
			var evict []*Session
			for sid, s := range m.sessions {
				if s.idleSince(cutoff) {
					evict = append(evict, s)
					delete(m.sessions, sid)
				}
			}
			m.mu.Unlock()
			for _, s := range evict {
				s.close()
			}
		}
	}
}

// keyedMutex is a per-key mutex with refcounted, self-pruning entries.
type keyedMutex struct {
	mu sync.Mutex
	m  map[string]*kmEntry
}

type kmEntry struct {
	mu   sync.Mutex
	refs int
}

func (k *keyedMutex) Lock(key string) {
	k.mu.Lock()
	if k.m == nil {
		k.m = map[string]*kmEntry{}
	}
	e := k.m[key]
	if e == nil {
		e = &kmEntry{}
		k.m[key] = e
	}
	e.refs++
	k.mu.Unlock()
	e.mu.Lock()
}

func (k *keyedMutex) Unlock(key string) {
	k.mu.Lock()
	e := k.m[key]
	e.mu.Unlock()
	e.refs--
	if e.refs == 0 {
		delete(k.m, key)
	}
	k.mu.Unlock()
}
