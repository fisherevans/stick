// Package agent defines the session-bound agent abstraction and its event
// stream. An Agent is one Claude Code session's worth of compute; a Factory
// mints one per stick-holding session. The stub implementation here lets the
// whole platform (auth, semaphore, streaming API) be built and contract-tested
// without the real Claude Code runtime or the cluster LXC behind it.
//
// Events an agent emits during a turn mirror the wire contract (docs/contract.md):
// token, tool_start, tool_end, structured_output, and a terminal turn_completed
// or error. The turn_started frame is emitted by the API layer when the stick is
// acquired, not by the agent.
package agent

import "context"

// Kind is the event discriminator; its string value is the SSE `event:` name.
type Kind string

const (
	KindToken            Kind = "token"
	KindToolStart        Kind = "tool_start"
	KindToolEnd          Kind = "tool_end"
	KindStructuredOutput Kind = "structured_output"
	KindTurnCompleted    Kind = "turn_completed"
	KindError            Kind = "error"
)

// Event is one frame of a turn. Data is marshaled to JSON for the SSE `data:`
// line; use the payload types below.
type Event struct {
	Kind Kind
	Data any
}

// Payloads. Field tags are the wire shape.

type TokenData struct {
	Text string `json:"text"`
}

type ToolStartData struct {
	Tool       string `json:"tool"`
	ToolCallID string `json:"tool_call_id"`
	Title      string `json:"title,omitempty"`
}

type ToolEndData struct {
	ToolCallID string `json:"tool_call_id"`
	Status     string `json:"status"` // "ok" | "error"
	Summary    string `json:"summary,omitempty"`
}

type StructuredOutputData struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

type TurnCompletedData struct {
	TurnID     string `json:"turn_id"`
	StopReason string `json:"stop_reason"`
}

type ErrorData struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Agent runs turns for one session. Implementations are single-turn-at-a-time;
// the session layer serializes calls.
type Agent interface {
	// RunTurn executes one turn and returns a channel of events that closes after
	// a terminal (turn_completed or error) event. The turnID is supplied by the
	// caller so the API and agent agree on it. Cancelling ctx aborts the turn.
	RunTurn(ctx context.Context, turnID, input string) <-chan Event

	// Close tears the agent down (kills the underlying session/process).
	Close() error
}

// Factory mints an Agent for a session. sessionKey and workdir let the real
// implementation give each session its own worktree; the stub ignores them.
type Factory interface {
	NewAgent(ctx context.Context, sessionKey string) (Agent, error)
}
