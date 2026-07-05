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

import (
	"context"
	"encoding/json"
)

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
	Usage      *Usage `json:"usage,omitempty"`
}

// Usage is the resource accounting for a completed turn. It is surfaced to the
// caller on turn_completed and mirrored into metrics. Token counts always
// populate; CostUSD is Anthropic's list-price estimate the CLI reports - nonzero
// even under a Max subscription token (where there is no per-request charge), and
// only real spend under API-key billing. So treat tokens as the durable signal
// and CostUSD as an estimate until/unless stick moves to an API key.
type Usage struct {
	Model                    string  `json:"model,omitempty"`
	InputTokens              int64   `json:"input_tokens"`
	OutputTokens             int64   `json:"output_tokens"`
	CacheReadInputTokens     int64   `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int64   `json:"cache_creation_input_tokens"`
	CostUSD                  float64 `json:"cost_usd"`
	DurationMS               int64   `json:"duration_ms"`
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

// Factory mints an Agent for a session. consumer + sessionKey let the real
// implementation place the session in the right environment (see Profile); cfg is
// the caller-supplied session configuration bound for the life of the session
// (tools, persona, model, tool policy, seed). The stub ignores all of it.
type Factory interface {
	NewAgent(ctx context.Context, consumer, sessionKey string, cfg SessionConfig) (Agent, error)
}

// SessionConfig is the caller-supplied configuration bound to a session when it is
// created. It is set once (on the create/first-turn request), remembered by the
// session manager, and re-applied verbatim if an idle-evicted session is
// transparently recreated - so a consumer's persona, tools, and seed survive a
// rehydrate without the consumer detecting the eviction.
type SessionConfig struct {
	// Tools are the consumer-declared output tools the session exposes (see Tool).
	Tools []Tool
	// SystemPrompt replaces the agent's default system prompt for the session
	// (claude --system-prompt), so the LLM runs as the consumer's persona rather
	// than as a generic coding agent. Empty keeps the default.
	SystemPrompt string
	// Model overrides the factory's default model for this session (claude
	// --model). Empty uses the factory default.
	Model string
	// AllowTools / DenyTools are the session's tool policy (claude --allowedTools /
	// --disallowedTools), e.g. DenyTools ["ReportFindings","ToolSearch"] to drop
	// noise tools while keeping useful ones like Bash/WebSearch. A denylist is used
	// rather than an allowlist because an allowlist (claude --tools) filters the set
	// in a way that drops the consumer's MCP output tools on resume turns; a
	// denylist leaves every tool - including output tools - and just removes the
	// named ones.
	AllowTools []string
	DenyTools  []string
	// Seed is optional grounding context prepended to the session's first turn
	// (e.g. reference material). Because it is remembered with the config, it is
	// re-applied when an evicted session is recreated, so the grounding survives.
	Seed string
}

// IsZero reports whether the config carries nothing to bind - i.e. a turn that
// supplied no session config, so the session manager should keep whatever it
// already remembered rather than overwrite it.
func (c SessionConfig) IsZero() bool {
	return len(c.Tools) == 0 && c.SystemPrompt == "" && c.Model == "" &&
		len(c.AllowTools) == 0 && len(c.DenyTools) == 0 && c.Seed == ""
}

// Tool is a consumer-declared tool the agent can call during a turn. Today only
// "output" tools exist: the agent calls the tool with a structured argument, and
// stick surfaces that argument to the consumer as a structured_output frame
// (named OutputName). This is the reliable, schema-validated path for structured
// results - the runtime validates the call against InputSchema, so it holds up
// for multi-step workflows where a trailing text block would not. Tools are
// exposed to the Claude Code session over MCP (see internal/mcp).
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
	// OutputName is the structured_output `name` stick emits when the agent calls
	// this tool; defaults to Name. Lets a consumer name a tool `emit_node` but
	// receive a structured_output named `node`.
	OutputName string `json:"output_name,omitempty"`
	// Required marks an output tool the turn MUST produce. If the agent finishes a
	// turn without having called a required tool, stick issues a bounded repair
	// turn asking for it, guaranteeing the consumer a structured_output frame (or
	// an explicit error). See the agent's turn loop.
	Required bool `json:"required,omitempty"`
}

// Profile is a per-consumer session environment. A consumer with a profile runs
// its sessions in a configured working directory (e.g. one with its data mounted
// and its context/binaries pre-seeded) instead of a bare scratch dir, so
// filesystem-oriented agent workflows (like ramble's composer) run unchanged.
type Profile struct {
	// Workdir is the base directory the consumer's sessions run in. Empty falls
	// back to the factory's per-consumer scratch dir.
	Workdir string `json:"workdir"`
	// AddDirs are extra directories to grant the agent tool access to
	// (claude --add-dir), e.g. an NFS data mount outside the workdir.
	AddDirs []string `json:"add_dirs"`
	// SharedWorkdir runs every session for this consumer directly in Workdir
	// (not a per-key subdir). Suitable when the consumer's data is addressed
	// inside the turn (ramble keys artifacts by project id) and turns are
	// serialized by the pool. Per-session conversation continuity still holds
	// via the session's own resume id.
	SharedWorkdir bool `json:"shared_workdir"`
}
