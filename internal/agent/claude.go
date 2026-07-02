package agent

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

// ClaudeFactory mints ClaudeAgents backed by the real Claude Code CLI. Each
// session gets its own working directory and a stable session UUID; turns run as
// `claude -p` subprocesses (first turn assigns the UUID, later turns --resume it),
// which keeps idle warm sessions off the RAM budget - a session holds a stick,
// but a claude process only exists while a turn is streaming. That matters on the
// 1 GB LXC (nottingham-cloud#121, #126).
//
// Auth is the Max OAuth token in the environment (CLAUDE_CODE_OAUTH_TOKEN),
// materialized by containers/stick/bootstrap.sh; no per-token API cost.
type ClaudeFactory struct {
	SessionsDir string // base dir for per-session workdirs
	Model       string // optional model alias (e.g. "opus"); "" = CLI default
}

// NewClaudeFactory builds a factory. sessionsDir is created if missing.
func NewClaudeFactory(sessionsDir, model string) (*ClaudeFactory, error) {
	if sessionsDir == "" {
		sessionsDir = "/opt/stick/sessions"
	}
	if err := os.MkdirAll(sessionsDir, 0o700); err != nil {
		return nil, fmt.Errorf("create sessions dir: %w", err)
	}
	return &ClaudeFactory{SessionsDir: sessionsDir, Model: model}, nil
}

func (f *ClaudeFactory) NewAgent(_ context.Context, sessionKey string) (Agent, error) {
	workdir := filepath.Join(f.SessionsDir, sanitize(sessionKey))
	if err := os.MkdirAll(workdir, 0o700); err != nil {
		return nil, fmt.Errorf("create session workdir: %w", err)
	}
	return &ClaudeAgent{
		workdir:   workdir,
		sessionID: uuidV4(),
		model:     f.Model,
	}, nil
}

// ClaudeAgent runs turns via the claude CLI, resuming one conversation.
type ClaudeAgent struct {
	workdir   string
	sessionID string
	model     string

	mu    sync.Mutex
	first bool // set true after the first turn assigns the session id
}

func (a *ClaudeAgent) RunTurn(ctx context.Context, turnID, input string) <-chan Event {
	out := make(chan Event)
	go func() {
		defer close(out)
		emit := func(e Event) bool {
			select {
			case out <- e:
				return true
			case <-ctx.Done():
				return false
			}
		}

		args := []string{
			"-p", input,
			"--output-format", "stream-json",
			"--include-partial-messages",
			"--verbose",
			"--dangerously-skip-permissions", // headless: no interactive perm prompts
		}
		a.mu.Lock()
		if a.first {
			args = append(args, "--resume", a.sessionID)
		} else {
			args = append(args, "--session-id", a.sessionID)
			a.first = true
		}
		a.mu.Unlock()
		if a.model != "" {
			args = append(args, "--model", a.model)
		}

		cmd := exec.CommandContext(ctx, "claude", args...)
		cmd.Dir = a.workdir
		cmd.Env = os.Environ() // inherits CLAUDE_CODE_OAUTH_TOKEN + HOME
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			emit(Event{KindError, ErrorData{Code: "agent_failed", Message: err.Error()}})
			return
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			emit(Event{KindError, ErrorData{Code: "agent_failed", Message: err.Error()}})
			return
		}

		seenTool := map[string]bool{}
		completed := false
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 128*1024), 8*1024*1024)
		for sc.Scan() {
			if !a.handleLine(sc.Bytes(), turnID, seenTool, &completed, emit) {
				break
			}
		}
		werr := cmd.Wait()
		if !completed {
			msg := "claude exited before completing the turn"
			if werr != nil {
				msg = werr.Error()
			}
			emit(Event{KindError, ErrorData{Code: "agent_failed", Message: msg}})
		}
	}()
	return out
}

// handleLine parses one stream-json line and emits mapped events. Returns false
// if the consumer went away (emit failed).
func (a *ClaudeAgent) handleLine(line []byte, turnID string, seenTool map[string]bool, completed *bool, emit func(Event) bool) bool {
	var msg claudeLine
	if err := json.Unmarshal(line, &msg); err != nil {
		return true // tolerate non-JSON / partial lines
	}
	switch msg.Type {
	case "stream_event":
		// Incremental text deltas -> token frames.
		if msg.Event != nil && msg.Event.Type == "content_block_delta" &&
			msg.Event.Delta != nil && msg.Event.Delta.Type == "text_delta" && msg.Event.Delta.Text != "" {
			return emit(Event{KindToken, TokenData{Text: msg.Event.Delta.Text}})
		}
	case "assistant":
		// Tool calls the assistant decided on -> tool_start (deduped by id).
		if msg.Message != nil {
			for _, b := range msg.Message.Content {
				if b.Type == "tool_use" && b.ID != "" && !seenTool[b.ID] {
					seenTool[b.ID] = true
					if !emit(Event{KindToolStart, ToolStartData{Tool: b.Name, ToolCallID: b.ID, Title: b.Name}}) {
						return false
					}
				}
			}
		}
	case "user":
		// Tool results -> tool_end.
		if msg.Message != nil {
			for _, b := range msg.Message.Content {
				if b.Type == "tool_result" && b.ToolUseID != "" {
					status := "ok"
					if b.IsError {
						status = "error"
					}
					if !emit(Event{KindToolEnd, ToolEndData{ToolCallID: b.ToolUseID, Status: status}}) {
						return false
					}
				}
			}
		}
	case "result":
		*completed = true
		if msg.IsError {
			return emit(Event{KindError, ErrorData{Code: "agent_failed", Message: firstNonEmptyStr(msg.Result, msg.Subtype)}})
		}
		return emit(Event{KindTurnCompleted, TurnCompletedData{TurnID: turnID, StopReason: firstNonEmptyStr(msg.Subtype, "end_turn")}})
	}
	return true
}

func (a *ClaudeAgent) Close() error {
	return os.RemoveAll(a.workdir)
}

// --- stream-json shapes (only the fields we use) ---

type claudeLine struct {
	Type    string         `json:"type"`
	Subtype string         `json:"subtype"`
	IsError bool           `json:"is_error"`
	Result  string         `json:"result"`
	Message *claudeMessage `json:"message"`
	Event   *claudeEvent   `json:"event"`
}

type claudeMessage struct {
	Content []claudeBlock `json:"content"`
}

type claudeBlock struct {
	Type      string `json:"type"`
	Name      string `json:"name"`        // tool_use
	ID        string `json:"id"`          // tool_use
	ToolUseID string `json:"tool_use_id"` // tool_result
	IsError   bool   `json:"is_error"`    // tool_result
}

type claudeEvent struct {
	Type  string       `json:"type"`
	Delta *claudeDelta `json:"delta"`
}

type claudeDelta struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func firstNonEmptyStr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func sanitize(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
			b = append(b, c)
		default:
			b = append(b, '_')
		}
	}
	if len(b) == 0 {
		return "session"
	}
	return string(b)
}

func uuidV4() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
