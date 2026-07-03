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
	"sort"
	"sync"
	"syscall"
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
	SessionsDir string             // base dir for per-session scratch workdirs
	Model       string             // optional model alias (e.g. "opus"); "" = CLI default
	Profiles    map[string]Profile // per-consumer environment overrides
	SelfPath    string             // path to the stick binary (spawned as the MCP server for tools)
}

// NewClaudeFactory builds a factory. sessionsDir is created if missing.
func NewClaudeFactory(sessionsDir, model string, profiles map[string]Profile) (*ClaudeFactory, error) {
	if sessionsDir == "" {
		sessionsDir = "/opt/stick/sessions"
	}
	if err := os.MkdirAll(sessionsDir, 0o700); err != nil {
		return nil, fmt.Errorf("create sessions dir: %w", err)
	}
	if profiles == nil {
		profiles = map[string]Profile{}
	}
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve stick binary path: %w", err)
	}
	return &ClaudeFactory{SessionsDir: sessionsDir, Model: model, Profiles: profiles, SelfPath: self}, nil
}

func (f *ClaudeFactory) NewAgent(_ context.Context, consumer, sessionKey string, tools []Tool) (Agent, error) {
	var workdir string
	var addDirs []string
	if p, ok := f.Profiles[consumer]; ok && p.Workdir != "" {
		if p.SharedWorkdir {
			workdir = p.Workdir
		} else {
			workdir = filepath.Join(p.Workdir, sanitize(sessionKey))
		}
		addDirs = p.AddDirs
	} else {
		// Generic default: an isolated scratch dir per (consumer, key).
		workdir = filepath.Join(f.SessionsDir, sanitize(consumer), sanitize(sessionKey))
	}
	if err := os.MkdirAll(workdir, 0o700); err != nil {
		return nil, fmt.Errorf("create session workdir: %w", err)
	}
	a := &ClaudeAgent{
		workdir:   workdir,
		sessionID: uuidV4(),
		model:     f.Model,
		addDirs:   addDirs,
		keepDir:   workdir != f.SessionsDir && isProfileDir(f.Profiles, consumer),
	}
	if len(tools) > 0 {
		if err := a.setupTools(f.SelfPath, tools); err != nil {
			return nil, fmt.Errorf("set up session tools: %w", err)
		}
	}
	return a, nil
}

// setupTools writes the MCP tools file + config into the session workdir and
// records the tool_use name -> structured_output name mapping. selfPath is the
// stick binary the CLI spawns as the stdio MCP server (`stick mcp-serve`).
func (a *ClaudeAgent) setupTools(selfPath string, tools []Tool) error {
	defs := make([]mcpToolDef, 0, len(tools))
	a.outputTools = map[string]string{}
	for _, t := range tools {
		out := t.OutputName
		if out == "" {
			out = t.Name
		}
		a.outputTools["mcp__stick__"+t.Name] = out
		defs = append(defs, mcpToolDef{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema})
	}
	toolsPath := filepath.Join(a.workdir, ".stick-tools.json")
	toolsJSON, err := json.Marshal(defs)
	if err != nil {
		return err
	}
	if err := os.WriteFile(toolsPath, toolsJSON, 0o600); err != nil {
		return err
	}
	cfg := map[string]any{"mcpServers": map[string]any{
		"stick": map[string]any{"command": selfPath, "args": []string{"mcp-serve", "--tools", toolsPath}},
	}}
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	a.mcpConfigPath = filepath.Join(a.workdir, ".stick-mcp.json")
	return os.WriteFile(a.mcpConfigPath, cfgJSON, 0o600)
}

// mcpToolDef mirrors internal/mcp.ToolDef (declared here to avoid an import cycle
// between agent and mcp; mcp imports nothing from agent, and the on-disk shape is
// the contract between them).
type mcpToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// isProfileDir reports whether the consumer runs in a configured profile workdir
// (which stick must not delete on session close - it's the consumer's mounted
// data, not a scratch dir).
func isProfileDir(profiles map[string]Profile, consumer string) bool {
	p, ok := profiles[consumer]
	return ok && p.Workdir != ""
}

// ClaudeAgent runs turns via the claude CLI, resuming one conversation.
type ClaudeAgent struct {
	workdir       string
	sessionID     string
	model         string
	addDirs       []string
	keepDir       bool              // profile workdirs are not deleted on Close
	mcpConfigPath string            // --mcp-config for consumer-declared tools ("" = none)
	outputTools   map[string]string // mcp tool_use name -> structured_output name

	mu           sync.Mutex
	first        bool  // set true after the first turn assigns the session id
	lastMaxRSSKB int64 // peak RSS of the most recent turn's claude process (KB)
}

// LastMaxRSSKB returns the peak resident set size (KB) of the most recently
// completed turn's claude subprocess, for resource-pressure metrics. 0 if no
// turn has completed yet.
func (a *ClaudeAgent) LastMaxRSSKB() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastMaxRSSKB
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
		for _, d := range a.addDirs {
			args = append(args, "--add-dir", d)
		}
		if a.mcpConfigPath != "" {
			// Expose the consumer's declared tools over MCP. --dangerously-skip-permissions
			// already allows them; the tool_use args are read from the stream below.
			args = append(args, "--mcp-config", a.mcpConfigPath, "--strict-mcp-config")
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
		outIDs := map[string]bool{} // tool_use ids that are output-tool calls (their results are acks, not tool_end)
		completed := false
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 128*1024), 8*1024*1024)
		for sc.Scan() {
			if !a.handleLine(sc.Bytes(), turnID, seenTool, outIDs, &completed, emit) {
				break
			}
		}
		werr := cmd.Wait()
		if cmd.ProcessState != nil {
			if ru, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
				a.mu.Lock()
				a.lastMaxRSSKB = ru.Maxrss // Linux: kilobytes
				a.mu.Unlock()
			}
		}
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
func (a *ClaudeAgent) handleLine(line []byte, turnID string, seenTool, outIDs map[string]bool, completed *bool, emit func(Event) bool) bool {
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
		// Tool calls the assistant decided on.
		if msg.Message != nil {
			for _, b := range msg.Message.Content {
				if b.Type != "tool_use" || b.ID == "" || seenTool[b.ID] {
					continue
				}
				seenTool[b.ID] = true
				// A declared output tool -> the structured result the consumer wants,
				// carried in the tool_use input (name is mcp__stick__<tool>). Emit
				// structured_output, not tool_start; its ack tool_result is suppressed below.
				if out, ok := a.outputTools[b.Name]; ok {
					outIDs[b.ID] = true
					val := b.Input
					if len(val) == 0 {
						val = json.RawMessage("null")
					}
					if !emit(Event{KindStructuredOutput, StructuredOutputData{Name: out, Value: val}}) {
						return false
					}
					continue
				}
				if !emit(Event{KindToolStart, ToolStartData{Tool: b.Name, ToolCallID: b.ID, Title: b.Name}}) {
					return false
				}
			}
		}
	case "user":
		// Tool results -> tool_end (except output-tool acks, which are internal).
		if msg.Message != nil {
			for _, b := range msg.Message.Content {
				if b.Type == "tool_result" && b.ToolUseID != "" && !outIDs[b.ToolUseID] {
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
		u := &Usage{
			Model:      joinModels(msg.ModelUsage),
			CostUSD:    msg.TotalCostUSD,
			DurationMS: msg.DurationMS,
		}
		if msg.Usage != nil {
			u.InputTokens = msg.Usage.InputTokens
			u.OutputTokens = msg.Usage.OutputTokens
			u.CacheReadInputTokens = msg.Usage.CacheReadInputTokens
			u.CacheCreationInputTokens = msg.Usage.CacheCreationInputTokens
		}
		return emit(Event{KindTurnCompleted, TurnCompletedData{TurnID: turnID, StopReason: firstNonEmptyStr(msg.Subtype, "end_turn"), Usage: u}})
	}
	return true
}

func (a *ClaudeAgent) Close() error {
	if a.keepDir {
		return nil // profile workdir holds the consumer's data; never delete it
	}
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

	// Populated on the terminal "result" line.
	TotalCostUSD float64                     `json:"total_cost_usd"`
	DurationMS   int64                       `json:"duration_ms"`
	Usage        *claudeUsage                `json:"usage"`
	ModelUsage   map[string]claudeModelUsage `json:"modelUsage"`
}

type claudeUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
}

type claudeModelUsage struct {
	CostUSD float64 `json:"costUSD"`
}

// joinModels renders the model(s) a turn used as a stable tag value. Usually one
// model; joined with "+" and sorted for determinism when a turn spans models.
func joinModels(m map[string]claudeModelUsage) string {
	if len(m) == 0 {
		return ""
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	out := names[0]
	for _, n := range names[1:] {
		out += "+" + n
	}
	return out
}

type claudeMessage struct {
	Content []claudeBlock `json:"content"`
}

type claudeBlock struct {
	Type      string          `json:"type"`
	Name      string          `json:"name"`        // tool_use
	ID        string          `json:"id"`          // tool_use
	Input     json.RawMessage `json:"input"`       // tool_use args
	ToolUseID string          `json:"tool_use_id"` // tool_result
	IsError   bool            `json:"is_error"`    // tool_result
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
