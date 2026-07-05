package agent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
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

func (f *ClaudeFactory) NewAgent(_ context.Context, consumer, sessionKey string, cfg SessionConfig) (Agent, error) {
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
	model := f.Model
	if cfg.Model != "" {
		model = cfg.Model // per-session override of the factory default
	}
	a := &ClaudeAgent{
		workdir:      workdir,
		sessionID:    uuidV4(),
		model:        model,
		addDirs:      addDirs,
		keepDir:      workdir != f.SessionsDir && isProfileDir(f.Profiles, consumer),
		systemPrompt: cfg.SystemPrompt,
		allowTools:   cfg.AllowTools,
		denyTools:    cfg.DenyTools,
		seed:         cfg.Seed,
		tools:        cfg.Tools,
	}
	if len(cfg.Tools) > 0 {
		if err := a.setupTools(f.SelfPath, cfg.Tools); err != nil {
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

	// Caller-supplied session config, applied as CLI flags on each turn.
	systemPrompt string
	allowTools   []string
	denyTools    []string
	seed         string
	tools        []Tool

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

// maxRepairAttempts bounds the guaranteed-output repair loop: if a turn declares
// a required output tool and finishes without calling it, stick nudges the session
// (on the same warm conversation) to produce it. Small because with the output
// tool present and the nudge explicit, one repair almost always suffices; the cap
// stops a pathological loop.
const maxRepairAttempts = 2

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

		a.mu.Lock()
		first := !a.first
		a.first = true
		a.mu.Unlock()

		// For required output tools, steer the primary turn to call them - the
		// reliable path (a turn that is told to call the tool does so on both first
		// and resume turns). The repair loop below is only a backstop for a miss.
		mainInput := input
		if d := a.requiredDirective(); d != "" {
			mainInput = input + "\n\n" + d
		}

		// Main turn: stream everything to the consumer.
		st, aborted := a.runOnce(ctx, mainInput, first, emit)
		if aborted {
			return
		}
		if st.agentErr != "" {
			emit(Event{KindError, ErrorData{Code: "agent_failed", Message: st.agentErr}})
			return
		}
		total := st.usage

		// First-class structured output: if the turn declared required output tools
		// and the agent finished without calling one, nudge it to produce them. The
		// repair runs on the same warm session; its prose/tool chatter is suppressed
		// (the consumer only sees the structured_output it yields), so the guarantee
		// is invisible - the consumer just always gets its frame.
		missing := a.unmetRequired(st.emitted)
		for attempt := 0; len(missing) > 0 && attempt < maxRepairAttempts; attempt++ {
			repairEmit := func(e Event) bool {
				if e.Kind == KindStructuredOutput {
					return emit(e)
				}
				return true // swallow the nudge's tokens/tool chatter
			}
			rst, rAborted := a.runOnce(ctx, repairInput(missing), false, repairEmit)
			if rAborted {
				return
			}
			total = addUsage(total, rst.usage)
			for name := range rst.emitted {
				st.emitted[name] = true
			}
			if rst.agentErr != "" {
				break
			}
			missing = a.unmetRequired(st.emitted)
		}
		if len(missing) > 0 {
			emit(Event{KindError, ErrorData{
				Code:    "output_not_produced",
				Message: "agent did not produce required output(s): " + missingOutputNames(missing),
			}})
			return
		}
		emit(Event{KindTurnCompleted, TurnCompletedData{
			TurnID: turnID, StopReason: firstNonEmptyStr(st.stopReason, "end_turn"), Usage: total,
		}})
	}()
	return out
}

// runOnce executes one `claude` invocation for this session and processes its
// stream, invoking emit for streamed events (token/tool_start/tool_end/
// structured_output). It captures the terminal result (usage, stop reason, or
// error) into the returned turnState rather than emitting it, so the caller can
// interpose the repair loop before the turn's single terminal frame. first
// controls seed injection and --session-id vs --resume. Returns aborted=true if
// the consumer went away mid-stream (emit failed).
func (a *ClaudeAgent) runOnce(ctx context.Context, input string, first bool, emit func(Event) bool) (st *turnState, aborted bool) {
	st = &turnState{
		seenTool:    map[string]bool{},
		outToolIDs:  map[string]bool{},
		pendingOut:  map[string]json.RawMessage{},
		pendingName: map[string]string{},
		emitted:     map[string]bool{},
	}

	// On the first turn, prepend the seed grounding (if any). The seed is bound to
	// the session config, so a recreated (post-eviction) session re-applies it on
	// its own first turn - the grounding survives a rehydrate.
	if first && a.seed != "" {
		input = a.seed + "\n\n" + input
	}

	args := []string{
		"-p", input,
		"--output-format", "stream-json",
		"--include-partial-messages",
		"--verbose",
		"--dangerously-skip-permissions", // headless: no interactive perm prompts
	}
	if first {
		args = append(args, "--session-id", a.sessionID)
	} else {
		args = append(args, "--resume", a.sessionID)
	}
	if a.model != "" {
		args = append(args, "--model", a.model)
	}
	// Persona: replace the default coding-agent system prompt so the session runs
	// as the consumer's persona. --exclude-dynamic-system-prompt-sections is not
	// used here because --system-prompt already replaces the default wholesale.
	if a.systemPrompt != "" {
		args = append(args, "--system-prompt", a.systemPrompt)
	}
	// Tool policy. Uses --disallowedTools (a denylist) rather than --tools (an
	// allowlist): --tools filters the tool set in a way that drops the consumer's
	// MCP output tools on resume turns (a CLI interaction verified empirically),
	// which would break multi-turn structured output. A denylist keeps every tool -
	// including the MCP output tools - and just removes the named noise, which is
	// what a consumer wants (exclude ReportFindings/ToolSearch, keep Bash/WebSearch).
	if len(a.allowTools) > 0 {
		args = append(args, "--allowedTools", strings.Join(a.allowTools, ","))
	}
	if len(a.denyTools) > 0 {
		args = append(args, "--disallowedTools", strings.Join(a.denyTools, ","))
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
		st.agentErr = err.Error()
		return st, false
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		st.agentErr = err.Error()
		return st, false
	}

	completed := false
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 128*1024), 8*1024*1024)
	for sc.Scan() {
		if !a.handleLine(sc.Bytes(), st, &completed, emit) {
			aborted = true
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
	if aborted {
		return st, true
	}
	if !completed && st.agentErr == "" {
		st.agentErr = "claude exited before completing the turn"
		if werr != nil {
			st.agentErr = werr.Error()
		}
	}
	return st, false
}

// outputName is the structured_output name a tool produces (OutputName, or Name).
func outputName(t Tool) string {
	if t.OutputName != "" {
		return t.OutputName
	}
	return t.Name
}

// requiredDirective is the instruction stick appends to a turn that has required
// output tools, telling the model to deliver its result through them. Injecting
// this on the primary turn is what makes required output reliable; the repair
// loop only catches the occasional miss.
func (a *ClaudeAgent) requiredDirective() string {
	var names []string
	for _, t := range a.tools {
		if t.Required {
			names = append(names, "`"+t.Name+"`")
		}
	}
	if len(names) == 0 {
		return ""
	}
	tool := "tool"
	if len(names) > 1 {
		tool = "tools"
	}
	return "When you finish, deliver your result by calling the " + tool + " " +
		strings.Join(names, ", ") + " with the structured argument - that call is how " +
		"your result reaches the caller; a plain-text reply is not recorded."
}

// unmetRequired returns the session's required tools whose structured output has
// not yet been emitted (by tool, so the repair nudge can name the actual tool).
func (a *ClaudeAgent) unmetRequired(emitted map[string]bool) []Tool {
	var missing []Tool
	for _, t := range a.tools {
		if t.Required && !emitted[outputName(t)] {
			missing = append(missing, t)
		}
	}
	return missing
}

// repairInput nudges the session to record its result through the output tool(s)
// it skipped. Phrasing matters: a blunt "you MUST call this tool now" reads to the
// model like an injected instruction and it sometimes refuses (claiming the tool
// doesn't exist), so this asks naturally, by the real tool name, to deliver the
// result it already produced.
func repairInput(missing []Tool) string {
	names := make([]string, len(missing))
	for i, t := range missing {
		names[i] = "`" + t.Name + "`"
	}
	tool := "tool"
	if len(names) > 1 {
		tool = "tools"
	}
	return "To record your answer for the caller, please use the " + tool + " " +
		strings.Join(names, ", ") + " now, passing your result as the argument. " +
		"That is how the result is returned - a plain-text reply is not captured."
}

// missingOutputNames renders unmet required tools by their output name, for the
// consumer-facing error when even repair could not produce them.
func missingOutputNames(missing []Tool) string {
	names := make([]string, len(missing))
	for i, t := range missing {
		names[i] = outputName(t)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func addUsage(a, b *Usage) *Usage {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return &Usage{
		Model:                    firstNonEmptyStr(a.Model, b.Model),
		InputTokens:              a.InputTokens + b.InputTokens,
		OutputTokens:             a.OutputTokens + b.OutputTokens,
		CacheReadInputTokens:     a.CacheReadInputTokens + b.CacheReadInputTokens,
		CacheCreationInputTokens: a.CacheCreationInputTokens + b.CacheCreationInputTokens,
		CostUSD:                  a.CostUSD + b.CostUSD,
		DurationMS:               a.DurationMS + b.DurationMS,
	}
}

// turnState is the per-invocation bookkeeping handleLine threads across stream
// lines. It also captures the terminal result (usage/stop reason/error) so the
// RunTurn loop can interpose the output-repair pass before emitting the turn's
// single terminal frame.
type turnState struct {
	seenTool    map[string]bool            // normal tools already announced (tool_start dedup)
	outToolIDs  map[string]bool            // tool_use ids that are output-tool calls
	pendingOut  map[string]json.RawMessage // id -> latest well-formed output-tool input
	pendingName map[string]string          // id -> structured_output name
	emitted     map[string]bool            // structured_output names emitted this invocation

	usage      *Usage // captured from the terminal result line
	stopReason string
	agentErr   string // non-empty if the agent reported an error
}

// handleLine parses one stream-json line, emitting mapped streaming events and
// capturing the terminal result into st. Returns false if the consumer went away
// (emit failed).

func (a *ClaudeAgent) handleLine(line []byte, st *turnState, completed *bool, emit func(Event) bool) bool {
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
		// Tool calls the assistant decided on. With --include-partial-messages the
		// same tool_use id can appear across several assistant frames as its input
		// streams in; for output tools we capture the latest well-formed input and
		// emit a single structured_output when its tool_result lands (below).
		if msg.Message != nil {
			for _, b := range msg.Message.Content {
				if b.Type != "tool_use" || b.ID == "" {
					continue
				}
				if out, ok := a.outputTools[b.Name]; ok {
					st.outToolIDs[b.ID] = true
					st.pendingName[b.ID] = out
					// Skip the CLI's partial/unparsed snapshots; keep the last good one.
					if len(b.Input) > 0 && !bytes.Contains(b.Input, []byte("__unparsedToolInput")) {
						st.pendingOut[b.ID] = b.Input
					}
					continue
				}
				if !st.seenTool[b.ID] {
					st.seenTool[b.ID] = true
					if !emit(Event{KindToolStart, ToolStartData{Tool: b.Name, ToolCallID: b.ID, Title: b.Name}}) {
						return false
					}
				}
			}
		}
	case "user":
		// Tool results. An output-tool result is the "call complete" signal: emit
		// its captured input as structured_output and swallow the ack. Other tools
		// map to tool_end.
		if msg.Message != nil {
			for _, b := range msg.Message.Content {
				if b.Type != "tool_result" || b.ToolUseID == "" {
					continue
				}
				if st.outToolIDs[b.ToolUseID] {
					val, ok := st.pendingOut[b.ToolUseID]
					if !ok {
						val = json.RawMessage("null")
					}
					delete(st.pendingOut, b.ToolUseID)
					name := st.pendingName[b.ToolUseID]
					st.emitted[name] = true
					if !emit(Event{KindStructuredOutput, StructuredOutputData{Name: name, Value: val}}) {
						return false
					}
					continue
				}
				status := "ok"
				if b.IsError {
					status = "error"
				}
				if !emit(Event{KindToolEnd, ToolEndData{ToolCallID: b.ToolUseID, Status: status}}) {
					return false
				}
			}
		}
	case "result":
		// Terminal line: capture rather than emit, so RunTurn can run the output
		// repair before sending the turn's single terminal frame.
		*completed = true
		if msg.IsError {
			st.agentErr = firstNonEmptyStr(msg.Result, msg.Subtype, "agent error")
			return true
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
		st.usage = u
		st.stopReason = firstNonEmptyStr(msg.Subtype, "end_turn")
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

func firstNonEmptyStr(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
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
