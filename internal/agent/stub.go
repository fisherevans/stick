package agent

import (
	"context"
	"strings"
	"time"
)

// StubFactory mints StubAgents. It stands in for the real Claude Code runtime so
// the platform can be exercised end to end locally.
type StubFactory struct{}

func (StubFactory) NewAgent(_ context.Context, sessionKey string) (Agent, error) {
	return &StubAgent{key: sessionKey}, nil
}

// StubAgent echoes the input back as a few token frames, runs one fake tool
// cycle, and emits a structured_output, then completes. It exercises every event
// kind the contract defines.
type StubAgent struct {
	key    string
	closed bool
}

func (a *StubAgent) RunTurn(ctx context.Context, turnID, input string) <-chan Event {
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

		// A fake tool cycle so consumers can render a pending state.
		if !emit(Event{KindToolStart, ToolStartData{Tool: "echo", ToolCallID: "tc_1", Title: "Thinking"}}) {
			return
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return
		}
		if !emit(Event{KindToolEnd, ToolEndData{ToolCallID: "tc_1", Status: "ok", Summary: "done"}}) {
			return
		}

		// Echo the input as word-by-word token frames.
		for i, word := range strings.Fields(input) {
			text := word
			if i > 0 {
				text = " " + word
			}
			if !emit(Event{KindToken, TokenData{Text: text}}) {
				return
			}
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}

		if !emit(Event{KindStructuredOutput, StructuredOutputData{
			Name:  "echo",
			Value: map[string]any{"session": a.key, "words": len(strings.Fields(input))},
		}}) {
			return
		}
		emit(Event{KindTurnCompleted, TurnCompletedData{TurnID: turnID, StopReason: "end_turn"}})
	}()
	return out
}

func (a *StubAgent) Close() error {
	a.closed = true
	return nil
}
