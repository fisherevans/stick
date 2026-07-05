package agent

import (
	"strings"
	"testing"
)

func TestUnmetRequiredAndDirective(t *testing.T) {
	a := &ClaudeAgent{tools: []Tool{
		{Name: "emit_node", OutputName: "node", Required: true},
		{Name: "lookup", Required: false},    // not required
		{Name: "emit_stats", Required: true}, // output name defaults to Name
	}}

	// Nothing emitted yet: both required tools are unmet.
	unmet := a.unmetRequired(map[string]bool{})
	if len(unmet) != 2 {
		t.Fatalf("want 2 unmet, got %d (%v)", len(unmet), unmet)
	}

	// After the "node" output lands, only emit_stats remains.
	unmet = a.unmetRequired(map[string]bool{"node": true})
	if len(unmet) != 1 || unmet[0].Name != "emit_stats" {
		t.Fatalf("want only emit_stats unmet, got %v", unmet)
	}

	// Both emitted (note emit_stats' output name is its Name): none unmet.
	if got := a.unmetRequired(map[string]bool{"node": true, "emit_stats": true}); len(got) != 0 {
		t.Fatalf("want none unmet, got %v", got)
	}

	// The directive names the actual tool names to call, not output names.
	d := a.requiredDirective()
	if !strings.Contains(d, "`emit_node`") || !strings.Contains(d, "`emit_stats`") {
		t.Fatalf("directive missing tool names: %q", d)
	}
	if strings.Contains(d, "lookup") {
		t.Fatalf("directive should not mention non-required tools: %q", d)
	}

	// A session with no required tools has no directive.
	none := &ClaudeAgent{tools: []Tool{{Name: "x"}}}
	if none.requiredDirective() != "" {
		t.Fatal("expected empty directive with no required tools")
	}
}

func TestRepairInputNamesTools(t *testing.T) {
	got := repairInput([]Tool{{Name: "emit_answer", OutputName: "answer"}})
	if !strings.Contains(got, "`emit_answer`") {
		t.Fatalf("repair should name the tool emit_answer, got %q", got)
	}
	if strings.Contains(got, "answer`,") || strings.Contains(got, "`answer`") {
		t.Fatalf("repair should not reference the output name: %q", got)
	}
}

func TestAddUsage(t *testing.T) {
	a := &Usage{InputTokens: 10, OutputTokens: 5, CostUSD: 0.1, Model: "opus"}
	b := &Usage{InputTokens: 3, OutputTokens: 2, CostUSD: 0.05}
	sum := addUsage(a, b)
	if sum.InputTokens != 13 || sum.OutputTokens != 7 {
		t.Fatalf("token sum wrong: %+v", sum)
	}
	if sum.CostUSD < 0.149 || sum.CostUSD > 0.151 {
		t.Fatalf("cost sum wrong: %v", sum.CostUSD)
	}
	if sum.Model != "opus" {
		t.Fatalf("model should carry through: %q", sum.Model)
	}
	if addUsage(nil, b) != b || addUsage(a, nil) != a {
		t.Fatal("addUsage should pass through a nil operand")
	}
}

func TestSessionConfigIsZero(t *testing.T) {
	if !(SessionConfig{}).IsZero() {
		t.Fatal("empty config should be zero")
	}
	for _, c := range []SessionConfig{
		{SystemPrompt: "x"},
		{Model: "opus"},
		{Seed: "ctx"},
		{DenyTools: []string{"ToolSearch"}},
		{Tools: []Tool{{Name: "t"}}},
	} {
		if c.IsZero() {
			t.Fatalf("config %+v should be non-zero", c)
		}
	}
}
