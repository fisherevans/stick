package mcp

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateAgainstSchema(t *testing.T) {
	tools := []ToolDef{
		{
			Name:        "emit_answer",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"value":{"type":"number"}},"required":["value"]}`),
		},
		{Name: "no_schema"}, // no schema -> accept anything
	}
	v := compileValidators(tools)

	tests := []struct {
		name    string
		tool    string
		arg     string
		wantErr bool
	}{
		{"valid", "emit_answer", `{"value":391}`, false},
		{"missing required", "emit_answer", `{}`, true},
		{"wrong type", "emit_answer", `{"value":"lots"}`, true},
		{"not json", "emit_answer", `{value:}`, true},
		{"no schema accepts anything", "no_schema", `{"anything":true}`, false},
		{"unknown tool accepts", "mystery", `{"x":1}`, false}, // nil validator -> accept
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := validate(v[tt.tool], json.RawMessage(tt.arg))
			if tt.wantErr && msg == "" {
				t.Fatalf("expected a validation error, got none")
			}
			if !tt.wantErr && msg != "" {
				t.Fatalf("expected valid, got error: %s", msg)
			}
			// The error must name the offending field so the agent can fix it.
			if tt.name == "missing required" && !strings.Contains(msg, "value") {
				t.Fatalf("error should mention the missing field: %q", msg)
			}
		})
	}
}
