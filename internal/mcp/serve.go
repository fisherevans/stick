// Package mcp implements the minimal stdio MCP server stick exposes to each
// Claude Code session so consumer-declared tools are callable. It speaks just
// enough of the Model Context Protocol (initialize, tools/list, tools/call) over
// newline-delimited JSON-RPC on stdin/stdout.
//
// For output tools the server does no real work: it advertises the tools (so the
// runtime validates calls against their schemas) and returns a fixed ack. The
// structured argument the agent passed is captured by the stick process from the
// session's stream (the tool_use block carries the full input), not here - so
// this server stays stateless and stick owns the routing. See internal/agent and
// the "Consumer-declared tools" design (fisherevans/stick#9).
//
// It is run as the `stick mcp-serve --tools <file>` subcommand; the CLI spawns it
// per session via a generated --mcp-config. Logs go to stderr (stdout is the
// protocol channel).
package mcp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// ToolDef is the on-disk shape of a tool the server advertises. It mirrors the
// MCP tool object; stick writes the tools file from the session's agent.Tool set.
type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"` // absent for notifications
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// Serve runs the stdio MCP loop over in/out, advertising tools. It returns when
// in reaches EOF. Each tool's inputSchema is compiled once so tools/call can
// validate the agent's argument and reject bad input in-band (see validators).
func Serve(in io.Reader, out io.Writer, tools []ToolDef) error {
	validators := compileValidators(tools)
	enc := json.NewEncoder(out)
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			continue // tolerate garbage
		}
		// Notifications (no id, e.g. notifications/initialized) get no response.
		if len(req.ID) == 0 {
			continue
		}
		switch req.Method {
		case "initialize":
			var p struct {
				ProtocolVersion string `json:"protocolVersion"`
			}
			_ = json.Unmarshal(req.Params, &p)
			if p.ProtocolVersion == "" {
				p.ProtocolVersion = "2025-06-18"
			}
			writeResult(enc, req.ID, map[string]any{
				"protocolVersion": p.ProtocolVersion,
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "stick", "version": "1"},
			})
		case "tools/list":
			list := make([]map[string]any, 0, len(tools))
			for _, t := range tools {
				schema := t.InputSchema
				if len(schema) == 0 {
					schema = json.RawMessage(`{"type":"object"}`)
				}
				list = append(list, map[string]any{
					"name": t.Name, "description": t.Description, "inputSchema": schema,
				})
			}
			writeResult(enc, req.ID, map[string]any{"tools": list})
		case "tools/call":
			// Validate the argument against the tool's schema and reject bad input
			// in-band: an isError result makes the agent see the failure and correct
			// within the turn - the same loop you'd get fighting any CLI that demands
			// valid structured input. Only a valid call is "recorded"; stick reads the
			// structured argument itself from the session stream.
			var p struct {
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			}
			_ = json.Unmarshal(req.Params, &p)
			if msg := validate(validators[p.Name], p.Arguments); msg != "" {
				writeResult(enc, req.ID, map[string]any{
					"isError": true,
					"content": []map[string]any{{"type": "text", "text": msg}},
				})
				break
			}
			writeResult(enc, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": "recorded"}},
			})
		default:
			writeError(enc, req.ID, -32601, "method not found")
		}
	}
	return sc.Err()
}

// compileValidators compiles each tool's inputSchema into a validator, keyed by
// tool name. A tool with no schema (or an uncompilable one) maps to nil, which
// validate treats as "accept anything".
func compileValidators(tools []ToolDef) map[string]*jsonschema.Schema {
	out := make(map[string]*jsonschema.Schema, len(tools))
	for _, t := range tools {
		if len(t.InputSchema) == 0 {
			out[t.Name] = nil
			continue
		}
		doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(t.InputSchema)))
		if err != nil {
			fmt.Fprintf(os.Stderr, "mcp: tool %q schema unreadable, skipping validation: %v\n", t.Name, err)
			out[t.Name] = nil
			continue
		}
		c := jsonschema.NewCompiler()
		res := "stick:" + t.Name
		if err := c.AddResource(res, doc); err != nil {
			out[t.Name] = nil
			continue
		}
		sch, err := c.Compile(res)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mcp: tool %q schema won't compile, skipping validation: %v\n", t.Name, err)
			out[t.Name] = nil
			continue
		}
		out[t.Name] = sch
	}
	return out
}

// validate checks raw arguments against a compiled schema, returning "" if valid
// (or if there is no schema) or a human-readable, model-actionable error message
// describing what is wrong so the agent can fix its next call.
func validate(sch *jsonschema.Schema, raw json.RawMessage) string {
	if sch == nil {
		return ""
	}
	arg := any(map[string]any{})
	if len(raw) > 0 {
		v, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
		if err != nil {
			return "the tool argument was not valid JSON: " + err.Error()
		}
		arg = v
	}
	if err := sch.Validate(arg); err != nil {
		// jsonschema's Error() is a readable, location-tagged description of every
		// failure - exactly what the agent needs to correct its next call.
		return "the argument does not match the tool's schema. Fix these and call it again:\n" + err.Error()
	}
	return ""
}

// LoadTools reads the tools file written by stick.
func LoadTools(path string) ([]ToolDef, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var tools []ToolDef
	if err := json.Unmarshal(raw, &tools); err != nil {
		return nil, fmt.Errorf("parse tools file: %w", err)
	}
	return tools, nil
}

func writeResult(enc *json.Encoder, id json.RawMessage, result any) {
	_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func writeError(enc *json.Encoder, id json.RawMessage, code int, msg string) {
	_ = enc.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": msg}})
}
