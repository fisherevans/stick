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
// in reaches EOF.
func Serve(in io.Reader, out io.Writer, tools []ToolDef) error {
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
			// Output tools: acknowledge. stick reads the structured argument from
			// the session stream, so nothing is returned to the agent but an ack.
			writeResult(enc, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": "recorded"}},
			})
		default:
			writeError(enc, req.ID, -32601, "method not found")
		}
	}
	return sc.Err()
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
