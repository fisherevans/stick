// Command refclient is a minimal, dependency-free reference consumer for stick.
// It opens a turn against a session key and renders the SSE stream the way a real
// consumer would: an hourglass while queued, a "researching..." line for tool
// activity, the streamed assistant text, and any structured output. Copy its
// shape when building a consumer (bloom-bot mirrors this).
//
// Usage:
//
//	STICK_URL=http://localhost:8080 STICK_SECRET=... \
//	  go run ./examples/refclient <session-key> <input...>
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

func main() {
	base := envOr("STICK_URL", "http://localhost:8080")
	secret := os.Getenv("STICK_SECRET")
	if secret == "" || len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: STICK_SECRET=... STICK_URL=... refclient <session-key> <input...>")
		os.Exit(2)
	}
	key := os.Args[1]
	input := strings.Join(os.Args[2:], " ")

	body, _ := json.Marshal(map[string]string{"input": input})
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/sessions/"+key+"/turns", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "request:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "http %d: %s\n", resp.StatusCode, readAll(resp.Body))
		os.Exit(1)
	}

	for event, data := range sseFrames(resp.Body) {
		switch event {
		case "queued":
			var d struct {
				QueuePosition int `json:"queue_position"`
			}
			_ = json.Unmarshal(data, &d)
			fmt.Printf("\r⏳ queued (position %d)   ", d.QueuePosition)
		case "turn_started":
			fmt.Print("\r")
		case "tool_start":
			var d struct {
				Title string `json:"title"`
			}
			_ = json.Unmarshal(data, &d)
			fmt.Printf("\n[%s...]\n", firstNonEmpty(d.Title, "working"))
		case "tool_end":
			// no-op for display; the next tokens continue the answer
		case "token":
			var d struct {
				Text string `json:"text"`
			}
			_ = json.Unmarshal(data, &d)
			fmt.Print(d.Text)
		case "structured_output":
			fmt.Printf("\n[structured] %s\n", data)
		case "turn_completed":
			fmt.Println()
			return
		case "error":
			fmt.Fprintf(os.Stderr, "\nerror: %s\n", data)
			os.Exit(1)
		}
	}
}

// sseFrames yields (event, data) pairs from a text/event-stream body. Comment
// lines (heartbeats) are skipped. Go 1.23 range-over-func.
func sseFrames(r interface{ Read([]byte) (int, error) }) func(func(string, json.RawMessage) bool) {
	return func(yield func(string, json.RawMessage) bool) {
		sc := bufio.NewScanner(bufio.NewReader(r))
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		var event string
		var data []byte
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "": // frame boundary
				if event != "" {
					if !yield(event, json.RawMessage(data)) {
						return
					}
				}
				event, data = "", nil
			case strings.HasPrefix(line, ":"):
				// comment / heartbeat, ignore
			case strings.HasPrefix(line, "event:"):
				event = strings.TrimSpace(line[len("event:"):])
			case strings.HasPrefix(line, "data:"):
				data = append(data, strings.TrimSpace(line[len("data:"):])...)
			}
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func readAll(r interface{ Read([]byte) (int, error) }) string {
	var b bytes.Buffer
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return b.String()
}
