package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// sseWriter serializes named events to a text/event-stream response, flushing
// each so consumers see incremental frames. Not safe for concurrent use; one
// turn writes to one sseWriter from one goroutine.
type sseWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

// newSSE prepares the response for streaming. It returns false if the
// ResponseWriter can't flush (no streaming possible).
func newSSE(w http.ResponseWriter) (*sseWriter, bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, false
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // defeat proxy buffering
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	return &sseWriter{w: w, flusher: flusher}, true
}

// event writes one named event with a JSON data payload.
func (s *sseWriter) event(name string, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", name, payload); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}

// comment writes an SSE comment line (heartbeat); clients ignore it.
func (s *sseWriter) comment(text string) error {
	if _, err := fmt.Fprintf(s.w, ": %s\n\n", text); err != nil {
		return err
	}
	s.flusher.Flush()
	return nil
}
