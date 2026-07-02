// Package auth validates the per-consumer provisioned client secrets that gate
// stick. There is no OIDC or login flow: a consumer presents a static bearer
// secret an operator provisioned, and the registry maps it to a consumer id used
// for quota and metrics. Secrets are compared in constant time.
//
// The registry is loaded from config (a JSON map of consumer id -> secret,
// materialized from Bitwarden at deploy time). It is read-only after construction.
package auth

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
)

// Registry maps client secrets to consumer ids.
type Registry struct {
	// byHash keys on the SHA-256 of the secret so the map itself never holds the
	// raw secret, and lookups are O(1) before the constant-time confirm.
	byHash map[[32]byte]string
}

// NewRegistry builds a registry from a consumerID -> secret map.
func NewRegistry(secrets map[string]string) *Registry {
	r := &Registry{byHash: make(map[[32]byte]string, len(secrets))}
	for consumer, secret := range secrets {
		r.byHash[sha256.Sum256([]byte(secret))] = consumer
	}
	return r
}

// Consumer returns the consumer id for a presented secret, or ("", false).
// Lookup is on the SHA-256 of the secret: the raw secret is never stored, and the
// 32-byte array key makes the map compare exact and independent of secret length.
func (r *Registry) Consumer(secret string) (string, bool) {
	sum := sha256.Sum256([]byte(secret))
	consumer, ok := r.byHash[sum]
	return consumer, ok
}

type ctxKey struct{}

// consumerKey is the context key under which the authenticated consumer id is stored.
var consumerKey = ctxKey{}

// withConsumer returns a context carrying the consumer id.
func withConsumer(ctx context.Context, consumer string) context.Context {
	return context.WithValue(ctx, consumerKey, consumer)
}

// ConsumerFrom returns the authenticated consumer id from a request context set
// by Middleware. The bool is false if the request did not pass through it.
func ConsumerFrom(ctx context.Context) (string, bool) {
	c, ok := ctx.Value(consumerKey).(string)
	return c, ok
}

// Middleware authenticates the bearer secret and injects the consumer id into the
// request context. Missing/unknown -> 401. On success it calls next.
func (r *Registry) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		secret, ok := bearer(req)
		if !ok {
			unauthorized(w)
			return
		}
		consumer, ok := r.Consumer(secret)
		if !ok {
			unauthorized(w)
			return
		}
		next.ServeHTTP(w, req.WithContext(withConsumer(req.Context(), consumer)))
	})
}

func bearer(req *http.Request) (string, bool) {
	h := req.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	return strings.TrimSpace(h[len(prefix):]), true
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte(`{"code":"unauthenticated","message":"missing or unknown client secret"}`))
}
