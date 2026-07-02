// Package config loads stick's runtime configuration from the environment and a
// client-secret registry file. Secrets never live in the binary or in git: the
// registry file is materialized from Bitwarden at deploy time (per the
// nottingham-cloud app-contract secrets rule).
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the fully-resolved runtime configuration.
type Config struct {
	ListenAddr  string        // STICK_LISTEN, default ":8080"
	Capacity    int           // STICK_CAPACITY (number of sticks), default 2
	IdleTimeout time.Duration // STICK_IDLE_TIMEOUT seconds, default 900
	AgentMode   string        // STICK_AGENT ("stub" | "claude"), default "stub"

	// Secrets maps consumer id -> client secret. Loaded from STICK_SECRETS_FILE
	// (JSON object) if set; otherwise from STICK_SECRETS_JSON inline (dev only).
	Secrets map[string]string
}

// Load reads configuration from the environment. It returns an error if a
// referenced secrets file is unreadable or malformed.
func Load() (*Config, error) {
	c := &Config{
		ListenAddr:  env("STICK_LISTEN", ":8080"),
		Capacity:    envInt("STICK_CAPACITY", 2),
		IdleTimeout: time.Duration(envInt("STICK_IDLE_TIMEOUT", 900)) * time.Second,
		AgentMode:   env("STICK_AGENT", "stub"),
		Secrets:     map[string]string{},
	}

	if path := os.Getenv("STICK_SECRETS_FILE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read STICK_SECRETS_FILE: %w", err)
		}
		if err := json.Unmarshal(raw, &c.Secrets); err != nil {
			return nil, fmt.Errorf("parse STICK_SECRETS_FILE: %w", err)
		}
	} else if inline := os.Getenv("STICK_SECRETS_JSON"); inline != "" {
		if err := json.Unmarshal([]byte(inline), &c.Secrets); err != nil {
			return nil, fmt.Errorf("parse STICK_SECRETS_JSON: %w", err)
		}
	}

	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
