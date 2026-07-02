// Command stick runs the platform service: authenticated, semaphore-limited,
// streamed Claude Code agent sessions for programmatic consumers. See
// docs/contract.md for the surface it exposes.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fisherevans/stick/internal/agent"
	"github.com/fisherevans/stick/internal/api"
	"github.com/fisherevans/stick/internal/auth"
	"github.com/fisherevans/stick/internal/config"
	"github.com/fisherevans/stick/internal/semaphore"
	"github.com/fisherevans/stick/internal/session"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	factory := selectFactory(cfg.AgentMode, log)
	pool := semaphore.New(cfg.Capacity)
	mgr := session.NewManager(factory, cfg.IdleTimeout)
	defer mgr.Close()
	srv := api.NewServer(pool, mgr, auth.NewRegistry(cfg.Secrets))

	httpSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// No write timeout: SSE turns are long-lived streams.
	}

	log.Info("stick starting",
		"addr", cfg.ListenAddr,
		"capacity", cfg.Capacity,
		"idle_timeout", cfg.IdleTimeout.String(),
		"agent", cfg.AgentMode,
		"consumers", len(cfg.Secrets),
	)

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Info("stick shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}

// selectFactory picks the agent runtime. Only the stub exists today; the real
// Claude-Code-per-worktree factory lands with the cluster LXC (P1b, issue #2).
func selectFactory(mode string, log *slog.Logger) agent.Factory {
	switch mode {
	case "claude":
		log.Warn("STICK_AGENT=claude not implemented yet; using stub", "issue", "fisherevans/stick#2")
		return agent.StubFactory{}
	default:
		return agent.StubFactory{}
	}
}
