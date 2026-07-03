// Command stick runs the platform service: authenticated, semaphore-limited,
// streamed Claude Code agent sessions for programmatic consumers. See
// docs/contract.md for the surface it exposes.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fisherevans/stick/internal/agent"
	"github.com/fisherevans/stick/internal/api"
	"github.com/fisherevans/stick/internal/auth"
	"github.com/fisherevans/stick/internal/config"
	"github.com/fisherevans/stick/internal/mcp"
	"github.com/fisherevans/stick/internal/metrics"
	"github.com/fisherevans/stick/internal/semaphore"
	"github.com/fisherevans/stick/internal/session"
)

func main() {
	// Subcommand: run as the stdio MCP server the CLI spawns for a session's
	// consumer-declared tools. Kept in the same binary so there's one artifact to
	// deploy; the session's --mcp-config points `command` at this exe + mcp-serve.
	if len(os.Args) > 1 && os.Args[1] == "mcp-serve" {
		runMCPServe(os.Args[2:])
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	factory, err := selectFactory(cfg, log)
	if err != nil {
		log.Error("agent factory", "err", err)
		os.Exit(1)
	}
	pool := semaphore.New(cfg.Capacity)
	mgr := session.NewManager(factory, cfg.IdleTimeout)
	defer mgr.Close()

	// Metrics: agentless Datadog v2 submission, enabled iff a key is configured.
	var sink *metrics.Sink
	if cfg.DDAPIKey != "" {
		sink = metrics.New(cfg.DDAPIKey, cfg.DDSite, cfg.MetricsHostTag, log)
	} else {
		sink = metrics.NewDisabled()
	}
	metricsCtx, stopMetrics := context.WithCancel(context.Background())
	defer stopMetrics()
	go sink.Run(metricsCtx, cfg.MetricsFlush, func() {
		st := pool.Stats()
		sink.Gauge("stick.pool.sticks_total", float64(st.Total))
		sink.Gauge("stick.pool.sticks_in_use", float64(st.InUse))
		sink.Gauge("stick.pool.queue_depth", float64(st.QueueDepth))
		sink.Gauge("stick.sessions.live", float64(mgr.Count()))
		if memMB, load1, ok := hostStats(); ok {
			sink.Gauge("stick.host.mem_available_mb", memMB)
			sink.Gauge("stick.host.load1", load1)
		}
	})

	srv := api.NewServer(pool, mgr, auth.NewRegistry(cfg.Secrets), sink)

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
		"metrics", sink.Enabled(),
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

// runMCPServe runs the stdio MCP server for a session's declared tools and exits.
func runMCPServe(args []string) {
	fs := flag.NewFlagSet("mcp-serve", flag.ExitOnError)
	toolsPath := fs.String("tools", "", "path to the tools JSON file stick wrote for this session")
	_ = fs.Parse(args)
	var tools []mcp.ToolDef
	if *toolsPath != "" {
		t, err := mcp.LoadTools(*toolsPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "mcp-serve:", err)
			os.Exit(1)
		}
		tools = t
	}
	if err := mcp.Serve(os.Stdin, os.Stdout, tools); err != nil {
		fmt.Fprintln(os.Stderr, "mcp-serve:", err)
		os.Exit(1)
	}
}

// hostStats reads box-level pressure from /proc: available memory (MB) and the
// 1-minute load average. Lets the metrics sampler surface resource pressure and
// competing processes on the LXC without a Datadog agent. Returns ok=false on a
// non-Linux host or unreadable /proc.
func hostStats() (memMB, load1 float64, ok bool) {
	mem, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(mem), "\n") {
		if strings.HasPrefix(line, "MemAvailable:") {
			var kb float64
			if _, err := fmt.Sscanf(line, "MemAvailable: %f kB", &kb); err == nil {
				memMB = kb / 1024
			}
			break
		}
	}
	load, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return memMB, 0, true
	}
	fields := strings.Fields(string(load))
	if len(fields) > 0 {
		load1, _ = strconv.ParseFloat(fields[0], 64)
	}
	return memMB, load1, true
}

// selectFactory picks the agent runtime: the real Claude Code CLI or the stub.
func selectFactory(cfg *config.Config, log *slog.Logger) (agent.Factory, error) {
	switch cfg.AgentMode {
	case "claude":
		log.Info("using claude agent factory", "sessions_dir", cfg.SessionsDir, "model", cfg.ClaudeModel, "profiles", len(cfg.Profiles))
		return agent.NewClaudeFactory(cfg.SessionsDir, cfg.ClaudeModel, cfg.Profiles)
	default:
		return agent.StubFactory{}, nil
	}
}
