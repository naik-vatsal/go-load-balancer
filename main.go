// Command go-load-balancer is a production-grade HTTP load balancer.
//
// It reads a YAML config file, starts a pool of backend connections,
// runs a background health checker, and serves incoming requests through
// a configurable balancing algorithm with rate limiting, circuit breaking,
// and Prometheus metrics — all wired together in this file.
//
// Usage:
//
//	go-load-balancer -config config.yaml
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/naik-vatsal/go-load-balancer/balancer"
	"github.com/naik-vatsal/go-load-balancer/config"
	"github.com/naik-vatsal/go-load-balancer/health"
	"github.com/naik-vatsal/go-load-balancer/metrics"
	"github.com/naik-vatsal/go-load-balancer/middleware"
	"github.com/naik-vatsal/go-load-balancer/proxy"
)

func main() {
	// Structured logging via slog (Go 1.21+). JSON format makes logs
	// easy to ingest into Elasticsearch, Loki, or Cloud Logging.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// run is extracted from main so it can return an error cleanly, keeping
// main free of logic and making the startup sequence easy to read.
func run(logger *slog.Logger) error {
	// ── Flags ──────────────────────────────────────────────────────────
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	flag.Parse()

	// ── Config ─────────────────────────────────────────────────────────
	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger.Info("config loaded",
		"algorithm", cfg.Balancer.Algorithm,
		"backends", len(cfg.Backends),
	)

	// ── Backend pool ────────────────────────────────────────────────────
	urls := make([]string, len(cfg.Backends))
	weights := make([]int, len(cfg.Backends))
	for i, b := range cfg.Backends {
		urls[i] = b.URL
		weights[i] = b.Weight
	}
	pool, err := balancer.NewPool(urls, weights)
	if err != nil {
		return fmt.Errorf("create pool: %w", err)
	}

	// ── Balancer ────────────────────────────────────────────────────────
	var bal balancer.Balancer
	switch cfg.Balancer.Algorithm {
	case "least_conn":
		bal = balancer.NewLeastConn(pool)
	case "weighted_round_robin":
		bal = balancer.NewWeightedRoundRobin(pool)
	default: // "round_robin" and any future unknown value
		bal = balancer.NewRoundRobin(pool)
	}
	logger.Info("balancer ready", "algorithm", bal.Name())

	// ── Metrics ─────────────────────────────────────────────────────────
	m := metrics.New()

	// ── Circuit breakers ────────────────────────────────────────────────
	var cbRegistry *middleware.CircuitBreakerRegistry
	if cfg.CircuitBreaker.Enabled {
		cbRegistry = middleware.NewCircuitBreakerRegistry(
			cfg.CircuitBreaker.MaxFailures,
			cfg.CircuitBreaker.Timeout,
		)
		logger.Info("circuit breakers enabled",
			"max_failures", cfg.CircuitBreaker.MaxFailures,
			"timeout", cfg.CircuitBreaker.Timeout,
		)
	}

	// ── Health checker ──────────────────────────────────────────────────
	checker := health.New(health.Config{
		Pool:     pool,
		Interval: cfg.Health.Interval,
		Timeout:  cfg.Health.Timeout,
		Path:     cfg.Health.Path,
		Logger:   logger,
		OnChange: func(backendURL string, healthy bool) {
			val := 0.0
			if healthy {
				val = 1.0
			}
			m.BackendHealthy.WithLabelValues(backendURL).Set(val)
		},
	})

	// ── Proxy handler ────────────────────────────────────────────────────
	proxyHandler := proxy.New(proxy.Config{
		Balancer:        bal,
		CircuitBreakers: cbRegistry,
		MaxRetries:      2,
		Logger:          logger,
	}, pool)

	// ── Middleware stack ─────────────────────────────────────────────────
	// Stack from outermost to innermost:
	//   rate limiter → proxy
	// Metrics are emitted inside the proxy for accurate per-backend data.
	var handler http.Handler = proxyHandler
	if cfg.RateLimit.Enabled {
		handler = middleware.NewRateLimiter(
			cfg.RateLimit.Rate,
			cfg.RateLimit.Burst,
			logger,
			handler,
		)
		logger.Info("rate limiter enabled",
			"rate", cfg.RateLimit.Rate,
			"burst", cfg.RateLimit.Burst,
		)
	}

	// ── HTTP servers ─────────────────────────────────────────────────────
	// Two servers: the proxy (main traffic) and optionally the metrics endpoint.
	// Separating them prevents a scrape from timing out during high load.
	mux := http.NewServeMux()
	mux.Handle("/", handler)

	proxyServer := &http.Server{
		Addr:         cfg.Server.Address,
		Handler:      mux,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	var metricsServer *http.Server
	if cfg.Metrics.Enabled {
		metricsMux := http.NewServeMux()
		metricsMux.Handle(cfg.Metrics.Path, m.Handler())
		metricsServer = &http.Server{
			Addr:        ":9090",
			Handler:     metricsMux,
			ReadTimeout: 5 * time.Second,
		}
	}

	// ── Graceful shutdown ────────────────────────────────────────────────
	// We use a single context that gets cancelled on SIGINT/SIGTERM.
	// The health checker and both servers all respect this context.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start health checker in a background goroutine.
	go checker.Run(ctx)
	logger.Info("health checker started",
		"interval", cfg.Health.Interval,
		"path", cfg.Health.Path,
	)

	// Start proxy server.
	go func() {
		logger.Info("proxy listening", "address", cfg.Server.Address)
		if err := proxyServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("proxy server error", "err", err)
		}
	}()

	// Start metrics server (optional).
	if metricsServer != nil {
		go func() {
			logger.Info("metrics listening", "address", metricsServer.Addr, "path", cfg.Metrics.Path)
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server error", "err", err)
			}
		}()
	}

	// Block until signal received.
	<-ctx.Done()
	logger.Info("shutdown signal received, draining connections...")

	// Give in-flight requests 30 seconds to complete before hard-closing.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := proxyServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("proxy server shutdown: %w", err)
	}
	if metricsServer != nil {
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("metrics server shutdown: %w", err)
		}
	}

	logger.Info("shutdown complete")
	return nil
}
