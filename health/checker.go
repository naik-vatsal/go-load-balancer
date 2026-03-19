// Package health implements a background goroutine that periodically
// probes each backend and updates its health flag. The proxy never
// touches a backend that the checker has marked unhealthy, which means
// a crashed upstream is removed from rotation within one interval
// without any operator intervention.
//
// Design choice: we use one goroutine per backend rather than a single
// sequential loop. This ensures a slow/hung backend doesn't delay
// health updates for healthy ones. The overhead is negligible for
// typical pool sizes (< 100 backends).
package health

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/naik-vatsal/go-load-balancer/balancer"
)

// StatusChangeFunc is called whenever a backend transitions between
// healthy and unhealthy. Callers can use it to update metrics or log
// alerts without the checker needing to know about those systems.
type StatusChangeFunc func(backendURL string, healthy bool)

// Checker runs a health-check loop for every backend in the pool.
type Checker struct {
	pool      *balancer.Pool
	interval  time.Duration
	timeout   time.Duration
	path      string      // HTTP path to probe, e.g. "/health"
	client    *http.Client
	onChange  StatusChangeFunc // may be nil
	logger    *slog.Logger
}

// Config bundles all Checker constructor parameters so the signature
// stays readable as we add options.
type Config struct {
	Pool     *balancer.Pool
	Interval time.Duration
	Timeout  time.Duration
	Path     string
	OnChange StatusChangeFunc // optional; called on status transitions
	Logger   *slog.Logger     // optional; falls back to default logger
}

// New creates a Checker. Call Run to start the background goroutines.
func New(cfg Config) *Checker {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Checker{
		pool:     cfg.Pool,
		interval: cfg.Interval,
		timeout:  cfg.Timeout,
		path:     cfg.Path,
		// Dedicated client with a tight timeout so probes don't pile up
		// if an upstream is slow rather than completely dead.
		client: &http.Client{
			Timeout: cfg.Timeout,
			// Don't follow redirects — a redirect is not a healthy signal.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		onChange: cfg.OnChange,
		logger:   cfg.Logger,
	}
}

// Run starts one goroutine per backend that probes on the configured
// interval. It blocks until ctx is cancelled, then waits for all
// probe goroutines to finish before returning — making it safe to
// defer-cancel the context and then call Run in a goroutine.
func (c *Checker) Run(ctx context.Context) {
	backends := c.pool.Backends()
	var wg sync.WaitGroup
	wg.Add(len(backends))

	for _, b := range backends {
		go func(b *balancer.Backend) {
			defer wg.Done()
			c.runForBackend(ctx, b)
		}(b)
	}

	wg.Wait()
}

// runForBackend probes a single backend on every tick until ctx is done.
func (c *Checker) runForBackend(ctx context.Context, b *balancer.Backend) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Probe immediately on first tick rather than waiting a full interval.
	c.probe(b)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probe(b)
		}
	}
}

// probe performs a single HTTP GET to the backend's health path and
// updates the backend's healthy flag. Transitions are logged and the
// onChange callback is invoked so external systems can react.
func (c *Checker) probe(b *balancer.Backend) {
	targetURL := b.URL.JoinPath(c.path).String()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, targetURL, nil)
	if err != nil {
		// This should never happen for well-formed URLs loaded from config.
		c.logger.Error("health: failed to build request", "url", targetURL, "err", err)
		c.markUnhealthy(b)
		return
	}

	resp, err := c.client.Do(req)
	if err != nil {
		c.logger.Warn("health: probe failed", "backend", b.URL.Host, "err", err)
		c.markUnhealthy(b)
		return
	}
	// Drain and close to allow connection reuse — skipping this causes
	// the client to open a new TCP connection on every probe.
	resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		c.markHealthy(b)
	} else {
		c.logger.Warn("health: probe returned non-2xx", "backend", b.URL.Host, "status", resp.StatusCode)
		c.markUnhealthy(b)
	}
}

func (c *Checker) markHealthy(b *balancer.Backend) {
	wasHealthy := b.IsHealthy()
	b.SetHealthy(true)
	if !wasHealthy {
		c.logger.Info("health: backend recovered", "backend", b.URL.Host)
		if c.onChange != nil {
			c.onChange(b.URL.String(), true)
		}
	}
}

func (c *Checker) markUnhealthy(b *balancer.Backend) {
	wasHealthy := b.IsHealthy()
	b.SetHealthy(false)
	if wasHealthy {
		c.logger.Warn("health: backend marked unhealthy", "backend", b.URL.Host)
		if c.onChange != nil {
			c.onChange(b.URL.String(), false)
		}
	}
}
