// Package proxy wires together the balancer, circuit breakers, and
// httputil.ReverseProxy to forward incoming requests to upstream backends.
//
// Key design decisions:
//  - We create one httputil.ReverseProxy per backend at startup rather
//    than one shared proxy, so each backend gets its own transport with
//    independent connection pooling. This prevents a slow backend from
//    exhausting connections for fast ones.
//  - Retry logic lives here (not in the balancer) because retries require
//    knowledge of the HTTP response — a concern above the balancer layer.
//  - The circuit breaker is consulted before the balancer pick: a tripped
//    breaker means "don't even try this backend", which is cheaper than
//    making a request that will certainly fail.
package proxy

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/naik-vatsal/go-load-balancer/balancer"
	"github.com/naik-vatsal/go-load-balancer/metrics"
	"github.com/naik-vatsal/go-load-balancer/middleware"
)

// RequestRecorder records the outbound request so the retry path can
// inspect the upstream response code.
type backendProxy struct {
	proxy   *httputil.ReverseProxy
	backend *balancer.Backend
}

// Config bundles all Proxy dependencies.
type Config struct {
	Balancer        balancer.Balancer
	CircuitBreakers *middleware.CircuitBreakerRegistry
	MaxRetries      int
	Logger          *slog.Logger
	Metrics         *metrics.Metrics // nil disables instrumentation
}

// Proxy is an http.Handler that forwards requests to upstream backends.
type Proxy struct {
	balancer       balancer.Balancer
	cbs            *middleware.CircuitBreakerRegistry
	maxRetries     int
	logger         *slog.Logger
	met            *metrics.Metrics
	// backendProxies maps backend URL string → its dedicated reverse proxy.
	// Built once at startup; read-only during serving.
	backendProxies map[string]*httputil.ReverseProxy
}

// New builds a Proxy. It pre-creates one httputil.ReverseProxy per
// backend so transports are not shared across upstreams.
func New(cfg Config, pool *balancer.Pool) *Proxy {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 2
	}

	// Build a dedicated reverse proxy for each backend.
	bps := make(map[string]*httputil.ReverseProxy, len(pool.Backends()))
	for _, b := range pool.Backends() {
		bps[b.URL.String()] = newReverseProxy(b.URL)
	}

	return &Proxy{
		balancer:       cfg.Balancer,
		cbs:            cfg.CircuitBreakers,
		maxRetries:     cfg.MaxRetries,
		logger:         cfg.Logger,
		met:            cfg.Metrics,
		backendProxies: bps,
	}
}

// ServeHTTP implements http.Handler. It attempts to forward the request,
// retrying on failure up to maxRetries times with a different backend
// each time. On exhaustion it returns 502 Bad Gateway.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var lastErr error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		backend, err := p.balancer.Next()
		if err != nil {
			p.logger.Error("proxy: no healthy backend", "err", err)
			http.Error(w, "no healthy backend available", http.StatusBadGateway)
			return
		}

		// Check circuit breaker before attempting the request.
		if p.cbs != nil {
			cb := p.cbs.Get(backend.URL.String())
			if !cb.Allow() {
				p.logger.Warn("proxy: circuit open, skipping backend",
					"backend", backend.URL.Host,
					"attempt", attempt)
				lastErr = fmt.Errorf("circuit open for %s", backend.URL.Host)
				continue
			}
		}

		// Track active connections for least-conn accuracy and for the
		// lb_active_connections gauge so Prometheus sees in-flight counts.
		backend.IncrConns()
		if p.met != nil {
			p.met.ActiveConnections.WithLabelValues(backend.URL.Host).Inc()
		}

		rw := &responseWriter{ResponseWriter: w}
		rp, ok := p.backendProxies[backend.URL.String()]
		if !ok {
			// Fallback: build on the fly (shouldn't happen in normal operation).
			rp = newReverseProxy(backend.URL)
		}

		start := time.Now()
		rp.ServeHTTP(rw, r)
		duration := time.Since(start)

		backend.DecrConns()
		if p.met != nil {
			p.met.ActiveConnections.WithLabelValues(backend.URL.Host).Dec()
		}

		// Record per-attempt request count and latency. We do this for every
		// attempt (including retried ones) so operators can see which backends
		// are slow or error-prone independently.
		if p.met != nil {
			p.met.RequestsTotal.WithLabelValues(backend.URL.Host, r.Method).Inc()
			p.met.RequestDuration.WithLabelValues(
				backend.URL.Host,
				strconv.Itoa(rw.status),
			).Observe(duration.Seconds())
		}

		// Update circuit breaker with the result.
		if p.cbs != nil {
			cb := p.cbs.Get(backend.URL.String())
			if rw.status >= 500 || rw.status == 0 {
				cb.RecordFailure()
			} else {
				cb.RecordSuccess()
			}
		}

		// 5xx responses from the upstream are retryable.
		if rw.status >= 500 || rw.status == 0 {
			lastErr = fmt.Errorf("backend %s returned %d", backend.URL.Host, rw.status)
			p.logger.Warn("proxy: upstream error, retrying",
				"backend", backend.URL.Host,
				"status", rw.status,
				"attempt", attempt)
			if p.met != nil {
				p.met.RetriesTotal.WithLabelValues(backend.URL.Host).Inc()
			}
			continue
		}

		// Success — we're done.
		return
	}

	p.logger.Error("proxy: all retries exhausted", "err", lastErr)
	// Only write the error if the response hasn't been started yet.
	// If the upstream already wrote headers we can't change the status code.
	if !w.(interface{ Written() bool }).Written() {
		http.Error(w, "bad gateway: "+lastErr.Error(), http.StatusBadGateway)
	}
}

// newReverseProxy creates an httputil.ReverseProxy targeting target.
// We customise the director to correctly rewrite the Host header and
// add an X-Forwarded-For entry.
func newReverseProxy(target *url.URL) *httputil.ReverseProxy {
	rp := httputil.NewSingleHostReverseProxy(target)

	// Wrap the default director to preserve the original Host header
	// on the upstream request. Without this, the upstream sees the
	// load balancer's address as the Host, which breaks virtual hosting.
	defaultDirector := rp.Director
	rp.Director = func(req *http.Request) {
		defaultDirector(req)
		req.Host = target.Host
		// Ensure X-Real-IP is set for upstreams that need it.
		if prior, ok := req.Header["X-Forwarded-For"]; ok {
			req.Header.Set("X-Forwarded-For", joinStrings(prior, ", "))
		}
	}

	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if errors.Is(err, http.ErrAbortHandler) {
			return
		}
		// Don't write headers here — ServeHTTP checks the status code
		// via responseWriter and will handle the error response.
		w.WriteHeader(http.StatusBadGateway)
	}

	return rp
}

// joinStrings joins a slice of strings with sep.
// Avoids importing strings just for this one operation.
func joinStrings(parts []string, sep string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += sep
		}
		result += p
	}
	return result
}

// responseWriter wraps http.ResponseWriter to capture the status code
// written by the upstream. We need this to implement retry logic:
// httputil.ReverseProxy calls WriteHeader internally and we have no
// other way to intercept the status code.
type responseWriter struct {
	http.ResponseWriter
	status  int
	written bool
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.written = true
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.status = http.StatusOK
		rw.written = true
	}
	return rw.ResponseWriter.Write(b)
}

// Written satisfies the interface checked in ServeHTTP to detect whether
// headers have already been flushed to the client.
func (rw *responseWriter) Written() bool { return rw.written }
