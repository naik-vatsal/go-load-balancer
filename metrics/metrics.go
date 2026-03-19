// Package metrics registers and exposes Prometheus metrics for the
// load balancer. We use the standard prometheus/client_golang library
// and expose everything on a dedicated /metrics HTTP endpoint so
// Prometheus can scrape it without touching the proxy traffic path.
//
// All metrics are namespaced under "lb_" to avoid collisions in a
// shared Prometheus instance.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all registered Prometheus collectors. Passing this
// struct instead of using package-level variables makes it easy to
// create isolated metric sets in tests.
type Metrics struct {
	// RequestsTotal counts every request forwarded to an upstream,
	// labelled by backend host and HTTP method.
	RequestsTotal *prometheus.CounterVec

	// RequestDuration is a histogram of end-to-end proxy latency in
	// seconds, labelled by backend and response status code.
	RequestDuration *prometheus.HistogramVec

	// ActiveConnections is a gauge showing how many upstream connections
	// are currently in-flight per backend.
	ActiveConnections *prometheus.GaugeVec

	// BackendHealthy is a gauge that is 1 when a backend is healthy and
	// 0 when it is not. Useful for alerting.
	BackendHealthy *prometheus.GaugeVec

	// CircuitBreakerState tracks the state of each per-backend circuit
	// breaker as a gauge: 0=closed, 1=open, 2=half_open.
	CircuitBreakerState *prometheus.GaugeVec

	// RateLimitedTotal counts requests rejected by the rate limiter,
	// labelled by client IP.
	RateLimitedTotal *prometheus.CounterVec

	// RetriesTotal counts the number of retried requests, labelled by
	// backend, so operators can spot unreliable upstreams quickly.
	RetriesTotal *prometheus.CounterVec

	registry *prometheus.Registry
}

// New creates and registers all metrics with a fresh registry. Using
// a non-default registry avoids polluting the global state in tests.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lb",
				Name:      "requests_total",
				Help:      "Total number of requests forwarded to a backend.",
			},
			[]string{"backend", "method"},
		),

		RequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "lb",
				Name:      "request_duration_seconds",
				Help:      "End-to-end latency of proxied requests in seconds.",
				// Buckets cover 1ms → 10s, which is wide enough for most APIs.
				Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"backend", "status"},
		),

		ActiveConnections: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "lb",
				Name:      "active_connections",
				Help:      "Number of requests currently being proxied per backend.",
			},
			[]string{"backend"},
		),

		BackendHealthy: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "lb",
				Name:      "backend_healthy",
				Help:      "1 if the backend is healthy, 0 otherwise.",
			},
			[]string{"backend"},
		),

		CircuitBreakerState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "lb",
				Name:      "circuit_breaker_state",
				Help:      "Circuit breaker state per backend: 0=closed, 1=open, 2=half_open.",
			},
			[]string{"backend"},
		),

		RateLimitedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lb",
				Name:      "rate_limited_total",
				Help:      "Total number of requests rejected by the rate limiter.",
			},
			[]string{"client_ip"},
		),

		RetriesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lb",
				Name:      "retries_total",
				Help:      "Total number of retried upstream requests.",
			},
			[]string{"backend"},
		),

		registry: reg,
	}

	// Register all collectors. We panic here intentionally — if metrics
	// fail to register the binary is misconfigured and should not start.
	reg.MustRegister(
		m.RequestsTotal,
		m.RequestDuration,
		m.ActiveConnections,
		m.BackendHealthy,
		m.CircuitBreakerState,
		m.RateLimitedTotal,
		m.RetriesTotal,
		// Standard Go runtime and process metrics.
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	return m
}

// Handler returns an http.Handler that serves the Prometheus metrics
// page for this metrics instance's registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true, // enables Content-Type negotiation for OpenMetrics format
	})
}

// CircuitBreakerStateValue maps state strings to numeric gauge values.
func CircuitBreakerStateValue(state string) float64 {
	switch state {
	case "open":
		return 1
	case "half_open":
		return 2
	default: // "closed" or unknown
		return 0
	}
}
