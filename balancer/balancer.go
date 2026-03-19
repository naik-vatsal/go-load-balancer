// Package balancer defines the core abstractions for the load-balancing
// layer. The central design decision is the Balancer interface: every
// algorithm (round-robin, least-conn, weighted) satisfies it, so the
// proxy can swap strategies without knowing their internals.
package balancer

import (
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
)

// ErrNoHealthyBackend is returned when every backend in the pool is
// currently marked unhealthy. The proxy uses this sentinel to decide
// whether to return a 502 or retry.
var ErrNoHealthyBackend = errors.New("balancer: no healthy backend available")

// Balancer is the single interface every algorithm must implement.
// Next() picks the backend for the current request; it must be safe
// to call from multiple goroutines concurrently.
type Balancer interface {
	Next() (*Backend, error)
	// Name returns the algorithm identifier, useful for logging/metrics.
	Name() string
}

// Backend represents one upstream server together with its runtime
// state (health, active connection count). We embed a sync.Mutex only
// inside the fields that need independent locking; the struct itself is
// accessed through the pool, which owns a separate read-write lock.
type Backend struct {
	URL    *url.URL
	Weight int // used by weighted round-robin; other algorithms ignore it

	// healthy is accessed via atomic load/store so health-checker
	// goroutines and proxy goroutines never need to hold a mutex just
	// to read or flip the flag.
	healthy atomic.Bool

	// mu protects ActiveConns — the only field written at request time.
	mu          sync.Mutex
	ActiveConns int
}

// IsHealthy returns true when the backend is accepting requests.
// Uses an atomic read so no lock is needed on the hot path.
func (b *Backend) IsHealthy() bool {
	return b.healthy.Load()
}

// SetHealthy atomically updates the health flag. Called exclusively
// by the health checker, which may run in its own goroutine.
func (b *Backend) SetHealthy(healthy bool) {
	b.healthy.Store(healthy)
}

// IncrConns increments the active connection counter. Called just
// before forwarding a request so least-conn has an accurate view.
func (b *Backend) IncrConns() {
	b.mu.Lock()
	b.ActiveConns++
	b.mu.Unlock()
}

// DecrConns decrements the active connection counter. Always called
// via defer so it fires even when the upstream returns an error.
func (b *Backend) DecrConns() {
	b.mu.Lock()
	if b.ActiveConns > 0 {
		b.ActiveConns--
	}
	b.mu.Unlock()
}

// GetActiveConns returns a snapshot of the current connection count.
// The caller must treat this as advisory — it may have already changed.
func (b *Backend) GetActiveConns() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ActiveConns
}

// Pool is the shared collection of backends. A single Pool is created
// at startup and shared between the Balancer, the health checker, and
// the metrics layer — each accessing it through its own interface.
//
// We use a sync.RWMutex so that the common case (reading healthy
// backends) never blocks on writes (health-status updates), which are
// rare compared to request throughput.
type Pool struct {
	mu       sync.RWMutex
	backends []*Backend
}

// NewPool constructs a Pool from raw URL strings and weights.
// All backends start healthy=true; the health checker will correct
// this within its first interval if a backend is actually down.
func NewPool(urls []string, weights []int) (*Pool, error) {
	backends := make([]*Backend, 0, len(urls))
	for i, rawURL := range urls {
		u, err := url.Parse(rawURL)
		if err != nil {
			return nil, errors.New("balancer: invalid backend URL " + rawURL + ": " + err.Error())
		}
		w := 1
		if i < len(weights) && weights[i] > 0 {
			w = weights[i]
		}
		b := &Backend{
			URL:    u,
			Weight: w,
		}
		b.SetHealthy(true) // optimistic start
		backends = append(backends, b)
	}
	return &Pool{backends: backends}, nil
}

// Backends returns a snapshot of all backends (healthy or not).
// Callers that only want healthy backends should filter themselves.
func (p *Pool) Backends() []*Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	// Return a copy of the slice header so the caller can iterate
	// without holding the lock, but the underlying Backend pointers
	// are shared — that is intentional for health updates.
	out := make([]*Backend, len(p.backends))
	copy(out, p.backends)
	return out
}

// HealthyBackends returns only the backends currently marked healthy.
func (p *Pool) HealthyBackends() []*Backend {
	all := p.Backends()
	out := make([]*Backend, 0, len(all))
	for _, b := range all {
		if b.IsHealthy() {
			out = append(out, b)
		}
	}
	return out
}
