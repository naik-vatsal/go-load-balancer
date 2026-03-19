// Round-robin is the simplest fair-distribution algorithm: it cycles
// through healthy backends in order, giving each one an equal share of
// traffic regardless of response time or active connections.
//
// We use an atomic counter instead of a mutex-protected int because
// the increment-and-mod operation on the counter itself is the only
// critical section, and atomic.AddUint64 is significantly cheaper than
// a mutex on a hot path with many concurrent goroutines.
package balancer

import (
	"sync/atomic"
)

// RoundRobin distributes requests across healthy backends in order.
type RoundRobin struct {
	pool    *Pool
	counter atomic.Uint64 // monotonically increasing, wraps via modulo
}

// NewRoundRobin creates a RoundRobin balancer backed by pool.
func NewRoundRobin(pool *Pool) *RoundRobin {
	return &RoundRobin{pool: pool}
}

// Next picks the next healthy backend. The counter advances regardless
// of whether the selected index is healthy; this ensures we make
// progress even when some backends are down rather than repeatedly
// retrying the same index.
//
// Time complexity: O(n) worst case when all but one backend is down,
// O(1) amortised when most backends are healthy.
func (rr *RoundRobin) Next() (*Backend, error) {
	backends := rr.pool.HealthyBackends()
	if len(backends) == 0 {
		return nil, ErrNoHealthyBackend
	}

	// Atomically increment and take modulo. Because len(backends) may
	// vary between calls (health changes), we re-fetch it each time
	// rather than caching it — correctness over micro-optimisation.
	idx := rr.counter.Add(1) - 1 // subtract 1 so we start at index 0
	return backends[idx%uint64(len(backends))], nil
}

// Name implements Balancer.
func (rr *RoundRobin) Name() string { return "round_robin" }
