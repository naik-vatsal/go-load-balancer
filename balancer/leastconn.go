// Least-connections routes each new request to the backend that
// currently has the fewest active connections. This is more adaptive
// than round-robin when backends have heterogeneous response times:
// a slow backend naturally receives fewer new requests because its
// connection count stays elevated longer.
//
// Trade-off: we must scan all healthy backends on every call — O(n).
// For typical pools (< 100 backends) this is negligible; for very large
// pools a min-heap would reduce this to O(log n), but adds complexity
// that isn't justified here.
package balancer

// LeastConn routes to the backend with the fewest active connections.
type LeastConn struct {
	pool *Pool
}

// NewLeastConn creates a LeastConn balancer backed by pool.
func NewLeastConn(pool *Pool) *LeastConn {
	return &LeastConn{pool: pool}
}

// Next returns the healthy backend with the lowest active connection
// count. If multiple backends are tied, the first one in the list wins
// (which gives round-robin behaviour when the pool is idle — a nice
// property that prevents permanent hot-spotting at zero load).
func (lc *LeastConn) Next() (*Backend, error) {
	backends := lc.pool.HealthyBackends()
	if len(backends) == 0 {
		return nil, ErrNoHealthyBackend
	}

	best := backends[0]
	bestConns := best.GetActiveConns()

	for _, b := range backends[1:] {
		if c := b.GetActiveConns(); c < bestConns {
			best = b
			bestConns = c
		}
	}
	return best, nil
}

// Name implements Balancer.
func (lc *LeastConn) Name() string { return "least_conn" }
