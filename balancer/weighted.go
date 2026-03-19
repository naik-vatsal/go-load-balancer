// Weighted round-robin assigns traffic proportionally to each backend's
// weight. A backend with weight 3 receives 3× as many requests as one
// with weight 1.
//
// Implementation: Smooth Weighted Round-Robin (SWRR), the same
// algorithm used by Nginx. Unlike naive approaches that replicate
// backends N times into a slice, SWRR uses O(n) memory and distributes
// load smoothly without long runs to the same backend.
//
// Algorithm per-call:
//  1. Add each backend's weight to its current effective weight.
//  2. Pick the backend with the highest effective weight.
//  3. Subtract the total weight from the winner's effective weight.
//
// This guarantees that over any window of sum(weights) requests the
// distribution is exactly proportional to the configured weights.
package balancer

import (
	"sync"
)

// weightedBackend extends Backend with the mutable effective-weight
// used by SWRR. We keep it separate so the core Backend struct stays
// algorithm-agnostic.
type weightedBackend struct {
	backend         *Backend
	currentWeight   int // mutable; protected by WeightedRoundRobin.mu
}

// WeightedRoundRobin implements Smooth Weighted Round-Robin.
type WeightedRoundRobin struct {
	pool     *Pool
	mu       sync.Mutex
	wBackends []*weightedBackend // parallel to pool backends, stable order
}

// NewWeightedRoundRobin creates a WeightedRoundRobin balancer.
// wBackends mirrors the pool's backend slice so effective weights are
// preserved across calls.
func NewWeightedRoundRobin(pool *Pool) *WeightedRoundRobin {
	backends := pool.Backends()
	wb := make([]*weightedBackend, len(backends))
	for i, b := range backends {
		wb[i] = &weightedBackend{backend: b}
	}
	return &WeightedRoundRobin{pool: pool, wBackends: wb}
}

// Next picks the next backend using the SWRR algorithm, considering
// only healthy backends. The mutex is held only for the short
// arithmetic section — not across the actual HTTP proxy.
func (w *WeightedRoundRobin) Next() (*Backend, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Build the set of healthy weighted backends for this call.
	// We must check health inside the lock so currentWeight updates
	// remain consistent with the healthy subset.
	var eligible []*weightedBackend
	totalWeight := 0
	for _, wb := range w.wBackends {
		if wb.backend.IsHealthy() {
			eligible = append(eligible, wb)
			totalWeight += wb.backend.Weight
		}
	}

	if len(eligible) == 0 {
		return nil, ErrNoHealthyBackend
	}

	// Step 1: bump each eligible backend's currentWeight by its weight.
	for _, wb := range eligible {
		wb.currentWeight += wb.backend.Weight
	}

	// Step 2: pick the backend with the highest currentWeight.
	best := eligible[0]
	for _, wb := range eligible[1:] {
		if wb.currentWeight > best.currentWeight {
			best = wb
		}
	}

	// Step 3: subtract total weight from the winner so future calls
	// don't keep selecting it.
	best.currentWeight -= totalWeight

	return best.backend, nil
}

// Name implements Balancer.
func (w *WeightedRoundRobin) Name() string { return "weighted_round_robin" }
