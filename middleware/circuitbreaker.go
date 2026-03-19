// Circuit breaker per-backend implementation.
//
// A circuit breaker prevents cascading failures: when a backend becomes
// unhealthy, continuing to send requests wastes resources and adds
// latency for end users. The breaker automatically stops traffic to the
// failing backend and periodically checks if it has recovered.
//
// State machine:
//
//	Closed ──(maxFailures consecutive failures)──► Open
//	Open   ──(timeout elapsed)────────────────────► Half-Open
//	Half-Open ──(success)──────────────────────────► Closed
//	Half-Open ──(failure)──────────────────────────► Open
//
// "Closed" means the circuit is complete and traffic flows normally.
// "Open" means the circuit is broken and all requests are rejected.
// "Half-Open" allows a single probe request to test recovery.
package middleware

import (
	"sync"
	"time"
)

// state represents the current circuit breaker state.
type state int

const (
	stateClosed   state = iota // normal operation
	stateOpen                  // failing — reject all requests
	stateHalfOpen              // testing — allow one probe
)

// CircuitBreaker protects a single backend. It is safe for concurrent use.
type CircuitBreaker struct {
	maxFailures int
	timeout     time.Duration // how long the breaker stays Open before moving to Half-Open

	mu           sync.Mutex
	currentState state
	failures     int       // consecutive failures in Closed state
	openedAt     time.Time // when the breaker last transitioned to Open
}

// newCircuitBreaker creates a CircuitBreaker with the given thresholds.
func newCircuitBreaker(maxFailures int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		maxFailures:  maxFailures,
		timeout:      timeout,
		currentState: stateClosed,
	}
}

// Allow returns true if the circuit breaker permits a request to proceed.
//   - Closed: always allows.
//   - Open: allows only if the timeout has elapsed (transitions to Half-Open).
//   - Half-Open: allows exactly one probe request.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.currentState {
	case stateClosed:
		return true

	case stateOpen:
		if time.Since(cb.openedAt) >= cb.timeout {
			// Timeout elapsed — allow one probe to test recovery.
			cb.currentState = stateHalfOpen
			return true
		}
		return false

	case stateHalfOpen:
		// Only allow the single probe that triggered the Half-Open transition.
		// Subsequent calls block while the probe is in-flight.
		return false

	default:
		return false
	}
}

// RecordSuccess is called after a successful upstream response. It
// transitions the breaker back to Closed and resets the failure counter.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.currentState = stateClosed
}

// RecordFailure is called after a failed upstream response. Once
// consecutive failures exceed maxFailures, the breaker opens.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	if cb.currentState == stateHalfOpen || cb.failures >= cb.maxFailures {
		cb.currentState = stateOpen
		cb.openedAt = time.Now()
		cb.failures = 0
	}
}

// State returns a human-readable state name for logging/metrics.
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.currentState {
	case stateClosed:
		return "closed"
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// CircuitBreakerRegistry holds one CircuitBreaker per backend URL.
// The proxy uses Get(backendURL) to retrieve the appropriate breaker
// before forwarding a request.
type CircuitBreakerRegistry struct {
	mu          sync.RWMutex
	breakers    map[string]*CircuitBreaker
	maxFailures int
	timeout     time.Duration
}

// NewCircuitBreakerRegistry creates a registry. Breakers are created
// lazily on the first Get call for a given backend URL.
func NewCircuitBreakerRegistry(maxFailures int, timeout time.Duration) *CircuitBreakerRegistry {
	return &CircuitBreakerRegistry{
		breakers:    make(map[string]*CircuitBreaker),
		maxFailures: maxFailures,
		timeout:     timeout,
	}
}

// Get returns the CircuitBreaker for backendURL, creating it if needed.
// We double-check under the write lock to avoid creating duplicate breakers
// in a race between two goroutines calling Get simultaneously.
func (r *CircuitBreakerRegistry) Get(backendURL string) *CircuitBreaker {
	r.mu.RLock()
	if cb, ok := r.breakers[backendURL]; ok {
		r.mu.RUnlock()
		return cb
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	// Double-checked locking: another goroutine may have created it.
	if cb, ok := r.breakers[backendURL]; ok {
		return cb
	}
	cb := newCircuitBreaker(r.maxFailures, r.timeout)
	r.breakers[backendURL] = cb
	return cb
}

// All returns a copy of the current breaker map for metrics/debug use.
func (r *CircuitBreakerRegistry) All() map[string]*CircuitBreaker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]*CircuitBreaker, len(r.breakers))
	for k, v := range r.breakers {
		out[k] = v
	}
	return out
}
