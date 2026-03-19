// Package middleware provides HTTP middleware that can be layered around
// the proxy handler. Middlewares are plain http.Handler wrappers so they
// compose cleanly with any standard-library server.
package middleware

import (
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// tokenBucket implements a basic token-bucket rate limiter for a single
// key (e.g. a client IP address). It is not exported; callers interact
// through RateLimiter.
//
// Why token bucket?  It allows short bursts above the steady-state rate
// (up to Burst tokens), which is more user-friendly than a strict
// fixed-window counter that punishes the first request after a quiet
// period. Nginx, AWS API Gateway, and most production systems use the
// same algorithm.
type tokenBucket struct {
	tokens     float64
	maxTokens  float64 // = burst size
	refillRate float64 // tokens per nanosecond
	lastRefill time.Time
	mu         sync.Mutex
}

// allow returns true if the bucket has at least one token, consuming it.
// It first refills based on elapsed time since the last call.
func (tb *tokenBucket) allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	// Add tokens proportional to elapsed time, capped at maxTokens.
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}

	if tb.tokens < 1 {
		return false
	}
	tb.tokens--
	return true
}

// RateLimiter is an HTTP middleware that enforces a per-client-IP
// token-bucket rate limit. Each unique IP gets its own independent
// bucket, so one heavy client cannot affect others.
type RateLimiter struct {
	rate   float64 // tokens/second — steady-state request rate
	burst  int     // maximum bucket capacity
	next   http.Handler
	logger *slog.Logger

	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

// NewRateLimiter wraps next with per-IP rate limiting.
//   - rate: maximum sustained requests per second per IP.
//   - burst: maximum requests allowed in a sudden spike (bucket size).
func NewRateLimiter(rate float64, burst int, logger *slog.Logger, next http.Handler) *RateLimiter {
	if logger == nil {
		logger = slog.Default()
	}
	return &RateLimiter{
		rate:    rate,
		burst:   burst,
		next:    next,
		logger:  logger,
		buckets: make(map[string]*tokenBucket),
	}
}

// ServeHTTP checks the client's token bucket and either forwards the
// request or responds with 429 Too Many Requests.
func (rl *RateLimiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	bucket := rl.getOrCreate(ip)

	if !bucket.allow() {
		rl.logger.Warn("rate limit exceeded", "ip", ip, "path", r.URL.Path)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	rl.next.ServeHTTP(w, r)
}

// getOrCreate returns the token bucket for ip, creating it on first use.
// The bucket map is protected by a mutex; buckets themselves use their
// own mutex for allow() calls, so the lock here is only held briefly.
func (rl *RateLimiter) getOrCreate(ip string) *tokenBucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if tb, ok := rl.buckets[ip]; ok {
		return tb
	}
	tb := &tokenBucket{
		tokens:     float64(rl.burst), // start full so first requests aren't throttled
		maxTokens:  float64(rl.burst),
		refillRate: rl.rate,
		lastRefill: time.Now(),
	}
	rl.buckets[ip] = tb
	return tb
}

// clientIP extracts the real client IP from the request, honouring
// X-Forwarded-For when the load balancer itself sits behind a proxy.
func clientIP(r *http.Request) string {
	// X-Forwarded-For: client, proxy1, proxy2 — first entry is the origin.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take only the first (leftmost) address.
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return xff[:i]
			}
		}
		return xff
	}
	// Fall back to RemoteAddr (strips the port).
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
