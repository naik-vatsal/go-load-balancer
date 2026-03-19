package balancer_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/naik-vatsal/go-load-balancer/balancer"
)

// newTestPool is a helper that builds a Pool from plain URL strings.
// All weights default to 1 unless provided.
func newTestPool(t *testing.T, urls []string, weights []int) *balancer.Pool {
	t.Helper()
	pool, err := balancer.NewPool(urls, weights)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return pool
}

// setAllHealthy marks every backend in pool as healthy (true) or not.
func setAllHealthy(pool *balancer.Pool, healthy bool) {
	for _, b := range pool.Backends() {
		b.SetHealthy(healthy)
	}
}

// ── Round-Robin ──────────────────────────────────────────────────────────────

func TestRoundRobin_CyclesInOrder(t *testing.T) {
	urls := []string{
		"http://backend1:8080",
		"http://backend2:8080",
		"http://backend3:8080",
	}
	pool := newTestPool(t, urls, nil)
	rr := balancer.NewRoundRobin(pool)

	// Over 2 full cycles the selected URLs should repeat in order.
	want := []string{
		"http://backend1:8080",
		"http://backend2:8080",
		"http://backend3:8080",
		"http://backend1:8080",
		"http://backend2:8080",
		"http://backend3:8080",
	}
	for i, w := range want {
		b, err := rr.Next()
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if got := b.URL.String(); got != w {
			t.Errorf("call %d: got %q, want %q", i, got, w)
		}
	}
}

func TestRoundRobin_SkipsUnhealthyBackends(t *testing.T) {
	urls := []string{
		"http://a:8080",
		"http://b:8080", // will be marked unhealthy
		"http://c:8080",
	}
	pool := newTestPool(t, urls, nil)
	// Mark backend b unhealthy.
	pool.Backends()[1].SetHealthy(false)

	rr := balancer.NewRoundRobin(pool)
	for i := 0; i < 6; i++ {
		b, err := rr.Next()
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if b.URL.Host == "b:8080" {
			t.Errorf("call %d: selected unhealthy backend b", i)
		}
	}
}

func TestRoundRobin_NoHealthyBackends(t *testing.T) {
	pool := newTestPool(t, []string{"http://a:8080"}, nil)
	setAllHealthy(pool, false)

	rr := balancer.NewRoundRobin(pool)
	_, err := rr.Next()
	if err != balancer.ErrNoHealthyBackend {
		t.Errorf("expected ErrNoHealthyBackend, got %v", err)
	}
}

func TestRoundRobin_ConcurrentSafety(t *testing.T) {
	urls := []string{"http://a:8080", "http://b:8080", "http://c:8080"}
	pool := newTestPool(t, urls, nil)
	rr := balancer.NewRoundRobin(pool)

	const goroutines = 100
	const callsEach = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range callsEach {
				if _, err := rr.Next(); err != nil {
					t.Errorf("concurrent Next: %v", err)
				}
			}
		}()
	}
	wg.Wait()
}

// ── Least-Connections ────────────────────────────────────────────────────────

func TestLeastConn_PicksLowestConnCount(t *testing.T) {
	urls := []string{"http://a:8080", "http://b:8080", "http://c:8080"}
	pool := newTestPool(t, urls, nil)
	backends := pool.Backends()

	// Simulate: a=5 conns, b=1 conn, c=3 conns.
	for range 5 {
		backends[0].IncrConns()
	}
	backends[1].IncrConns()
	for range 3 {
		backends[2].IncrConns()
	}

	lc := balancer.NewLeastConn(pool)
	b, err := lc.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.URL.Host != "b:8080" {
		t.Errorf("expected b:8080 (least conns), got %s", b.URL.Host)
	}
}

func TestLeastConn_NoHealthyBackends(t *testing.T) {
	pool := newTestPool(t, []string{"http://a:8080"}, nil)
	setAllHealthy(pool, false)

	lc := balancer.NewLeastConn(pool)
	_, err := lc.Next()
	if err != balancer.ErrNoHealthyBackend {
		t.Errorf("expected ErrNoHealthyBackend, got %v", err)
	}
}

func TestLeastConn_ConcurrentSafety(t *testing.T) {
	urls := []string{"http://a:8080", "http://b:8080"}
	pool := newTestPool(t, urls, nil)
	lc := balancer.NewLeastConn(pool)

	var wg sync.WaitGroup
	const goroutines = 50
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range 500 {
				b, err := lc.Next()
				if err != nil {
					t.Errorf("concurrent Next: %v", err)
					return
				}
				b.IncrConns()
				b.DecrConns()
			}
		}()
	}
	wg.Wait()
}

// ── Weighted Round-Robin ─────────────────────────────────────────────────────

func TestWeightedRoundRobin_DistributionMatchesWeights(t *testing.T) {
	urls := []string{"http://a:8080", "http://b:8080", "http://c:8080"}
	weights := []int{3, 1, 2} // a gets 3/6, b gets 1/6, c gets 2/6 of requests
	pool := newTestPool(t, urls, weights)
	wrr := balancer.NewWeightedRoundRobin(pool)

	counts := map[string]int{}
	const total = 600
	for range total {
		b, err := wrr.Next()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[b.URL.Host]++
	}

	// Allow ±5% tolerance around the ideal distribution.
	want := map[string]int{
		"a:8080": 300, // 3/6 * 600
		"b:8080": 100, // 1/6 * 600
		"c:8080": 200, // 2/6 * 600
	}
	tolerance := 30 // 5% of 600
	for host, wantCount := range want {
		got := counts[host]
		diff := got - wantCount
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			t.Errorf("host %s: got %d requests, want %d ±%d", host, got, wantCount, tolerance)
		}
	}
}

func TestWeightedRoundRobin_NoHealthyBackends(t *testing.T) {
	pool := newTestPool(t, []string{"http://a:8080"}, []int{1})
	setAllHealthy(pool, false)

	wrr := balancer.NewWeightedRoundRobin(pool)
	_, err := wrr.Next()
	if err != balancer.ErrNoHealthyBackend {
		t.Errorf("expected ErrNoHealthyBackend, got %v", err)
	}
}

func TestWeightedRoundRobin_ConcurrentSafety(t *testing.T) {
	urls := []string{"http://a:8080", "http://b:8080"}
	pool := newTestPool(t, urls, []int{2, 1})
	wrr := balancer.NewWeightedRoundRobin(pool)

	var wg sync.WaitGroup
	const goroutines = 50
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range 200 {
				if _, err := wrr.Next(); err != nil {
					t.Errorf("concurrent Next: %v", err)
				}
			}
		}()
	}
	wg.Wait()
}

// ── Pool ────────────────────────────────────────────────────────────────────

func TestPool_InvalidURL(t *testing.T) {
	_, err := balancer.NewPool([]string{"://bad-url"}, nil)
	if err == nil {
		t.Error("expected error for invalid URL, got nil")
	}
}

func TestPool_HealthyBackends_FiltersCorrectly(t *testing.T) {
	urls := make([]string, 5)
	for i := range urls {
		urls[i] = fmt.Sprintf("http://host%d:8080", i)
	}
	pool := newTestPool(t, urls, nil)

	// Mark indices 1 and 3 unhealthy.
	backends := pool.Backends()
	backends[1].SetHealthy(false)
	backends[3].SetHealthy(false)

	healthy := pool.HealthyBackends()
	if len(healthy) != 3 {
		t.Errorf("expected 3 healthy backends, got %d", len(healthy))
	}
}

func TestBackend_ConnCountNeverNegative(t *testing.T) {
	pool := newTestPool(t, []string{"http://a:8080"}, nil)
	b := pool.Backends()[0]

	// DecrConns on zero-count backend should not go negative.
	b.DecrConns()
	if c := b.GetActiveConns(); c != 0 {
		t.Errorf("expected 0, got %d", c)
	}
}
