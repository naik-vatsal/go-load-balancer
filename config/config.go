// Package config loads and validates the YAML configuration file.
// We keep all runtime knobs in one place so operators only need to
// edit a single file — no recompilation required.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration structure. Every field maps
// directly to a YAML key so the file serves as self-documentation.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Backends []Backend      `yaml:"backends"`
	Balancer BalancerConfig `yaml:"balancer"`
	Health   HealthConfig   `yaml:"health"`
	RateLimit RateLimitConfig `yaml:"rate_limit"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
	Metrics  MetricsConfig  `yaml:"metrics"`
}

// ServerConfig controls the listening address and connection timeouts.
type ServerConfig struct {
	Address      string        `yaml:"address"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
}

// Backend represents a single upstream server.
// Weight is only used by the weighted round-robin algorithm; other
// algorithms ignore it.
type Backend struct {
	URL    string `yaml:"url"`
	Weight int    `yaml:"weight"` // 1–100, default 1
}

// BalancerConfig selects which load-balancing algorithm to use.
// Valid values: "round_robin", "least_conn", "weighted_round_robin"
type BalancerConfig struct {
	Algorithm string `yaml:"algorithm"`
}

// HealthConfig controls the background health-check loop.
// Interval is how often each backend is probed; Timeout is the
// per-probe deadline so a slow backend doesn't stall the checker.
type HealthConfig struct {
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
	Path     string        `yaml:"path"` // HTTP path to probe, e.g. "/health"
}

// RateLimitConfig configures the per-client-IP token-bucket limiter.
// Rate is tokens added per second; Burst is the maximum bucket size.
type RateLimitConfig struct {
	Enabled bool    `yaml:"enabled"`
	Rate    float64 `yaml:"rate"`  // tokens/second
	Burst   int     `yaml:"burst"` // max burst size
}

// CircuitBreakerConfig controls the per-backend circuit breaker.
// After MaxFailures consecutive failures the breaker opens for
// Timeout, then moves to half-open to test recovery.
type CircuitBreakerConfig struct {
	Enabled     bool          `yaml:"enabled"`
	MaxFailures int           `yaml:"max_failures"`
	Timeout     time.Duration `yaml:"timeout"` // how long the breaker stays open
}

// MetricsConfig controls the Prometheus metrics endpoint.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"` // default "/metrics"
}

// Load reads the YAML file at path, decodes it into Config, then
// validates the result. Returning a descriptive error here saves
// operators from cryptic panics at startup.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: open %q: %w", path, err)
	}
	defer f.Close()

	var cfg Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true) // reject unknown keys — catches typos early
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: decode %q: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: validate: %w", err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

// validate checks that required fields are present and values are sane.
// We fail fast here rather than allowing a misconfigured binary to start.
func (c *Config) validate() error {
	if c.Server.Address == "" {
		return fmt.Errorf("server.address is required")
	}
	if len(c.Backends) == 0 {
		return fmt.Errorf("at least one backend is required")
	}
	for i, b := range c.Backends {
		if b.URL == "" {
			return fmt.Errorf("backends[%d].url is required", i)
		}
	}

	valid := map[string]bool{
		"round_robin":         true,
		"least_conn":          true,
		"weighted_round_robin": true,
	}
	if c.Balancer.Algorithm != "" && !valid[c.Balancer.Algorithm] {
		return fmt.Errorf("balancer.algorithm %q is not valid (choose round_robin, least_conn, or weighted_round_robin)", c.Balancer.Algorithm)
	}

	if c.RateLimit.Enabled {
		if c.RateLimit.Rate <= 0 {
			return fmt.Errorf("rate_limit.rate must be > 0")
		}
		if c.RateLimit.Burst <= 0 {
			return fmt.Errorf("rate_limit.burst must be > 0")
		}
	}

	if c.CircuitBreaker.Enabled {
		if c.CircuitBreaker.MaxFailures <= 0 {
			return fmt.Errorf("circuit_breaker.max_failures must be > 0")
		}
	}

	return nil
}

// applyDefaults fills in zero values with sensible production-safe
// defaults so the config file can stay concise for simple use cases.
func (c *Config) applyDefaults() {
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = 30 * time.Second
	}
	if c.Server.WriteTimeout == 0 {
		c.Server.WriteTimeout = 30 * time.Second
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = 90 * time.Second
	}
	if c.Balancer.Algorithm == "" {
		c.Balancer.Algorithm = "round_robin"
	}
	if c.Health.Interval == 0 {
		c.Health.Interval = 10 * time.Second
	}
	if c.Health.Timeout == 0 {
		c.Health.Timeout = 2 * time.Second
	}
	if c.Health.Path == "" {
		c.Health.Path = "/health"
	}
	if c.Metrics.Path == "" {
		c.Metrics.Path = "/metrics"
	}
	if c.CircuitBreaker.Timeout == 0 {
		c.CircuitBreaker.Timeout = 60 * time.Second
	}
	// Normalise backend weights: 0 → 1 so weighted algorithm is safe.
	for i := range c.Backends {
		if c.Backends[i].Weight <= 0 {
			c.Backends[i].Weight = 1
		}
	}
}
