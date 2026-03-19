# go-load-balancer

A layer-7 HTTP load balancer written in Go that distributes traffic across upstream backends with health checking, circuit breaking, rate limiting, and Prometheus observability — built from scratch using only the standard library and two dependencies.

---

## Architecture

```
                                  ┌──────────────────────────────────────────────────┐
                                  │                 go-load-balancer                  │
                                  │                                                    │
                                  │   ┌────────────────────────────────────────────┐  │
              HTTP request        │   │  Rate Limiter                              │  │
  Client ───────────────────────► │   │  Token bucket, per client IP               │  │
                                  │   │  429 Too Many Requests on exhaustion       │  │
                                  │   └────────────────────┬───────────────────────┘  │
                                  │                        │ allowed                   │
                                  │   ┌────────────────────▼───────────────────────┐  │
                                  │   │  Balancer                                  │  │
                                  │   │  round_robin / least_conn /                │  │
                                  │   │  weighted_round_robin                      │  │
                                  │   └──────┬─────────────┬─────────────┬─────────┘  │
                                  │          │             │             │             │
                                  │   ┌──────▼──┐   ┌─────▼───┐  ┌─────▼───┐        │
                                  │   │   CB #1  │   │   CB #2  │  │  CB #3  │        │
                                  │   │ Closed   │   │  Open    │  │ Closed  │        │
                                  │   └──────┬──┘   └─────┬────┘  └─────┬───┘        │
                                  │          │        rejected           │             │
                                  │   ┌──────▼──┐                 ┌─────▼───┐        │
                                  │   │ Proxy #1 │                 │ Proxy #3 │        │
                                  │   │ + Retry  │                 │ + Retry  │        │
                                  │   └──────┬──┘                 └─────┬───┘        │
                                  └──────────┼───────────────────────────┼────────────┘
                                             │                           │
                             ┌───────────────┘                           └───────────────┐
                             ▼                                                           ▼
                     ┌───────────────┐                                         ┌───────────────┐
                     │  Backend :8081 │                                         │  Backend :8083 │
                     │  weight=1      │                                         │  weight=2      │
                     └───────────────┘                                         └───────────────┘

  Background (one goroutine per backend):
  ┌──────────────────────────────────────────────────────────────────────────────────────────┐
  │  Health Checker  ──GET /health──►  backend  ──200 OK──►  SetHealthy(true)  (atomic)      │
  │                                                ──err──►  SetHealthy(false) (atomic)      │
  └──────────────────────────────────────────────────────────────────────────────────────────┘

  Observability (separate port, never blocks proxy traffic):
  ┌──────────────────────────────────────────────────────────────────────────────────────────┐
  │  :9090/metrics  ◄──scrape──  Prometheus  ──query──►  Grafana                            │
  └──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Package layout

```
go-load-balancer/
├── main.go                       # Startup wiring: config → pool → balancer → server
├── config/
│   └── config.go                 # YAML loader, strict field validation, defaults
├── balancer/
│   ├── balancer.go               # Balancer interface, Backend (atomic health), Pool
│   ├── roundrobin.go             # Atomic counter — zero mutex on hot path
│   ├── leastconn.go              # O(n) scan for minimum active connections
│   ├── weighted.go               # Smooth Weighted Round-Robin (Nginx algorithm)
│   └── balancer_test.go          # Table-driven tests + race detector
├── health/
│   └── checker.go                # One goroutine per backend, HTTP GET probe
├── proxy/
│   └── proxy.go                  # Per-backend ReverseProxy, retry on 5xx
├── middleware/
│   ├── ratelimit.go              # Token-bucket with per-IP bucket map
│   └── circuitbreaker.go        # Closed/Open/Half-Open state machine + registry
├── metrics/
│   └── metrics.go                # 7 Prometheus collectors, isolated registry
├── cmd/
│   └── backends/
│       └── main.go               # Three local test backends on :8081-:8083
├── grafana/
│   └── provisioning/
│       └── datasources/
│           └── prometheus.yaml   # Auto-wires Prometheus datasource into Grafana
├── config.yaml                   # Runtime configuration
├── docker-compose.yml            # Prometheus + Grafana stack
└── prometheus.yml                # Scrape config targeting :9090/metrics
```

---

## Features

| Feature | Implementation |
|---|---|
| **Round Robin** | Atomic `uint64` counter, no mutex on the request path |
| **Least Connections** | Scans healthy backends each request; picks minimum active conns |
| **Smooth Weighted Round Robin** | Nginx's SWRR algorithm; O(n) memory, smooth distribution |
| **Health checking** | One goroutine per backend; probes on configurable interval and path |
| **Circuit breaker** | Per-backend Closed → Open → Half-Open state machine |
| **Rate limiting** | Token-bucket per client IP; honours `X-Forwarded-For` |
| **Retry logic** | Retries 5xx responses against a different backend each time |
| **Prometheus metrics** | 7 metrics covering latency, errors, connection counts, and breaker state |
| **Grafana** | Datasource auto-provisioned; no manual UI setup |
| **Graceful shutdown** | 30-second drain on SIGINT/SIGTERM before hard close |
| **Structured logging** | JSON via `log/slog`; every log line carries structured key-value fields |
| **Single config file** | All knobs in `config.yaml`; validated at startup with descriptive errors |

---

## Running Locally

### Prerequisites

- Go 1.22+
- Docker + Docker Compose (optional, for Prometheus/Grafana)

### 1. Start the test backends

The repo includes three minimal HTTP servers that respond to `GET /` and `GET /health`:

```bash
go run ./cmd/backends
```

Expected output:
```
time=... level=INFO msg="backend listening" addr=:8081
time=... level=INFO msg="backend listening" addr=:8082
time=... level=INFO msg="backend listening" addr=:8083
```

Verify they are up:
```bash
curl http://localhost:8081/        # → "Response from backend :8081"
curl http://localhost:8082/health  # → 200 OK
curl http://localhost:8083/        # → "Response from backend :8083"
```

### 2. Start the load balancer

```bash
go run . -config config.yaml
```

Expected output:
```
{"level":"INFO","msg":"config loaded","algorithm":"weighted_round_robin","backends":3}
{"level":"INFO","msg":"balancer ready","algorithm":"weighted_round_robin"}
{"level":"INFO","msg":"circuit breakers enabled","max_failures":5,"timeout":30s}
{"level":"INFO","msg":"rate limiter enabled","rate":100,"burst":200}
{"level":"INFO","msg":"health checker started","interval":10s,"path":"/health"}
{"level":"INFO","msg":"metrics listening","address":":9090","path":"/metrics"}
{"level":"INFO","msg":"proxy listening","address":":8080"}
```

Send traffic through the balancer:
```bash
# Single request
curl http://localhost:8080/

# Watch weighted distribution across backends (backend :8083 gets ~2× the requests)
for i in $(seq 1 8); do curl -s http://localhost:8080/; done
```

### 3. Build a standalone binary

```bash
go build -o lb .
./lb -config config.yaml
```

### 4. Start Prometheus + Grafana

```bash
docker compose up -d
```

| Service | URL | Credentials |
|---|---|---|
| Prometheus | http://localhost:9091 | — |
| Grafana | http://localhost:3000 | admin / admin |

The Prometheus datasource is provisioned automatically. In Grafana, go to **Explore → Prometheus** and run:

```promql
rate(lb_requests_total[1m])
histogram_quantile(0.99, rate(lb_request_duration_seconds_bucket[5m]))
lb_backend_healthy
lb_circuit_breaker_state
```

Verify Prometheus is scraping:
```bash
curl http://localhost:9091/-/healthy   # → "Prometheus Server is Healthy."
curl http://localhost:9090/metrics     # → raw metric text from the load balancer
```

### 5. Development commands

```bash
go build ./...            # compile all packages
go vet ./...              # static analysis
go test ./... -race -v    # all tests with race detector
go test ./balancer/... -race -v -run TestWeighted   # single test
```

---

## Configuration Reference

All fields live in a single `config.yaml` file. Unknown fields are rejected at startup to catch typos early.

### `server`

| Field | Type | Default | Description |
|---|---|---|---|
| `address` | string | — | TCP address to listen on, e.g. `":8080"` |
| `read_timeout` | duration | `30s` | Maximum time to read the full request including body |
| `write_timeout` | duration | `30s` | Maximum time to write the response |
| `idle_timeout` | duration | `90s` | Maximum keep-alive idle time before closing the connection |

### `backends`

A list of upstream servers. At least one entry is required.

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | — | Full base URL of the backend, e.g. `"http://10.0.0.1:8080"` |
| `weight` | int | `1` | Relative weight for `weighted_round_robin`; ignored by other algorithms |

### `balancer`

| Field | Type | Default | Description |
|---|---|---|---|
| `algorithm` | string | `round_robin` | `round_robin` · `least_conn` · `weighted_round_robin` |

### `health`

| Field | Type | Default | Description |
|---|---|---|---|
| `interval` | duration | `10s` | How often each backend is probed |
| `timeout` | duration | `2s` | Per-probe HTTP deadline; slow backends fail fast |
| `path` | string | `/health` | HTTP path to `GET`; a 2xx response means healthy |

### `rate_limit`

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `false` | Enable per-IP token-bucket rate limiting |
| `rate` | float | — | Steady-state tokens added per second per client IP |
| `burst` | int | — | Maximum token bucket capacity (peak spike allowance) |

### `circuit_breaker`

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `false` | Enable per-backend circuit breakers |
| `max_failures` | int | — | Consecutive failures required to open the breaker |
| `timeout` | duration | `60s` | How long the breaker stays Open before attempting a Half-Open probe |

### `metrics`

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `false` | Enable the Prometheus metrics endpoint |
| `path` | string | `/metrics` | HTTP path for the metrics endpoint (served on `:9090`) |

---

## Prometheus Metrics

All metrics are prefixed `lb_` to avoid collisions in a shared Prometheus instance.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lb_requests_total` | Counter | `backend`, `method` | Total requests forwarded to each upstream |
| `lb_request_duration_seconds` | Histogram | `backend`, `status` | End-to-end proxy latency (12 buckets: 1ms–10s) |
| `lb_active_connections` | Gauge | `backend` | Requests currently in-flight per backend |
| `lb_backend_healthy` | Gauge | `backend` | `1` = healthy, `0` = unhealthy |
| `lb_circuit_breaker_state` | Gauge | `backend` | `0`=closed · `1`=open · `2`=half_open |
| `lb_rate_limited_total` | Counter | `client_ip` | Requests rejected by the rate limiter |
| `lb_retries_total` | Counter | `backend` | Upstream requests that required a retry |

---

## Design Decisions

### Smooth Weighted Round Robin over naive weighted replication

The naive weighted approach replicates backend entries in a slice proportional to their weight: a backend with weight 5 appears 5 times, weight 3 appears 3 times. This causes long consecutive runs to the same backend — a weight-5 backend serves 5 requests in a row before the others get a turn. It also wastes memory: a pool with large weights grows the slice to `sum(weights)` entries.

This project uses Nginx's **Smooth Weighted Round Robin** (SWRR) algorithm instead. Each call: (1) adds each backend's configured weight to its running `currentWeight`, (2) picks the backend with the highest `currentWeight`, (3) subtracts `sum(weights)` from the winner. Over any window of `sum(weights)` requests the distribution is exactly proportional to the weights, and no backend ever receives more than one extra request in a row. Memory stays O(n) regardless of weight magnitude.

```
Weights: A=3, B=1, C=2  →  sum=6

Call   currentWeight before pick   winner   currentWeight after
  1    A=3  B=1  C=2               A        A=-3 B=1  C=2
  2    A=0  B=2  C=4               C        A=0  B=2  C=-2
  3    A=3  B=3  C=0               A        A=-3 B=3  C=0
  4    A=0  B=4  C=2               B        A=0  B=-2 C=2
  5    A=3  B=-1 C=4               C        A=3  B=-1 C=-2
  6    A=6  B=0  C=0               A        A=0  B=0  C=0  ← full cycle

Result: A served 3×, B served 1×, C served 2× — exact, no long runs.
```

### Atomic operations over mutexes on the hot path

Every HTTP request reads `Backend.healthy` to decide whether the backend is eligible. At high throughput this happens thousands of times per second across many goroutines. A `sync.Mutex` here would serialize those reads, creating a bottleneck that grows linearly with concurrency.

`sync/atomic.Bool` (backed by a single 32-bit compare-and-swap instruction on x86) has no goroutine scheduling overhead and scales to any number of concurrent readers. The health checker writes at most once per interval (typically every 10 seconds) — a negligible write rate. This is the classic read-heavy, write-rare pattern where atomics outperform mutexes by a significant margin.

The round-robin counter uses `atomic.Uint64.Add` for the same reason: incrementing a shared counter under a mutex would force goroutines to queue, capping throughput. The atomic add is effectively free at the CPU level.

### One `httputil.ReverseProxy` per backend

`httputil.ReverseProxy` owns an `http.Transport`, which maintains a pool of persistent TCP connections to its target. If all backends share a single proxy and transport, a slow backend that exhausts the connection pool starves the fast ones — all backends get caught behind a single congested pool.

By creating one `ReverseProxy` per backend at startup (stored in a `map[string]*httputil.ReverseProxy`), each backend gets an independent connection pool with its own concurrency limits and timeouts. A slow or saturated backend can exhaust only its own pool; the others are completely unaffected. The map is built once and is read-only during serving, so no locking is needed to look up a proxy.

### Circuit breaker threshold choices

The defaults — **5 consecutive failures** to open, **30-second timeout** before a half-open probe — are chosen to balance two failure modes:

- **Too sensitive** (e.g., 2 failures, 5-second timeout): a single momentary blip (GC pause, transient network error) trips the breaker and removes a healthy backend from rotation unnecessarily.
- **Too tolerant** (e.g., 20 failures, 5-minute timeout): a genuinely dead backend absorbs hundreds of failed requests before the breaker opens, each one adding latency for end users.

Five consecutive failures represents a clear signal of sustained degradation rather than transient noise. The 30-second open period gives the backend enough time to restart, recover from a memory spike, or for a deploy to finish rolling out, without waiting so long that capacity stays reduced unnecessarily.

Both thresholds are exposed in `config.yaml` so operators can tune them for their SLO — a payment processing service might use a lower threshold than a bulk data pipeline.

### Metrics on a separate port

The Prometheus scraper issues an HTTP request that reads every registered metric. At large cardinality this response can be several megabytes and take hundreds of milliseconds. If the metrics endpoint shared port `:8080` with the proxy, every scrape would occupy a worker goroutine and connection slot that should be serving real traffic.

Serving metrics on `:9090` gives the scraper its own listener, its own `http.Server` instance with tighter timeouts, and no interaction with the proxy's connection pool. The proxy's latency histogram and SLOs are not affected by observability overhead — which would otherwise create a confounding variable in the very data you are trying to measure.

---

## What Would Be Added at Production Scale

These are the gaps between this implementation and a system that handles production traffic:

**Persistence and clustering**
The in-memory health state and circuit breaker counters are local to one process. A horizontally scaled deployment needs a shared store (etcd, Redis, or a gossip protocol) so all LB instances agree on which backends are healthy and circuit breakers trip consistently across the fleet.

**Dynamic backend registration**
`config.yaml` requires a restart to add or remove backends. At scale, backends register and deregister continuously (deploys, autoscaling). Integration with a service registry (Consul, Kubernetes Endpoints, AWS Cloud Map) would let the pool update without interruption.

**TLS termination**
Real deployments terminate TLS at the load balancer, manage certificate rotation (ACME/Let's Encrypt), and optionally apply mTLS for backend connections. `net/http` supports this natively; the main work is certificate lifecycle management.

**Connection limiting per backend**
The current `http.Transport` per backend uses Go's default `MaxIdleConnsPerHost` (100). Under very high concurrency this should be tuned to match each backend's capacity, and a hard concurrency limit should shedload rather than queue indefinitely.

**Observability depth**
Current metrics cover the happy path. Production needs: connection pool saturation gauges, queue depth, per-percentile latency alerts, error budget burn rate, and structured access logs correlated with trace IDs.

**Adaptive algorithms**
Peak Exponentially Weighted Moving Average (EWMA) routing — used by Finagle and Envoy — weights backends by a combination of latency and connection count, adapting faster than least-conn to backends that are slow but not failing. This outperforms all three current algorithms under heterogeneous load.

**Sticky sessions**
Stateful applications (session affinity, cache locality) need consistent hashing so the same client always reaches the same backend unless that backend is unavailable. Rendezvous hashing or a ring-based consistent hash minimises redistribution when backends join or leave.

**Admin API**
A `GET /admin/backends` endpoint returning live backend state (health, active connections, circuit breaker state) and a `POST /admin/backends/:id/drain` to gracefully remove a backend for maintenance without modifying config files or restarting the process.
