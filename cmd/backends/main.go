// Command backends starts three minimal HTTP servers on :8081, :8082,
// and :8083 to act as upstream targets for the load balancer during
// local development and manual testing.
//
// Each server handles two routes:
//   GET /        → "Response from backend :<port>\n"
//   GET /health  → 200 OK (used by the health checker)
//
// All three servers start concurrently via goroutines and the process
// blocks until it receives an interrupt signal, at which point all
// servers shut down gracefully.
//
// Usage:
//
//	go run ./cmd/backends
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ports := []string{"8081", "8082", "8083"}
	servers := make([]*http.Server, len(ports))

	for i, port := range ports {
		// Capture port in loop-local variable so the closure below is correct.
		p := port
		mux := http.NewServeMux()

		// Root handler returns a human-readable string that identifies
		// which backend served the request — useful when tailing logs or
		// running curl in a loop to verify the balancer is distributing load.
		mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprintf(w, "Response from backend :%s\n", p)
		})

		// Health endpoint returns 200 with no body — that is all the
		// health checker needs to mark this backend as healthy.
		mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		servers[i] = &http.Server{
			Addr:         ":" + port,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
			IdleTimeout:  30 * time.Second,
		}
	}

	// Start all servers concurrently. Each goroutine logs a fatal error
	// if ListenAndServe returns unexpectedly (i.e., not due to shutdown).
	var wg sync.WaitGroup
	for _, srv := range servers {
		wg.Add(1)
		go func(s *http.Server) {
			defer wg.Done()
			logger.Info("backend listening", "addr", s.Addr)
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("backend server error", "addr", s.Addr, "err", err)
			}
		}(srv)
	}

	// Block until SIGINT or SIGTERM is received.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	logger.Info("shutdown signal received")

	// Graceful shutdown: give in-flight requests 5 seconds to complete.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, srv := range servers {
		if err := srv.Shutdown(ctx); err != nil {
			logger.Error("shutdown error", "addr", srv.Addr, "err", err)
		}
	}

	wg.Wait()
	logger.Info("all backends stopped")
}
