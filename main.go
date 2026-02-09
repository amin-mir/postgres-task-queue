package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"dns/internal/handler"
	"dns/internal/postgres"
	"dns/internal/reaper"
	"dns/internal/runner"
)

func main() {
	pgURL := flag.String(
		"pg-url",
		"postgres://postgres:mysecretpassword@localhost:5432/postgres?sslmode=disable",
		"postgres address",
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pgPool, err := connectPostgres(ctx, *pgURL)
	if err != nil {
		log.Fatalf("Failed to connect to postgres: %v", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	pg := postgres.New(pgPool)
	mux := http.NewServeMux()
	h := handler.New(pg, logger)
	h.Register(mux)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	var shutdownWg sync.WaitGroup

	shutdownWg.Go(func() {
		logger.Info("Server is running.")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server.ListenAndServe unexpected error: %v", err)
		}
	})

	taskRunner := runner.New(pg, logger)
	shutdownWg.Go(func() {
		logger.Info("TaskRunner is running.")
		if err := taskRunner.Run(ctx); err != nil && err != context.Canceled {
			logger.Warn("TasRunner.Run did not shutdown gracefully", "err", err)
		}
		logger.Info("TaskRunner stopped.")
	})

	taskReaper := reaper.New(pg, logger)
	shutdownWg.Go(func() {
		logger.Info("TaskReaper is running.")
		taskReaper.Reap(ctx)
		logger.Info("TaskReaper stopped.")
	})

	// Wait until we receive a shutdown signal.
	<-ctx.Done()

	shutdownWg.Go(func() {
		shutdownServer(server, 10*time.Second)
		logger.Info("Server stopped.")
	})
	shutdownWg.Wait()
}

func connectPostgres(ctx context.Context, pgURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		return nil, err
	}

	err = pool.Ping(ctx)
	return pool, err
}

func shutdownServer(srv *http.Server, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
}
