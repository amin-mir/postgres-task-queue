package reaper

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"dns/internal/tasks"

	"github.com/avast/retry-go/v5"
)

const (
	defaultDBTimeout          = time.Second
	defaultReapInterval       = 10 * time.Second
	defaultRetryAttempts      = 10
	defaultReapIterationSleep = 5 * time.Second
)

type config struct {
	DBTimeout          time.Duration
	ReapInterval       time.Duration
	RetryAttempts      uint
	ReapIterationSleep time.Duration
}

func (cfg *config) sanitize() {
	if cfg.DBTimeout <= 0 {
		cfg.DBTimeout = defaultDBTimeout
	}
	if cfg.DBTimeout > 5*time.Second {
		cfg.DBTimeout = 5 * time.Second
	}

	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = defaultReapInterval
	}
	if cfg.ReapInterval > time.Hour {
		cfg.ReapInterval = time.Hour
	}

	// RetryAttemps is uint.
	if cfg.RetryAttempts == 0 || cfg.RetryAttempts > 20 {
		cfg.RetryAttempts = defaultRetryAttempts
	}

	if cfg.ReapIterationSleep <= 0 || cfg.ReapIterationSleep > 10*time.Second {
		cfg.ReapIterationSleep = defaultReapIterationSleep
	}
}

func configFromOpts(opts ...Opt) config {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.sanitize()
	return cfg
}

type Opt func(*config)

func WithDBTimeout(d time.Duration) Opt {
	return func(cfg *config) { cfg.DBTimeout = d }
}

func WithReapInterval(d time.Duration) Opt {
	return func(cfg *config) { cfg.ReapInterval = d }
}

func WithRetryAttempts(n uint) Opt {
	return func(cfg *config) { cfg.RetryAttempts = n }
}

func WithReapIterationSleep(d time.Duration) Opt {
	return func(cfg *config) { cfg.ReapIterationSleep = d }
}

type Reaper struct {
	cfg    config
	db     tasks.DB
	logger *slog.Logger
	wg     sync.WaitGroup
}

func New(db tasks.DB, logger *slog.Logger, opts ...Opt) *Reaper {
	cfg := configFromOpts(opts...)

	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	return &Reaper{
		cfg:    cfg,
		db:     db,
		logger: logger,
	}
}

// Reap will block until context is canceled.
func (r *Reaper) Reap(ctx context.Context) {
	r.wg.Go(func() { r.reapQueued(ctx) })
	r.wg.Go(func() { r.reapFailed(ctx) })
	r.wg.Wait()
}

func (r *Reaper) reapFailed(ctx context.Context) {
	reap(
		ctx, r.cfg,
		r.logger.With("type", "failed"),
		r.reapUpdateStatusFailed,
	)
}

func (r *Reaper) reapQueued(ctx context.Context) {
	reap(
		ctx, r.cfg,
		r.logger.With("type", "queued"),
		r.reapUpdateStatusQueued,
	)
}

func reap(
	ctx context.Context,
	cfg config,
	logger *slog.Logger,
	reapFn func(context.Context) ([]int64, error),
) {
	ticker := time.Tick(cfg.ReapInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			ids, err := reapFn(ctx)
			if err != nil {
				logger.ErrorContext(ctx, "failed to reap tasks", "err", err)
				sleep(ctx, cfg.ReapIterationSleep)
				continue
			}
			logger.InfoContext(ctx, "reaped tasks", "count", len(ids))
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func (r *Reaper) reapUpdateStatusQueued(ctx context.Context) ([]int64, error) {
	retrier := r.newRetrier(ctx)

	return retrier.Do(func() ([]int64, error) {
		dbCtx, dbCancel := context.WithTimeout(ctx, r.cfg.DBTimeout)
		defer dbCancel()
		return r.db.ReaperUpdateStatusQueued(dbCtx)
	})
}

func (r *Reaper) reapUpdateStatusFailed(ctx context.Context) ([]int64, error) {
	retrier := r.newRetrier(ctx)

	return retrier.Do(func() ([]int64, error) {
		dbCtx, dbCancel := context.WithTimeout(ctx, r.cfg.DBTimeout)
		defer dbCancel()
		return r.db.ReaperUpdateStatusFailed(dbCtx)
	})
}

func (r *Reaper) newRetrier(ctx context.Context) *retry.RetrierWithData[[]int64] {
	// No need to set a timeout for the retry context as we're specifying the
	// total number of retries.
	return retry.NewWithData[[]int64](
		retry.Context(ctx),
		retry.Attempts(r.cfg.RetryAttempts),
	)
}
