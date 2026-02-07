package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/avast/retry-go/v5"

	"dns/internal/tasks"
)

const (
	defaultDBTimeout   = time.Second
	defaultTaskTimeout = 5 * time.Second
	defaultConcurrency = 100

	defaultTaskSleep           = 3 * time.Second
	defaultRetryAttempts       = 10
	defaultGracefulWaitTimeout = 5 * time.Second
	defaultRunIterationSleep   = time.Second
)

var (
	ErrGracefulShutdownTimeout = errors.New("could not stop all task goroutines")
)

type config struct {
	DBTimeout   time.Duration
	TaskTimeout time.Duration
	Concurrency int
}

func (cfg *config) sanitize() {
	if cfg.DBTimeout <= 0 {
		cfg.DBTimeout = defaultDBTimeout
	}
	if cfg.DBTimeout > 5*time.Second {
		cfg.DBTimeout = 5 * time.Second
	}

	// 2*DBTimeout <= TaskTimeout <= 20*time.Second
	if cfg.TaskTimeout > 20*time.Second {
		cfg.DBTimeout = 20 * time.Second
	}
	if cfg.TaskTimeout < 2*cfg.DBTimeout {
		cfg.TaskTimeout = 2 * cfg.DBTimeout
	}

	if cfg.Concurrency <= 0 || cfg.Concurrency > 1000 {
		cfg.Concurrency = defaultConcurrency
	}
}

func configFromOpts(opts ...Opt) config {
	cfg := config{
		DBTimeout:   defaultDBTimeout,
		TaskTimeout: defaultTaskTimeout,
		Concurrency: defaultConcurrency,
	}
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

func WithTaskTimeout(d time.Duration) Opt {
	return func(cfg *config) { cfg.TaskTimeout = d }
}

func WithConcurrency(n int) Opt {
	return func(cfg *config) { cfg.Concurrency = n }
}

type Runner struct {
	cfg    config
	db     tasks.DB
	logger *slog.Logger
	sem    chan struct{}
	wg     sync.WaitGroup
}

func New(db tasks.DB, logger *slog.Logger, opts ...Opt) *Runner {
	cfg := configFromOpts(opts...)

	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	return &Runner{
		cfg:    cfg,
		sem:    make(chan struct{}, cfg.Concurrency),
		db:     db,
		logger: logger,
	}
}

func (r *Runner) Run(ctx context.Context) error {
loop:
	for {
		tt, err := r.dequeueTasks(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			r.logger.ErrorContext(ctx, "DB.DequeueTasks failed", "err", err)
			time.Sleep(defaultRunIterationSleep)
			continue
		}

		if len(tt) == 0 {
			r.logger.InfoContext(ctx, "DB.DequeueTasks returned 0 tasks")
			time.Sleep(defaultRunIterationSleep)
			continue
		}

		for i := range tt {
			if !r.acquire(ctx) {
				break loop
			}
			switch tt[i].Type {
			case tasks.TypeRunQuery:
				r.runTask(ctx, r.runQuery, &tt[i])
			case tasks.TypeSendEmail:
				r.runTask(ctx, r.sendEmail, &tt[i])
			default:
				// Expecting the caller of Run to validate the task type so we should
				// never reach this part of the code.
				panic(fmt.Sprintf("invalid task type: %s", tt[i].Type))
			}
		}
	}

	return r.wait()
}

func (r *Runner) acquire(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case r.sem <- struct{}{}:
		return true
	}
}

// Wait waits until all tasks' goroutines finish execution gracefully or we
// return after the wait timeout.
func (r *Runner) wait() error {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-time.After(defaultGracefulWaitTimeout):
		return ErrGracefulShutdownTimeout
	case <-done:
		return nil
	}
}

// runTask will run the given task with the given CONTEXT. The task will be part of
// Runner's WAITGROUP. It will return the SEMAPHOR back once the task finishes and
// call the corresponding DB FUNCTION based on the result of the task. The tasks
// are responsible for respecting the ctx.Done. This will allow the task and DB calls
// to have separate timeouts by deriving the parent context and using the correct timeout.
func (r *Runner) runTask(
	ctx context.Context,
	taskFn func(context.Context, *tasks.Task) error,
	task *tasks.Task,
) {
	r.wg.Go(func() {
		defer func() { <-r.sem }()

		if err := taskFn(ctx, task); err != nil {
			r.taskFailed(ctx, task.ID, err)
			return
		}

		r.taskSucceeded(ctx, task.ID)
	})
}

func (r *Runner) sendEmail(ctx context.Context, task *tasks.Task) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.TaskTimeout)
	defer cancel()

	select {
	case <-time.After(defaultTaskSleep):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runner) runQuery(ctx context.Context, task *tasks.Task) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.TaskTimeout)
	defer cancel()

	select {
	case <-time.After(defaultTaskSleep):
		// Simulate failure.
		if rand.Float64() < 0.3 {
			return errors.New("random error")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runner) dequeueTasks(ctx context.Context) ([]tasks.Task, error) {
	retrier := newRetrier(ctx)

	var tt []tasks.Task
	err := retrier.Do(func() error {
		// Creating a context with timeout for each request to the database.
		ctx, cancel := context.WithTimeout(ctx, r.cfg.DBTimeout)
		defer cancel()

		var err error
		tt, err = r.db.DequeueTasks(ctx, r.cfg.Concurrency)
		if err != nil {
			r.logger.ErrorContext(ctx, "DB.DequeueTasks failed", "err", err)
			return err
		}
		return nil
	})

	return tt, err
}

func (r *Runner) taskFailed(ctx context.Context, id int64, lastError error) {
	retrier := newRetrier(ctx)

	retrier.Do(func() error {
		// Creating a context with timeout for each request to the database.
		ctx, cancel := context.WithTimeout(ctx, r.cfg.DBTimeout)
		defer cancel()

		if err := r.db.UpdateTaskStatusFailed(ctx, id, lastError.Error()); err != nil {
			r.logger.ErrorContext(ctx, "DB.UpdateTaskStatusFailed failed", "err", err)
			return err
		}
		return nil
	})
}

func (r *Runner) taskSucceeded(ctx context.Context, id int64) {
	retrier := newRetrier(ctx)

	retrier.Do(func() error {
		// Creating a context with timeout for each request to the database.
		ctx, cancel := context.WithTimeout(ctx, r.cfg.DBTimeout)
		defer cancel()

		if err := r.db.UpdateTaskStatusSucceeded(ctx, id); err != nil {
			r.logger.ErrorContext(ctx, "DB.UpdateTaskStatusSucceeded failed", "err", err)
			return err
		}
		return nil
	})
}

func newRetrier(ctx context.Context) *retry.Retrier {
	// No need to set a timeout for the retry context as we're specifying the
	// total number of reties.
	return retry.New(
		retry.Context(ctx),
		retry.Attempts(defaultRetryAttempts),
	)
}
