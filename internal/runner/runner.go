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
	defaultDBTimeout         = time.Second
	defaultTaskSleep         = 3 * time.Second
	defaultTaskTimeout       = 5 * time.Second
	defaultConcurrency       = 100
	defaultRetryAttempts     = 10
	defaultRunIterationSleep = time.Second
	defaultGracefulWait      = 5 * time.Second
)

var (
	ErrGracefulShutdownTimeout = errors.New("could not stop all task goroutines")
)

type config struct {
	DBTimeout         time.Duration
	TaskSleep         time.Duration
	TaskTimeout       time.Duration
	Concurrency       int
	RetryAttempts     uint
	RunIterationSleep time.Duration
	GracefulWait      time.Duration
}

func (cfg *config) sanitize() {
	if cfg.DBTimeout <= 0 {
		cfg.DBTimeout = defaultDBTimeout
	}
	if cfg.DBTimeout > 5*time.Second {
		cfg.DBTimeout = 5 * time.Second
	}

	if cfg.TaskSleep <= 0 {
		cfg.TaskSleep = defaultTaskSleep
	}
	if cfg.TaskSleep > 10*time.Second {
		cfg.TaskSleep = 10 * time.Second
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

	if cfg.RetryAttempts <= 0 || cfg.RetryAttempts >= 20 {
		cfg.RetryAttempts = defaultRetryAttempts
	}

	if cfg.RunIterationSleep <= 0 || cfg.RunIterationSleep > 10*time.Second {
		cfg.RunIterationSleep = defaultRunIterationSleep
	}

	if cfg.GracefulWait <= 0 || cfg.GracefulWait > 10*time.Second {
		cfg.GracefulWait = defaultGracefulWait
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

func WithTaskSleep(d time.Duration) Opt {
	return func(cfg *config) { cfg.TaskSleep = d }
}

func WithTaskTimeout(d time.Duration) Opt {
	return func(cfg *config) { cfg.TaskTimeout = d }
}

func WithConcurrency(n int) Opt {
	return func(cfg *config) { cfg.Concurrency = n }
}

func WithRetryAttempts(n uint) Opt {
	return func(cfg *config) { cfg.RetryAttempts = n }
}

func WithRunIterationSleep(d time.Duration) Opt {
	return func(cfg *config) { cfg.RunIterationSleep = d }
}

func WithGracefulWait(d time.Duration) Opt {
	return func(cfg *config) { cfg.GracefulWait = d }
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

// Run returns context.Canceled when context is canceled but all the tasks finish
// within the graceful shutdown period. If there are still pending tasks
// it will return ErrGracefulShutdownTimeout.
func (r *Runner) Run(ctx context.Context) error {
loop:
	for {
		tt, err := r.dequeueTasks(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			r.logger.ErrorContext(ctx, "dequeueTasks failed", "err", err)
			time.Sleep(r.cfg.RunIterationSleep)
			continue
		}

		if len(tt) == 0 {
			r.logger.InfoContext(ctx, "dequeueTasks returned 0 tasks")
			time.Sleep(r.cfg.RunIterationSleep)
			continue
		}

		for i := range tt {
			// another approach is to validate type before acquiring a slot.
			// if tt[i].Type != tasks.TypeRunQuery && tt[i].Type != tasks.TypeSendEmail {
			// 	panic(fmt.Sprintf("invalid task type: %s", tt[i].Type))
			// }
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
	case <-time.After(r.cfg.GracefulWait):
		return ErrGracefulShutdownTimeout
	case <-done:
		return context.Canceled
	}
}

// runTask will run the given task with the given CONTEXT. The task will be part of
// Runner's WAITGROUP. It will return the SEMAPHOR back once the task finishes and
// call the corresponding DB FUNCTION based on the result of the task. The tasks
// are responsible for respecting the ctx.Done. We don't derive a new context with timeout
// in this function as it allows the taskFn and DB calls to have separate timeouts by
// deriving the context the way they want. Currently taskFn will derive from the parent context
// but DB calls will use context.Background().
func (r *Runner) runTask(
	ctx context.Context,
	taskFn func(context.Context, *tasks.Task) error,
	task *tasks.Task,
) {
	r.wg.Go(func() {
		defer func() { <-r.sem }()

		if err := taskFn(ctx, task); err != nil {
			if err = r.taskFailed(ctx, task.ID, err); err != nil {
				r.logger.ErrorContext(ctx, "taskFailed failed", "err", err)
			}
			return
		}

		if err := r.taskSucceeded(ctx, task.ID); err != nil {
			r.logger.ErrorContext(ctx, "taskSucceeded failed", "err", err)
		}
	})
}

// Handles the task: send_email
func (r *Runner) sendEmail(ctx context.Context, task *tasks.Task) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.TaskTimeout)
	defer cancel()

	select {
	case <-time.After(r.cfg.TaskSleep):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Handles the task: run_query
func (r *Runner) runQuery(ctx context.Context, task *tasks.Task) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.TaskTimeout)
	defer cancel()

	select {
	case <-time.After(r.cfg.TaskSleep):
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
	retrier := r.newRetrier(ctx)

	var tt []tasks.Task
	err := retrier.Do(func() error {
		// Creating a context with timeout for each request to the database. We use the
		// context passed to this function (as opposed to taskFailed and taskSucceeded)
		// because when we receive the shutdown signal, we don't want to process tasks anymore.
		// It is possible that some tasks will be marked as running in the database as a result
		// of this decision, but that is fine because the reaper will later mark them as queued.
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

func (r *Runner) taskFailed(ctx context.Context, id int64, lastError error) error {
	retrier := r.newRetrier(ctx)

	return retrier.Do(func() error {
		// Creating an independent context with timeout for each request to the database
		// so that even when the parent context is canceled (shutdown signal), the correct
		// DB operation will continue. These requests won't be retired however as we're
		// using the passed context for that.
		dbCtx, dbCancel := context.WithTimeout(context.Background(), r.cfg.DBTimeout)
		defer dbCancel()

		return r.db.UpdateTaskStatusFailed(dbCtx, id, lastError.Error())
	})
}

func (r *Runner) taskSucceeded(ctx context.Context, id int64) error {
	retrier := r.newRetrier(ctx)

	return retrier.Do(func() error {
		// Creating an independent context with timeout for each request to the database
		// so that even when the parent context is canceled (shutdown signal), the correct
		// DB operation will continue. These requests won't be retired however as we're
		// using the passed context for that.
		dbCtx, dbCancel := context.WithTimeout(context.Background(), r.cfg.DBTimeout)
		defer dbCancel()

		return r.db.UpdateTaskStatusSucceeded(dbCtx, id)
	})
}

func (r *Runner) newRetrier(ctx context.Context) *retry.Retrier {
	// No need to set a timeout for the retry context as we're specifying the
	// total number of reties.
	return retry.New(
		retry.Context(ctx),
		retry.Attempts(r.cfg.RetryAttempts),
	)
}
