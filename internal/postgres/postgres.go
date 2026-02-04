package postgres

import (
	"context"
	"dns/internal/tasks"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	storeTaskQuery = "INSERT INTO tasks (name, type, payload) VALUES ($1, $2, $3) RETURNING id"

	getTaskQuery = `SELECT id, name, type, status, payload, created_at, updated_at, locked_at, last_error
FROM tasks
WHERE id = $1`

	getTaskStatusQuery = `SELECT status FROM tasks WHERE id = $1`

	updateTaskStatusSucceededOrRunningQuery = `WITH updated AS (
	UPDATE tasks
	SET status = $1, updated_at = now()
	WHERE id=$2
	RETURNING id, status, updated_at
)
INSERT INTO task_events (task_id, status, created_at)
SELECT id, status, updated_at
FROM updated
`

	updateTaskStatusFailedQuery = `WITH updated AS (
	UPDATE tasks
	SET status = 'failed', last_error = $1, updated_at = now()
	WHERE id=$2
	RETURNING id, status, updated_at
)
INSERT INTO task_events (task_id, status, created_at)
SELECT id, status, updated_at
FROM updated
`

	storeTaskEventQuery = `INSERT INTO task_events (task_id, status) VALUES ($1, $2)`

	getTaskEventsQuery = `SELECT task_id, status, created_at
FROM task_events
WHERE task_id = $1
ORDER BY created_at
LIMIT 10`

	dequeueTaskQuery = `WITH next AS (
SELECT id
FROM tasks
WHERE status = 'queued' AND locked_at IS NULL
ORDER BY created_at, id
LIMIT $1
FOR UPDATE SKIP LOCKED
)
UPDATE tasks t
SET status = 'running',
	locked_at = now(),
	updated_at = now()
FROM next
WHERE t.id = next.id
RETURNING t.*;`

	reaperUpdateStatusQueuedQuery = `WITH reaped AS (
	UPDATE tasks
	SET status = 'queued',
		attempts = attempts + 1,
		locked_at = NULL,
		updated_at = now()
	WHERE status = 'running'
		AND locked_at < now() - interval '10 minutes'
		AND attempts < 5;
	RETURNING id, status, updated_at
)
INSERT INTO task_events (task_id, status, created_at)
SELECT id, status, updated_at
FROM reaped
RETURNING task_id
`

	reaperUpdateStatusFailedQuery = `WITH reaped AS (
	UPDATE tasks
	SET status = 'failed',
		locked_at = NULL,
		last_error = 'max attempts exceeded',
		updated_at = now()
	WHERE status = 'running'
		AND locked_at < now() - interval '10 minutes'
		AND attempts >= 5;
	RETURNING id, status, updated_at
)
INSERT INTO task_events (task_id, status, created_at)
SELECT id, status, updated_at
FROM reaped
RETURNING task_id
`
)

type DB struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *DB {
	return &DB{pool: pool}
}

type StoreTaskReq struct {
	Name    string
	Type    string
	Payload map[string]string
}

func (db *DB) StoreTask(ctx context.Context, task StoreTaskReq) (int64, error) {
	b, err := json.Marshal(task.Payload)
	if err != nil {
		return 0, err
	}

	var id int64
	err = db.pool.QueryRow(ctx, storeTaskQuery, task.Name, task.Type, b).Scan(&id)
	return id, err
}

func (db *DB) GetTask(ctx context.Context, id int64) (tasks.Task, error) {
	rows, err := db.pool.Query(ctx, getTaskQuery, id)
	if err != nil {
		return tasks.Task{}, err
	}

	return pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[tasks.Task])
}

func (db *DB) GetTaskStatus(ctx context.Context, id int64) (string, error) {
	var status string
	err := db.pool.QueryRow(ctx, getTaskStatusQuery, id).Scan(&status)
	return status, err
}

func (db *DB) UpdateTaskStatusRunning(ctx context.Context, id int64) error {
	_, err := db.pool.Exec(ctx, updateTaskStatusSucceededOrRunningQuery, "running", id)
	return err
}

func (db *DB) UpdateTaskStatusSucceeded(ctx context.Context, id int64) error {
	_, err := db.pool.Exec(ctx, updateTaskStatusSucceededOrRunningQuery, "succeeded", id)
	return err
}

func (db *DB) UpdateTaskStatusFailed(ctx context.Context, id int64, lastError string) error {
	_, err := db.pool.Exec(ctx, updateTaskStatusFailedQuery, lastError, id)
	return err
}

func (db *DB) StoreTaskEvent(ctx context.Context, event tasks.Event) error {
	_, err := db.pool.Exec(ctx, storeTaskEventQuery, event.TaskID, event.Status)
	return err
}

func (db *DB) GetTaskEvents(ctx context.Context, taskID int64) ([]tasks.Event, error) {
	rows, err := db.pool.Query(ctx, getTaskEventsQuery, taskID)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, pgx.RowToStructByName[tasks.Event])
}

func (db *DB) DequeueTask(ctx context.Context, batchSize int) ([]tasks.Task, error) {
	rows, err := db.pool.Query(ctx, dequeueTaskQuery, batchSize)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, pgx.RowToStructByName[tasks.Task])
}

func (db *DB) ReaperUpdateStatusQueued(ctx context.Context) ([]int64, error) {
	rows, err := db.pool.Query(ctx, reaperUpdateStatusQueuedQuery)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func (db *DB) ReaperUpdateStatusFailed(ctx context.Context) ([]int64, error) {
	rows, err := db.pool.Query(ctx, reaperUpdateStatusQueuedQuery)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, pgx.RowTo[int64])
}
