package tasks

import (
	"context"
	"time"
)

const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusFailed    = "failed"
	StatusSucceeded = "succeeded"
)

type Task struct {
	ID        int64             `db:"id"`
	Name      string            `db:"name"`
	Type      string            `db:"type"`
	Payload   map[string]string `db:"payload"`
	Status    string            `db:"status"`
	CreatedAt time.Time         `db:"created_at"`
	UpdatedAt time.Time         `db:"updated_at"`
	Attempts  int32             `db:"attempts"`
	LockedAt  *time.Time        `db:"locked_at"`
	LastError *string           `db:"last_error"`
}

type Event struct {
	TaskID    int64     `json:"-"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type StoreTaskReq struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Payload map[string]string `json:"payload"`
}

//go:generate go tool mockgen -source=task.go -destination=mocks/mock_db.go -package=mocks DB
type DB interface {
	StoreTask(ctx context.Context, task StoreTaskReq) (int64, error)
	GetTaskStatus(ctx context.Context, id int64) (string, error)
	GetTaskEvents(ctx context.Context, taskID int64) ([]Event, error)

	UpdateTaskStatusSucceeded(ctx context.Context, id int64) error
	UpdateTaskStatusFailed(ctx context.Context, id int64, lastError string) error

	DequeueTasks(ctx context.Context, batchSize int) ([]Task, error)
	ReaperUpdateStatusQueued(ctx context.Context) ([]int64, error)
	ReaperUpdateStatusFailed(ctx context.Context) ([]int64, error)
}
