package tasks

import "time"

const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusFailed    = "failed"
	StatusSucceeded = "succeeded"
)

type Task struct {
	ID        int64             `json:"id"`
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Payload   map[string]string `json:"payload"`
	Status    string            `json:"status"`
	CreatedAt time.Time         `json:"-"`
	UpdatedAt time.Time         `json:"-"`
	Attempts  int32             `json:"-"`
	LockedAt  *time.Time        `json:"-"`
	LastError *string           `json:"-"`
}

type Event struct {
	TaskID    int64     `json:"-"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}
