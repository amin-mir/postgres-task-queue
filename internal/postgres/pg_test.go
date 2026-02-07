package postgres

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"dns/internal/tasks"
)

func TestStoreTaskAndGetTask(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := runPostgresAndConnect(t, ctx)

	db := New(pool)

	task := tasks.StoreTaskReq{
		Name: "task1",
		Type: "send_email",
		Payload: map[string]string{
			"param1": "val1",
			"param2": "val2",
		},
	}
	id, err := db.StoreTask(ctx, task)
	require.NoError(t, err)

	got, err := db.getTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, task.Name, got.Name)
	require.Equal(t, task.Type, got.Type)
	require.Equal(t, task.Payload, got.Payload)
	require.Equal(t, tasks.StatusQueued, got.Status)
}

func TestUpdateAndGetTaskStatus(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := runPostgresAndConnect(t, ctx)

	db := New(pool)

	task := tasks.StoreTaskReq{
		Name: "task1",
		Type: "send_email",
		Payload: map[string]string{
			"param1": "val1",
			"param2": "val2",
		},
	}
	id, err := db.StoreTask(ctx, task)
	require.NoError(t, err)

	err = db.updateTaskStatusRunning(ctx, id)
	require.NoError(t, err)
	status, err := db.GetTaskStatus(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "running", status)

	err = db.UpdateTaskStatusFailed(ctx, id, "unknown failure")
	require.NoError(t, err)
	status, err = db.GetTaskStatus(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "failed", status)

	err = db.UpdateTaskStatusSucceeded(ctx, id)
	require.NoError(t, err)
	status, err = db.GetTaskStatus(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "succeeded", status)

	events, err := db.GetTaskEvents(ctx, id)
	require.NoError(t, err)
	require.Equal(t, 3, len(events))
	require.Equal(t, "running", events[0].Status)
	require.Equal(t, "failed", events[1].Status)
	require.Equal(t, "succeeded", events[2].Status)
}

func TestStoreAndGetTaskEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := runPostgresAndConnect(t, ctx)

	db := New(pool)

	task := tasks.StoreTaskReq{
		Name: "task1",
		Type: "send_email",
		Payload: map[string]string{
			"param1": "val1",
			"param2": "val2",
		},
	}
	id, err := db.StoreTask(ctx, task)
	require.NoError(t, err)

	for range int64(5) {
		event := tasks.Event{
			TaskID: id,
			Status: "running",
		}
		err := db.storeTaskEvent(ctx, event)
		require.NoError(t, err)
	}

	events, err := db.GetTaskEvents(ctx, id)
	require.NoError(t, err)
	require.Equal(t, 5, len(events))
}

func TestDequeueTask(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := runPostgresAndConnect(t, ctx)

	db := New(pool)

	var taskIDs []int64
	for range 10 {
		task := tasks.StoreTaskReq{
			Name: "some task",
			Type: "send_email",
			Payload: map[string]string{
				"param1": "val1",
				"param2": "val2",
			},
		}
		id, err := db.StoreTask(ctx, task)
		require.NoError(t, err)
		taskIDs = append(taskIDs, id)
	}

	tasks, err := db.DequeueTasks(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, 4, len(tasks))

	var deqTaskIDs []int64
	for _, task := range tasks {
		require.Equal(t, task.Status, "running")
		require.WithinDuration(t, time.Now(), *task.LockedAt, 100*time.Millisecond)
		require.WithinDuration(t, time.Now(), task.UpdatedAt, 100*time.Millisecond)
		deqTaskIDs = append(deqTaskIDs, task.ID)
	}

	tasks, err = db.DequeueTasks(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, 4, len(tasks))
	for _, task := range tasks {
		// Ensure we won't fetch tasks already marked as running.
		require.NotContains(t, deqTaskIDs, task.ID)
		deqTaskIDs = append(deqTaskIDs, task.ID)
	}

	tasks, err = db.DequeueTasks(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, 2, len(tasks))
	for _, task := range tasks {
		// Ensure we won't fetch tasks already marked as running.
		require.NotContains(t, deqTaskIDs, task.ID)
	}

	tasks, err = db.DequeueTasks(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, 0, len(tasks))
}

func TestReaperUpdateStatusQueued(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := runPostgresAndConnect(t, ctx)

	db := New(pool)

	lockedAt := time.Now().Add(-time.Hour)
	for range 10 {
		task := tasks.Task{
			Name:   "some task",
			Type:   "send_email",
			Status: "running",
			Payload: map[string]string{
				"param1": "val1",
				"param2": "val2",
			},
			CreatedAt: time.Now().Add(-time.Hour),
			UpdatedAt: time.Now().Add(-time.Hour),
			Attempts:  3,
			LockedAt:  &lockedAt,
			LastError: nil,
		}
		_, err := db.insertTask(ctx, task)
		require.NoError(t, err)
	}

	for range 10 {
		task := tasks.Task{
			Name:   "some task",
			Type:   "send_email",
			Status: "queued",
			Payload: map[string]string{
				"param1": "val1",
				"param2": "val2",
			},
		}
		_, err := db.insertTask(ctx, task)
		require.NoError(t, err)
	}

	affectedIDs, err := db.ReaperUpdateStatusQueued(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, len(affectedIDs))

	task, err := db.getTask(ctx, affectedIDs[0])
	require.NoError(t, err)

	require.Equal(t, "queued", task.Status)
	require.Nil(t, task.LockedAt)
	require.Equal(t, int32(4), task.Attempts)
	require.WithinDuration(t, time.Now(), task.UpdatedAt, 100*time.Millisecond)

	events, err := db.GetTaskEvents(ctx, affectedIDs[0])
	require.NoError(t, err)
	require.Equal(t, "queued", events[0].Status)
}

func TestReaperUpdateStatusFailed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := runPostgresAndConnect(t, ctx)

	db := New(pool)

	lockedAt := time.Now().Add(-time.Hour)
	for range 10 {
		task := tasks.Task{
			Name:   "some task",
			Type:   "send_email",
			Status: "running",
			Payload: map[string]string{
				"param1": "val1",
				"param2": "val2",
			},
			CreatedAt: time.Now().Add(-time.Hour),
			UpdatedAt: time.Now().Add(-time.Hour),
			Attempts:  5,
			LockedAt:  &lockedAt,
			LastError: nil,
		}
		_, err := db.insertTask(ctx, task)
		require.NoError(t, err)
	}

	for range 10 {
		task := tasks.Task{
			Name:   "some task",
			Type:   "send_email",
			Status: "queued",
			Payload: map[string]string{
				"param1": "val1",
				"param2": "val2",
			},
		}
		_, err := db.insertTask(ctx, task)
		require.NoError(t, err)
	}

	affectedIDs, err := db.ReaperUpdateStatusFailed(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, len(affectedIDs))

	task, err := db.getTask(ctx, affectedIDs[0])
	require.NoError(t, err)

	require.Equal(t, "failed", task.Status)
	require.Nil(t, task.LockedAt)
	require.Equal(t, int32(5), task.Attempts)
	require.Equal(t, "max attempts exceeded", *task.LastError)
	require.WithinDuration(t, time.Now(), task.UpdatedAt, 100*time.Millisecond)

	events, err := db.GetTaskEvents(ctx, affectedIDs[0])
	require.NoError(t, err)
	require.Equal(t, "failed", events[0].Status)
}

func runPostgresAndConnect(t *testing.T, ctx context.Context) *pgxpool.Pool {
	pgContainer, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithInitScripts(filepath.Join("../..", "migrations", "000001_create_table_tasks.up.sql")),
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, pgContainer)
	require.NoError(t, err)

	pgURL, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(context.Background(), pgURL)
	require.NoError(t, err)
	return pool
}
