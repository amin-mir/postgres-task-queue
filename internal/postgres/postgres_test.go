package postgres

import (
	"context"
	"dns/internal/tasks"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestStoreTaskAndGetTask(t *testing.T) {
	ctx := context.Background()
	pool := runPostgresAndConnect(t, ctx)

	db := New(pool)

	task := StoreTaskReq{
		Name: "task1",
		Type: "send_email",
		Payload: map[string]string{
			"param1": "val1",
			"param2": "val2",
		},
	}
	id, err := db.StoreTask(ctx, task)
	require.NoError(t, err)

	got, err := db.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, task.Name, got.Name)
	require.Equal(t, task.Type, got.Type)
	require.Equal(t, task.Payload, got.Payload)
	require.Equal(t, tasks.StatusQueued, got.Status)
}

func TestUpdateAndGetTaskStatus(t *testing.T) {
	ctx := context.Background()
	pool := runPostgresAndConnect(t, ctx)

	db := New(pool)

	task := StoreTaskReq{
		Name: "task1",
		Type: "send_email",
		Payload: map[string]string{
			"param1": "val1",
			"param2": "val2",
		},
	}
	id, err := db.StoreTask(ctx, task)
	require.NoError(t, err)

	err = db.UpdateTaskStatusRunning(ctx, id)
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
	ctx := context.Background()
	pool := runPostgresAndConnect(t, ctx)

	db := New(pool)

	task := StoreTaskReq{
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
		err := db.StoreTaskEvent(ctx, event)
		require.NoError(t, err)
	}

	events, err := db.GetTaskEvents(ctx, id)
	require.NoError(t, err)
	require.Equal(t, 5, len(events))
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
