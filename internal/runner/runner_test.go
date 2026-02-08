package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"dns/internal/tasks"
	"dns/internal/tasks/mocks"
)

func TestRun(t *testing.T) {
	db := newDeps(t)
	r := New(db, nil,
		WithTaskSleep(20*time.Millisecond),
		WithRunIterationSleep(20*time.Millisecond),
	)

	deqTasks1 := []tasks.Task{
		{ID: 10, Type: tasks.TypeRunQuery},
		{ID: 20, Type: tasks.TypeSendEmail},
	}

	deqTasks2 := []tasks.Task{
		{ID: 30, Type: tasks.TypeRunQuery},
		{ID: 40, Type: tasks.TypeSendEmail},
	}

	ctx, cancel := context.WithCancel(context.Background())
	numFinished := 0
	taskFinished := func() {
		numFinished++
		if numFinished == 4 {
			cancel()
		}
	}

	deqTasksCalled := 0
	db.EXPECT().
		DequeueTasks(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, batchSize int) ([]tasks.Task, error) {
			deqTasksCalled++
			if deqTasksCalled == 1 {
				return deqTasks1, nil
			}
			if deqTasksCalled == 2 {
				return deqTasks2, nil
			}
			return nil, nil
		}).
		AnyTimes()

	db.EXPECT().
		UpdateTaskStatusFailed(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, id int64, lastError string) error {
			taskFinished()
			return nil
		}).
		MaxTimes(2)
	db.EXPECT().
		UpdateTaskStatusSucceeded(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, id int64) error {
			taskFinished()
			return nil
		}).
		MaxTimes(4)

	done := make(chan struct{})
	go func() {
		err := r.Run(ctx)
		require.ErrorIs(t, err, context.Canceled)
		close(done)
	}()

	<-done
}

func TestSendEmailRespectsCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := New(nil, nil)
	err := r.sendEmail(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRunQueryRespectsCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := New(nil, nil)
	err := r.runQuery(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDequeueTasks(t *testing.T) {
	dequeuedTasks := []tasks.Task{
		{ID: 10},
		{ID: 20},
		{ID: 30},
	}
	dbError := errors.New("operation failed")

	tests := []struct {
		name   string
		setup  func(t *testing.T) *mocks.MockDB
		assert func(t *testing.T, got []tasks.Task, err error)
	}{
		{
			name: "succeeds on first call",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					DequeueTasks(gomock.Any(), gomock.Any()).
					Return(dequeuedTasks, nil)
				return db
			},
			assert: func(t *testing.T, got []tasks.Task, err error) {
				require.Equal(t, dequeuedTasks, got)
				require.Nil(t, err)
			},
		},
		{
			name: "succeeds after 1 retry",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					DequeueTasks(gomock.Any(), gomock.Any()).
					Return(nil, dbError)
				db.EXPECT().
					DequeueTasks(gomock.Any(), gomock.Any()).
					Return(dequeuedTasks, nil)
				return db
			},
			assert: func(t *testing.T, got []tasks.Task, err error) {
				require.Equal(t, dequeuedTasks, got)
				require.Nil(t, err)
			},
		},
		{
			name: "fails after 2 retries",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					DequeueTasks(gomock.Any(), gomock.Any()).
					Return(nil, dbError)
				db.EXPECT().
					DequeueTasks(gomock.Any(), gomock.Any()).
					Return(nil, dbError)
				return db
			},
			assert: func(t *testing.T, got []tasks.Task, err error) {
				require.Nil(t, got)
				require.ErrorIs(t, err, dbError)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.setup(t)
			r := New(db, nil, WithRetryAttempts(2))
			got, err := r.dequeueTasks(context.Background())
			tt.assert(t, got, err)
		})
	}
}

func TestTaskFailed(t *testing.T) {
	taskID := int64(10)
	lastError := errors.New("task error")
	dbError := errors.New("operation failed")

	tests := []struct {
		name   string
		setup  func(t *testing.T) *mocks.MockDB
		assert func(t *testing.T, err error)
	}{
		{
			name: "succeeds on first call",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					UpdateTaskStatusFailed(gomock.Any(), taskID, lastError.Error()).
					Return(nil)
				return db
			},
			assert: func(t *testing.T, err error) {
				require.Nil(t, err)
			},
		},
		{
			name: "succeeds after 1 retry",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					UpdateTaskStatusFailed(gomock.Any(), taskID, lastError.Error()).
					Return(dbError)
				db.EXPECT().
					UpdateTaskStatusFailed(gomock.Any(), taskID, lastError.Error()).
					Return(nil)
				return db
			},
			assert: func(t *testing.T, err error) {
				require.Nil(t, err)
			},
		},
		{
			name: "fails after 2 retries",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					UpdateTaskStatusFailed(gomock.Any(), taskID, lastError.Error()).
					Return(dbError)
				db.EXPECT().
					UpdateTaskStatusFailed(gomock.Any(), taskID, lastError.Error()).
					Return(dbError)
				return db
			},
			assert: func(t *testing.T, err error) {
				require.ErrorIs(t, err, dbError)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.setup(t)
			r := New(db, nil, WithRetryAttempts(2))
			err := r.taskFailed(context.Background(), taskID, lastError)
			tt.assert(t, err)
		})
	}
}

func TestTaskSucceeded(t *testing.T) {
	taskID := int64(10)
	dbError := errors.New("operation failed")

	tests := []struct {
		name   string
		setup  func(t *testing.T) *mocks.MockDB
		assert func(t *testing.T, err error)
	}{
		{
			name: "succeeds on first call",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					UpdateTaskStatusSucceeded(gomock.Any(), taskID).
					Return(nil)
				return db
			},
			assert: func(t *testing.T, err error) {
				require.Nil(t, err)
			},
		},
		{
			name: "succeeds after 1 retry",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					UpdateTaskStatusSucceeded(gomock.Any(), taskID).
					Return(dbError)
				db.EXPECT().
					UpdateTaskStatusSucceeded(gomock.Any(), taskID).
					Return(nil)
				return db
			},
			assert: func(t *testing.T, err error) {
				require.Nil(t, err)
			},
		},
		{
			name: "fails after 2 retries",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					UpdateTaskStatusSucceeded(gomock.Any(), taskID).
					Return(dbError)
				db.EXPECT().
					UpdateTaskStatusSucceeded(gomock.Any(), taskID).
					Return(dbError)
				return db
			},
			assert: func(t *testing.T, err error) {
				require.ErrorIs(t, err, dbError)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.setup(t)
			r := New(db, nil, WithRetryAttempts(2))
			err := r.taskSucceeded(context.Background(), taskID)
			tt.assert(t, err)
		})
	}
}

func newDeps(t *testing.T) *mocks.MockDB {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	return mocks.NewMockDB(ctrl)
}
