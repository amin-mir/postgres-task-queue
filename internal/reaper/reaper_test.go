package reaper

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"dns/internal/tasks/mocks"
)

func TestReap(t *testing.T) {
	reapedIDs := []int64{1, 2, 3, 4}

	ctx, cancel := context.WithCancel(context.Background())
	db := newDeps(t)

	var called atomic.Int64
	db.EXPECT().
		ReaperUpdateStatusFailed(gomock.Any()).
		DoAndReturn(func(ctx context.Context) ([]int64, error) {
			if called.Add(1) == 5 {
				cancel()
			}
			return reapedIDs, nil
		}).
		AnyTimes()
	db.EXPECT().
		ReaperUpdateStatusQueued(gomock.Any()).
		DoAndReturn(func(ctx context.Context) ([]int64, error) {
			if called.Add(1) == 5 {
				cancel()
			}
			return reapedIDs, nil
		}).
		AnyTimes()

	done := make(chan struct{})
	r := New(db, slog.Default(),
		WithReapInterval(time.Second),
		WithReapInterval(10*time.Millisecond),
	)

	go func() {
		r.Reap(ctx)
		close(done)
	}()
	<-done
}

func TestReapUpdateStatusQueued(t *testing.T) {
	reapedIDs := []int64{1, 2, 3, 4}
	dbError := errors.New("operation failed")

	tests := []struct {
		name   string
		setup  func(t *testing.T) *mocks.MockDB
		assert func(t *testing.T, got []int64, err error)
	}{
		{
			name: "succeeds on first call",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					ReaperUpdateStatusQueued(gomock.Any()).
					Return(reapedIDs, nil)
				return db
			},
			assert: func(t *testing.T, got []int64, err error) {
				require.Equal(t, reapedIDs, got)
				require.Nil(t, err)
			},
		},
		{
			name: "succeeds after 1 retry",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					ReaperUpdateStatusQueued(gomock.Any()).
					Return(nil, dbError)
				db.EXPECT().
					ReaperUpdateStatusQueued(gomock.Any()).
					Return(reapedIDs, nil)
				return db
			},
			assert: func(t *testing.T, got []int64, err error) {
				require.Equal(t, reapedIDs, got)
				require.Nil(t, err)
			},
		},
		{
			name: "fails after 2 retries",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					ReaperUpdateStatusQueued(gomock.Any()).
					Return(nil, dbError)
				db.EXPECT().
					ReaperUpdateStatusQueued(gomock.Any()).
					Return(nil, dbError)
				return db
			},
			assert: func(t *testing.T, got []int64, err error) {
				require.Nil(t, got)
				require.ErrorIs(t, err, dbError)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.setup(t)
			r := New(db, nil, WithRetryAttempts(2))
			ids, err := r.reapUpdateStatusQueued(context.Background())
			tt.assert(t, ids, err)
		})
	}
}

func TestReapUpdateStatusFailed(t *testing.T) {
	reapedIDs := []int64{1, 2, 3, 4}
	dbError := errors.New("operation failed")

	tests := []struct {
		name   string
		setup  func(t *testing.T) *mocks.MockDB
		assert func(t *testing.T, got []int64, err error)
	}{
		{
			name: "succeeds on first call",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					ReaperUpdateStatusFailed(gomock.Any()).
					Return(reapedIDs, nil)
				return db
			},
			assert: func(t *testing.T, got []int64, err error) {
				require.Equal(t, reapedIDs, got)
				require.Nil(t, err)
			},
		},
		{
			name: "succeeds after 1 retry",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					ReaperUpdateStatusFailed(gomock.Any()).
					Return(nil, dbError)
				db.EXPECT().
					ReaperUpdateStatusFailed(gomock.Any()).
					Return(reapedIDs, nil)
				return db
			},
			assert: func(t *testing.T, got []int64, err error) {
				require.Equal(t, reapedIDs, got)
				require.Nil(t, err)
			},
		},
		{
			name: "fails after 2 retries",
			setup: func(t *testing.T) *mocks.MockDB {
				db := newDeps(t)
				db.EXPECT().
					ReaperUpdateStatusFailed(gomock.Any()).
					Return(nil, dbError)
				db.EXPECT().
					ReaperUpdateStatusFailed(gomock.Any()).
					Return(nil, dbError)
				return db
			},
			assert: func(t *testing.T, got []int64, err error) {
				require.Nil(t, got)
				require.ErrorIs(t, err, dbError)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.setup(t)
			r := New(db, nil, WithRetryAttempts(2))
			ids, err := r.reapUpdateStatusFailed(context.Background())
			tt.assert(t, ids, err)
		})
	}
}

func newDeps(t *testing.T) *mocks.MockDB {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	return mocks.NewMockDB(ctrl)
}
