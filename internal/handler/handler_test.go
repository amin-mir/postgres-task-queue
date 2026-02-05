package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"dns/internal/tasks"
	"dns/internal/tasks/mocks"
)

func TestStoreTask(t *testing.T) {
	type tc struct {
		name   string
		body   func(t *testing.T) []byte
		setup  func(*mocks.MockDB)
		status int
		output createTaskResp
	}

	validReq := tasks.StoreTaskReq{
		Name: "test name",
		Type: TaskTypeSendEmail,
		Payload: map[string]string{
			"arg1": "val1",
			"arg2": "val2",
		},
	}

	tests := []tc{
		{
			name: "invalid input",
			body: func(t *testing.T) []byte {
				return []byte("invalid json")
			},
			setup:  func(md *mocks.MockDB) {},
			status: http.StatusBadRequest,
			output: createTaskResp{},
		},
		{
			name: "success",
			body: func(t *testing.T) []byte {
				b, err := json.Marshal(validReq)
				require.NoError(t, err)
				return b
			},
			setup: func(db *mocks.MockDB) {
				db.EXPECT().
					StoreTask(gomock.Any(), validReq).
					Return(int64(10), nil).
					Times(1)
			},
			status: http.StatusOK,
			output: createTaskResp{ID: 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newDeps(t)
			tt.setup(db)

			logger := slog.New(slog.DiscardHandler)
			h := New(db, logger)
			mux := http.NewServeMux()
			h.Register(mux)

			req := httptest.NewRequest("POST", "/tasks", bytes.NewReader(tt.body(t)))
			resp := doHTTPRequest(mux, req)

			require.Equal(t, tt.status, resp.Code)
			if tt.status == http.StatusOK {
				require.Contains(t, resp.Header().Get("Content-Type"), "application/json")

				var got createTaskResp
				err := json.NewDecoder(resp.Body).Decode(&got)
				require.NoError(t, err)
				require.Equal(t, tt.output, got)
			}
		})
	}
}

func TestGetTaskStatus(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		setup  func(*mocks.MockDB)
		status int
		want   getTaskStatusResp
	}{
		{
			name:   "missing id path param",
			url:    "/tasks",
			setup:  func(md *mocks.MockDB) {},
			status: http.StatusMethodNotAllowed,
			want:   getTaskStatusResp{},
		},
		{
			name:   "invalid id path param",
			url:    "/tasks/abc",
			setup:  func(md *mocks.MockDB) {},
			status: http.StatusBadRequest,
			want:   getTaskStatusResp{},
		},
		{
			name: "success",
			url:  "/tasks/10",
			setup: func(db *mocks.MockDB) {
				db.EXPECT().GetTaskStatus(gomock.Any(), int64(10)).Return("queued", nil).Times(1)
			},
			status: http.StatusOK,
			want:   getTaskStatusResp{Status: "queued"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newDeps(t)
			tt.setup(db)

			logger := slog.New(slog.DiscardHandler)
			h := New(db, logger)
			mux := http.NewServeMux()
			h.Register(mux)

			req := httptest.NewRequest("GET", tt.url, nil)
			resp := doHTTPRequest(mux, req)

			require.Equal(t, tt.status, resp.Code)
			if tt.status == http.StatusOK {
				require.Contains(t, resp.Header().Get("Content-Type"), "application/json")

				var got getTaskStatusResp
				err := json.NewDecoder(resp.Body).Decode(&got)
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestGetTaskHistory(t *testing.T) {
	dbTaskEvents := []tasks.Event{
		{
			Status:    "queued",
			CreatedAt: time.Now().Add(-2 * time.Hour),
		},
		{
			Status:    "running",
			CreatedAt: time.Now().Add(-1 * time.Hour),
		},
		{
			Status:    "succeeded",
			CreatedAt: time.Now(),
		},
	}

	var respTaskEvents []taskEvent
	for _, e := range dbTaskEvents {
		te := taskEvent{Status: e.Status, CreatedAt: e.CreatedAt.UnixMilli()}
		respTaskEvents = append(respTaskEvents, te)
	}

	tests := []struct {
		name   string
		url    string
		setup  func(*mocks.MockDB)
		status int
		want   getTaskHistoryResp
	}{
		{
			name:   "missing id path param",
			url:    "/tasks//history",
			setup:  func(md *mocks.MockDB) {},
			status: http.StatusMovedPermanently,
			want:   getTaskHistoryResp{},
		},
		{
			name:   "invalid id path param",
			url:    "/tasks/abc/history",
			setup:  func(md *mocks.MockDB) {},
			status: http.StatusBadRequest,
			want:   getTaskHistoryResp{},
		},
		{
			name: "success",
			url:  "/tasks/10/history",
			setup: func(db *mocks.MockDB) {
				db.EXPECT().
					GetTaskEvents(gomock.Any(), int64(10)).
					Return(dbTaskEvents, nil).
					Times(1)
			},
			status: http.StatusOK,
			want:   getTaskHistoryResp{History: respTaskEvents},
		},
		{
			name: "DB error",
			url:  "/tasks/10/history",
			setup: func(db *mocks.MockDB) {
				db.EXPECT().
					GetTaskEvents(gomock.Any(), int64(10)).
					Return(nil, errors.New("db error")).
					Times(1)
			},
			status: http.StatusInternalServerError,
			want:   getTaskHistoryResp{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newDeps(t)
			tt.setup(db)

			logger := slog.New(slog.DiscardHandler)
			h := New(db, logger)
			mux := http.NewServeMux()
			h.Register(mux)

			req := httptest.NewRequest("GET", tt.url, nil)
			resp := doHTTPRequest(mux, req)

			require.Equal(t, tt.status, resp.Code)
			if tt.status == http.StatusOK {
				require.Contains(t, resp.Header().Get("Content-Type"), "application/json")

				var got getTaskHistoryResp
				err := json.NewDecoder(resp.Body).Decode(&got)
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func newDeps(t *testing.T) *mocks.MockDB {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	return mocks.NewMockDB(ctrl)
}

// We don't have to worry about closing the recorder body.
// If we returned w.Result() which is *http.Response, then the caller should
// defer resp.Body.Close().
func doHTTPRequest(mux http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}
