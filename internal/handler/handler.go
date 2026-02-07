package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"dns/internal/tasks"
)

type Handler struct {
	db     tasks.DB
	logger *slog.Logger
}

func New(db tasks.DB, logger *slog.Logger) *Handler {
	return &Handler{
		db:     db,
		logger: logger,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /tasks", h.createTask)
	mux.HandleFunc("GET /tasks/{id}", h.getTaskStatus)
	mux.HandleFunc("GET /tasks/{id}/history", h.getTaskHistory)
}

type createTaskResp struct {
	ID int64 `json:"id"`
}

func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req tasks.StoreTaskReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.ErrorContext(ctx, "createTask request decode error", "err", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if !validateCreateTaskReq(&req) {
		h.logger.ErrorContext(ctx, "invalid createTask request decode error", "req", req)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	id, err := h.db.StoreTask(ctx, req)
	if err != nil {
		h.logger.ErrorContext(ctx, "DB.StoreTask error", "err", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := createTaskResp{
		ID: id,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.ErrorContext(ctx, "JSON encode error", "err", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func validateCreateTaskReq(req *tasks.StoreTaskReq) bool {
	if req.Name == "" {
		return false
	}

	if req.Type != tasks.TypeSendEmail && req.Type != tasks.TypeRunQuery {
		return false
	}

	if len(req.Payload) == 0 {
		return false
	}

	return true
}

type getTaskStatusResp struct {
	Status string `json:"status"`
}

func (h *Handler) getTaskStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.logger.InfoContext(ctx, "getTaskStatus invalid path param", "id", idStr)
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	status, err := h.db.GetTaskStatus(ctx, id)
	if err != nil {
		h.logger.ErrorContext(ctx, "DB.GetTaskStatus error", "err", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := getTaskStatusResp{
		Status: status,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.ErrorContext(ctx, "JSON encode error", "err", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

type taskEvent struct {
	Status    string `json:"status"`
	CreatedAt int64  `json:"created_at"`
}

type getTaskHistoryResp struct {
	History []taskEvent
}

func (h *Handler) getTaskHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		h.logger.InfoContext(ctx, "getTaskHistory invalid path param", "id", idStr)
		http.Error(w, "Invalid task ID", http.StatusBadRequest)
		return
	}

	events, err := h.db.GetTaskEvents(ctx, id)
	if err != nil {
		h.logger.ErrorContext(ctx, "DB.GetTaskEvents error", "err", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := getTaskHistoryResp{
		History: make([]taskEvent, len(events)),
	}
	for i, e := range events {
		resp.History[i] = taskEvent{
			Status:    e.Status,
			CreatedAt: e.CreatedAt.UnixMilli(),
		}
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.logger.ErrorContext(ctx, "JSON encode error", "err", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}
