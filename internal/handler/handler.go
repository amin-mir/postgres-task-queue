package handler

import "net/http"

type Handler struct {
}

func New() *Handler {
	return &Handler{}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /tasks", h.createTask)
	mux.HandleFunc("GET /tasks/{id}", h.getTask)
	mux.HandleFunc("GET /tasks/{id}/history", h.getTaskHistory)
}

type createTaskResp struct {
	ID string `json:"id"`
}

func (h *Handler) createTask(w http.ResponseWriter, r *http.Request) {

}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {

}

func (h *Handler) getTaskHistory(w http.ResponseWriter, r *http.Request) {

}
