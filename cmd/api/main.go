package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/dist_task_que_prac/internal/config"
	"github.com/dist_task_que_prac/internal/producer"
)

// taskRequest is the JSON body clients POST to /tasks.
type taskRequest struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

func main() {
	cfg := config.LoadConfig()

	p, err := producer.New(cfg)
	if err != nil {
		slog.Error("failed to create producer", "err", err)
		os.Exit(1)
	}
	defer p.Close()

	mux := http.NewServeMux()

	// POST /tasks — publish a task to the queue.
	mux.HandleFunc("POST /tasks", func(w http.ResponseWriter, r *http.Request) {
		var req taskRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		if req.Type == "" {
			http.Error(w, "missing task type", http.StatusBadRequest)
			return
		}

		id, err := p.Publish(r.Context(), producer.Task{
			Type:    req.Type,
			Payload: req.Payload,
		})
		if err != nil {
			slog.Error("publish failed", "err", err)
			http.Error(w, "publish failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"task_id": id})
	})

	// GET /healthz — readiness probe.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              ":8090",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	slog.Info("api listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
