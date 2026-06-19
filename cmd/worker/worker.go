package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dist_task_que_prac/internal/config"
	"github.com/dist_task_que_prac/internal/consumer"
)

func main() {
	workerName := flag.String("name", "", "Unique worker name defaults to hostname")
	flag.Parse()

	if *workerName == "" {
		host, _ := os.Hostname()
		*workerName = fmt.Sprintf("worker-%s", host)
	}

	cfg := config.LoadConfig()

	w, err := consumer.New(cfg, *workerName)
	if err != nil {
		slog.Error("Failed to create worker", "Error", err)
		os.Exit(1)
	}
	defer w.Close()

	w.RegisterHandler(consumer.TaskSendWebhook, webhookHandler())
	w.RegisterHandler(consumer.TaskSendEmail, emailHandler(cfg.SMTP))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("Worker started", "name", *workerName, "group", cfg.Redis.ConsumerGroupKey, "stream", cfg.Redis.StreamKey)

	if err := w.Run(ctx); err != nil {
		slog.Error("Worker entered in an Error", "Error", err)
		os.Exit(1)
	}
	slog.Info("Worker shut down cleanly")
}
