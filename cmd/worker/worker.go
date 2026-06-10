package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	w, err := consumer.New(*cfg, *workerName)
	if err != nil {
		slog.Error("Failed to create worker", "Error", err)
		os.Exit(1)
	}

	w.RegisterHandler("PrintFibonacci", fibonacci)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("Worker started", "name", *workerName, "group", cfg.ConsumerGroupKey, "stream", cfg.StreamKey)

	if err := w.Run(ctx); err != nil {
		slog.Error("Worker entered in an Error", "Error", err)
		os.Exit(1)
	}
	slog.Info("Worker shut down cleanly")
}

// Custom task that can be performed

func fibonacci(ctx context.Context, msg *consumer.Message) error {
	n := 5
	var fib func(n int) int

	fib = func(n int) int {
		if n <= 1 {
			return n
		}
		return fib(n-1) + fib(n-2)
	}

	fmt.Println(fib(n))

	slog.Info("handling example task", "task_id", msg.TaskID, "payload", msg.Payload)
	time.Sleep(50 * time.Millisecond)
	return nil
}
