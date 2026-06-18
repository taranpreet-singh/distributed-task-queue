package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/dist_task_que_prac/internal/config"
	"github.com/dist_task_que_prac/internal/producer"
)

func main() {
	taskType := flag.String("type", "PrintFibonacci", "Type of the task")
	count := flag.Int("count", 5, "Number of tasks to produce")
	delay := flag.Int("delay", 500, "Delay between producing tasks in ms")
	flag.Parse()

	cfg := config.LoadConfig()

	p, err := producer.New(cfg)
	if err != nil {
		slog.Error("failed to create producer", "err", err)
		os.Exit(1)
	}
	defer p.Close()

	ctx := context.Background()
	for i := range *count {
		task := producer.Task{
			Type: *taskType,
			Payload: map[string]any{
				"index":   i,
				"message": fmt.Sprintf("task-%d", i),
			},
		}

		id, err := p.Publish(ctx, task)
		if err != nil {
			slog.Error("failed to publish task", "err", err, "index", i)
			continue
		}

		slog.Info("published", "task_id", id, "type", *taskType, "index", i)

		if *delay > 0 {
			time.Sleep(time.Duration(*delay) * time.Millisecond)
		}
	}
}
