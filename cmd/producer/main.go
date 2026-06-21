package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/dist_task_que_prac/internal/config"
	"github.com/dist_task_que_prac/internal/consumer"
	"github.com/dist_task_que_prac/internal/producer"
)

var emailRecipients = []string{
	"alice@example.com",
	"bob@example.com",
	"charlie@example.com",
}

var emailSubjects = []string{
	"Your order has been placed",
	"Welcome to our platform",
	"Password reset request",
	"Weekly digest",
}

func randomWebhookTask(i int, webhookURL string) producer.Task {
	return producer.Task{
		Type: string(consumer.TaskSendWebhook),
		Payload: map[string]any{
			"url": webhookURL,
			"body": map[string]any{
				"event": "task.created",
				"index": i,
				"data":  fmt.Sprintf("payload-%d", i),
			},
			"headers": map[string]any{
				"X-Task-Index": fmt.Sprintf("%d", i),
			},
		},
	}
}

func randomEmailTask(i int) producer.Task {
	return producer.Task{
		Type: string(consumer.TaskSendEmail),
		Payload: map[string]any{
			"to":      emailRecipients[rand.Intn(len(emailRecipients))],
			"subject": emailSubjects[rand.Intn(len(emailSubjects))],
			"body":    fmt.Sprintf("This is automated email #%d sent via the task queue.", i),
		},
	}
}

func main() {
	count := flag.Int("count", 1000, "Number of tasks to produce")
	delay := flag.Int("delay", 100, "Delay between tasks in ms")
	flag.Parse()

	cfg := config.LoadConfig()
	webhookURL := cfg.WebhookURL

	p, err := producer.New(cfg)
	if err != nil {
		slog.Error("failed to create producer", "err", err)
		os.Exit(1)
	}
	defer p.Close()

	ctx := context.Background()
	for i := range *count {
		var task producer.Task
		if webhookURL != "" && rand.Intn(2) == 0 {
			task = randomWebhookTask(i, webhookURL)
		} else {
			task = randomEmailTask(i)
		}

		id, err := p.Publish(ctx, task)
		if err != nil {
			slog.Error("failed to publish task", "err", err, "index", i)
			continue
		}

		slog.Info("published", "task_id", id, "type", task.Type, "index", i)

		if *delay > 0 {
			time.Sleep(time.Duration(*delay) * time.Millisecond)
		}
	}
}
