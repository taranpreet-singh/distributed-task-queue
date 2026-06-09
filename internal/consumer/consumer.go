package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	dlq "github.com/dist_task_que_prac/internal/DLQ"
	"github.com/dist_task_que_prac/internal/config"
	"github.com/redis/go-redis/v9"
)

type Message struct {
	EntryID   string
	TaskID    string
	Type      string
	Payload   map[string]any
	Priority  int
	CreatedAt int64
}

type HandlerFunc func(ctx context.Context, msg *Message) error

type Worker struct {
	rdb     *redis.Client
	cfg     config.Config
	name    string
	handler map[string]HandlerFunc // "video-processing": ProcessVideo()
	sem     chan struct{}
	wg      sync.WaitGroup
	dlq     *dlq.DLQ
}

func New(cfg config.Config, workerName string) (*Worker, error) {
	opts, err := redis.ParseURL(cfg.RedisUrl)
	if err != nil {
		return nil, err
	}

	rdb := redis.NewClient(opts)
	if rdb.Ping(context.Background()).Err() != nil {
		return nil, err
	}

	dlq := dlq.New(rdb, cfg)

	return &Worker{
		rdb:     rdb,
		cfg:     cfg,
		name:    workerName,
		handler: make(map[string]HandlerFunc),
		sem:     make(chan struct{}, 10),
		dlq:     dlq,
	}, nil
}

func (w *Worker) RegisterHandler(taskType string, handler HandlerFunc) {
	w.handler[taskType] = handler
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.ensureConsumerGroupExists(ctx); err != nil {
		slog.Error("Failed to create Consumer Group", "Error", err)
	}

	if err := w.reclaimPending(ctx); err != nil {
		slog.Warn("Error reclaiming pending tasks", "Error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msgs, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Streams:  []string{w.cfg.StreamKey, ">"},
			Group:    w.cfg.ConsumerGroupKey,
			Consumer: w.name,
			Block:    time.Duration(w.cfg.BlockMs) * time.Millisecond,
			Count:    w.cfg.BatchSize,
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) {
				// No new messages in block window; loop and try again.
				continue
			}
			if errors.Is(err, context.Canceled) {
				return nil
			}
			slog.Error("xreadgroup error", "err", err)
			time.Sleep(time.Second)
			continue
		}

		for _, stream := range msgs {
			w.processXMessages(ctx, stream.Messages)
		}
	}
}

func (w *Worker) reclaimPending(ctx context.Context) error {
	for {
		msgs, _, err := w.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   w.cfg.StreamKey,
			Group:    w.cfg.ConsumerGroupKey,
			Consumer: w.name,
			MinIdle:  time.Duration(w.cfg.ClaimedIdleMs) * time.Millisecond,
			Start:    "0-0",
			Count:    w.cfg.BatchSize,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil
			}
			return fmt.Errorf("xautoclaim: %w", err)
		}

		if len(msgs) == 0 {
			return nil
		}

		slog.Info("reclaimed pending messages", "count", len(msgs))

		w.processXMessages(ctx, msgs)
	}
}

func (w *Worker) processXMessages(ctx context.Context, msgs []redis.XMessage) {
	for _, entry := range msgs {
		w.sem <- struct{}{}
		w.wg.Add(1)
		go func(e redis.XMessage) {
			defer w.wg.Done()
			defer func() { <-w.sem }()
			w.process(ctx, e)
		}(entry)
	}
}

func (w *Worker) process(ctx context.Context, entry redis.XMessage) {
	msg, err := decode(entry)
	if err != nil {
		slog.Error("decode failed", "id", entry.ID, "err", err)
		_ = w.rdb.XAck(ctx, w.cfg.StreamKey, w.cfg.ConsumerGroupKey, entry.ID)
		return
	}

	handler, ok := w.handler[msg.Type]
	if !ok {
		slog.Warn("no handler registered", "task_type", msg.Type, "id", entry.ID)
		_ = w.rdb.XAck(ctx, w.cfg.StreamKey, w.cfg.ConsumerGroupKey, entry.ID)
		return
	}

	if handlerErr := handler(ctx, &msg); handlerErr != nil {
		slog.Error("handler error", "task_type", msg.Type, "id", entry.ID, "err", handlerErr)
		// metrics.TasksFailed.WithLabelValues(msg.TaskType, w.name).Inc()
		w.handleFailure(ctx, entry, msg, handlerErr)
		return
	}

	_ = w.rdb.XAck(ctx, w.cfg.StreamKey, w.cfg.ConsumerGroupKey, entry.ID)
	slog.Info("Task completed", "task_type", msg.Type, "id", entry.ID)
}

func (w *Worker) handleFailure(ctx context.Context, entry redis.XMessage, msg Message, err error) {
	pending, err := w.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: w.cfg.StreamKey,
		Group:  w.cfg.ConsumerGroupKey,
		Start:  msg.EntryID,
		End:    msg.EntryID,
		Count:  1,
	}).Result()
	if err != nil {
		slog.Error("Could not read PEL entry", "Error", err)
		return
	}

	deliveryCount := pending[0].RetryCount

	if deliveryCount >= int64(w.cfg.MaxRetries) {
		slog.Error("Max retries exceeded, Moving to DLQ", "id", msg.EntryID, "delivery", deliveryCount)
		if err := w.dlq.Send(ctx, entry, msg.Type, err); err != nil {
			slog.Error("dlq send failure: %w", "id", entry.ID, "Error", err)
		}
		return
	}
	// let it be, it will be auto-claimed after the idle time is over
}

func (w *Worker) ensureConsumerGroupExists(ctx context.Context) error {
	// this will create the Stream if it doesn't exists. But, will thorw an error for the group
	err := w.rdb.XGroupCreateMkStream(ctx, w.cfg.StreamKey, w.cfg.ConsumerGroupKey, "0").Err()

	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			fmt.Println("Consumer Group already exists, skipping creation")
			return nil
		}
		return fmt.Errorf("Error creating Group: %w", err)
	}

	fmt.Println("Consumer group created successfully!")
	return nil
}

func decode(msg redis.XMessage) (Message, error) {
	get := func(key string) string {
		if v, ok := msg.Values[key].(string); ok {
			return v
		}
		return ""
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(get("payload")), &payload); err != nil {
		return Message{}, fmt.Errorf("Payload unmarshal failed: %w", err)
	}

	var priority int
	fmt.Sscanf(get("priority"), "%d", &priority)
	var createdAt int64
	fmt.Sscanf(get("created_at"), "%d", &createdAt)

	return Message{
		EntryID:   msg.ID,
		TaskID:    get("task_id"),
		Type:      get("task_type"),
		Payload:   payload,
		Priority:  priority,
		CreatedAt: createdAt,
	}, nil
}
