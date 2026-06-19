package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dist_task_que_prac/internal/config"
	"github.com/dist_task_que_prac/internal/dlq"
	"github.com/redis/go-redis/v9"
)

type TaskType string

const (
	TaskSendWebhook TaskType = "SendWebhook"
	TaskSendEmail   TaskType = "SendEmail"
)

type Message struct {
	EntryID   string
	TaskID    string
	Type      TaskType
	Payload   map[string]any
	Priority  int
	CreatedAt int64
}

type HandlerFunc func(ctx context.Context, msg *Message) error

type Worker struct {
	rdb     *redis.Client
	cfg     config.Config
	name    string
	handler map[TaskType]HandlerFunc
	sem     chan struct{}
	wg      sync.WaitGroup
	dlq     *dlq.DLQ
}

func New(cfg config.Config, workerName string) (*Worker, error) {
	opts, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return nil, err
	}

	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	d := dlq.New(rdb, cfg.Redis.DLQStreamKey)

	return &Worker{
		rdb:     rdb,
		cfg:     cfg,
		name:    workerName,
		handler: make(map[TaskType]HandlerFunc),
		sem:     make(chan struct{}, cfg.Concurrency),
		dlq:     d,
	}, nil
}

func (w *Worker) RegisterHandler(taskType TaskType, handler HandlerFunc) {
	w.handler[taskType] = handler
}

// Close waits for all in-flight handlers to finish and then closes the Redis connection.
func (w *Worker) Close() error {
	w.wg.Wait()
	return w.rdb.Close()
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.ensureConsumerGroupExists(ctx); err != nil {
		return err
	}

	if err := w.reclaimPending(ctx); err != nil {
		slog.Warn("error reclaiming pending tasks on startup", "err", err)
	}

	reclaimTicker := time.NewTicker(30 * time.Second)
	defer reclaimTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-reclaimTicker.C:
			if err := w.reclaimPending(ctx); err != nil {
				slog.Warn("periodic reclaim error", "err", err)
			}
		default:
		}

		msgs, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Streams:  []string{w.cfg.Redis.StreamKey, ">"},
			Group:    w.cfg.Redis.ConsumerGroupKey,
			Consumer: w.name,
			Block:    time.Duration(w.cfg.Redis.BlockMs) * time.Millisecond,
			Count:    w.cfg.Redis.BatchSize,
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) {
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
	cursor := "0-0"
	for {
		msgs, nextCursor, err := w.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   w.cfg.Redis.StreamKey,
			Group:    w.cfg.Redis.ConsumerGroupKey,
			Consumer: w.name,
			MinIdle:  time.Duration(w.cfg.Redis.ClaimedIdleMs) * time.Millisecond,
			Start:    cursor,
			Count:    w.cfg.Redis.BatchSize,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil
			}
			return fmt.Errorf("xautoclaim: %w", err)
		}

		if len(msgs) > 0 {
			slog.Info("reclaimed pending messages", "count", len(msgs))
			w.processXMessages(ctx, msgs)
		}

		if nextCursor == "0-0" {
			return nil
		}
		cursor = nextCursor
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
		if dlqErr := w.dlq.Send(ctx, entry, "unknown", err); dlqErr != nil {
			slog.Error("dlq send failed for decode error", "id", entry.ID, "err", dlqErr)
		}
		w.xack(ctx, entry.ID)
		return
	}

	handler, ok := w.handler[msg.Type]
	if !ok {
		slog.Warn("no handler registered", "task_type", msg.Type, "id", entry.ID)
		noHandlerErr := fmt.Errorf("no handler registered for task_type %q", msg.Type)
		if dlqErr := w.dlq.Send(ctx, entry, string(msg.Type), noHandlerErr); dlqErr != nil {
			slog.Error("dlq send failed for unknown handler", "id", entry.ID, "err", dlqErr)
		}
		w.xack(ctx, entry.ID)
		return
	}

	stopHeartbeat := w.startHeartbeat(ctx, entry.ID)
	defer stopHeartbeat()

	if handlerErr := handler(ctx, &msg); handlerErr != nil {
		slog.Error("handler error", "task_type", msg.Type, "id", entry.ID, "err", handlerErr)
		w.handleFailure(ctx, entry, msg, handlerErr)
		return
	}

	w.xack(ctx, entry.ID)
	slog.Info("task completed", "task_type", msg.Type, "id", entry.ID)
}

func (w *Worker) handleFailure(ctx context.Context, entry redis.XMessage, msg Message, err error) {
	pending, pelErr := w.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: w.cfg.Redis.StreamKey,
		Group:  w.cfg.Redis.ConsumerGroupKey,
		Start:  msg.EntryID,
		End:    msg.EntryID,
		Count:  1,
	}).Result()
	if pelErr != nil {
		slog.Error("could not read PEL entry", "id", msg.EntryID, "err", pelErr)
		return
	}
	if len(pending) == 0 {
		slog.Warn("PEL entry not found, task may have been claimed by another worker", "id", msg.EntryID)
		return
	}

	deliveryCount := pending[0].RetryCount

	if deliveryCount >= int64(w.cfg.MaxRetries) {
		slog.Error("max retries exceeded, moving to DLQ", "id", msg.EntryID, "delivery", deliveryCount)
		if dlqErr := w.dlq.Send(ctx, entry, string(msg.Type), err); dlqErr != nil {
			slog.Error("dlq send failure", "id", entry.ID, "err", dlqErr)
		}
		w.xack(ctx, entry.ID)
		return
	}
	// task stays in PEL; will be reclaimed after ClaimedIdleMs
}

// startHeartbeat periodically calls XCLAIM to reset the idle timer on an in-flight
// entry, preventing other workers from stealing it during long-running handlers.
// Returns a stop function that must be called (via defer) when the handler finishes.
func (w *Worker) startHeartbeat(ctx context.Context, id string) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Duration(w.cfg.Redis.HeartbeatMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.rdb.XClaim(ctx, &redis.XClaimArgs{
					Stream:   w.cfg.Redis.StreamKey,
					Group:    w.cfg.Redis.ConsumerGroupKey,
					Consumer: w.name,
					MinIdle:  0,
					Messages: []string{id},
				}).Err(); err != nil {
					slog.Warn("heartbeat xclaim failed", "id", id, "err", err)
				}
			}
		}
	}()
	return func() { close(done) }
}

// xack is a helper that ACKs a message and logs if it fails.
func (w *Worker) xack(ctx context.Context, id string) {
	if err := w.rdb.XAck(ctx, w.cfg.Redis.StreamKey, w.cfg.Redis.ConsumerGroupKey, id).Err(); err != nil {
		slog.Error("xack failed", "id", id, "err", err)
	}
}

func (w *Worker) ensureConsumerGroupExists(ctx context.Context) error {
	err := w.rdb.XGroupCreateMkStream(ctx, w.cfg.Redis.StreamKey, w.cfg.Redis.ConsumerGroupKey, "0").Err()
	if err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			slog.Debug("consumer group already exists, skipping creation")
			return nil
		}
		return fmt.Errorf("error creating group: %w", err)
	}
	slog.Info("consumer group created", "group", w.cfg.Redis.ConsumerGroupKey, "stream", w.cfg.Redis.StreamKey)
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
		return Message{}, fmt.Errorf("payload unmarshal failed: %w", err)
	}

	var priority int
	if raw := get("priority"); raw != "" {
		if p, err := strconv.Atoi(raw); err == nil {
			priority = p
		} else {
			slog.Warn("failed to parse priority field", "id", msg.ID, "value", raw)
		}
	}

	var createdAt int64
	if raw := get("created_at"); raw != "" {
		if c, err := strconv.ParseInt(raw, 10, 64); err == nil {
			createdAt = c
		} else {
			slog.Warn("failed to parse created_at field", "id", msg.ID, "value", raw)
		}
	}

	return Message{
		EntryID:   msg.ID,
		TaskID:    get("task_id"),
		Type:      TaskType(get("task_type")),
		Payload:   payload,
		Priority:  priority,
		CreatedAt: createdAt,
	}, nil
}
