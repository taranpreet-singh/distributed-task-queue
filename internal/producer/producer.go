package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dist_task_que_prac/internal/config"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Task struct {
	Type     string
	Payload  map[string]any
	Priority int
}

type Producer struct {
	rdb *redis.Client
	cfg config.Config
}

func New(cfg config.Config) (*Producer, error) {
	opts, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return nil, fmt.Errorf("Redis URL Parsing Failed: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &Producer{
		rdb: rdb,
		cfg: cfg,
	}, nil
}

func (p *Producer) Close() error {
	return p.rdb.Close()
}

func (p *Producer) Publish(ctx context.Context, task Task) (string, error) {
	args, err := getXaddArgs(p.cfg.Redis.StreamKey, &task)
	if err != nil {
		return "", err
	}

	id, err := p.rdb.XAdd(ctx, args).Result()

	if err != nil {
		return "", fmt.Errorf("XAdd error: %w", err)
	}

	return id, nil
}

func (p *Producer) PublishBatch(ctx context.Context, tasks []Task) ([]string, error) {
	pipe := p.rdb.Pipeline()

	cmds := make([]*redis.StringCmd, len(tasks))
	for i, t := range tasks {
		args, err := getXaddArgs(p.cfg.Redis.StreamKey, &t)
		if err != nil {
			return nil, fmt.Errorf("Marshal failed for task %d: %w", i, err)
		}

		cmds[i] = pipe.XAdd(ctx, args)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("Pipeline Exec Failed: %w", err)
	}

	id := make([]string, len(tasks))
	for i, cmd := range cmds {
		id[i] = cmd.Val()
	}

	return id, nil
}

func getXaddArgs(streamKey string, task *Task) (*redis.XAddArgs, error) {
	payload, err := json.Marshal(task.Payload)
	if err != nil {
		return nil, fmt.Errorf("Marshal payload failed. %w", err)
	}

	return &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: 100000,
		Approx: true,
		Values: map[string]any{
			"task_id":    uuid.NewString(),
			"task_type":  task.Type,
			"payload":    string(payload),
			"priority":   task.Priority,
			"created_at": time.Now().UnixMilli(),
		},
	}, nil
}
