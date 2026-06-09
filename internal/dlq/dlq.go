package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dist_task_que_prac/internal/config"
	"github.com/redis/go-redis/v9"
)

type DLQ struct {
	rdb *redis.Client
	cfg config.Config
}

func New(rdb *redis.Client, cfg config.Config) *DLQ {
	return &DLQ{
		rdb: rdb,
		cfg: cfg,
	}
}

func (dlq *DLQ) Send(ctx context.Context, msg redis.XMessage, taskType string, reason error) error {
	msgJson, _ := json.Marshal(msg.Values)
	_, err := dlq.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: dlq.cfg.DLQStreamKey,
		Values: map[string]any{
			"entryId":   msg.ID,
			"reason":    reason.Error(),
			"msg":       string(msgJson),
			"failed_at": time.Now().UnixMilli(),
			"task_type": taskType,
		},
	}).Result()

	if err != nil {
		return fmt.Errorf("xadd dlq: %w", err)
	}
	return nil
}
