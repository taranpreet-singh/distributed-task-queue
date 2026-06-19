package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type DLQ struct {
	rdb       *redis.Client
	streamKey string
}

func New(rdb *redis.Client, streamKey string) *DLQ {
	return &DLQ{rdb: rdb, streamKey: streamKey}
}

func (dlq *DLQ) Send(ctx context.Context, msg redis.XMessage, taskType string, reason error) error {
	msgJson, marshalErr := json.Marshal(msg.Values)
	if marshalErr != nil {
		return fmt.Errorf("marshal dlq message: %w", marshalErr)
	}
	_, err := dlq.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: dlq.streamKey,
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
