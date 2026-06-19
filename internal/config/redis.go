package config

type RedisConfig struct {
	URL              string
	StreamKey        string
	ConsumerGroupKey string
	DLQStreamKey     string
	BlockMs          int
	ClaimedIdleMs    int
	HeartbeatMs      int
	BatchSize        int64
}

func loadRedisConfig() RedisConfig {
	return RedisConfig{
		URL:              getEnv("REDIS_URL", "redis://localhost:6379"),
		StreamKey:        getEnv("STREAM_KEY", "tasks"),
		ConsumerGroupKey: getEnv("CONSUMER_GROUP_KEY", "task-consumers"),
		DLQStreamKey:     getEnv("DLQ_STREAM_KEY", "task:dlq"),
		BlockMs:          getEnvInt("BLOCK_MS", 5000),
		ClaimedIdleMs:    getEnvInt("CLAIMED_IDLE_MS", 60000),
		HeartbeatMs:      getEnvInt("HEARTBEAT_MS", 10000),
		BatchSize:        int64(getEnvInt("BATCH_SIZE", 10)),
	}
}
