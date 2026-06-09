package config

import (
	"log/slog"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	RedisUrl         string
	StreamKey        string
	ConsumerGroupKey string
	BlockMs          int
	MaxRetries       int
	DLQStreamKey     string
	ClaimedIdleMs    int // TODO: Better to add heartbeat mechanism instead of idle time.
	// TODO: This will reset the idle time when the task is still running, let say for each 10 s.
	BatchSize int64
}

func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		slog.Debug("No .env file found, using environment variables")
	}

	return &Config{
		RedisUrl:         getEnv("REDIS_URL", "redis://localhost:6379"),
		StreamKey:        getEnv("STREAM_KEY", "tasks"),
		ConsumerGroupKey: getEnv("CONSUMER_GROUP_KEY", "task-consumers"),
		BlockMs:          getEnvInt("BLOCK_MS", 5000), // every BlockMs ms, the consumer will check for new tasks
		MaxRetries:       getEnvInt("MAX_RETRIES", 3),
		DLQStreamKey:     getEnv("DLQ_STREAM_KEY", "task:dlq"),
		ClaimedIdleMs:    getEnvInt("CLAIMED_IDLE_MS", 60000), // if a task is idle for ClaimedIdleMs, it is considerd as failed and will be claimed by other consumers
		BatchSize:        int64(getEnvInt("BATCH_SIZE", 10)),
	}
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultValue
}
