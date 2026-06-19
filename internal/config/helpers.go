package config

import (
	"log/slog"
	"os"
	"strconv"
)

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
		slog.Warn("invalid integer env var, using default", "key", key, "value", v, "default", defaultValue)
	}
	return defaultValue
}
