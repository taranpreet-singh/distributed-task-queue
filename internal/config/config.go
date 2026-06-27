package config

import (
	"log/slog"

	"github.com/joho/godotenv"
)

type Config struct {
	Redis       RedisConfig
	SMTP        SMTPConfig
	MaxRetries  int
	Concurrency int
}

func LoadConfig() Config {
	if err := godotenv.Load(); err != nil {
		slog.Debug("No .env file found, using environment variables")
	}

	return Config{
		Redis:       loadRedisConfig(),
		SMTP:        loadSMTPConfig(),
		MaxRetries:  getEnvInt("MAX_RETRIES", 3),
		Concurrency: getEnvInt("CONCURRENCY", 10),
	}
}
