package config

type SMTPConfig struct {
	Host string
	Port int
	User string
	Pass string
	From string
}

func loadSMTPConfig() SMTPConfig {
	return SMTPConfig{
		Host: getEnv("SMTP_HOST", "localhost"),
		Port: getEnvInt("SMTP_PORT", 1025),
		User: getEnv("SMTP_USER", ""),
		Pass: getEnv("SMTP_PASS", ""),
		From: getEnv("SMTP_FROM", "noreply@example.com"),
	}
}
