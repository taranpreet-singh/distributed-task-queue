package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/smtp"

	"github.com/dist_task_que_prac/internal/config"
	"github.com/dist_task_que_prac/internal/consumer"
)

// emailHandler sends an email via SMTP.
// Expected payload fields:
//
//	to      string — recipient address
//	subject string — email subject
//	body    string — plain-text email body
func emailHandler(cfg config.SMTPConfig) consumer.HandlerFunc {
	return func(ctx context.Context, msg *consumer.Message) error {
		to, ok := msg.Payload["to"].(string)
		if !ok || to == "" {
			return fmt.Errorf("missing or invalid payload field: to")
		}
		subject, _ := msg.Payload["subject"].(string)
		body, _ := msg.Payload["body"].(string)

		raw := fmt.Sprintf(
			"From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s",
			cfg.From, to, subject, body,
		)

		addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

		var auth smtp.Auth
		if cfg.User != "" {
			auth = smtp.PlainAuth("", cfg.User, cfg.Pass, cfg.Host)
		}

		if err := smtp.SendMail(addr, auth, cfg.From, []string{to}, []byte(raw)); err != nil {
			return fmt.Errorf("smtp send: %w", err)
		}

		slog.Info("email sent", "task_id", msg.TaskID, "to", to, "subject", subject)
		return nil
	}
}

// flakyHandler succeeds or fails at random to simulate an unreliable
// downstream dependency. Use it to exercise the retry + DLQ paths: most
// tasks recover on a later attempt, a few exhaust retries and land in the DLQ.
// Expected payload fields:
//
//	fail_rate float — probability of failure per attempt, 0..1 (optional, default 0.5)
func flakyHandler() consumer.HandlerFunc {
	return func(ctx context.Context, msg *consumer.Message) error {
		failRate := 0.5
		if fr, ok := msg.Payload["fail_rate"].(float64); ok {
			failRate = fr
		}

		if rand.Float64() < failRate {
			return fmt.Errorf("flaky handler: simulated random failure (fail_rate=%.2f)", failRate)
		}

		slog.Info("flaky task succeeded", "task_id", msg.TaskID)
		return nil
	}
}
