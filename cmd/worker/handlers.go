package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/smtp"

	"github.com/dist_task_que_prac/internal/config"
	"github.com/dist_task_que_prac/internal/consumer"
)

// webhookHandler delivers a JSON payload via HTTP POST to a target URL.
// Expected payload fields:
//
//	url     string         — destination endpoint
//	body    map[string]any — JSON body to POST
//	headers map[string]any — optional extra request headers
func webhookHandler() consumer.HandlerFunc {
	client := &http.Client{}

	return func(ctx context.Context, msg *consumer.Message) error {
		url, ok := msg.Payload["url"].(string)
		if !ok || url == "" {
			return fmt.Errorf("missing or invalid payload field: url")
		}

		body, _ := msg.Payload["body"].(map[string]any)
		bodyJSON, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal webhook body: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyJSON))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		if headers, ok := msg.Payload["headers"].(map[string]any); ok {
			for k, v := range headers {
				if s, ok := v.(string); ok {
					req.Header.Set(k, s)
				}
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("http post: %w", err)
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("webhook returned non-2xx status: %d", resp.StatusCode)
		}

		slog.Info("webhook delivered", "task_id", msg.TaskID, "url", url, "status", resp.StatusCode)
		return nil
	}
}

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
