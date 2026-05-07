package multitudesexporter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/multitudes/otel-collector/multitudesauthextension"
)

type multitudesLogsExporter struct {
	cfg      *Config
	logger   *zap.Logger
	client   *http.Client
	endpoint string

	marshaler plog.Marshaler
}

func newLogsExporter(cfg *Config, logger *zap.Logger) *multitudesLogsExporter {
	return &multitudesLogsExporter{
		cfg:       cfg,
		logger:    logger,
		marshaler: &plog.JSONMarshaler{},
	}
}

func (e *multitudesLogsExporter) Start(_ context.Context, _ component.Host) error {
	e.client = &http.Client{Timeout: e.cfg.Timeout}
	endpoint := strings.TrimRight(e.cfg.Endpoint, "/")
	if !strings.HasSuffix(endpoint, "/v1/logs") {
		endpoint = endpoint + "/v1/logs"
	}
	e.endpoint = endpoint
	e.logger.Info("Multitudes logs exporter started",
		zap.String("endpoint", e.endpoint),
		zap.Bool("fallback_token_set", e.cfg.FallbackToken != ""),
	)
	return nil
}

func (e *multitudesLogsExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (e *multitudesLogsExporter) Shutdown(_ context.Context) error {
	if e.client != nil {
		e.client.CloseIdleConnections()
	}
	return nil
}

// ConsumeLogs is called by the OTel pipeline with each batch of logs.
// Each ResourceLogs block may carry a different per-client Bearer token in the
// internal resource attribute set by the multitudeslogsprocessor.
// We split by token and make one HTTP call per unique token.
func (e *multitudesLogsExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	type tokenBatch struct {
		logs      plog.Logs
		rlIndices []int
	}
	byToken := make(map[string]*tokenBatch)
	tokenOrder := make([]string, 0)

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)

		token := e.cfg.FallbackToken
		source := "fallback"
		if v, ok := rl.Resource().Attributes().Get(multitudesauthextension.InternalApiKeyAttr); ok {
			if k := v.AsString(); k != "" {
				token = k
				source = "per-client"
			}
		}
		if token != "" {
			e.logger.Info("logs exporter: resolved Bearer token for export",
				zap.String("source", source),
			)
		} else {
			e.logger.Warn("logs exporter: no Bearer token resolved for export")
		}

		if _, seen := byToken[token]; !seen {
			byToken[token] = &tokenBatch{logs: plog.NewLogs()}
			tokenOrder = append(tokenOrder, token)
		}
		dest := byToken[token].logs.ResourceLogs().AppendEmpty()
		rl.CopyTo(dest)
		// Strip the internal attribute from the copy so it is never forwarded in the payload.
		dest.Resource().Attributes().Remove(multitudesauthextension.InternalApiKeyAttr)
		byToken[token].rlIndices = append(byToken[token].rlIndices, i)
	}

	successfulIndices := make(map[int]bool)
	var firstErr error
	for _, token := range tokenOrder {
		batch := byToken[token]
		if err := e.exportLogsWithToken(ctx, batch.logs, token); err != nil {
			e.logger.Error("Failed to export logs",
				zap.Int("log_record_count", batch.logs.LogRecordCount()),
				zap.Error(err),
			)
			if firstErr == nil {
				firstErr = err
			}
		} else {
			for _, idx := range batch.rlIndices {
				successfulIndices[idx] = true
			}
		}
	}

	// Remove successfully-exported resource logs so an upstream retry only
	// covers the batches that actually failed.
	if firstErr != nil {
		pos := 0
		ld.ResourceLogs().RemoveIf(func(_ plog.ResourceLogs) bool {
			remove := successfulIndices[pos]
			pos++
			return remove
		})
	}

	return firstErr
}

func (e *multitudesLogsExporter) exportLogsWithToken(ctx context.Context, ld plog.Logs, token string) error {
	if token == "" {
		e.logger.Warn("No Bearer token available for log export batch; dropping logs",
			zap.Int("log_record_count", ld.LogRecordCount()),
		)
		return nil
	}

	body, err := e.marshaler.MarshalLogs(ld)
	if err != nil {
		return fmt.Errorf("marshal logs: %w", err)
	}

	var lastErr error
	retryConfig := e.cfg.RetryOnFailure
	backoff := retryConfig.InitialInterval
	deadline := time.Now().Add(retryConfig.MaxElapsedTime)

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := e.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http request: %w", err)
		} else {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("unexpected status %d", resp.StatusCode)
			// Permanent client errors (4xx except 408 and 429) won't succeed on retry.
			if resp.StatusCode >= 400 && resp.StatusCode < 500 &&
				resp.StatusCode != http.StatusRequestTimeout &&
				resp.StatusCode != http.StatusTooManyRequests {
				break
			}
		}

		if !retryConfig.Enabled || time.Now().After(deadline) {
			break
		}

		e.logger.Warn("Log export attempt failed, will retry",
			zap.Int("attempt", attempt+1),
			zap.Duration("backoff", backoff),
			zap.Error(lastErr),
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > retryConfig.MaxInterval {
			backoff = retryConfig.MaxInterval
		}
	}

	return lastErr
}
