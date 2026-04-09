package multitudesexporter

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// internalApiKeyAttr must equal multitudesauthextension.InternalApiKeyAttr.
// Defined as a local constant to avoid a cross-module import.
const internalApiKeyAttr = "multitudes.internal.bearer_token"

// redactToken shows the first 4 characters of a Bearer token followed by ***
// so log readers can confirm which key is in use without exposing the full secret.
func redactToken(t string) string {
	if len(t) <= 4 {
		return "***"
	}
	return t[:4] + "***"
}

type multitudesExporter struct {
	cfg    *Config
	logger *zap.Logger
	client *http.Client

	marshaler pmetric.Marshaler
}

func newExporter(cfg *Config, logger *zap.Logger) *multitudesExporter {
	return &multitudesExporter{
		cfg:       cfg,
		logger:    logger,
		marshaler: &pmetric.JSONMarshaler{},
	}
}

func (e *multitudesExporter) Start(_ context.Context, _ component.Host) error {
	e.client = &http.Client{Timeout: e.cfg.Timeout}
	e.logger.Info("Multitudes exporter started",
		zap.String("endpoint", e.cfg.Endpoint),
		zap.Bool("fallback_token_set", e.cfg.FallbackToken != ""),
	)
	return nil
}

func (e *multitudesExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true} // we strip the internal api key attribute
}

func (e *multitudesExporter) Shutdown(_ context.Context) error {
	if e.client != nil {
		e.client.CloseIdleConnections()
	}
	return nil
}

// ConsumeMetrics is called by the OTel pipeline with each batch of aggregated
// metrics. Each ResourceMetrics block may carry a different per-client Bearer
// token in the internal resource attribute set by the aggregation processor.
// We split by token and make one HTTP call per unique token.
func (e *multitudesExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Partition ResourceMetrics by Bearer token.
	byToken := make(map[string]pmetric.Metrics)
	tokenOrder := make([]string, 0) // preserve a consistent order for logging

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)

		token := e.cfg.FallbackToken
		source := "fallback"
		if v, ok := rm.Resource().Attributes().Get(internalApiKeyAttr); ok {
			if k := v.AsString(); k != "" {
				token = k
				source = "per-client"
			}
		}
		// Strip the internal attribute so it is never forwarded in the payload.
		rm.Resource().Attributes().Remove(internalApiKeyAttr)
		e.logger.Info("exporter: resolved Bearer token for export",
			zap.String("source", source),
			zap.String("token_prefix", redactToken(token)),
		)

		if _, seen := byToken[token]; !seen {
			byToken[token] = pmetric.NewMetrics()
			tokenOrder = append(tokenOrder, token)
		}
		rm.CopyTo(byToken[token].ResourceMetrics().AppendEmpty())
	}

	var firstErr error
	for _, token := range tokenOrder {
		batch := byToken[token]
		if err := e.exportWithToken(ctx, batch, token); err != nil {
			e.logger.Error("Failed to export metrics",
				zap.Int("data_points", batch.DataPointCount()),
				zap.Error(err),
			)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (e *multitudesExporter) exportWithToken(ctx context.Context, md pmetric.Metrics, token string) error {
	if token == "" {
		e.logger.Warn("No Bearer token available for export batch; dropping metrics",
			zap.Int("data_points", md.DataPointCount()),
		)
		return nil
	}

	body, err := e.marshaler.MarshalMetrics(md)
	if err != nil {
		return fmt.Errorf("marshal metrics: %w", err)
	}

	var lastErr error
	retryConfig := e.cfg.RetryOnFailure
	backoff := retryConfig.InitialInterval
	deadline := time.Now().Add(retryConfig.MaxElapsedTime)

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.Endpoint, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := e.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http request: %w", err)
		} else {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("unexpected status %d", resp.StatusCode)
		}

		if !retryConfig.Enabled || time.Now().After(deadline) {
			break
		}

		e.logger.Warn("Export attempt failed, will retry",
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
