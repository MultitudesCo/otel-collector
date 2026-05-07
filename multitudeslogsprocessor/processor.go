package multitudeslogsprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/multitudes/otel-collector/multitudesauthextension"
)

type logsProcessor struct {
	logger       *zap.Logger
	nextConsumer consumer.Logs
}

func newLogsProcessor(logger *zap.Logger, nextConsumer consumer.Logs) *logsProcessor {
	return &logsProcessor{
		logger:       logger,
		nextConsumer: nextConsumer,
	}
}

func (p *logsProcessor) Start(_ context.Context, _ component.Host) error {
	p.logger.Info("Multitudes logs processor started")
	return nil
}

func (p *logsProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// ConsumeLogs injects the per-client Bearer token (placed in ctx by the
// multitudes_auth extension) into each ResourceLogs resource attribute.
// This ensures the token survives through the batch processor so the
// exporter can use it when posting to v1/logs.
func (p *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	token, hasToken := multitudesauthextension.GetApiKeyFromContext(ctx)

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		attrs := ld.ResourceLogs().At(i).Resource().Attributes()
		// Always strip any client-supplied value of the internal attribute to
		// prevent a misconfigured client from injecting a token directly.
		attrs.Remove(multitudesauthextension.InternalApiKeyAttr)
		if hasToken {
			attrs.PutStr(multitudesauthextension.InternalApiKeyAttr, token)
		}
	}

	if hasToken {
		p.logger.Debug("Injected API key into resource logs",
			zap.Int("resource_logs", ld.ResourceLogs().Len()),
		)
	} else {
		p.logger.Debug("No API key in context; logs will use fallback token at export time")
	}

	return p.nextConsumer.ConsumeLogs(ctx, ld)
}
