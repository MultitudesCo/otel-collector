package aggregationprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type aggregationProcessor struct {
	logger     *zap.Logger
	config     *Config
	aggregator *MetricAggregator
	nextConsumer consumer.Metrics
	cancel     context.CancelFunc
}

func newAggregationProcessor(
	set processor.Settings,
	cfg *Config,
	nextConsumer consumer.Metrics,
) (*aggregationProcessor, error) {
	logger := set.Logger

	aggregator := NewMetricAggregator(cfg.AttributeKey, cfg.AggregationInterval)

	ap := &aggregationProcessor{
		logger:       logger,
		config:       cfg,
		aggregator:   aggregator,
		nextConsumer: nextConsumer,
	}

	return ap, nil
}

func (ap *aggregationProcessor) Start(ctx context.Context, host component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	ap.cancel = cancel

	// Start the emission ticker
	go ap.emitLoop(ctx)

	ap.logger.Info("Aggregation processor started",
		zap.String("attribute_key", ap.config.AttributeKey),
		zap.Duration("aggregation_interval", ap.config.AggregationInterval),
		zap.Duration("emit_interval", ap.config.EmitInterval),
	)

	return nil
}

func (ap *aggregationProcessor) Shutdown(ctx context.Context) error {
	if ap.cancel != nil {
		ap.cancel()
	}

	// Emit any remaining metrics
	ap.emitMetrics(ctx)

	ap.logger.Info("Aggregation processor shut down")
	return nil
}

func (ap *aggregationProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (ap *aggregationProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Add metrics to the aggregator
	ap.aggregator.AddMetrics(md)

	ap.logger.Debug("Received metrics for aggregation",
		zap.Int("resource_metrics", md.ResourceMetrics().Len()),
		zap.Int("total_data_points", md.DataPointCount()),
	)

	// Don't pass through - we'll emit aggregated metrics on the schedule
	return nil
}

func (ap *aggregationProcessor) emitLoop(ctx context.Context) {
	ticker := time.NewTicker(ap.config.EmitInterval)
	defer ticker.Stop()

	debugLog("DEBUG: emitLoop started")

	for {
		select {
		case <-ctx.Done():
			debugLog("DEBUG: emitLoop stopped")
			return
		case <-ticker.C:
			debugLog("DEBUG: emitLoop tick - checking for completed metrics")
			ap.emitMetrics(ctx)
		}
	}
}

func (ap *aggregationProcessor) emitMetrics(ctx context.Context) {
	now := time.Now()
	aggregatedMetrics := ap.aggregator.GetAndClearCompletedMetrics(now)

	if aggregatedMetrics.ResourceMetrics().Len() == 0 {
		ap.logger.Debug("No completed metrics to emit")
		return
	}

	dataPointCount := aggregatedMetrics.DataPointCount()
	ap.logger.Info("Emitting aggregated metrics",
		zap.Int("data_points", dataPointCount),
		zap.Time("timestamp", now),
	)

	// Send aggregated metrics to the next consumer
	if err := ap.nextConsumer.ConsumeMetrics(ctx, aggregatedMetrics); err != nil {
		ap.logger.Error("Failed to emit aggregated metrics", zap.Error(err))
	}
}
