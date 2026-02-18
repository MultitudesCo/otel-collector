package aggregationprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

const (
	// Type is the value of the "type" key in configuration.
	Type = "aggregation"

	// Default configuration values
	defaultAttributeKey        = "user.email"
	defaultAggregationInterval = time.Hour
	defaultEmitInterval        = time.Minute
)

var processorCapabilities = consumer.Capabilities{MutatesData: false}

// NewFactory returns a new factory for the aggregation processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(Type),
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		AttributeKey:        defaultAttributeKey,
		AggregationInterval: defaultAggregationInterval,
		EmitInterval:        defaultEmitInterval,
	}
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	oCfg := cfg.(*Config)

	return newAggregationProcessor(set, oCfg, nextConsumer)
}
