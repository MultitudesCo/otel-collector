package multitudeslogsprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

const (
	// Type is the value of the "type" key in configuration.
	Type = "multitudes_logs"
)

// NewFactory returns a new factory for the Multitudes logs processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(Type),
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createLogsProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	oCfg := cfg.(*Config)
	if err := oCfg.Validate(); err != nil {
		return nil, err
	}
	return newLogsProcessor(set.Logger, nextConsumer), nil
}
