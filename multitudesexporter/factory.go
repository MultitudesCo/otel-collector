package multitudesexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

const (
	Type           = "multitudes"
	defaultTimeout = 30 * time.Second
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(Type),
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Timeout: defaultTimeout,
		RetryOnFailure: RetryConfig{
			Enabled:         true,
			InitialInterval: 5 * time.Second,
			MaxInterval:     30 * time.Second,
			MaxElapsedTime:  300 * time.Second,
		},
	}
}

func createMetricsExporter(
	_ context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	oCfg := cfg.(*Config)
	return newExporter(oCfg, set.Logger), nil
}
