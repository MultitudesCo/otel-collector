package multitudesexporter

import (
	"fmt"
	"net/url"
	"time"
)

// Config defines configuration for the Multitudes exporter.
type Config struct {
	// Endpoint is the Multitudes OTLP ingestion URL.
	// e.g. "https://integrations.multitudes.co/ai/otel"
	Endpoint string `mapstructure:"endpoint"`

	// FallbackToken is used as the Authorization: Bearer value when no
	// per-client API key is present in the outgoing ResourceMetrics. Typically
	// sourced from the MULTITUDES_INTEGRATION_TOKEN environment variable.
	// Optional when all clients supply their own key via the auth extension.
	FallbackToken string `mapstructure:"fallback_token"`

	// Timeout for each export HTTP call.
	Timeout time.Duration `mapstructure:"timeout"`

	// RetryOnFailure configures retry behaviour.
	RetryOnFailure RetryConfig `mapstructure:"retry_on_failure"`
}

// RetryConfig mirrors the standard OTel retry settings.
type RetryConfig struct {
	Enabled         bool          `mapstructure:"enabled"`
	InitialInterval time.Duration `mapstructure:"initial_interval"`
	MaxInterval     time.Duration `mapstructure:"max_interval"`
	MaxElapsedTime  time.Duration `mapstructure:"max_elapsed_time"`
}

func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return fmt.Errorf("endpoint must be specified")
	}
	if _, err := url.ParseRequestURI(cfg.Endpoint); err != nil {
		return fmt.Errorf("endpoint is not a valid URL: %w", err)
	}
	return nil
}
