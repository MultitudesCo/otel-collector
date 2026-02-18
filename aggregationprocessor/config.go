package aggregationprocessor

import (
	"fmt"
	"time"
)

// Config defines configuration for the aggregation processor
type Config struct {
	// AttributeKey is the resource attribute to group by (e.g., "user.email")
	AttributeKey string `mapstructure:"attribute_key"`

	// AggregationInterval is the time window for aggregation (e.g., "1h")
	AggregationInterval time.Duration `mapstructure:"aggregation_interval"`

	// EmitInterval is how often to check for completed windows and emit metrics
	// Should be less than AggregationInterval (e.g., "1m" for 1h windows)
	EmitInterval time.Duration `mapstructure:"emit_interval"`
}

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	if cfg.AttributeKey == "" {
		return fmt.Errorf("attribute_key must be specified")
	}

	if cfg.AggregationInterval <= 0 {
		return fmt.Errorf("aggregation_interval must be positive")
	}

	if cfg.EmitInterval <= 0 {
		return fmt.Errorf("emit_interval must be positive")
	}

	if cfg.EmitInterval >= cfg.AggregationInterval {
		return fmt.Errorf("emit_interval must be less than aggregation_interval")
	}

	return nil
}
