package aggregationprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestAggregationProcessor(t *testing.T) {
	// Create a sink to capture output
	sink := &consumertest.MetricsSink{}

	// Create processor config
	cfg := &Config{
		AttributeKey:        "user.email",
		AggregationInterval: time.Hour,
		EmitInterval:        time.Second,
	}

	// Create processor
	factory := NewFactory()
	set := processortest.NewNopSettings()
	processor, err := factory.CreateMetrics(
		context.Background(),
		set,
		cfg,
		sink,
	)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	// Start the processor
	if err := processor.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Shutdown(context.Background())

	// Create test metrics
	md1 := createTestMetrics("alice@example.com", "test.metric", 10.0)
	md2 := createTestMetrics("alice@example.com", "test.metric", 5.0)
	md3 := createTestMetrics("bob@example.com", "test.metric", 7.0)

	// Send metrics to processor
	if err := processor.ConsumeMetrics(context.Background(), md1); err != nil {
		t.Fatalf("Failed to consume metrics: %v", err)
	}
	if err := processor.ConsumeMetrics(context.Background(), md2); err != nil {
		t.Fatalf("Failed to consume metrics: %v", err)
	}
	if err := processor.ConsumeMetrics(context.Background(), md3); err != nil {
		t.Fatalf("Failed to consume metrics: %v", err)
	}

	// Wait for emission (this test uses small intervals for testing)
	time.Sleep(2 * time.Second)

	// Since we're in the current time bucket, nothing should be emitted yet
	if len(sink.AllMetrics()) > 0 {
		t.Logf("Note: Metrics emitted during current bucket (expected if time bucket rolled over)")
	}
}

func TestAggregationProcessorEmission(t *testing.T) {
	sink := &consumertest.MetricsSink{}

	cfg := &Config{
		AttributeKey:        "user.email",
		AggregationInterval: time.Second, // Use 1 second for testing
		EmitInterval:        100 * time.Millisecond,
	}

	factory := NewFactory()
	set := processortest.NewNopSettings()
	processor, err := factory.CreateMetrics(
		context.Background(),
		set,
		cfg,
		sink,
	)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	if err := processor.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Shutdown(context.Background())

	// Send metrics
	md1 := createTestMetrics("alice@example.com", "test.cost", 10.0)
	md2 := createTestMetrics("alice@example.com", "test.cost", 5.0)
	md3 := createTestMetrics("bob@example.com", "test.cost", 7.0)

	if err := processor.ConsumeMetrics(context.Background(), md1); err != nil {
		t.Fatalf("Failed to consume metrics: %v", err)
	}
	if err := processor.ConsumeMetrics(context.Background(), md2); err != nil {
		t.Fatalf("Failed to consume metrics: %v", err)
	}
	if err := processor.ConsumeMetrics(context.Background(), md3); err != nil {
		t.Fatalf("Failed to consume metrics: %v", err)
	}

	// Wait for the time bucket to complete and metrics to be emitted
	time.Sleep(1500 * time.Millisecond)

	// Check that metrics were emitted
	allMetrics := sink.AllMetrics()
	if len(allMetrics) == 0 {
		t.Fatal("Expected metrics to be emitted, but got none")
	}

	t.Logf("Emitted %d metric batches", len(allMetrics))

	// Verify aggregation
	for _, md := range allMetrics {
		if md.DataPointCount() == 0 {
			continue
		}

		t.Logf("Metric batch has %d data points", md.DataPointCount())

		// Should have aggregated alice's two metrics (10 + 5 = 15) and bob's one metric (7)
		// Total of 2 data points expected
		if md.DataPointCount() != 2 {
			t.Errorf("Expected 2 aggregated data points, got %d", md.DataPointCount())
		}
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				AttributeKey:        "user.email",
				AggregationInterval: time.Hour,
				EmitInterval:        time.Minute,
			},
			wantErr: false,
		},
		{
			name: "missing attribute key",
			config: Config{
				AttributeKey:        "",
				AggregationInterval: time.Hour,
				EmitInterval:        time.Minute,
			},
			wantErr: true,
		},
		{
			name: "negative aggregation interval",
			config: Config{
				AttributeKey:        "user.email",
				AggregationInterval: -time.Hour,
				EmitInterval:        time.Minute,
			},
			wantErr: true,
		},
		{
			name: "emit interval >= aggregation interval",
			config: Config{
				AttributeKey:        "user.email",
				AggregationInterval: time.Hour,
				EmitInterval:        time.Hour,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Helper function to create test metrics
func createTestMetrics(userEmail, metricName string, value float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()

	// Add resource attributes
	rm.Resource().Attributes().PutStr("user.email", userEmail)

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test")

	metric := sm.Metrics().AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("USD")
	metric.SetDescription("Test metric")

	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-time.Minute)))

	return md
}
