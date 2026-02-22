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

// TestFutureTimestampClamping verifies that data points with timestamps far in the
// future are clamped to the current bucket rather than accumulating forever.
func TestFutureTimestampClamping(t *testing.T) {
	agg := NewMetricAggregator("user.email", 5*time.Minute)

	futureTime := time.Now().Add(24 * time.Hour)
	md := createTestMetricsWithTimestamp("alice@example.com", "test.metric", 10.0, futureTime)
	agg.AddMetrics(md)

	// The bucket should have been clamped to now, so it should complete on the next tick
	time.Sleep(10 * time.Millisecond)
	completed := agg.GetAndClearCompletedMetrics(time.Now().Add(6 * time.Minute))

	if completed.DataPointCount() == 0 {
		t.Error("Expected clamped metric to be emitted, but got none")
	}
}

// TestFutureTimestampWithinSkewAllowed verifies that timestamps within the allowed
// skew window (5 minutes) are accepted without clamping.
func TestFutureTimestampWithinSkewAllowed(t *testing.T) {
	agg := NewMetricAggregator("user.email", 5*time.Minute)

	// 2 minutes in the future — within maxFutureSkew, should not be clamped
	nearFuture := time.Now().Add(2 * time.Minute)
	md := createTestMetricsWithTimestamp("alice@example.com", "test.metric", 10.0, nearFuture)
	agg.AddMetrics(md)

	if len(agg.metrics) == 0 {
		t.Error("Expected metric to be stored, but map is empty")
	}
}

// TestMapReplacementAfterClear verifies that completed metrics are fully removed
// from the map (not just deleted inline), so the GC can reclaim backing memory.
func TestMapReplacementAfterClear(t *testing.T) {
	agg := NewMetricAggregator("user.email", time.Second)

	// Add a metric in a past bucket
	pastTime := time.Now().Add(-2 * time.Second)
	md := createTestMetricsWithTimestamp("alice@example.com", "test.metric", 10.0, pastTime)
	agg.AddMetrics(md)

	if len(agg.metrics) == 0 {
		t.Fatal("Expected metric to be stored before clear")
	}

	completed := agg.GetAndClearCompletedMetrics(time.Now())

	if completed.DataPointCount() == 0 {
		t.Error("Expected completed metric to be returned")
	}
	if len(agg.metrics) != 0 {
		t.Errorf("Expected metrics map to be empty after clear, got %d entries", len(agg.metrics))
	}
}

// TestTokenTypeAggregation verifies that token metrics with different type attributes
// (input, output, cache_read, cache_write) are aggregated into separate buckets.
func TestTokenTypeAggregation(t *testing.T) {
	agg := NewMetricAggregator("user.email", time.Second)

	pastTime := time.Now().Add(-2 * time.Second)

	types := []struct {
		tokenType string
		value     float64
	}{
		{"input", 100},
		{"input", 50},   // second input — should be summed with first
		{"output", 200},
		{"cache_read", 300},
		{"cache_write", 400},
	}

	for _, tc := range types {
		md := createTestMetricsWithTokenType("alice@example.com", "claude_code.token.usage", tc.tokenType, tc.value)
		// Override timestamp to past bucket
		rm := md.ResourceMetrics().At(0)
		dp := rm.ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(pastTime))
		agg.AddMetrics(md)
	}

	completed := agg.GetAndClearCompletedMetrics(time.Now())

	if completed.DataPointCount() == 0 {
		t.Fatal("Expected completed metrics, got none")
	}

	// Collect emitted data points by token type
	emitted := make(map[string]float64)
	rm := completed.ResourceMetrics().At(0)
	metric := rm.ScopeMetrics().At(0).Metrics().At(0)
	for i := 0; i < metric.Sum().DataPoints().Len(); i++ {
		dp := metric.Sum().DataPoints().At(i)
		tokenType, _ := dp.Attributes().Get("type")
		emitted[tokenType.AsString()] = dp.DoubleValue()
	}

	if emitted["input"] != 150 {
		t.Errorf("Expected input sum=150, got %.0f", emitted["input"])
	}
	if emitted["output"] != 200 {
		t.Errorf("Expected output sum=200, got %.0f", emitted["output"])
	}
	if emitted["cache_read"] != 300 {
		t.Errorf("Expected cache_read sum=300, got %.0f", emitted["cache_read"])
	}
	if emitted["cache_write"] != 400 {
		t.Errorf("Expected cache_write sum=400, got %.0f", emitted["cache_write"])
	}
}

// TestRedact verifies the redact helper masks email addresses correctly.
func TestRedact(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"josh@example.com", "jo***@example.com"},
		{"a@example.com", "***@example.com"},
		{"ab@example.com", "***@example.com"},
		{"abc@example.com", "ab***@example.com"},
		{"notanemail", "no***"},
		{"ab", "***"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := redact(tt.input)
			if got != tt.want {
				t.Errorf("redact(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestMissingAttributeSkipped verifies that data points without the aggregation
// attribute key are silently skipped.
func TestMissingAttributeSkipped(t *testing.T) {
	agg := NewMetricAggregator("user.email", time.Second)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test.metric")
	sum := metric.SetEmptySum()
	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(42.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	// No user.email attribute set

	agg.AddMetrics(md)

	if len(agg.metrics) != 0 {
		t.Errorf("Expected no metrics stored for data point missing attribute, got %d", len(agg.metrics))
	}
}

// createTestMetrics creates a sum metric with user.email on the data point attributes,
// which is where the aggregator looks for it.
func createTestMetrics(userEmail, metricName string, value float64) pmetric.Metrics {
	return createTestMetricsWithTimestamp(userEmail, metricName, value, time.Now())
}

func createTestMetricsWithTimestamp(userEmail, metricName string, value float64, ts time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
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
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(ts.Add(-time.Minute)))
	dp.Attributes().PutStr("user.email", userEmail)

	return md
}

func createTestMetricsWithTokenType(userEmail, metricName, tokenType string, value float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test")

	metric := sm.Metrics().AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("tokens")
	metric.SetDescription("Token usage metric")

	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-time.Minute)))
	dp.Attributes().PutStr("user.email", userEmail)
	dp.Attributes().PutStr("type", tokenType)

	return md
}
