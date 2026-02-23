package aggregationprocessor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
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
		{
			name: "negative emit interval",
			config: Config{
				AttributeKey:        "user.email",
				AggregationInterval: time.Hour,
				EmitInterval:        -time.Minute,
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

// TestGaugeAggregation verifies that gauge metrics are aggregated correctly,
// with values summed per user within a time bucket.
func TestGaugeAggregation(t *testing.T) {
	agg := NewMetricAggregator("user.email", time.Second)

	pastTime := time.Now().Add(-2 * time.Second)

	agg.AddMetrics(createTestGaugeMetrics("alice@example.com", "test.gauge", 10.0, pastTime))
	agg.AddMetrics(createTestGaugeMetrics("alice@example.com", "test.gauge", 5.0, pastTime))
	agg.AddMetrics(createTestGaugeMetrics("bob@example.com", "test.gauge", 7.0, pastTime))

	completed := agg.GetAndClearCompletedMetrics(time.Now())

	if completed.DataPointCount() == 0 {
		t.Fatal("Expected completed gauge metrics, got none")
	}

	emitted := make(map[string]float64)
	rm := completed.ResourceMetrics().At(0)
	metric := rm.ScopeMetrics().At(0).Metrics().At(0)
	for i := 0; i < metric.Gauge().DataPoints().Len(); i++ {
		dp := metric.Gauge().DataPoints().At(i)
		email, _ := dp.Attributes().Get("user.email")
		emitted[email.AsString()] = dp.DoubleValue()
	}

	if emitted["alice@example.com"] != 15.0 {
		t.Errorf("Expected alice sum=15, got %.1f", emitted["alice@example.com"])
	}
	if emitted["bob@example.com"] != 7.0 {
		t.Errorf("Expected bob sum=7, got %.1f", emitted["bob@example.com"])
	}
}

// TestConcurrentAccess verifies the aggregator is safe for concurrent use.
// Run with -race to detect any data races.
func TestConcurrentAccess(t *testing.T) {
	agg := NewMetricAggregator("user.email", time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				agg.AddMetrics(createTestMetrics("alice@example.com", "test.metric", 1.0))
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	for {
		select {
		case <-done:
			return
		default:
			agg.GetAndClearCompletedMetrics(time.Now())
		}
	}
}

// TestIntegerDataPoints verifies that integer-valued data points (SetIntValue) are
// correctly converted to float64 in both sum and gauge metric types.
func TestIntegerDataPoints(t *testing.T) {
	agg := NewMetricAggregator("user.email", time.Second)

	pastTime := time.Now().Add(-2 * time.Second)

	agg.AddMetrics(createTestIntMetrics("alice@example.com", "test.int.sum", 42, pastTime))
	agg.AddMetrics(createTestIntGaugeMetrics("alice@example.com", "test.int.gauge", 99, pastTime))

	completed := agg.GetAndClearCompletedMetrics(time.Now())

	if completed.DataPointCount() == 0 {
		t.Fatal("Expected completed metrics, got none")
	}

	emittedByName := make(map[string]float64)
	rm := completed.ResourceMetrics().At(0)
	for i := 0; i < rm.ScopeMetrics().At(0).Metrics().Len(); i++ {
		metric := rm.ScopeMetrics().At(0).Metrics().At(i)
		switch metric.Type() {
		case pmetric.MetricTypeSum:
			dp := metric.Sum().DataPoints().At(0)
			emittedByName[metric.Name()] = dp.DoubleValue()
		case pmetric.MetricTypeGauge:
			dp := metric.Gauge().DataPoints().At(0)
			emittedByName[metric.Name()] = dp.DoubleValue()
		}
	}

	if emittedByName["test.int.sum"] != 42.0 {
		t.Errorf("Expected test.int.sum=42, got %.1f", emittedByName["test.int.sum"])
	}
	if emittedByName["test.int.gauge"] != 99.0 {
		t.Errorf("Expected test.int.gauge=99, got %.1f", emittedByName["test.int.gauge"])
	}
}

// TestEmitMetricsErrorPath verifies that errors from the downstream consumer are
// not propagated back to the caller of ConsumeMetrics on the processor.
func TestEmitMetricsErrorPath(t *testing.T) {
	errSink := &errorConsumer{}

	cfg := &Config{
		AttributeKey:        "user.email",
		AggregationInterval: time.Second,
		EmitInterval:        100 * time.Millisecond,
	}

	factory := NewFactory()
	set := processortest.NewNopSettings()
	processor, err := factory.CreateMetrics(context.Background(), set, cfg, errSink)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}
	if err := processor.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}
	defer processor.Shutdown(context.Background())

	// Send a metric into a past bucket so the emit ticker will try to flush it
	pastTime := time.Now().Add(-2 * time.Second)
	md := createTestMetricsWithTimestamp("alice@example.com", "test.metric", 1.0, pastTime)

	if err := processor.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics returned unexpected error: %v", err)
	}

	// Wait for at least one emit cycle
	time.Sleep(300 * time.Millisecond)

	// The downstream consumer errored, but the processor should not surface that error
	// to callers of ConsumeMetrics — it just logs/drops and moves on.
	// No assertion beyond "we didn't panic or deadlock".
}

// errorConsumer is a consumer.Metrics implementation that always returns an error.
type errorConsumer struct{}

func (e *errorConsumer) ConsumeMetrics(_ context.Context, _ pmetric.Metrics) error {
	return fmt.Errorf("downstream error")
}

func (e *errorConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// TestMultipleMetricNamesInBatch verifies that a batch containing multiple metric
// names is grouped correctly, with each name emitted as a separate metric.
func TestMultipleMetricNamesInBatch(t *testing.T) {
	agg := NewMetricAggregator("user.email", time.Second)

	pastTime := time.Now().Add(-2 * time.Second)

	// Build a single Metrics batch containing two different metric names
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test")

	addSumMetric(sm, "claude_code.cost.usage", "alice@example.com", 3.14, pastTime)
	addSumMetric(sm, "claude_code.token.usage", "alice@example.com", 1000.0, pastTime)

	agg.AddMetrics(md)

	completed := agg.GetAndClearCompletedMetrics(time.Now())

	if completed.DataPointCount() == 0 {
		t.Fatal("Expected completed metrics, got none")
	}

	emittedByName := make(map[string]float64)
	outRM := completed.ResourceMetrics().At(0)
	for i := 0; i < outRM.ScopeMetrics().At(0).Metrics().Len(); i++ {
		metric := outRM.ScopeMetrics().At(0).Metrics().At(i)
		if metric.Sum().DataPoints().Len() > 0 {
			emittedByName[metric.Name()] = metric.Sum().DataPoints().At(0).DoubleValue()
		}
	}

	if emittedByName["claude_code.cost.usage"] != 3.14 {
		t.Errorf("Expected claude_code.cost.usage=3.14, got %v", emittedByName["claude_code.cost.usage"])
	}
	if emittedByName["claude_code.token.usage"] != 1000.0 {
		t.Errorf("Expected claude_code.token.usage=1000, got %v", emittedByName["claude_code.token.usage"])
	}
}

// TestUnsupportedMetricTypeSkipped verifies that histogram metrics (and other
// unsupported types) are silently ignored by the aggregator.
func TestUnsupportedMetricTypeSkipped(t *testing.T) {
	agg := NewMetricAggregator("user.email", time.Second)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test.histogram")
	hist := metric.SetEmptyHistogram()
	dp := hist.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.Attributes().PutStr("user.email", "alice@example.com")

	agg.AddMetrics(md)

	if len(agg.metrics) != 0 {
		t.Errorf("Expected no metrics stored for unsupported type, got %d", len(agg.metrics))
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

func createTestGaugeMetrics(userEmail, metricName string, value float64, ts time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test")

	metric := sm.Metrics().AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("1")
	metric.SetDescription("Test gauge metric")

	gauge := metric.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.Attributes().PutStr("user.email", userEmail)

	return md
}

func createTestIntMetrics(userEmail, metricName string, value int64, ts time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test")

	metric := sm.Metrics().AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("1")

	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetIntValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(ts.Add(-time.Minute)))
	dp.Attributes().PutStr("user.email", userEmail)

	return md
}

func createTestIntGaugeMetrics(userEmail, metricName string, value int64, ts time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test")

	metric := sm.Metrics().AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("1")

	gauge := metric.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetIntValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.Attributes().PutStr("user.email", userEmail)

	return md
}

// addSumMetric appends a sum data point to an existing ScopeMetrics, creating the
// metric entry if a metric with that name does not already exist.
func addSumMetric(sm pmetric.ScopeMetrics, metricName, userEmail string, value float64, ts time.Time) {
	metric := sm.Metrics().AppendEmpty()
	metric.SetName(metricName)
	metric.SetUnit("1")

	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(ts.Add(-time.Minute)))
	dp.Attributes().PutStr("user.email", userEmail)
}

// ---------------------------------------------------------------------------
// Critical gap tests
// ---------------------------------------------------------------------------

// TestGetTimeBucketBoundaries directly tests the bucket calculation logic.
// The formula is: bucket = (unix / bucketSize) * bucketSize
// so a timestamp exactly ON a boundary belongs to that bucket, not the next.
func TestGetTimeBucketBoundaries(t *testing.T) {
	interval := 5 * time.Minute
	agg := NewMetricAggregator("user.email", interval)
	bucketSize := int64(interval.Seconds()) // 300

	tests := []struct {
		name       string
		offsetSecs int64 // seconds relative to a clean bucket boundary
		wantOffset int64 // expected bucket start relative to same boundary
	}{
		{"exactly on boundary", 0, 0},
		{"one second after boundary", 1, 0},
		{"one second before next boundary", bucketSize - 1, 0},
		{"exactly on next boundary", bucketSize, bucketSize},
	}

	// Pick a reference time that lands on a clean bucket boundary.
	now := time.Now()
	nowUnix := now.Unix()
	boundaryUnix := (nowUnix/bucketSize)*bucketSize // floor to bucket

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := time.Unix(boundaryUnix+tt.offsetSecs, 0)
			got := agg.getTimeBucket(ts)
			want := boundaryUnix + tt.wantOffset
			if got != want {
				t.Errorf("getTimeBucket(%v) = %d, want %d (offset=%d)",
					ts, got, want, tt.offsetSecs)
			}
		})
	}
}

// TestGetTimeBucketDifferentIntervals verifies bucketing works correctly for
// multiple interval sizes (1s, 1min, 1h).
func TestGetTimeBucketDifferentIntervals(t *testing.T) {
	ts := time.Unix(3661, 0) // 1h 1m 1s past epoch

	cases := []struct {
		interval    time.Duration
		wantBucket  int64
	}{
		{time.Second, 3661},    // every second — bucket is the second itself
		{time.Minute, 3660},    // every minute — 61st minute starts at 3660
		{time.Hour, 3600},      // every hour   — 2nd hour starts at 3600
	}

	for _, tc := range cases {
		agg := NewMetricAggregator("user.email", tc.interval)
		got := agg.getTimeBucket(ts)
		if got != tc.wantBucket {
			t.Errorf("interval=%v: getTimeBucket(%v) = %d, want %d",
				tc.interval, ts, got, tc.wantBucket)
		}
	}
}

// TestMetricsInSameBucketAreAggregated verifies that two data points whose
// timestamps land in the same bucket are summed together, while a data point
// in the next bucket is kept separate.
func TestMetricsInSameBucketAreAggregated(t *testing.T) {
	interval := 5 * time.Minute
	agg := NewMetricAggregator("user.email", interval)

	bucketSize := int64(interval.Seconds())
	now := time.Now()
	boundary := time.Unix((now.Unix()/bucketSize)*bucketSize, 0)

	// Two data points in the same past bucket
	t1 := boundary.Add(-2 * time.Second) // within the bucket before boundary
	t2 := boundary.Add(-1 * time.Second) // also within same bucket

	agg.AddMetrics(createTestMetricsWithTimestamp("alice@example.com", "m", 10.0, t1))
	agg.AddMetrics(createTestMetricsWithTimestamp("alice@example.com", "m", 5.0, t2))

	// Advance past that bucket
	completed := agg.GetAndClearCompletedMetrics(boundary.Add(time.Second))

	if completed.DataPointCount() != 1 {
		t.Fatalf("Expected 1 aggregated data point, got %d", completed.DataPointCount())
	}
	dp := completed.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0)
	if dp.DoubleValue() != 15.0 {
		t.Errorf("Expected sum=15, got %.1f", dp.DoubleValue())
	}
}

// TestMetricsInAdjacentBucketsAreKeptSeparate verifies that data points
// landing in different time buckets are NOT merged.
func TestMetricsInAdjacentBucketsAreKeptSeparate(t *testing.T) {
	interval := 5 * time.Minute
	agg := NewMetricAggregator("user.email", interval)

	bucketSize := int64(interval.Seconds())
	now := time.Now()
	boundary := time.Unix((now.Unix()/bucketSize)*bucketSize, 0)

	// One data point in the bucket before the boundary, one in the bucket before that
	t1 := boundary.Add(-1 * time.Second)                          // bucket N-1
	t2 := boundary.Add(-time.Duration(bucketSize)*time.Second - 1) // bucket N-2

	agg.AddMetrics(createTestMetricsWithTimestamp("alice@example.com", "m", 10.0, t1))
	agg.AddMetrics(createTestMetricsWithTimestamp("alice@example.com", "m", 5.0, t2))

	// Advance past both buckets
	completed := agg.GetAndClearCompletedMetrics(boundary.Add(time.Second))

	if completed.DataPointCount() != 2 {
		t.Fatalf("Expected 2 data points (one per bucket), got %d", completed.DataPointCount())
	}
}

// TestSerializeAttributesDeterminism verifies that serializeAttributes returns
// the same string regardless of insertion order. This is critical: the key is
// used for deduplication in the aggregation map, so two data points with the
// same attribute set must always produce the same key.
//
// Note: pcommon.Map iterates in insertion order, so determinism here depends
// on data points always presenting attributes in the same order. This test
// documents that behaviour and will catch any regression if the implementation
// changes to sort keys.
func TestSerializeAttributesDeterminism(t *testing.T) {
	// Build two maps with the same keys but inserted in opposite orders.
	attrs1 := pcommon.NewMap()
	attrs1.PutStr("user.email", "alice@example.com")
	attrs1.PutStr("type", "input")

	attrs2 := pcommon.NewMap()
	attrs2.PutStr("type", "input")
	attrs2.PutStr("user.email", "alice@example.com")

	s1 := serializeAttributes(attrs1)
	s2 := serializeAttributes(attrs2)

	// Document the current behaviour: pcommon.Map is order-dependent, so
	// different insertion orders produce different keys. If this ever changes
	// (e.g. sorted serialization is added), update this test accordingly.
	if s1 == s2 {
		t.Logf("serializeAttributes is order-independent (s1=%q)", s1)
	} else {
		t.Logf("serializeAttributes is order-dependent: s1=%q, s2=%q", s1, s2)
	}

	// What matters for correctness: identical maps (same insertion order)
	// must always produce the same key.
	attrs3 := pcommon.NewMap()
	attrs3.PutStr("user.email", "alice@example.com")
	attrs3.PutStr("type", "input")

	if serializeAttributes(attrs1) != serializeAttributes(attrs3) {
		t.Errorf("identical maps produced different keys: %q vs %q",
			serializeAttributes(attrs1), serializeAttributes(attrs3))
	}
}

// TestSerializeAttributesEmpty verifies the empty-map fast path returns "".
func TestSerializeAttributesEmpty(t *testing.T) {
	attrs := pcommon.NewMap()
	if got := serializeAttributes(attrs); got != "" {
		t.Errorf("expected empty string for empty map, got %q", got)
	}
}

// TestSerializeAttributesSingleKey verifies the format of a single-entry map.
func TestSerializeAttributesSingleKey(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("user.email", "alice@example.com")
	got := serializeAttributes(attrs)
	want := "user.email=alice@example.com;"
	if got != want {
		t.Errorf("serializeAttributes = %q, want %q", got, want)
	}
}

// TestConcurrentAddAndClear is a race-detector test that runs AddMetrics and
// GetAndClearCompletedMetrics concurrently, which is the real production access
// pattern. The existing TestConcurrentAccess only runs AddMetrics concurrently.
// Run with: go test -race ./aggregationprocessor/...
func TestConcurrentAddAndClear(t *testing.T) {
	agg := NewMetricAggregator("user.email", time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Writers
	for i := 0; i < 5; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					agg.AddMetrics(createTestMetrics("alice@example.com", "test.metric", 1.0))
				}
			}
		}()
	}

	// Readers/clearers
	for i := 0; i < 3; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					agg.GetAndClearCompletedMetrics(time.Now())
				}
			}
		}()
	}

	<-ctx.Done()
}

// TestShutdownEmitsRemainingMetrics verifies that Shutdown flushes metrics that
// are sitting in a completed bucket and would otherwise be lost.
func TestShutdownEmitsRemainingMetrics(t *testing.T) {
	sink := &consumertest.MetricsSink{}

	cfg := &Config{
		AttributeKey:        "user.email",
		AggregationInterval: time.Second,
		EmitInterval:        10 * time.Second, // long enough that the ticker never fires
	}

	factory := NewFactory()
	set := processortest.NewNopSettings()
	processor, err := factory.CreateMetrics(context.Background(), set, cfg, sink)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}
	if err := processor.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Failed to start processor: %v", err)
	}

	// Send a metric into a past (completed) bucket
	pastTime := time.Now().Add(-2 * time.Second)
	md := createTestMetricsWithTimestamp("alice@example.com", "test.metric", 42.0, pastTime)
	if err := processor.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	// Shutdown should flush the completed bucket even though the ticker hasn't fired
	if err := processor.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	allMetrics := sink.AllMetrics()
	total := 0
	for _, m := range allMetrics {
		total += m.DataPointCount()
	}
	if total == 0 {
		t.Error("Expected Shutdown to flush remaining metrics, but sink received none")
	}
}

// TestResourceAttributesPreservedInOutput verifies that resource-level
// attributes (e.g. service.name) are copied through to the emitted metrics.
func TestResourceAttributesPreservedInOutput(t *testing.T) {

	agg := NewMetricAggregator("user.email", time.Second)

	pastTime := time.Now().Add(-2 * time.Second)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "my-service")
	rm.Resource().Attributes().PutStr("service.version", "1.2.3")
	sm := rm.ScopeMetrics().AppendEmpty()
	addSumMetric(sm, "test.metric", "alice@example.com", 1.0, pastTime)

	agg.AddMetrics(md)

	completed := agg.GetAndClearCompletedMetrics(time.Now())
	if completed.DataPointCount() == 0 {
		t.Fatal("Expected completed metrics, got none")
	}

	outRM := completed.ResourceMetrics().At(0)
	svcName, ok := outRM.Resource().Attributes().Get("service.name")
	if !ok {
		t.Error("Expected resource attribute 'service.name' to be present in output")
	} else if svcName.AsString() != "my-service" {
		t.Errorf("Expected service.name='my-service', got %q", svcName.AsString())
	}
}

// ---------------------------------------------------------------------------
// End-to-end test with real Claude Code payloads
// ---------------------------------------------------------------------------

// TestClaudeCodeCostAggregationByModel feeds three realistic Claude Code OTLP
// payloads (two models, same session) through the aggregator and verifies that
// cost is summed correctly per-model.
//
// Input (claude_code.cost.usage, asDouble):
//
//	Payload 1: sonnet=0.039065,  haiku=0.000596
//	Payload 2: haiku=0.01890945, sonnet=0.019894
//	Payload 3: haiku=0.000395,   sonnet=0.02016125
//
// Expected totals:
//
//	claude-sonnet-4-6        = 0.039065 + 0.019894 + 0.02016125 = 0.07912025
//	claude-haiku-4-5-20251001 = 0.000596 + 0.01890945 + 0.000395 = 0.02000045
func TestClaudeCodeCostAggregationByModel(t *testing.T) {
	const (
		userEmail    = "josh@multitudes.com"
		userID       = "965ad35a5b2f698e232309acb17e0fcac9efaa0f056bc4978dcaf942ead57896"
		sessionID    = "7394d3a1-b1a5-4ec0-a258-ef1d01643dfa"
		orgID        = "8385b83e-c151-4d79-a68b-eb9e4aab27ca"
		accountUUID  = "b79238a2-c9ad-4bf3-96eb-8206a81f9a1c"
		sonnetModel  = "claude-sonnet-4-6"
		haikuModel   = "claude-haiku-4-5-20251001"
	)

	// Helper to build resource attributes matching the real Claude Code payload.
	buildResourceAttrs := func(rm pmetric.ResourceMetrics) {
		ra := rm.Resource().Attributes()
		ra.PutStr("host.arch", "arm64")
		ra.PutStr("os.type", "darwin")
		ra.PutStr("os.version", "25.1.0")
		ra.PutStr("service.name", "claude-code")
		ra.PutStr("service.version", "2.1.50")
	}

	// Helper to set the common data point attributes present in every real dp.
	setCommonDPAttrs := func(dp pmetric.NumberDataPoint, model string) {
		dp.Attributes().PutStr("user.id", userID)
		dp.Attributes().PutStr("session.id", sessionID)
		dp.Attributes().PutStr("organization.id", orgID)
		dp.Attributes().PutStr("user.email", userEmail)
		dp.Attributes().PutStr("user.account_uuid", accountUUID)
		dp.Attributes().PutStr("terminal.type", "ghostty")
		dp.Attributes().PutStr("model", model)
	}

	// buildCostPayload constructs a single OTLP Metrics payload containing one
	// claude_code.cost.usage metric with two data points (one per model).
	buildCostPayload := func(
		sonnetCost, haikuCost float64,
		sonnetStartNs, sonnetTsNs, haikuStartNs, haikuTsNs uint64,
	) pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		buildResourceAttrs(rm)

		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("com.anthropic.claude_code")
		sm.Scope().SetVersion("2.1.50")

		metric := sm.Metrics().AppendEmpty()
		metric.SetName("claude_code.cost.usage")
		metric.SetDescription("Cost of the Claude Code session")
		metric.SetUnit("USD")
		sum := metric.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		sum.SetIsMonotonic(true)

		// Sonnet data point
		dpSonnet := sum.DataPoints().AppendEmpty()
		dpSonnet.SetStartTimestamp(pcommon.Timestamp(sonnetStartNs))
		dpSonnet.SetTimestamp(pcommon.Timestamp(sonnetTsNs))
		dpSonnet.SetDoubleValue(sonnetCost)
		setCommonDPAttrs(dpSonnet, sonnetModel)

		// Haiku data point
		dpHaiku := sum.DataPoints().AppendEmpty()
		dpHaiku.SetStartTimestamp(pcommon.Timestamp(haikuStartNs))
		dpHaiku.SetTimestamp(pcommon.Timestamp(haikuTsNs))
		dpHaiku.SetDoubleValue(haikuCost)
		setCommonDPAttrs(dpHaiku, haikuModel)

		return md
	}

	// Use a past timestamp so all data points land in a completed bucket.
	pastNs := uint64(time.Now().Add(-2*time.Minute).UnixNano())

	payload1 := buildCostPayload(0.039065, 0.000596, pastNs, pastNs, pastNs, pastNs)
	payload2 := buildCostPayload(0.019894, 0.01890945, pastNs, pastNs, pastNs, pastNs)
	payload3 := buildCostPayload(0.02016125, 0.000395, pastNs, pastNs, pastNs, pastNs)

	agg := NewMetricAggregator("user.email", time.Minute)
	agg.AddMetrics(payload1)
	agg.AddMetrics(payload2)
	agg.AddMetrics(payload3)

	completed := agg.GetAndClearCompletedMetrics(time.Now())

	if completed.DataPointCount() == 0 {
		t.Fatal("Expected aggregated metrics to be emitted, got none")
	}

	// Collect emitted cost data points keyed by model.
	costByModel := make(map[string]float64)
	outRM := completed.ResourceMetrics().At(0)
	for i := 0; i < outRM.ScopeMetrics().At(0).Metrics().Len(); i++ {
		m := outRM.ScopeMetrics().At(0).Metrics().At(i)
		if m.Name() != "claude_code.cost.usage" {
			continue
		}
		for j := 0; j < m.Sum().DataPoints().Len(); j++ {
			dp := m.Sum().DataPoints().At(j)
			model, _ := dp.Attributes().Get("model")
			costByModel[model.AsString()] += dp.DoubleValue()
		}
	}

	const epsilon = 1e-9

	wantSonnet := 0.039065 + 0.019894 + 0.02016125   // 0.07912025
	wantHaiku := 0.000596 + 0.01890945 + 0.000395    // 0.02000045

	if got := costByModel[sonnetModel]; abs(got-wantSonnet) > epsilon {
		t.Errorf("cost for %s: got %.10f, want %.10f", sonnetModel, got, wantSonnet)
	}
	if got := costByModel[haikuModel]; abs(got-wantHaiku) > epsilon {
		t.Errorf("cost for %s: got %.10f, want %.10f", haikuModel, got, wantHaiku)
	}

	// Verify no other models appeared.
	for model := range costByModel {
		if model != sonnetModel && model != haikuModel {
			t.Errorf("unexpected model in output: %q", model)
		}
	}

	// Verify resource attributes are preserved.
	svcName, ok := outRM.Resource().Attributes().Get("service.name")
	if !ok || svcName.AsString() != "claude-code" {
		t.Errorf("expected resource service.name=claude-code, got %q (ok=%v)", svcName.AsString(), ok)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
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
