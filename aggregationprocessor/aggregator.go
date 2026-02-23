package aggregationprocessor

import (
	"fmt"
	"os"
	"sync"
	"time"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func isDebug() bool {
	return os.Getenv("MULTITUDES_DEBUG") != ""
}

func debugLog(args ...any) {
	if isDebug() {
		fmt.Println(args...)
	}
}

// aggregationKey uniquely identifies a metric for aggregation
type aggregationKey struct {
	attributeValue string // e.g., user email
	metricName     string
	timeBucket     int64 // Unix timestamp of bucket start
	dpAttributes string // Serialized data point attributes
}

// aggregatedMetric holds accumulated metric data
type aggregatedMetric struct {
	key            aggregationKey
	metricType     pmetric.MetricType
	sum            float64
	count          int64
	unit           string
	description    string
	isMonotonic    bool
	resourceAttrs  pcommon.Map
	dpAttrs        pcommon.Map
	lastTimestamp  int64
	startTimestamp int64
}

// MetricAggregator manages metric aggregation state
type MetricAggregator struct {
	mu                  sync.RWMutex
	aggregationInterval time.Duration
	attributeKey        string
	metrics map[aggregationKey]*aggregatedMetric
}

// NewMetricAggregator creates a new metric aggregator
func NewMetricAggregator(attributeKey string, aggregationInterval time.Duration) *MetricAggregator {
	return &MetricAggregator{
		attributeKey:        attributeKey,
		aggregationInterval: aggregationInterval,
		metrics:             make(map[aggregationKey]*aggregatedMetric),
	}
}

// AddMetrics adds metrics to the aggregation state
func (ma *MetricAggregator) AddMetrics(md pmetric.Metrics) {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	debugLog("DEBUG: AddMetrics called with", md.ResourceMetrics().Len(), "resource metrics")

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resourceAttrs := rm.Resource().Attributes()

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)

			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)
				ma.processMetric(metric, resourceAttrs)
			}
		}
	}
}

// processMetric processes a single metric and adds it to aggregation state
func (ma *MetricAggregator) processMetric(metric pmetric.Metric, resourceAttrs pcommon.Map) {
	metricName := metric.Name()
	metricType := metric.Type()

	debugLog("DEBUG: Processing metric:", metricName, "type:", metricType, "unit:", metric.Unit())

	switch metricType {
	case pmetric.MetricTypeSum:
		ma.processSum(metric.Sum(), metricName, resourceAttrs, metric.Unit(), metric.Description())
	case pmetric.MetricTypeGauge:
		ma.processGauge(metric.Gauge(), metricName, resourceAttrs, metric.Unit(), metric.Description())
	default:
		debugLog("DEBUG: Skipping unsupported metric type:", metricType, "for metric:", metricName)
	}
}

// processSum processes sum metrics
func (ma *MetricAggregator) processSum(sum pmetric.Sum, metricName string, resourceAttrs pcommon.Map, unit, description string) {
	for i := 0; i < sum.DataPoints().Len(); i++ {
		dp := sum.DataPoints().At(i)

		attributeValue, found := dp.Attributes().Get(ma.attributeKey)
		if !found {
			// Skip data points without the required attribute
			debugLog("DEBUG: Data point missing attribute", ma.attributeKey, "- skipping (metric:", metricName, ")")
			continue
		}

		timestamp := dp.Timestamp().AsTime()
		timeBucket := ma.getTimeBucket(timestamp)

		dpAttrsKey := serializeAttributes(dp.Attributes())

		key := aggregationKey{
			attributeValue: attributeValue.AsString(),
			metricName:     metricName,
			timeBucket:     timeBucket,
			dpAttributes:   dpAttrsKey,
		}

		agg, exists := ma.metrics[key]
		if !exists {
			agg = &aggregatedMetric{
				key:            key,
				metricType:     pmetric.MetricTypeSum,
				unit:           unit,
				description:    description,
				isMonotonic:    sum.IsMonotonic(),
				resourceAttrs:  pcommon.NewMap(),
				dpAttrs:        pcommon.NewMap(),
				startTimestamp: dp.StartTimestamp().AsTime().UnixNano(),
			}
			resourceAttrs.CopyTo(agg.resourceAttrs)
			dp.Attributes().CopyTo(agg.dpAttrs)
			ma.metrics[key] = agg
		}

		// Accumulate value
		var dpValue float64
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			dpValue = dp.DoubleValue()
		case pmetric.NumberDataPointValueTypeInt:
			dpValue = float64(dp.IntValue())
		}
		agg.sum += dpValue

		agg.count++
		agg.lastTimestamp = timestamp.UnixNano()

		if isDebug() {
			debugLog(fmt.Sprintf("DEBUG: [sum] %s{%s} value=%.4f running_sum=%.4f count=%d bucket=%d",
				metricName, formatAttributes(dp.Attributes()), dpValue, agg.sum, agg.count, timeBucket))
		}
	}
}

// processGauge processes gauge metrics (similar to sum but for gauges)
func (ma *MetricAggregator) processGauge(gauge pmetric.Gauge, metricName string, resourceAttrs pcommon.Map, unit, description string) {
	for i := 0; i < gauge.DataPoints().Len(); i++ {
		dp := gauge.DataPoints().At(i)

		attributeValue, found := dp.Attributes().Get(ma.attributeKey)
		if !found {
			// Skip data points without the required attribute
			debugLog("DEBUG: Data point missing attribute", ma.attributeKey, "- skipping (metric:", metricName, ")")
			continue
		}

		timestamp := dp.Timestamp().AsTime()
		timeBucket := ma.getTimeBucket(timestamp)
		dpAttrsKey := serializeAttributes(dp.Attributes())

		key := aggregationKey{
			attributeValue: attributeValue.AsString(),
			metricName:     metricName,
			timeBucket:     timeBucket,
			dpAttributes:   dpAttrsKey,
		}

		agg, exists := ma.metrics[key]
		if !exists {
			agg = &aggregatedMetric{
				key:           key,
				metricType:    pmetric.MetricTypeGauge,
				unit:          unit,
				description:   description,
				resourceAttrs: pcommon.NewMap(),
				dpAttrs:       pcommon.NewMap(),
			}
			resourceAttrs.CopyTo(agg.resourceAttrs)
			dp.Attributes().CopyTo(agg.dpAttrs)
			ma.metrics[key] = agg
		}

		// For gauges, we sum the values (could also use last value, max, min, etc.)
		var dpValue float64
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			dpValue = dp.DoubleValue()
		case pmetric.NumberDataPointValueTypeInt:
			dpValue = float64(dp.IntValue())
		}
		agg.sum += dpValue

		agg.count++
		agg.lastTimestamp = timestamp.UnixNano()

		if isDebug() {
			debugLog(fmt.Sprintf("DEBUG: [gauge] %s{%s} value=%.4f running_sum=%.4f count=%d bucket=%d",
				metricName, formatAttributes(dp.Attributes()), dpValue, agg.sum, agg.count, timeBucket))
		}
	}
}

// GetAndClearCompletedMetrics returns aggregated metrics for completed time buckets
func (ma *MetricAggregator) GetAndClearCompletedMetrics(now time.Time) pmetric.Metrics {
	ma.mu.Lock()
	defer ma.mu.Unlock()

	currentBucket := ma.getTimeBucket(now)
	md := pmetric.NewMetrics()

	// DEBUG: Log checking for completed metrics
	debugLog("DEBUG: GetAndClearCompletedMetrics - currentBucket:", currentBucket, "totalMetrics:", len(ma.metrics))

	// Find all completed metrics (time buckets before current)
	completedMetrics := make(map[aggregationKey]*aggregatedMetric)
	for key, agg := range ma.metrics {
		debugLog("DEBUG: Checking metric bucket:", key.timeBucket, "< currentBucket:", currentBucket, "?", key.timeBucket < currentBucket)
		if key.timeBucket < currentBucket {
			completedMetrics[key] = agg
			delete(ma.metrics, key) // Remove from active state
		}
	}

	if len(completedMetrics) == 0 {
		debugLog("DEBUG: No completed metrics found")
		return md
	}

	if isDebug() {
		debugLog("DEBUG: Found", len(completedMetrics), "completed metrics to emit:")
		for key, agg := range completedMetrics {
			debugLog(fmt.Sprintf("DEBUG:   -> %s{%s=%s} sum=%.4f count=%d bucket=%d",
				key.metricName, ma.attributeKey, key.attributeValue, agg.sum, agg.count, key.timeBucket))
		}
	}

	// Build the metrics output
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("aggregationprocessor")

	// Group by metric name
	metricsByName := make(map[string][]*aggregatedMetric)
	for _, agg := range completedMetrics {
		metricsByName[agg.key.metricName] = append(metricsByName[agg.key.metricName], agg)
	}

	// Create aggregated metrics
	for metricName, aggs := range metricsByName {
		if len(aggs) == 0 {
			continue
		}

		// Use the first aggregated metric as a template
		template := aggs[0]

		metric := sm.Metrics().AppendEmpty()
		metric.SetName(metricName)
		metric.SetUnit(template.unit)
		metric.SetDescription(template.description)

		switch template.metricType {
		case pmetric.MetricTypeSum:
			sum := metric.SetEmptySum()
			sum.SetIsMonotonic(template.isMonotonic)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

			for _, agg := range aggs {
				dp := sum.DataPoints().AppendEmpty()
				dp.SetDoubleValue(agg.sum)
				dp.SetTimestamp(pcommon.Timestamp(agg.lastTimestamp))
				dp.SetStartTimestamp(pcommon.Timestamp(agg.startTimestamp))
				agg.dpAttrs.CopyTo(dp.Attributes())
				// Add the aggregation attribute to the data point
				dp.Attributes().PutStr(ma.attributeKey, agg.key.attributeValue)
			}

		case pmetric.MetricTypeGauge:
			gauge := metric.SetEmptyGauge()

			for _, agg := range aggs {
				dp := gauge.DataPoints().AppendEmpty()
				dp.SetDoubleValue(agg.sum)
				dp.SetTimestamp(pcommon.Timestamp(agg.lastTimestamp))
				agg.dpAttrs.CopyTo(dp.Attributes())
				dp.Attributes().PutStr(ma.attributeKey, agg.key.attributeValue)
			}
		}
	}

	return md
}

// getTimeBucket calculates the time bucket for a given timestamp
func (ma *MetricAggregator) getTimeBucket(t time.Time) int64 {
	bucketSize := int64(ma.aggregationInterval.Seconds())
	return t.Unix() / bucketSize * bucketSize
}

// formatAttributes formats attributes as a comma-separated label string for debug logging
func formatAttributes(attrs pcommon.Map) string {
	result := ""
	attrs.Range(func(k string, v pcommon.Value) bool {
		if result != "" {
			result += ", "
		}
		result += k + "=" + v.AsString()
		return true
	})
	return result
}

// serializeAttributes creates a string key from attributes for deduplication
func serializeAttributes(attrs pcommon.Map) string {
	if attrs.Len() == 0 {
		return ""
	}
	// Simple serialization - in production might want something more robust
	result := ""
	attrs.Range(func(k string, v pcommon.Value) bool {
		result += k + "=" + v.AsString() + ";"
		return true
	})
	return result
}
