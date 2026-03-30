package aggregationprocessor

import (
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func isDebug() bool {
	return os.Getenv("MULTITUDES_DEBUG") != ""
}

func debugLog(args ...any) {
	if isDebug() {
		fmt.Println(args...)
	}
}

// redact masks a value for safe logging (e.g. "josh@example.com" -> "jo***@example.com")
func redact(s string) string {
	for i, c := range s {
		if c == '@' {
			if i > 2 {
				return s[:2] + "***" + s[i:]
			}
			return "***" + s[i:]
		}
	}
	// Not an email, mask all but first 2 chars
	if len(s) > 2 {
		return s[:2] + "***"
	}
	return "***"
}

// aggregationKey uniquely identifies a metric for aggregation
type aggregationKey struct {
	attributeValue string // e.g., user email
	metricName     string
	timeBucket     int64  // Unix timestamp of bucket start
	dpAttributes   string // Serialized data point attributes
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
	metrics             map[aggregationKey]*aggregatedMetric
	logger              *zap.Logger
}

// NewMetricAggregator creates a new metric aggregator
func NewMetricAggregator(attributeKey string, aggregationInterval time.Duration, logger *zap.Logger) *MetricAggregator {
	return &MetricAggregator{
		attributeKey:        attributeKey,
		aggregationInterval: aggregationInterval,
		metrics:             make(map[aggregationKey]*aggregatedMetric),
		logger:              logger,
	}
}

// maxFutureSkew is how far in the future a timestamp is allowed to be before it gets clamped.
const maxFutureSkew = 5 * time.Minute

// maxMetricEntries is the maximum number of entries allowed in the active metrics map.
// This guards against unbounded growth from misconfigured client sending high-cardinality
// attribute values (e.g. UUIDs instead of email).
const maxMetricEntries = 10_000

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

// resolveAttributeValue looks up ma.attributeKey on the data point attributes first,
// then falls back to resource attributes. Returns the value and whether it was found.
func (ma *MetricAggregator) resolveAttributeValue(dpAttrs pcommon.Map, resourceAttrs pcommon.Map, metricName string) (pcommon.Value, bool) {
	if v, ok := dpAttrs.Get(ma.attributeKey); ok {
		debugLog("DEBUG: Found", ma.attributeKey, "on data point attributes for metric:", metricName, "value:", redact(v.AsString()))
		return v, true
	}
	if v, ok := resourceAttrs.Get(ma.attributeKey); ok {
		debugLog("DEBUG: Found", ma.attributeKey, "on resource attributes (not data point) for metric:", metricName, "value:", redact(v.AsString()))
		return v, true
	}
	return pcommon.Value{}, false
}

// dpFloat extracts the float64 value from a data point regardless of its stored type.
func dpFloat(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	default:
		return dp.DoubleValue()
	}
}

// processSum processes sum metrics
func (ma *MetricAggregator) processSum(sum pmetric.Sum, metricName string, resourceAttrs pcommon.Map, unit, description string) {
	for i := 0; i < sum.DataPoints().Len(); i++ {
		dp := sum.DataPoints().At(i)

		attributeValue, found := ma.resolveAttributeValue(dp.Attributes(), resourceAttrs, metricName)
		if !found {
			ma.logger.Warn("dropping data point: required attribute not found",
				zap.String("attribute_key", ma.attributeKey),
				zap.String("metric_name", metricName),
			)
			continue
		}

		timestamp := dp.Timestamp().AsTime()
		// Clamp future timestamps to now to prevent buckets that never complete.
		if now := time.Now(); timestamp.After(now.Add(maxFutureSkew)) {
			debugLog("DEBUG: Clamping future timestamp", timestamp, "to now for metric:", metricName)
			timestamp = now
		}
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
			if len(ma.metrics) >= maxMetricEntries {
				ma.logger.Warn("metric map at capacity, dropping data point",
					zap.String("metric_name", metricName),
					zap.Int("limit", maxMetricEntries),
				)
				continue
			}
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
			// Remove the aggregation key attribute from dpAttrs — it is always
			// emitted separately via PutStr at output time, so storing it here
			// would cause a duplicate and make dpAttrs inconsistent depending on
			// whether the attribute came from the data point or resource attrs.
			agg.dpAttrs.Remove(ma.attributeKey)
			ma.metrics[key] = agg
		}

		dpValue := dpFloat(dp)
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

		attributeValue, found := ma.resolveAttributeValue(dp.Attributes(), resourceAttrs, metricName)
		if !found {
			ma.logger.Warn("dropping data point: required attribute not found",
				zap.String("attribute_key", ma.attributeKey),
				zap.String("metric_name", metricName),
			)
			continue
		}

		timestamp := dp.Timestamp().AsTime()
		// Clamp future timestamps to now to prevent buckets that never complete.
		if now := time.Now(); timestamp.After(now.Add(maxFutureSkew)) {
			debugLog("DEBUG: Clamping future timestamp", timestamp, "to now for metric:", metricName)
			timestamp = now
		}
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
			if len(ma.metrics) >= maxMetricEntries {
				ma.logger.Warn("metric map at capacity, dropping data point",
					zap.String("metric_name", metricName),
					zap.Int("limit", maxMetricEntries),
				)
				continue
			}
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
			// Remove the aggregation key attribute from dpAttrs — it is always
			// emitted separately via PutStr at output time, so storing it here
			// would cause a duplicate and make dpAttrs inconsistent depending on
			// whether the attribute came from the data point or resource attrs.
			agg.dpAttrs.Remove(ma.attributeKey)
			ma.metrics[key] = agg
		}

		// For gauges, we sum the values (could also use last value, max, min, etc.)
		dpValue := dpFloat(dp)
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

	debugLog("DEBUG: GetAndClearCompletedMetrics - currentBucket:", currentBucket, "totalMetrics:", len(ma.metrics))

	// Find all completed metrics (time buckets before current).
	// We rebuild the active map rather than calling delete() on each key — this
	// allows the GC to reclaim the backing array of the old map, avoiding the
	// RSS oscillation that can falsely trigger the memory limiter.
	completedMetrics := make(map[aggregationKey]*aggregatedMetric)
	remaining := make(map[aggregationKey]*aggregatedMetric, len(ma.metrics))
	for key, agg := range ma.metrics {
		debugLog("DEBUG: Checking metric bucket:", key.timeBucket, "< currentBucket:", currentBucket, "?", key.timeBucket < currentBucket)
		if key.timeBucket < currentBucket {
			completedMetrics[key] = agg
		} else {
			remaining[key] = agg
		}
	}
	ma.metrics = remaining

	if len(completedMetrics) == 0 {
		debugLog("DEBUG: No completed metrics found")
		return md
	}

	if isDebug() {
		debugLog("DEBUG: Found", len(completedMetrics), "completed metrics to emit:")
		for key, agg := range completedMetrics {
			debugLog(fmt.Sprintf("DEBUG:   -> %s{%s=%s} sum=%.4f count=%d bucket=%d",
				key.metricName, ma.attributeKey, redact(key.attributeValue), agg.sum, agg.count, key.timeBucket))
		}
	}

	// Build the metrics output
	rm := md.ResourceMetrics().AppendEmpty()

	// Copy resource attributes from the first completed metric.
	// All metrics in a session share the same resource (service.name, etc.),
	// so using any one of them as the source is correct.
	for _, agg := range completedMetrics {
		agg.resourceAttrs.CopyTo(rm.Resource().Attributes())
		break
	}

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

// formatAttributes formats attributes as a comma-separated label string for debug logging.
// Values for keys containing "email" or "user" are redacted.
func formatAttributes(attrs pcommon.Map) string {
	result := ""
	attrs.Range(func(k string, v pcommon.Value) bool {
		if result != "" {
			result += ", "
		}
		val := v.AsString()
		if k == "user.email" || k == "user.id" || k == "user.name" {
			val = redact(val)
		}
		result += k + "=" + val
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
