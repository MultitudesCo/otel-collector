package multitudeslogsprocessor

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/multitudes/otel-collector/multitudesauthextension"
)

// maxDebugValueLen caps attribute values in debug output so large values
// (e.g. prompt text) don't flood the logs.
const maxDebugValueLen = 80

func isDebug() bool {
	return os.Getenv("MULTITUDES_DEBUG") != ""
}

func debugLog(args ...any) {
	if isDebug() {
		// Same timestamp format as the collector's own (zap) log lines.
		fmt.Println(append([]any{time.Now().UTC().Format("2006-01-02T15:04:05.000Z")}, args...)...)
	}
}

// debugLogRecords prints one DEBUG line per log record, mirroring the
// aggregation processor's debug output. The internal bearer-token attribute is
// never printed.
func debugLogRecords(ld plog.Logs) {
	if !isDebug() {
		return
	}
	debugLog("DEBUG: ConsumeLogs called with", ld.ResourceLogs().Len(), "resource logs,", ld.LogRecordCount(), "log records")
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		res := formatAttrs(rl.Resource().Attributes())
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				debugLog(fmt.Sprintf("DEBUG: [log] resource{%s} record{%s}", res, formatAttrs(lr.Attributes())))
			}
		}
	}
}

// formatAttrs renders attributes as sorted "key=value" pairs, skipping the
// internal token attribute and truncating long values.
func formatAttrs(attrs pcommon.Map) string {
	pairs := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		if k == multitudesauthextension.InternalApiKeyAttr {
			return true
		}
		val := v.AsString()
		if len(val) > maxDebugValueLen {
			val = val[:maxDebugValueLen] + "..."
		}
		pairs = append(pairs, k+"="+val)
		return true
	})
	sort.Strings(pairs)
	return strings.Join(pairs, ", ")
}
