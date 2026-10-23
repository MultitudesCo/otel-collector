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

// debugValueAllowlist is the set of attributes whose values are safe to print.
// Every other attribute is printed as its key and value length only, so prompt
// text, tool arguments and credentials never reach the collector logs.
var debugValueAllowlist = map[string]bool{
	"service.name":    true,
	"service.version": true,
	"event.name":      true,
	"event.timestamp": true,
	"model":           true,
	"app.version":     true,
	"terminal.type":   true,
	"success":         true,
	"status_code":     true,
	"duration_ms":     true,
}

// emailAttr is printed masked (e.g. "jo***@example.com") so it can be matched
// without exposing the full address.
const emailAttr = "user.email"

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
// internal token attribute. Only allowlisted values are printed; user.email is
// masked and all other values are replaced by their length.
func formatAttrs(attrs pcommon.Map) string {
	pairs := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		if k == multitudesauthextension.InternalApiKeyAttr {
			return true
		}
		val := v.AsString()
		switch {
		case k == emailAttr:
			val = redact(val)
		case !debugValueAllowlist[k]:
			val = fmt.Sprintf("<%d chars>", len(val))
		}
		pairs = append(pairs, k+"="+val)
		return true
	})
	sort.Strings(pairs)
	return strings.Join(pairs, ", ")
}

// redact masks a value for safe logging (e.g. "josh@example.com" -> "jo***@example.com").
func redact(s string) string {
	for i, c := range s {
		if c == '@' {
			if i > 2 {
				return s[:2] + "***" + s[i:]
			}
			return "***" + s[i:]
		}
	}
	if len(s) > 2 {
		return s[:2] + "***"
	}
	return "***"
}
