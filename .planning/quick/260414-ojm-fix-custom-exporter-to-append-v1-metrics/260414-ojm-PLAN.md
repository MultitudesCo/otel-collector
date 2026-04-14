---
phase: 260414-ojm-fix-custom-exporter-to-append-v1-metrics
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - multitudesexporter/exporter.go
  - multitudesexporter/config.go
  - multitudesexporter/exporter_test.go
autonomous: true
requirements: []
---

<objective>
Normalize the exporter endpoint to always POST to `{endpoint}/v1/metrics`, matching OTel OTLP/HTTP conventions. Users configure a base URL; the exporter appends the path automatically.

Purpose: Backward-compat with users who configured the standard OTLP exporter using a base URL without the `/v1/metrics` suffix.
Output: Updated exporter, config doc comment, and a test verifying the request path.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
</execution_context>

<context>
@multitudesexporter/exporter.go
@multitudesexporter/config.go
@multitudesexporter/exporter_test.go
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add normalized endpoint field and wire it through exporter</name>
  <files>multitudesexporter/exporter.go</files>
  <action>
1. Add `"strings"` to the import block.

2. Add an `endpoint string` field to `multitudesExporter`:
```go
type multitudesExporter struct {
    cfg      *Config
    logger   *zap.Logger
    client   *http.Client
    endpoint string
    marshaler pmetric.Marshaler
}
```

3. In `Start()`, compute and store the normalized endpoint before creating the HTTP client:
```go
func (e *multitudesExporter) Start(_ context.Context, _ component.Host) error {
    ep := strings.TrimRight(e.cfg.Endpoint, "/")
    if !strings.HasSuffix(ep, "/v1/metrics") {
        ep += "/v1/metrics"
    }
    e.endpoint = ep
    e.client = &http.Client{Timeout: e.cfg.Timeout}
    e.logger.Info("Multitudes exporter started",
        zap.String("endpoint", e.endpoint),
        zap.Bool("fallback_token_set", e.cfg.FallbackToken != ""),
    )
    return nil
}
```

4. In `exportWithToken`, replace `e.cfg.Endpoint` with `e.endpoint` on the line that builds the HTTP request:
```go
req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body))
```
  </action>
  <verify>cd multitudesexporter && go build ./... 2>&1</verify>
  <done>Package compiles cleanly; `e.endpoint` is used in the request, not `e.cfg.Endpoint`.</done>
</task>

<task type="auto">
  <name>Task 2: Update config.go doc comment and add path test</name>
  <files>multitudesexporter/config.go, multitudesexporter/exporter_test.go</files>
  <action>
**config.go** — update the `Endpoint` field comment:
```go
// Endpoint is the base Multitudes OTLP ingestion URL.
// Do NOT include /v1/metrics — the exporter appends it automatically.
// e.g. "https://integrations.multitudes.co/ai/otel"
```

**exporter_test.go** — add a new test after the existing factory tests that verifies the request arrives at `/v1/metrics`:

```go
func TestConsumeMetrics_SendsToV1MetricsPath(t *testing.T) {
    var receivedPath string
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        receivedPath = r.URL.Path
        w.WriteHeader(http.StatusOK)
    }))
    defer srv.Close()

    exp, _ := newTestExporter(srv.URL, "fallback-token")
    // newTestExporter bypasses Start(), so set the endpoint field directly.
    exp.endpoint = srv.URL + "/v1/metrics"

    md := makeMetrics(map[string]string{}, "test.metric", 1.0)
    if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
        t.Fatalf("ConsumeMetrics error: %v", err)
    }

    if receivedPath != "/v1/metrics" {
        t.Errorf("request path = %q, want %q", receivedPath, "/v1/metrics")
    }
}

func TestStart_NormalizesEndpointAppend(t *testing.T) {
    cases := []struct {
        raw  string
        want string
    }{
        {"https://example.com/ai/otel", "https://example.com/ai/otel/v1/metrics"},
        {"https://example.com/ai/otel/", "https://example.com/ai/otel/v1/metrics"},
        {"https://example.com/ai/otel/v1/metrics", "https://example.com/ai/otel/v1/metrics"},
        {"https://example.com/ai/otel/v1/metrics/", "https://example.com/ai/otel/v1/metrics/"},
    }
    for _, tc := range cases {
        cfg := &Config{
            Endpoint: tc.raw,
            Timeout:  5 * time.Second,
            RetryOnFailure: RetryConfig{Enabled: false},
        }
        exp := newExporter(cfg, zap.NewNop())
        exp.client = &http.Client{}
        _ = exp.Start(context.Background(), nil)
        if exp.endpoint != tc.want {
            t.Errorf("raw=%q: endpoint=%q, want %q", tc.raw, exp.endpoint, tc.want)
        }
    }
}
```

Note: `TestStart_NormalizesEndpointAppend` calls `exp.Start(context.Background(), nil)` — the `Host` parameter is `_ component.Host` so nil is safe.
  </action>
  <verify>cd /Users/kevinchan/Code/Multitudes/otel-collector/.claude/worktrees/auth-passthrough/multitudesexporter && go test ./... -count=1 2>&1</verify>
  <done>All tests pass including the two new tests. `TestStart_NormalizesEndpointAppend` covers append, no-double-append, and trailing-slash normalization cases.</done>
</task>

</tasks>

<verification>
```bash
cd /Users/kevinchan/Code/Multitudes/otel-collector/.claude/worktrees/auth-passthrough/multitudesexporter
go test ./... -count=1 -v 2>&1 | grep -E "^(=== RUN|--- PASS|--- FAIL|FAIL|ok)"
```
All tests green, no new failures.
</verification>

<success_criteria>
- `exportWithToken` posts to `{base}/v1/metrics` regardless of whether the user's configured endpoint has a trailing slash or already includes `/v1/metrics`
- `Start()` log line shows the fully-resolved endpoint (with `/v1/metrics`)
- `config.go` comment tells users NOT to include `/v1/metrics`
- Two new tests pass: one verifying the HTTP path, one verifying normalization edge cases
</success_criteria>

<output>
No SUMMARY needed for quick tasks.
</output>
