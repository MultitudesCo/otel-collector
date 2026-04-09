package multitudesexporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// --- redactToken ---

func TestRedactToken(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"abcdefgh", "abcd***"},
		{"ab",       "***"},
		{"abcd",     "***"},
		{"abcde",    "abcd***"},
		{"",         "***"},
	}
	for _, tt := range tests {
		got := redactToken(tt.input)
		if got != tt.want {
			t.Errorf("redactToken(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// --- helpers ---

func makeMetrics(resourceAttrs map[string]string, metricName string, value float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	for k, v := range resourceAttrs {
		rm.Resource().Attributes().PutStr(k, v)
	}
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	return md
}

func newTestExporter(endpoint, fallbackToken string) (*multitudesExporter, error) {
	cfg := &Config{
		Endpoint:      endpoint,
		FallbackToken: fallbackToken,
		Timeout:       5 * time.Second,
		RetryOnFailure: RetryConfig{
			Enabled:        false,
		},
	}
	exp := newExporter(cfg, zap.NewNop())
	exp.client = &http.Client{Timeout: 5 * time.Second}
	exp.marshaler = &pmetric.JSONMarshaler{}
	return exp, nil
}

// --- ConsumeMetrics: token resolution ---

func TestConsumeMetrics_UsesPerClientToken(t *testing.T) {
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, _ := newTestExporter(srv.URL, "fallback-token")
	md := makeMetrics(map[string]string{
		"user.email":       "dev@example.com",
		internalApiKeyAttr: "per-client-token",
	}, "test.metric", 1.0)

	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics error: %v", err)
	}

	if receivedAuth != "Bearer per-client-token" {
		t.Errorf("Authorization header = %q, want %q", receivedAuth, "Bearer per-client-token")
	}
}

func TestConsumeMetrics_FallsBackToFallbackToken(t *testing.T) {
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, _ := newTestExporter(srv.URL, "fallback-token")
	// No internalApiKeyAttr in resource attributes.
	md := makeMetrics(map[string]string{"user.email": "dev@example.com"}, "test.metric", 1.0)

	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics error: %v", err)
	}

	if receivedAuth != "Bearer fallback-token" {
		t.Errorf("Authorization header = %q, want %q", receivedAuth, "Bearer fallback-token")
	}
}

func TestConsumeMetrics_StripsInternalAttribute(t *testing.T) {
	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		r.Body.Read(buf)
		capturedBody = buf
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, _ := newTestExporter(srv.URL, "fallback-token")
	md := makeMetrics(map[string]string{
		"user.email":       "dev@example.com",
		internalApiKeyAttr: "secret-token",
	}, "test.metric", 1.0)

	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics error: %v", err)
	}

	if len(capturedBody) == 0 {
		t.Fatal("no request body captured")
	}
	body := string(capturedBody)
	if contains := "multitudes.internal.bearer_token"; indexByteSlice(capturedBody, contains) {
		t.Errorf("internal attribute %q must not appear in forwarded payload, body: %s", contains, body)
	}
}

func indexByteSlice(b []byte, substr string) bool {
	return len(b) >= len(substr) && string(b) != "" && findSubstring(string(b), substr)
}

func findSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) && (s[:len(substr)] == substr || findSubstring(s[1:], substr))))
}

func TestConsumeMetrics_SeparatesRequestsByToken(t *testing.T) {
	var mu sync.Mutex
	receivedTokens := []string{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedTokens = append(receivedTokens, r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, _ := newTestExporter(srv.URL, "fallback-token")

	// Build a pmetric.Metrics with two ResourceMetrics blocks, each with a different token.
	md := pmetric.NewMetrics()

	rm1 := md.ResourceMetrics().AppendEmpty()
	rm1.Resource().Attributes().PutStr("user.email", "alice@example.com")
	rm1.Resource().Attributes().PutStr(internalApiKeyAttr, "token-alice")
	sm1 := rm1.ScopeMetrics().AppendEmpty()
	m1 := sm1.Metrics().AppendEmpty()
	m1.SetName("test.metric")
	dp1 := m1.SetEmptySum().DataPoints().AppendEmpty()
	dp1.SetDoubleValue(1.0)
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	rm2 := md.ResourceMetrics().AppendEmpty()
	rm2.Resource().Attributes().PutStr("user.email", "bob@example.com")
	rm2.Resource().Attributes().PutStr(internalApiKeyAttr, "token-bob")
	sm2 := rm2.ScopeMetrics().AppendEmpty()
	m2 := sm2.Metrics().AppendEmpty()
	m2.SetName("test.metric")
	dp2 := m2.SetEmptySum().DataPoints().AppendEmpty()
	dp2.SetDoubleValue(2.0)
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedTokens) != 2 {
		t.Fatalf("expected 2 HTTP requests (one per token), got %d", len(receivedTokens))
	}
	tokenSet := map[string]bool{}
	for _, tok := range receivedTokens {
		tokenSet[tok] = true
	}
	if !tokenSet["Bearer token-alice"] {
		t.Error("expected a request with Bearer token-alice")
	}
	if !tokenSet["Bearer token-bob"] {
		t.Error("expected a request with Bearer token-bob")
	}
}

func TestConsumeMetrics_SameTokenBatchedInOneRequest(t *testing.T) {
	requestCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, _ := newTestExporter(srv.URL, "fallback-token")

	md := pmetric.NewMetrics()
	for i := 0; i < 3; i++ {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr(internalApiKeyAttr, "shared-token")
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("test.metric")
		dp := m.SetEmptySum().DataPoints().AppendEmpty()
		dp.SetDoubleValue(float64(i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	}

	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics error: %v", err)
	}

	if requestCount != 1 {
		t.Errorf("expected 1 HTTP request for same token across 3 ResourceMetrics, got %d", requestCount)
	}
}

func TestConsumeMetrics_DropsWhenNoToken(t *testing.T) {
	requestCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// No fallback token set, no per-client token in metrics.
	exp, _ := newTestExporter(srv.URL, "")
	md := makeMetrics(map[string]string{"user.email": "dev@example.com"}, "test.metric", 1.0)

	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("expected no error when dropping metrics with no token, got: %v", err)
	}

	if requestCount != 0 {
		t.Errorf("expected 0 HTTP requests when no token available, got %d", requestCount)
	}
}

// --- exportWithToken: retry behaviour ---

func TestExportWithToken_RetriesOnFailure(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &Config{
		Endpoint:      srv.URL,
		FallbackToken: "token",
		Timeout:       5 * time.Second,
		RetryOnFailure: RetryConfig{
			Enabled:         true,
			InitialInterval: 1 * time.Millisecond,
			MaxInterval:     5 * time.Millisecond,
			MaxElapsedTime:  1 * time.Second,
		},
	}
	exp := newExporter(cfg, zap.NewNop())
	exp.client = &http.Client{Timeout: 5 * time.Second}
	exp.marshaler = &pmetric.JSONMarshaler{}

	md := makeMetrics(nil, "test.metric", 1.0)
	if err := exp.exportWithToken(context.Background(), md, "token"); err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestExportWithToken_ReturnsErrorAfterExhaustedRetries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := &Config{
		Endpoint:      srv.URL,
		FallbackToken: "token",
		Timeout:       5 * time.Second,
		RetryOnFailure: RetryConfig{
			Enabled:         true,
			InitialInterval: 1 * time.Millisecond,
			MaxInterval:     2 * time.Millisecond,
			MaxElapsedTime:  10 * time.Millisecond,
		},
	}
	exp := newExporter(cfg, zap.NewNop())
	exp.client = &http.Client{Timeout: 5 * time.Second}
	exp.marshaler = &pmetric.JSONMarshaler{}

	md := makeMetrics(nil, "test.metric", 1.0)
	if err := exp.exportWithToken(context.Background(), md, "token"); err == nil {
		t.Error("expected an error after exhausted retries, got nil")
	}
}
