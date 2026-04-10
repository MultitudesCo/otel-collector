package multitudesexporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/multitudes/otel-collector/multitudesauthextension"
)

// --- factory validation ---

func TestCreateMetricsExporter_RejectsEmptyEndpoint(t *testing.T) {
	cfg := &Config{Endpoint: ""}
	_, err := createMetricsExporter(context.Background(), exporter.Settings{}, cfg)
	if err == nil {
		t.Error("expected an error for empty endpoint, got nil")
	}
}

func TestCreateMetricsExporter_RejectsMalformedEndpoint(t *testing.T) {
	cfg := &Config{Endpoint: "not a valid url"}
	_, err := createMetricsExporter(context.Background(), exporter.Settings{}, cfg)
	if err == nil {
		t.Error("expected an error for malformed endpoint, got nil")
	}
}

func TestCreateMetricsExporter_AcceptsValidConfig(t *testing.T) {
	cfg := &Config{Endpoint: "https://integrations.multitudes.co/ai/otel"}
	_, err := createMetricsExporter(context.Background(), exporter.Settings{}, cfg)
	if err != nil {
		t.Errorf("expected no error for valid config, got: %v", err)
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
			Enabled: false,
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
		"user.email": "dev@example.com",
		multitudesauthextension.InternalApiKeyAttr: "per-client-token",
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
	// No multitudesauthextension.InternalApiKeyAttr in resource attributes.
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
		"user.email": "dev@example.com",
		multitudesauthextension.InternalApiKeyAttr: "secret-token",
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
	rm1.Resource().Attributes().PutStr(multitudesauthextension.InternalApiKeyAttr, "token-alice")
	sm1 := rm1.ScopeMetrics().AppendEmpty()
	m1 := sm1.Metrics().AppendEmpty()
	m1.SetName("test.metric")
	dp1 := m1.SetEmptySum().DataPoints().AppendEmpty()
	dp1.SetDoubleValue(1.0)
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	rm2 := md.ResourceMetrics().AppendEmpty()
	rm2.Resource().Attributes().PutStr("user.email", "bob@example.com")
	rm2.Resource().Attributes().PutStr(multitudesauthextension.InternalApiKeyAttr, "token-bob")
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
		rm.Resource().Attributes().PutStr(multitudesauthextension.InternalApiKeyAttr, "shared-token")
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

func retryConfig() RetryConfig {
	return RetryConfig{
		Enabled:         true,
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     5 * time.Millisecond,
		MaxElapsedTime:  1 * time.Second,
	}
}

func newRetryExporter(t *testing.T, srv *httptest.Server) *multitudesExporter {
	t.Helper()
	cfg := &Config{
		Endpoint:       srv.URL,
		FallbackToken:  "token",
		Timeout:        5 * time.Second,
		RetryOnFailure: retryConfig(),
	}
	exp := newExporter(cfg, zap.NewNop())
	exp.client = &http.Client{Timeout: 5 * time.Second}
	exp.marshaler = &pmetric.JSONMarshaler{}
	return exp
}

// TestExportWithToken_NoRetryOn4xx verifies that permanent 4xx responses
// (excluding 408 and 429) cause an immediate bail-out with no further attempts.
func TestExportWithToken_NoRetryOn4xx(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			attempts := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts++
				w.WriteHeader(status)
			}))
			defer srv.Close()

			exp := newRetryExporter(t, srv)
			md := makeMetrics(nil, "test.metric", 1.0)
			if err := exp.exportWithToken(context.Background(), md, "token"); err == nil {
				t.Errorf("status %d: expected an error, got nil", status)
			}
			if attempts != 1 {
				t.Errorf("status %d: expected exactly 1 attempt (no retry), got %d", status, attempts)
			}
		})
	}
}

// TestExportWithToken_RetriesOn408 verifies that 408 Request Timeout is retried.
func TestExportWithToken_RetriesOn408(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusRequestTimeout)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp := newRetryExporter(t, srv)
	md := makeMetrics(nil, "test.metric", 1.0)
	if err := exp.exportWithToken(context.Background(), md, "token"); err != nil {
		t.Fatalf("expected success after retries on 408, got: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

// TestExportWithToken_RetriesOn429 verifies that 429 Too Many Requests is retried.
func TestExportWithToken_RetriesOn429(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp := newRetryExporter(t, srv)
	md := makeMetrics(nil, "test.metric", 1.0)
	if err := exp.exportWithToken(context.Background(), md, "token"); err != nil {
		t.Fatalf("expected success after retries on 429, got: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

// TestConsumeMetrics_PartialFailureTrimsMdForRetry verifies that when one
// token succeeds and another fails, the successfully-exported ResourceMetrics
// are removed from md so that an upstream retry does not duplicate them.
func TestConsumeMetrics_PartialFailureTrimsMdForRetry(t *testing.T) {
	aliceRequests := 0
	bobShouldFail := true

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		switch auth {
		case "Bearer token-alice":
			aliceRequests++
			w.WriteHeader(http.StatusOK)
		case "Bearer token-bob":
			if bobShouldFail {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusOK)
			}
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	defer srv.Close()

	exp, _ := newTestExporter(srv.URL, "")

	md := pmetric.NewMetrics()

	rmAlice := md.ResourceMetrics().AppendEmpty()
	rmAlice.Resource().Attributes().PutStr(multitudesauthextension.InternalApiKeyAttr, "token-alice")
	smAlice := rmAlice.ScopeMetrics().AppendEmpty()
	mAlice := smAlice.Metrics().AppendEmpty()
	mAlice.SetName("alice.metric")
	dpAlice := mAlice.SetEmptySum().DataPoints().AppendEmpty()
	dpAlice.SetDoubleValue(1.0)
	dpAlice.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	rmBob := md.ResourceMetrics().AppendEmpty()
	rmBob.Resource().Attributes().PutStr(multitudesauthextension.InternalApiKeyAttr, "token-bob")
	smBob := rmBob.ScopeMetrics().AppendEmpty()
	mBob := smBob.Metrics().AppendEmpty()
	mBob.SetName("bob.metric")
	dpBob := mBob.SetEmptySum().DataPoints().AppendEmpty()
	dpBob.SetDoubleValue(2.0)
	dpBob.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// First call: alice succeeds, bob fails.
	if err := exp.ConsumeMetrics(context.Background(), md); err == nil {
		t.Fatal("expected an error on partial failure, got nil")
	}

	// Alice's ResourceMetrics must have been removed from md; only bob's remain.
	if got := md.ResourceMetrics().Len(); got != 1 {
		t.Fatalf("expected 1 ResourceMetrics remaining after partial failure, got %d", got)
	}

	// Simulate retry: bob now succeeds.
	bobShouldFail = false
	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}

	// Alice must not have been re-sent on the retry.
	if aliceRequests != 1 {
		t.Errorf("alice was sent %d time(s), expected exactly 1 (no duplication on retry)", aliceRequests)
	}
}

// TestConsumeMetrics_OriginalMdRetainsInternalAttrAfterSuccess verifies that
// ConsumeMetrics does not strip the internal api-key attribute from the original
// md entries, even after a fully successful export. The attribute must only be
// removed from the payload copy that is sent to the backend.
func TestConsumeMetrics_OriginalMdRetainsInternalAttrAfterSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exp, _ := newTestExporter(srv.URL, "")
	md := makeMetrics(map[string]string{
		multitudesauthextension.InternalApiKeyAttr: "per-client-token",
	}, "test.metric", 1.0)

	if err := exp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rm := md.ResourceMetrics().At(0)
	v, ok := rm.Resource().Attributes().Get(multitudesauthextension.InternalApiKeyAttr)
	if !ok {
		t.Fatal("internal api-key attribute was stripped from the original md; retry would lose token")
	}
	if v.AsString() != "per-client-token" {
		t.Errorf("attribute value = %q, want %q", v.AsString(), "per-client-token")
	}
}

// TestConsumeMetrics_OriginalMdRetainsInternalAttrAfterFailure verifies the
// attribute is also untouched when the export fails so that the upstream
// retry_sender can re-present md and have token resolution succeed identically.
func TestConsumeMetrics_OriginalMdRetainsInternalAttrAfterFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	exp, _ := newTestExporter(srv.URL, "")
	md := makeMetrics(map[string]string{
		multitudesauthextension.InternalApiKeyAttr: "per-client-token",
	}, "test.metric", 1.0)

	_ = exp.ConsumeMetrics(context.Background(), md) // error expected; ignore it

	rm := md.ResourceMetrics().At(0)
	v, ok := rm.Resource().Attributes().Get(multitudesauthextension.InternalApiKeyAttr)
	if !ok {
		t.Fatal("internal api-key attribute was stripped from the original md; retry would lose token")
	}
	if v.AsString() != "per-client-token" {
		t.Errorf("attribute value = %q, want %q", v.AsString(), "per-client-token")
	}
}
