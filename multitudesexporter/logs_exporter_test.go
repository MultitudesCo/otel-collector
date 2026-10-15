package multitudesexporter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multitudes/otel-collector/multitudesauthextension"
)

func makeLogs(message string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr(message)
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	return ld
}

func newRetryLogsExporter(t *testing.T, srv *httptest.Server) *multitudesLogsExporter {
	t.Helper()
	cfg := &Config{
		Endpoint:       srv.URL,
		FallbackToken:  "token",
		Timeout:        5 * time.Second,
		RetryOnFailure: retryConfig(),
	}
	exp := newLogsExporter(cfg, zap.NewNop())
	if err := exp.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	return exp
}

// TestExportLogsWithToken_IncludesResponseBodyOn401 verifies that the error
// returned for a 401 response includes the server's response body, so the
// actual rejection reason (e.g. an expired or invalid token) reaches the logs.
func TestExportLogsWithToken_IncludesResponseBodyOn401(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"token expired"}`))
	}))
	defer srv.Close()

	exp := newRetryLogsExporter(t, srv)
	ld := makeLogs("hello")
	err := exp.exportLogsWithToken(context.Background(), ld, "token")
	if err == nil {
		t.Fatal("expected an error for 401 response, got nil")
	}
	if !strings.Contains(err.Error(), "token expired") {
		t.Errorf("error = %q, want it to contain the response body %q", err.Error(), "token expired")
	}
}

// TestExportLogsWithToken_IncludesResponseBodyOn403 mirrors the 401 case for
// a 403 Forbidden response.
func TestExportLogsWithToken_IncludesResponseBodyOn403(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"error":"insufficient scope"}`))
	}))
	defer srv.Close()

	exp := newRetryLogsExporter(t, srv)
	ld := makeLogs("hello")
	err := exp.exportLogsWithToken(context.Background(), ld, "token")
	if err == nil {
		t.Fatal("expected an error for 403 response, got nil")
	}
	if !strings.Contains(err.Error(), "insufficient scope") {
		t.Errorf("error = %q, want it to contain the response body %q", err.Error(), "insufficient scope")
	}
}

// TestExportLogsWithToken_MapsStatusToGRPCCode verifies that a non-retryable
// 4xx response carries a gRPC status matching its HTTP semantics, so the
// OTLP receiver reports the original status (401/403/400) to the sender
// instead of collapsing every permanent failure into a generic 500.
func TestExportLogsWithToken_MapsStatusToGRPCCode(t *testing.T) {
	tests := []struct {
		status   int
		wantCode codes.Code
	}{
		{http.StatusBadRequest, codes.InvalidArgument},
		{http.StatusUnauthorized, codes.Unauthenticated},
		{http.StatusForbidden, codes.PermissionDenied},
		{http.StatusNotFound, codes.InvalidArgument},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			exp := newRetryLogsExporter(t, srv)
			ld := makeLogs("hello")
			err := exp.exportLogsWithToken(context.Background(), ld, "token")
			if err == nil {
				t.Fatalf("status %d: expected an error, got nil", tc.status)
			}
			s, ok := status.FromError(err)
			if !ok {
				t.Fatalf("status %d: expected a gRPC status error, got: %v", tc.status, err)
			}
			if s.Code() != tc.wantCode {
				t.Errorf("status %d: gRPC code = %v, want %v", tc.status, s.Code(), tc.wantCode)
			}
		})
	}
}

// TestExportLogsWithToken_NoGRPCStatusOnServerFailure verifies that a
// transient server-side failure does NOT carry an explicit gRPC status, so
// the receiver falls back to its default retryable code.
func TestExportLogsWithToken_NoGRPCStatusOnServerFailure(t *testing.T) {
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
	exp := newLogsExporter(cfg, zap.NewNop())
	if err := exp.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	ld := makeLogs("hello")
	err := exp.exportLogsWithToken(context.Background(), ld, "token")
	if err == nil {
		t.Fatal("expected an error after exhausted retries, got nil")
	}
	if _, ok := status.FromError(err); ok {
		t.Errorf("expected no explicit gRPC status for a transient 500, got: %v", err)
	}
}

// addResourceLogs appends a ResourceLogs with the given service.name and the
// per-client token attribute the multitudeslogsprocessor would have injected.
func addResourceLogs(ld plog.Logs, serviceName, token string) {
	rl := ld.ResourceLogs().AppendEmpty()
	if serviceName != "" {
		rl.Resource().Attributes().PutStr("service.name", serviceName)
	}
	rl.Resource().Attributes().PutStr(multitudesauthextension.InternalApiKeyAttr, token)
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("log")
}

type exportedRequest struct {
	token    string
	services []string
}

// captureExports starts a server that records the token and the service.name of
// every resource in each exported request.
func captureExports(t *testing.T) (*httptest.Server, func() []exportedRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []exportedRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(body)
		if err != nil {
			t.Errorf("unmarshal exported logs: %v", err)
		}
		req := exportedRequest{token: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")}
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			service := ""
			if name, ok := ld.ResourceLogs().At(i).Resource().Attributes().Get("service.name"); ok {
				service = name.AsString()
			}
			req.services = append(req.services, service)
		}
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []exportedRequest {
		mu.Lock()
		defer mu.Unlock()
		sort.Slice(requests, func(i, j int) bool {
			return requests[i].token+strings.Join(requests[i].services, ",") <
				requests[j].token+strings.Join(requests[j].services, ",")
		})
		return append([]exportedRequest(nil), requests...)
	}
}

// TestConsumeLogs_SplitsRequestsByTokenAndServiceName verifies that resources
// from different services sharing one token are exported in separate requests,
// so the receiving service can attribute each request to a single provider.
func TestConsumeLogs_SplitsRequestsByTokenAndServiceName(t *testing.T) {
	srv, requests := captureExports(t)
	exp := newRetryLogsExporter(t, srv)

	ld := plog.NewLogs()
	addResourceLogs(ld, "claude-code", "org-a")
	addResourceLogs(ld, "codex-app-server", "org-a")
	addResourceLogs(ld, "atlantis-webhook-filter-prod", "org-a")
	addResourceLogs(ld, "codex-app-server", "org-b")

	if err := exp.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs() error: %v", err)
	}

	got := requests()
	want := []exportedRequest{
		{token: "org-a", services: []string{"atlantis-webhook-filter-prod"}},
		{token: "org-a", services: []string{"claude-code"}},
		{token: "org-a", services: []string{"codex-app-server"}},
		{token: "org-b", services: []string{"codex-app-server"}},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d requests %+v, want %d %+v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i].token != want[i].token || strings.Join(got[i].services, ",") != strings.Join(want[i].services, ",") {
			t.Errorf("request %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestConsumeLogs_KeepsResourcesOfOneServiceAndTokenTogether verifies that
// resources from the same service and token still share a single request.
func TestConsumeLogs_KeepsResourcesOfOneServiceAndTokenTogether(t *testing.T) {
	srv, requests := captureExports(t)
	exp := newRetryLogsExporter(t, srv)

	ld := plog.NewLogs()
	addResourceLogs(ld, "claude-code", "org-a")
	addResourceLogs(ld, "claude-code", "org-a")
	addResourceLogs(ld, "", "org-a")

	if err := exp.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs() error: %v", err)
	}

	got := requests()
	if len(got) != 2 {
		t.Fatalf("got %d requests %+v, want 2 (claude-code x2, and one without a service.name)", len(got), got)
	}
}
