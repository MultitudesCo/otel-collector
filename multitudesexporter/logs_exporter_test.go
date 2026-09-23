package multitudesexporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
