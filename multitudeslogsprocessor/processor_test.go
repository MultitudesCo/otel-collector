package multitudeslogsprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/multitudes/otel-collector/multitudesauthextension"
)

// createTestLogs builds a plog.Logs with the given number of ResourceLogs,
// each with a single log record.
func createTestLogs(resourceCount int) plog.Logs {
	ld := plog.NewLogs()
	for i := 0; i < resourceCount; i++ {
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr("test log record")
	}
	return ld
}

// createTestLogsWithToken builds a plog.Logs that already has the internal
// bearer-token resource attribute set — simulating a misconfigured client
// that tries to inject the attribute directly.
func createTestLogsWithClientToken(resourceCount int, token string) plog.Logs {
	ld := createTestLogs(resourceCount)
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		ld.ResourceLogs().At(i).Resource().Attributes().PutStr(
			multitudesauthextension.InternalApiKeyAttr, token,
		)
	}
	return ld
}

// TestLogsProcessorInjectsTokenFromContext verifies that when the context
// carries an API key (placed there by the auth extension), the processor
// writes it into every ResourceLogs resource attribute.
func TestLogsProcessorInjectsTokenFromContext(t *testing.T) {
	sink := &consumertest.LogsSink{}
	factory := NewFactory()
	set := processortest.NewNopSettings(component.MustNewType(Type))

	proc, err := factory.CreateLogs(context.Background(), set, &Config{}, sink)
	if err != nil {
		t.Fatalf("CreateLogs() error = %v", err)
	}

	if err := proc.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer proc.Shutdown(context.Background())

	const token = "test-bearer-token"
	ctx := multitudesauthextension.ContextWithApiKey(context.Background(), token)
	ld := createTestLogs(3)

	if err := proc.ConsumeLogs(ctx, ld); err != nil {
		t.Fatalf("ConsumeLogs() error = %v", err)
	}

	allLogs := sink.AllLogs()
	if len(allLogs) != 1 {
		t.Fatalf("Expected 1 log batch forwarded to sink, got %d", len(allLogs))
	}

	got := allLogs[0]
	if got.ResourceLogs().Len() != 3 {
		t.Fatalf("Expected 3 ResourceLogs, got %d", got.ResourceLogs().Len())
	}

	for i := 0; i < got.ResourceLogs().Len(); i++ {
		attrs := got.ResourceLogs().At(i).Resource().Attributes()
		val, ok := attrs.Get(multitudesauthextension.InternalApiKeyAttr)
		if !ok {
			t.Errorf("ResourceLogs[%d]: expected %q attribute to be set", i, multitudesauthextension.InternalApiKeyAttr)
			continue
		}
		if val.AsString() != token {
			t.Errorf("ResourceLogs[%d]: expected token %q, got %q", i, token, val.AsString())
		}
	}
}

// TestLogsProcessorNoTokenInContext verifies that when the context carries no
// API key the processor does not add the internal attribute to ResourceLogs.
func TestLogsProcessorNoTokenInContext(t *testing.T) {
	sink := &consumertest.LogsSink{}
	factory := NewFactory()
	set := processortest.NewNopSettings(component.MustNewType(Type))

	proc, err := factory.CreateLogs(context.Background(), set, &Config{}, sink)
	if err != nil {
		t.Fatalf("CreateLogs() error = %v", err)
	}

	if err := proc.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer proc.Shutdown(context.Background())

	ld := createTestLogs(2)

	if err := proc.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs() error = %v", err)
	}

	allLogs := sink.AllLogs()
	if len(allLogs) != 1 {
		t.Fatalf("Expected 1 log batch, got %d", len(allLogs))
	}

	got := allLogs[0]
	for i := 0; i < got.ResourceLogs().Len(); i++ {
		attrs := got.ResourceLogs().At(i).Resource().Attributes()
		if _, ok := attrs.Get(multitudesauthextension.InternalApiKeyAttr); ok {
			t.Errorf("ResourceLogs[%d]: expected no %q attribute when context has no token", i, multitudesauthextension.InternalApiKeyAttr)
		}
	}
}

// TestLogsProcessorStripsClientSuppliedToken verifies that any InternalApiKeyAttr
// value set directly by the client is removed, preventing token injection attacks.
// When the context also carries a valid token, the context token replaces it.
func TestLogsProcessorStripsClientSuppliedToken(t *testing.T) {
	sink := &consumertest.LogsSink{}
	factory := NewFactory()
	set := processortest.NewNopSettings(component.MustNewType(Type))

	proc, err := factory.CreateLogs(context.Background(), set, &Config{}, sink)
	if err != nil {
		t.Fatalf("CreateLogs() error = %v", err)
	}

	if err := proc.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer proc.Shutdown(context.Background())

	const (
		clientToken  = "malicious-client-token"
		contextToken = "legitimate-context-token"
	)

	ld := createTestLogsWithClientToken(2, clientToken)
	ctx := multitudesauthextension.ContextWithApiKey(context.Background(), contextToken)

	if err := proc.ConsumeLogs(ctx, ld); err != nil {
		t.Fatalf("ConsumeLogs() error = %v", err)
	}

	allLogs := sink.AllLogs()
	if len(allLogs) != 1 {
		t.Fatalf("Expected 1 log batch, got %d", len(allLogs))
	}

	got := allLogs[0]
	for i := 0; i < got.ResourceLogs().Len(); i++ {
		attrs := got.ResourceLogs().At(i).Resource().Attributes()
		val, ok := attrs.Get(multitudesauthextension.InternalApiKeyAttr)
		if !ok {
			t.Errorf("ResourceLogs[%d]: expected %q attribute to be set with context token", i, multitudesauthextension.InternalApiKeyAttr)
			continue
		}
		if val.AsString() == clientToken {
			t.Errorf("ResourceLogs[%d]: client-supplied token was not stripped", i)
		}
		if val.AsString() != contextToken {
			t.Errorf("ResourceLogs[%d]: expected context token %q, got %q", i, contextToken, val.AsString())
		}
	}
}

// TestLogsProcessorStripsClientTokenWithNoContextToken verifies that a
// client-supplied internal attribute is stripped even when the context has
// no token — no fallback to the client value.
func TestLogsProcessorStripsClientTokenWithNoContextToken(t *testing.T) {
	sink := &consumertest.LogsSink{}
	factory := NewFactory()
	set := processortest.NewNopSettings(component.MustNewType(Type))

	proc, err := factory.CreateLogs(context.Background(), set, &Config{}, sink)
	if err != nil {
		t.Fatalf("CreateLogs() error = %v", err)
	}

	if err := proc.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer proc.Shutdown(context.Background())

	ld := createTestLogsWithClientToken(1, "malicious-client-token")

	if err := proc.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs() error = %v", err)
	}

	allLogs := sink.AllLogs()
	if len(allLogs) != 1 {
		t.Fatalf("Expected 1 log batch, got %d", len(allLogs))
	}

	attrs := allLogs[0].ResourceLogs().At(0).Resource().Attributes()
	if _, ok := attrs.Get(multitudesauthextension.InternalApiKeyAttr); ok {
		t.Errorf("Expected client-supplied %q attribute to be stripped when context has no token", multitudesauthextension.InternalApiKeyAttr)
	}
}

// TestLogsProcessorForwardsToNextConsumer verifies that logs are passed
// through to the downstream consumer unchanged (bar the token attribute).
func TestLogsProcessorForwardsToNextConsumer(t *testing.T) {
	sink := &consumertest.LogsSink{}
	factory := NewFactory()
	set := processortest.NewNopSettings(component.MustNewType(Type))

	proc, err := factory.CreateLogs(context.Background(), set, &Config{}, sink)
	if err != nil {
		t.Fatalf("CreateLogs() error = %v", err)
	}

	if err := proc.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer proc.Shutdown(context.Background())

	ld := createTestLogs(5)
	if err := proc.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs() error = %v", err)
	}

	allLogs := sink.AllLogs()
	if len(allLogs) != 1 {
		t.Fatalf("Expected 1 log batch, got %d", len(allLogs))
	}

	got := allLogs[0]
	if got.ResourceLogs().Len() != 5 {
		t.Fatalf("Expected 5 ResourceLogs, got %d", got.ResourceLogs().Len())
	}

	for i := 0; i < got.ResourceLogs().Len(); i++ {
		body := got.ResourceLogs().At(i).ScopeLogs().At(0).LogRecords().At(0).Body().AsString()
		if body != "test log record" {
			t.Errorf("ResourceLogs[%d]: expected body %q, got %q", i, "test log record", body)
		}
	}
}

// TestLogsProcessorCapabilities verifies that the processor declares it
// mutates data (required for in-place attribute writes).
func TestLogsProcessorCapabilities(t *testing.T) {
	sink := &consumertest.LogsSink{}
	p := newLogsProcessor(nil, sink)

	caps := p.Capabilities()
	if !caps.MutatesData {
		t.Error("Expected MutatesData = true, got false")
	}
}

// TestConfigValidation verifies that the empty Config always passes validation.
func TestConfigValidation(t *testing.T) {
	cfg := &Config{}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() returned unexpected error: %v", err)
	}
}

// TestLogsProcessorEmptyLogs verifies the processor handles a Logs payload
// with no ResourceLogs entries without error.
func TestLogsProcessorEmptyLogs(t *testing.T) {
	sink := &consumertest.LogsSink{}
	factory := NewFactory()
	set := processortest.NewNopSettings(component.MustNewType(Type))

	proc, err := factory.CreateLogs(context.Background(), set, &Config{}, sink)
	if err != nil {
		t.Fatalf("CreateLogs() error = %v", err)
	}

	if err := proc.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer proc.Shutdown(context.Background())

	ctx := multitudesauthextension.ContextWithApiKey(context.Background(), "some-token")
	ld := plog.NewLogs() // zero ResourceLogs

	if err := proc.ConsumeLogs(ctx, ld); err != nil {
		t.Fatalf("ConsumeLogs() on empty logs error = %v", err)
	}

	if len(sink.AllLogs()) != 1 {
		t.Errorf("Expected 1 (empty) log batch forwarded, got %d", len(sink.AllLogs()))
	}
}
