package multitudesauthextension

import (
	"context"
	"testing"

	"go.uber.org/zap"
)

// --- extractBearer ---

func TestExtractBearer_LowercaseKey(t *testing.T) {
	headers := map[string][]string{
		"authorization": {"Bearer my-token"},
	}
	got := extractBearer(headers)
	if got != "my-token" {
		t.Errorf("got %q, want %q", got, "my-token")
	}
}

func TestExtractBearer_TitleCaseKey(t *testing.T) {
	// Go's net/http passes headers in canonical title-case form.
	headers := map[string][]string{
		"Authorization": {"Bearer my-token"},
	}
	got := extractBearer(headers)
	if got != "my-token" {
		t.Errorf("got %q, want %q", got, "my-token")
	}
}

func TestExtractBearer_MixedCaseKey(t *testing.T) {
	headers := map[string][]string{
		"AUTHORIZATION": {"Bearer MY-TOKEN"},
	}
	got := extractBearer(headers)
	if got != "MY-TOKEN" {
		t.Errorf("got %q, want %q", got, "MY-TOKEN")
	}
}

func TestExtractBearer_NoAuthorizationHeader(t *testing.T) {
	headers := map[string][]string{
		"content-type": {"application/json"},
	}
	got := extractBearer(headers)
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestExtractBearer_EmptyHeaders(t *testing.T) {
	got := extractBearer(map[string][]string{})
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestExtractBearer_LowercaseBearerScheme(t *testing.T) {
	headers := map[string][]string{
		"authorization": {"bearer lowercase-token"},
	}
	got := extractBearer(headers)
	if got != "lowercase-token" {
		t.Errorf("got %q, want %q", got, "lowercase-token")
	}
}

func TestExtractBearer_UppercaseBearerScheme(t *testing.T) {
	headers := map[string][]string{
		"authorization": {"BEARER UPPERCASE-TOKEN"},
	}
	got := extractBearer(headers)
	if got != "UPPERCASE-TOKEN" {
		t.Errorf("got %q, want %q", got, "UPPERCASE-TOKEN")
	}
}

func TestExtractBearer_NotBearerScheme(t *testing.T) {
	headers := map[string][]string{
		"authorization": {"Basic dXNlcjpwYXNz"},
	}
	got := extractBearer(headers)
	if got != "" {
		t.Errorf("expected empty string for non-Bearer scheme, got %q", got)
	}
}

func TestExtractBearer_BearerWithExtraWhitespace(t *testing.T) {
	headers := map[string][]string{
		"authorization": {"Bearer   spaced-token  "},
	}
	got := extractBearer(headers)
	if got != "spaced-token" {
		t.Errorf("got %q, want %q", got, "spaced-token")
	}
}

func TestExtractBearer_EmptyToken(t *testing.T) {
	headers := map[string][]string{
		"authorization": {"Bearer "},
	}
	got := extractBearer(headers)
	if got != "" {
		t.Errorf("expected empty string for empty Bearer token, got %q", got)
	}
}

// --- Authenticate + GetApiKeyFromContext ---

func newTestExtension() *multitudesAuth {
	return newExtension(zap.NewNop())
}

func TestAuthenticate_WithBearerToken(t *testing.T) {
	ext := newTestExtension()
	headers := map[string][]string{
		"authorization": {"Bearer test-key-123"},
	}

	ctx, err := ext.Authenticate(context.Background(), headers)
	if err != nil {
		t.Fatalf("Authenticate returned unexpected error: %v", err)
	}

	token, found := GetApiKeyFromContext(ctx)
	if !found {
		t.Fatal("expected token in context, got nothing")
	}
	if token != "test-key-123" {
		t.Errorf("got token %q, want %q", token, "test-key-123")
	}
}

func TestAuthenticate_WithTitleCaseHeader(t *testing.T) {
	ext := newTestExtension()
	headers := map[string][]string{
		"Authorization": {"Bearer title-case-key"},
	}

	ctx, err := ext.Authenticate(context.Background(), headers)
	if err != nil {
		t.Fatalf("Authenticate returned unexpected error: %v", err)
	}

	token, found := GetApiKeyFromContext(ctx)
	if !found {
		t.Fatal("expected token in context, got nothing")
	}
	if token != "title-case-key" {
		t.Errorf("got token %q, want %q", token, "title-case-key")
	}
}

func TestAuthenticate_WithoutAuthorizationHeader(t *testing.T) {
	ext := newTestExtension()
	headers := map[string][]string{
		"content-type": {"application/json"},
	}

	ctx, err := ext.Authenticate(context.Background(), headers)
	if err != nil {
		t.Fatalf("Authenticate should never return an error, got: %v", err)
	}

	_, found := GetApiKeyFromContext(ctx)
	if found {
		t.Error("expected no token in context when no Authorization header present")
	}
}

func TestAuthenticate_EmptyHeaders(t *testing.T) {
	ext := newTestExtension()

	ctx, err := ext.Authenticate(context.Background(), map[string][]string{})
	if err != nil {
		t.Fatalf("Authenticate should never return an error, got: %v", err)
	}

	_, found := GetApiKeyFromContext(ctx)
	if found {
		t.Error("expected no token in context for empty headers")
	}
}

// --- GetApiKeyFromContext ---

func TestGetApiKeyFromContext_NoValue(t *testing.T) {
	token, found := GetApiKeyFromContext(context.Background())
	if found {
		t.Error("expected found=false on plain context")
	}
	if token != "" {
		t.Errorf("expected empty token, got %q", token)
	}
}

func TestGetApiKeyFromContext_WrongType(t *testing.T) {
	// Storing the wrong type under the key should return not-found.
	ctx := context.WithValue(context.Background(), apiKeyContextKey{}, 12345)
	token, found := GetApiKeyFromContext(ctx)
	if found {
		t.Error("expected found=false when wrong type stored in context")
	}
	if token != "" {
		t.Errorf("expected empty token, got %q", token)
	}
}
