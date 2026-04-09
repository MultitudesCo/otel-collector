package multitudesauthextension

import (
	"context"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

// InternalApiKeyAttr is the pdata resource attribute used to carry the client's
// Bearer token through the aggregation pipeline. It is an implementation detail
// and is stripped before any metrics are forwarded to Multitudes.
// The aggregation processor and Multitudes exporter use this same constant.
const InternalApiKeyAttr = "multitudes.internal.bearer_token"

// apiKeyContextKey is the key used to store the extracted Bearer token in context.
type apiKeyContextKey struct{}

type multitudesAuth struct {
	logger *zap.Logger
}

func newExtension(logger *zap.Logger) *multitudesAuth {
	return &multitudesAuth{logger: logger}
}

func (e *multitudesAuth) Start(_ context.Context, _ component.Host) error {
	e.logger.Info("Multitudes auth extension started")
	return nil
}

func (e *multitudesAuth) Shutdown(_ context.Context) error {
	return nil
}

// Authenticate implements auth.Server. It is called by the OTLP receiver for
// every incoming request (both HTTP and gRPC). It extracts the Bearer token
// from the Authorization header and stores it in the returned context.
//
// The method never returns an error — requests without a token are allowed
// through. The exporter will fall back to MULTITUDES_INTEGRATION_TOKEN if no
// per-client key is present in the emitted metrics.
func (e *multitudesAuth) Authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	token := extractBearer(headers)

	if token == "" {
		e.logger.Debug("No Bearer token in incoming request; will use fallback token at export time")
		return ctx, nil
	}

	e.logger.Debug("Extracted Bearer token from incoming request")
	return context.WithValue(ctx, apiKeyContextKey{}, token), nil
}

// GetApiKeyFromContext retrieves the Bearer token that the auth extension stored
// in ctx. Returns ("", false) if no token was placed in context (e.g., the
// request had no Authorization header, or the auth extension is not in the
// pipeline).
func GetApiKeyFromContext(ctx context.Context) (token string, found bool) {
	token, ok := ctx.Value(apiKeyContextKey{}).(string)
	return token, ok && token != ""
}

// extractBearer pulls the Bearer token out of the Authorization header.
// Checks both lowercase and title-case forms since different OTel Collector
// versions normalise header keys differently.
func extractBearer(headers map[string][]string) string {
	for k, vals := range headers {
		if strings.ToLower(k) == "authorization" {
			for _, v := range vals {
				if after, ok := strings.CutPrefix(v, "Bearer "); ok {
					if t := strings.TrimSpace(after); t != "" {
						return t
					}
				}
			}
		}
	}
	return ""
}
