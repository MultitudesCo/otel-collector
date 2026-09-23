package multitudesexporter

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcErrorForHTTPStatus wraps msg in a gRPC status error matching the HTTP
// semantics of a non-retryable downstream response, so the OTLP receiver
// reports the original status to the sender instead of collapsing every
// permanent failure into a generic 500.
func grpcErrorForHTTPStatus(httpStatus int, msg string) error {
	var code codes.Code
	switch httpStatus {
	case http.StatusUnauthorized:
		code = codes.Unauthenticated
	case http.StatusForbidden:
		code = codes.PermissionDenied
	default:
		code = codes.InvalidArgument
	}
	return status.Error(code, msg)
}
