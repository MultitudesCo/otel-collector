# Build stage - compile custom collector with aggregation processor
FROM golang:1.23 AS builder

# Install OpenTelemetry Collector Builder
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.115.0

WORKDIR /build

# Copy builder configuration and custom processor
COPY builder-config.yaml .
COPY aggregationprocessor ./aggregationprocessor

# Build the custom collector
RUN CGO_ENABLED=0 builder --config=builder-config.yaml

# Runtime stage - minimal image with just the collector binary
FROM alpine:3.19

# Install ca-certificates for HTTPS and bash for startup script
RUN apk --no-cache add ca-certificates bash

WORKDIR /

# Copy the custom collector binary from builder
COPY --from=builder /build/dist/otelcol-multitudes /otelcol-multitudes

# Copy the production collector configuration
# For local development, docker-compose will volume mount otel-collector-config.local.yaml
COPY otel-collector-config.yaml /etc/otelcol-contrib/otel-collector-config.yaml

# Expose ports
# 4317: OTLP gRPC
# 4318: OTLP HTTP
# 55679: zPages diagnostics
# 13133: Health check
EXPOSE 4317 4318 55679 13133

# Default command (can be overridden in docker-compose or ECS)
ENTRYPOINT ["/otelcol-multitudes"]
CMD ["--config=/etc/otelcol-contrib/otel-collector-config.yaml"]
