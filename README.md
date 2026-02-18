# Multitudes OTel Collector

A custom [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) that runs in your network, aggregates AI usage metrics from your engineers, and sends them to Multitudes — without raw data ever leaving your environment.

## How it works

Tools like Claude Code emit raw OTLP metrics as engineers work. The Multitudes OTel Collector receives those metrics, aggregates them by user over a time window, and sends only the aggregated totals to Multitudes. Individual activity data stays inside your network.

```
[Claude Code / AI tools] → [Multitudes OTel Collector] → [Multitudes]
         raw metrics            aggregated by user          totals only
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- A Multitudes API token (provided by your Multitudes account team)

## Quick start (Docker)

This is the recommended way to run the collector in production.

**1. Clone this repository**

```bash
git clone https://github.com/multitudes/otel-collector.git
cd otel-collector
```

**2. Build the collector image**

```bash
docker build -t otelcol-multitudes:latest .
```

**3. Run the collector**

```bash
docker run -d \
  --name multitudes-otel-collector \
  --restart unless-stopped \
  -e MULTITUDES_INTEGRATION_TOKEN=your_bearer_token_here \
  -e MULTITUDES_INTEGRATION_ENDPOINT=https://integrations.multitudes.co/ai/otel \
  -p 127.0.0.1:4317:4317 \
  -p 127.0.0.1:4318:4318 \
  -p 127.0.0.1:13133:13133 \
  otelcol-multitudes:latest
```

The collector is now running and listening for OTLP metrics on:
- `localhost:4317` — gRPC
- `localhost:4318` — HTTP

## Docker Compose (evaluation / local testing)

A `docker-compose.yaml` is included for convenience when evaluating the collector locally. It is not recommended for production deployments.

**1. Configure your credentials**

```bash
cp .env.example .env
```

Edit `.env` and set your token and endpoint:

```env
MULTITUDES_INTEGRATION_TOKEN=your_bearer_token_here
MULTITUDES_INTEGRATION_ENDPOINT=https://integrations.multitudes.co/ai/otel
```

**2. Build and run**

```bash
docker build -t otelcol-multitudes:latest .
docker-compose up -d
```

To enable verbose debug logging:

```bash
DEBUG=true docker-compose up
```

## Configuring your AI tools

Point your AI tools at the collector by setting the OTLP endpoint environment variable:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

For Claude Code, add this to your shell profile (`.bashrc`, `.zshrc`, etc.) or set it in your Claude Code configuration.

## Repository structure

```
.
├── Dockerfile                      # Builds the custom collector image
├── docker-compose.yaml             # Convenience setup for local evaluation
├── builder-config.yaml             # OTel Collector Builder configuration
├── otel-collector-config.yaml      # Collector configuration (baked into image)
├── .env.example                    # Environment variable template
└── aggregationprocessor/           # Custom Go aggregation processor
    ├── processor.go                # Processor wiring and emit loop
    ├── aggregator.go               # Metric bucketing and aggregation logic
    ├── factory.go                  # OTel processor factory
    └── config.go                   # Configuration schema
```

## Configuration

The collector is configured via environment variables:

| Variable | Required | Description |
|---|---|---|
| `MULTITUDES_INTEGRATION_TOKEN` | Yes | Bearer token provided by Multitudes |
| `MULTITUDES_INTEGRATION_ENDPOINT` | Yes | Multitudes ingestion endpoint |
| `DEBUG` | No | Set to `true` to enable verbose debug logging |

### Aggregation

The collector aggregates metrics by `user.email` over 1-hour windows before sending to Multitudes. This means:

- Raw per-request metrics are never sent externally
- Only per-user totals per time window are transmitted
- Network egress is minimal

The aggregation window and other settings can be adjusted in `otel-collector-config.yaml`.

## Advanced: AWS SSM for token management

If you prefer to manage your Multitudes token in AWS SSM Parameter Store rather than an environment variable, you can fetch it at container startup.

Store your token in SSM:

```bash
aws ssm put-parameter \
  --name "/multitudes/integration-service/bearer-token" \
  --value "your_bearer_token_here" \
  --type SecureString
```

Then in your container startup script, fetch the parameter and pass it as an environment variable:

```bash
export MULTITUDES_INTEGRATION_TOKEN=$(aws ssm get-parameter \
  --name "/multitudes/integration-service/bearer-token" \
  --with-decryption \
  --query "Parameter.Value" \
  --output text)
```

The container will need an IAM role with `ssm:GetParameter` permission on the parameter.

## Ports

| Port | Protocol | Purpose |
|---|---|---|
| `4317` | gRPC | OTLP metrics ingestion |
| `4318` | HTTP | OTLP metrics ingestion |
| `13133` | HTTP | Health check (`/health`) |
| `55679` | HTTP | zPages diagnostics |

## Support

Contact your Multitudes account team or open an issue in this repository.
