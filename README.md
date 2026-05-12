# Multitudes OTel Collector

![Version](https://img.shields.io/github/v/tag/MultitudesCo/otel-collector?label=version&sort=semver)

A custom [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) that runs in your network, aggregates AI usage metrics from your engineers, and sends them to Multitudes.

This approach means that raw data does not leave your environment.

## How it works

Tools like Claude Code emit raw OTLP metrics as engineers work. The Multitudes OTel Collector receives those metrics, aggregates them by user over a time window, and sends the aggregated totals to Multitudes. Individual activity data stays inside your network.

```
[AI tools] → [Multitudes OTel Collector] → [Multitudes]
         raw metrics            aggregated by user          totals only
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- A Multitudes Integration token (generated from within the Multitudes app; [documentation on how to do that here](https://docs.multitudes.com/integrations/deployments-api#auth))

## Quick start

**1. Pull the image**

```bash
docker pull ghcr.io/multitudesco/otel-collector:latest
```

For production deployments, pin to a specific version instead of `latest` to avoid unexpected updates:

```bash
docker pull ghcr.io/multitudesco/otel-collector:1.2.0
```

See the version badge above for the current release.

**2. Run the collector**

```bash
docker run -d \
  --name multitudes-otel-collector \
  --restart unless-stopped \
  -e MULTITUDES_INTEGRATION_TOKEN=your_bearer_token_here \
  -e MULTITUDES_INTEGRATION_ENDPOINT=https://integrations.multitudes.co/ai/otel \
  -e MULTITUDES_DEBUG=1 \
  -p 127.0.0.1:4317:4317 \
  -p 127.0.0.1:4318:4318 \
  -p 127.0.0.1:13133:13133 \
  ghcr.io/multitudesco/otel-collector:latest

# To disable verbose debug logging in production, omit the line above:
#   -e MULTITUDES_DEBUG=1 \
```

The collector is now running and listening for OTLP metrics on:
- `localhost:4317` — gRPC
- `localhost:4318` — HTTP

**3. Configure your AI tools**

***Claude Code***

Each person using Claude Code should enable exporting metrics to the OTLP endpoint. This can be done by configuring a `~/.claude/settings.json` file.

For a local setup (collector running on the same machine), use `http://localhost:4318` as the endpoint:

```json
{
  "env": {
    "CLAUDE_CODE_ENABLE_TELEMETRY": "1",
    "OTEL_METRICS_EXPORTER": "otlp",
    "OTEL_LOGS_EXPORTER": "otlp",
    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
    "OTEL_METRIC_EXPORT_INTERVAL": "10000"
  }
}
```

***Codex***

Each person using Codex should enable exporting metrics to the OTLP endpoint. This can be done by configuring a `~/.codex/config.toml` file.

For a local setup (collector running on the same machine), use `http://localhost:4318/v1/logs` as the endpoint:

```toml
[otel]
exporter = { otlp-http = { endpoint = "http://localhost:4318/v1/logs", protocol = "binary" } }
log_user_prompt = false
```


If the collector is deployed as a shared service (e.g. on ECS or another host), replace `http://localhost:4318` with the hostname and port where that service is reachable, for example, `http://collector.internal:4318`.

**4. Validate the collector is working**

To confirm the collector is running and receiving metrics, tail its logs using the Docker CLI:

```bash
docker logs -f multitudes-otel-collector
```

You should see log output indicating the collector is active. Once AI tool sessions are underway, you will see incoming metric lines appear in the log stream. If no metrics appear, double-check your tool config file (for example `~/.claude/settings.json` or `~/.codex/config.toml`) and confirm the configured OTLP endpoint matches the collector address.


If you see warning lines like the following in the logs, it means incoming metrics are being dropped because they do not include a `user.email` attribute:

```text
Warn  dropping data point: required attribute not found  {"attribute_key": "user.email", "metric_name": "claude_code.cost.usage"}
```

The most common cause is that users are not logged in with their work email. Ensure each person has authenticated with the AI tool using their work email address before sending metrics. See [Logging in](#logging-in) below.

### Using server-managed settings files

Rather than needing each user to manually configure the settings/config file for each machine, these can be centrally managed in a number of ways.

Refer to the Claude Code documentation for more:
* [Settings files](https://code.claude.com/docs/en/settings#settings-files)
* [Server-managed settings](https://code.claude.com/docs/en/server-managed-settings)

Refer to the Codex documentation for more:
* [Config file](https://developers.openai.com/codex/config-basic)
* [Managed defaults](https://developers.openai.com/codex/enterprise/managed-configuration#managed-defaults-managed_configtoml)

### Logging in

Each person sending OTel metrics should be logged in using their work email. This allows Multitudes to correctly match the incoming metrics to users in Multitudes.

#### AWS Bedrock deployments

When using Claude Code with AWS Bedrock, the `user.email` attribute must be configured manually via the `OTEL_RESOURCE_ATTRIBUTES` environment variable. This is because Bedrock authentication does not automatically provide user email information.

Add the following to each developer's `~/.claude/settings.json`:

```json
{
  "env": {
    "OTEL_RESOURCE_ATTRIBUTES": "user.email=developer@company.com"
  }
}
```

You can automate this setup by extracting the email from your AWS authentication system (e.g., `aws sts get-caller-identity`) and deploying settings files centrally. See [Using server-managed settings files](#using-server-managed-settings-files) above.

## Releasing a new version

The version is stored in the `VERSION` file in the repository root. To release a new version:

1. Make your changes in a branch
2. Bump the version in `VERSION` (e.g. `1.1.0` → `1.2.0`)
3. Open and merge a PR into `prod`

CI will automatically create the git tag and build the versioned Docker image. If a PR does not require a version bump (e.g. documentation changes), leave `VERSION` unchanged.

## Building the image locally

If you would prefer to build the image locally instead of pulling from the GitHub Container Registry:

**1. Clone this repository**

```bash
git clone https://github.com/multitudesco/otel-collector.git
cd otel-collector
```

**2. Build the collector image**

```bash
docker build -t ghcr.io/multitudesco/otel-collector:latest .
```

## Repository structure

```
.
├── Dockerfile                      # Builds the custom collector image
├── builder-config.yaml             # OTel Collector Builder configuration
├── otel-collector-config.yaml      # Collector configuration (baked into image)
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
| `MULTITUDES_DEBUG` | No | Set to any non-empty value to enable verbose debug logging from the aggregation processor |

### Aggregation

The collector aggregates metrics by `user.email` over a 5 minute window before sending to Multitudes. This means:

- Raw per-request metrics are never sent externally
- Only per-user totals per time window are transmitted
- Reduction in network traffic

The aggregation window and other settings can be adjusted in `otel-collector-config.yaml`.

## Ports

| Port | Protocol | Purpose |
|---|---|---|
| `4317` | gRPC | OTLP metrics ingestion |
| `4318` | HTTP | OTLP metrics ingestion |
| `13133` | HTTP | Health check (`/health`) |
| `55679` | HTTP | zPages diagnostics |

## Deployment

Deploy the collector onto your infrastructure and expose the required ports for users to export metrics to.

Specific configuration guides for common deployment patterns like ECS Fargate will be coming soon.

## Support

Contact your Multitudes account team or open an issue in this repository.
