#!/bin/sh
# In debug mode (MULTITUDES_DEBUG set to any non-empty value), flush on a short
# cadence instead of the usual 5-minute windows. Metrics wait for the 1-minute
# aggregation window and then at most one 15-second batch, so expect data to
# arrive after roughly 1 to 1.5 minutes. Explicitly set values always win.
set -e

if [ -n "${MULTITUDES_DEBUG:-}" ]; then
  export MULTITUDES_BATCH_TIMEOUT="${MULTITUDES_BATCH_TIMEOUT:-15s}"
  export MULTITUDES_AGGREGATION_INTERVAL="${MULTITUDES_AGGREGATION_INTERVAL:-1m}"
  # emit_interval must be shorter than aggregation_interval.
  export MULTITUDES_EMIT_INTERVAL="${MULTITUDES_EMIT_INTERVAL:-15s}"
fi

exec /otelcol-multitudes "$@"
