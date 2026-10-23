#!/bin/sh
# In debug mode (MULTITUDES_DEBUG set to any non-empty value), flush everything
# on a short cadence so data is visible within ~1 minute instead of the usual
# 5-minute window. Explicitly set values always win.
set -e

if [ -n "${MULTITUDES_DEBUG:-}" ]; then
  export MULTITUDES_BATCH_TIMEOUT="${MULTITUDES_BATCH_TIMEOUT:-1m}"
  export MULTITUDES_AGGREGATION_INTERVAL="${MULTITUDES_AGGREGATION_INTERVAL:-1m}"
  # emit_interval must be shorter than aggregation_interval.
  export MULTITUDES_EMIT_INTERVAL="${MULTITUDES_EMIT_INTERVAL:-15s}"
fi

exec /otelcol-multitudes "$@"
