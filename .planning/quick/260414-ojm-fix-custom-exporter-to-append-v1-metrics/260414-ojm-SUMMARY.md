---
phase: 260414-ojm
plan: fix-custom-exporter-to-append-v1-metrics
subsystem: multitudesexporter
tags: [exporter, otlp, endpoint-normalization]
key-files:
  modified:
    - multitudesexporter/exporter.go
    - multitudesexporter/config.go
    - multitudesexporter/exporter_test.go
decisions:
  - Call exp.Start() in newTestExporter/newRetryExporter for DRY endpoint normalization rather than duplicating logic
metrics:
  duration: ~5m
  completed: 2026-04-14T05:48:07Z
  tasks_completed: 2
  files_modified: 3
---

# Quick Task 260414-ojm: Fix Custom Exporter to Append /v1/metrics Summary

**One-liner:** Exporter now auto-normalizes endpoint to append `/v1/metrics`, stripping trailing slashes, so users supply only the base URL.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Fix exporter.go — endpoint normalization | 412af12 | exporter.go, config.go |
| 2 | Add tests for normalization and path | 4c181ec | exporter_test.go |

## Changes Made

### exporter.go
- Added `endpoint string` field to `multitudesExporter` struct
- `Start()` now normalizes: trims trailing `/`, appends `/v1/metrics` if absent, stores in `e.endpoint`
- `Start()` logs `e.endpoint` (normalized) instead of `e.cfg.Endpoint`
- `exportWithToken()` uses `e.endpoint` instead of `e.cfg.Endpoint`
- Added `"strings"` import

### config.go
- Updated `Endpoint` field comment to document that `/v1/metrics` is appended automatically

### exporter_test.go
- Updated `newTestExporter` to call `exp.Start(context.Background(), nil)` instead of manually setting `exp.client`/`exp.marshaler`
- Updated `newRetryExporter` and two inline retry test helpers the same way
- Added `TestStart_NormalizesEndpointV1Metrics` (3 sub-cases: no suffix, already has suffix, trailing slash)
- Added `TestConsumeMetrics_SendsToV1MetricsPath` verifying HTTP request lands at `/v1/metrics`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed newRetryExporter and inline retry tests to call Start()**
- **Found during:** Task 2 (test updates)
- **Issue:** `newRetryExporter`, `TestExportWithToken_RetriesOnFailure`, and `TestExportWithToken_ReturnsErrorAfterExhaustedRetries` manually set `exp.client`/`exp.marshaler` without calling `Start()`, leaving `exp.endpoint == ""` — breaking `exportWithToken` after the fix
- **Fix:** Replaced manual field assignments with `exp.Start(context.Background(), nil)` in all three locations
- **Files modified:** multitudesexporter/exporter_test.go
- **Commit:** 4c181ec

## Self-Check: PASSED

- [x] `multitudesexporter/exporter.go` — modified (endpoint field, Start normalization, exportWithToken)
- [x] `multitudesexporter/config.go` — modified (comment updated)
- [x] `multitudesexporter/exporter_test.go` — modified (newTestExporter, newRetryExporter, new tests)
- [x] Commit 412af12 exists
- [x] Commit 4c181ec exists
- [x] `go test ./...` → ok (0.568s)
