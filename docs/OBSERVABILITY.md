# Observability view

The runtime exposes a deliberately small operator surface:

- `GET /ops/metrics`: accepted, duplicate, rejected, pending approvals, dead letters and audit entries.
- `WS /ws/ops`: sends a point-in-time snapshot for a realtime operator panel.
- JSON logs include `event_id` and action, but not message content or email fields.
- Audit records preserve decision, approval and dead-letter transitions.

A production deployment should scrape Prometheus/OpenTelemetry metrics and traces, add alert thresholds, redact logs centrally, and protect this surface with operator authentication and tenant scoping. The current view proves the seam without pretending to be a complete monitoring platform.
