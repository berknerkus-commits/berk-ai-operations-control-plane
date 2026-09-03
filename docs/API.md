# API contract

Base URL: `http://localhost:8000`

## `POST /v1/webhooks/operations`

Headers:
- `X-Webhook-Signature: sha256=<HMAC-SHA256 body digest>`
- `X-Correlation-Id: <caller trace id>` (optional; generated from event ID when omitted)

Body fields:
- `event_id` string, required, max 120; idempotency key
- `account_id` string, required
- `email` string, required; normalized to lowercase
- `message` string, required, max 10,000
- `source` string, optional
- `amount` number, optional, non-negative

Response: `Decision {event_id,status,intent,action,requires_approval,attempt,correlation_id}`.
- `401`: invalid or absent HMAC signature
- `422`: schema validation failure
- duplicate: existing decision is returned; no second side effect

## Operational endpoints

- `GET /health` — liveness response.
- `GET /ops/metrics` — accepted, duplicate, rejected, dead-letter, approval and audit counters.
- `WS /ws/ops` — sends a current metrics snapshot for a dashboard/realtime client.

The running API also exposes generated OpenAPI at `/openapi.json` and Swagger UI at `/docs`.
