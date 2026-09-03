# Threat model (concise)

## Assets
Webhook authenticity, tenant/account boundaries, approval decisions, CRM side effects, event payloads and audit integrity.

## Threats and controls shown
- Forged webhook → HMAC verification over exact raw bytes.
- Replay/duplicate delivery → event ID uniqueness and decision replay.
- Malformed/oversized input → typed boundary checks and a 1 MB body limit.
- Wrong model action → bounded intent schema plus deterministic policy and approval gate.
- Downstream outage → bounded retry and dead-letter path.
- Operator blindness → correlation IDs, audit trail, metrics and realtime snapshot.

## Explicit production work
Key rotation and timestamp replay windows, rate limiting/WAF, mTLS or signed connector credentials, RBAC/tenant isolation, PII retention/redaction, encrypted backups, tamper-evident audit storage, dependency/image scanning and incident runbooks.
