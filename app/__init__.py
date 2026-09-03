"""AI Operations Control Plane: durable intake, delivery retries, DLQ and audit."""
from __future__ import annotations
import hashlib, hmac, json, logging, os, sqlite3, threading, time
from dataclasses import asdict, dataclass
from typing import Any, Callable

log = logging.getLogger("ai_ops")
ALLOWED_INTENTS = {"sales", "support", "billing", "unknown"}
TRANSIENT = (TimeoutError, ConnectionError)

@dataclass(frozen=True)
class IntakeEvent:
    event_id: str; account_id: str; email: str; message: str; source: str
    amount: float = 0.0; correlation_id: str = ""
@dataclass(frozen=True)
class Decision:
    event_id: str; status: str; intent: str; action: str
    requires_approval: bool; attempt: int; correlation_id: str

def _schema(postgres: bool) -> str:
    if postgres:
        return """CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,payload JSONB NOT NULL,status TEXT NOT NULL,created_at DOUBLE PRECISION NOT NULL,updated_at DOUBLE PRECISION NOT NULL);
CREATE TABLE IF NOT EXISTS decisions(event_id TEXT PRIMARY KEY REFERENCES events(event_id),body JSONB NOT NULL,created_at DOUBLE PRECISION NOT NULL,updated_at DOUBLE PRECISION NOT NULL);
CREATE TABLE IF NOT EXISTS approvals(id BIGSERIAL PRIMARY KEY,event_id TEXT NOT NULL REFERENCES events(event_id),state TEXT NOT NULL,reviewer TEXT,created_at DOUBLE PRECISION NOT NULL,updated_at DOUBLE PRECISION NOT NULL);
CREATE TABLE IF NOT EXISTS retry_attempts(id BIGSERIAL PRIMARY KEY,event_id TEXT NOT NULL REFERENCES events(event_id),phase TEXT NOT NULL,attempt INTEGER NOT NULL,outcome TEXT NOT NULL,error TEXT,created_at DOUBLE PRECISION NOT NULL);
CREATE TABLE IF NOT EXISTS dead_letters(id BIGSERIAL PRIMARY KEY,event_id TEXT NOT NULL REFERENCES events(event_id),error TEXT NOT NULL,state TEXT NOT NULL,created_at DOUBLE PRECISION NOT NULL,updated_at DOUBLE PRECISION NOT NULL,recovered_at DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS audit(id BIGSERIAL PRIMARY KEY,event_id TEXT NOT NULL REFERENCES events(event_id),action TEXT NOT NULL,detail JSONB NOT NULL,created_at DOUBLE PRECISION NOT NULL);
CREATE INDEX IF NOT EXISTS audit_event_idx ON audit(event_id);"""
    return """CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,payload TEXT NOT NULL,status TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS decisions(event_id TEXT PRIMARY KEY REFERENCES events(event_id),body TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS approvals(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL REFERENCES events(event_id),state TEXT NOT NULL,reviewer TEXT,created_at REAL NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS retry_attempts(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL REFERENCES events(event_id),phase TEXT NOT NULL,attempt INTEGER NOT NULL,outcome TEXT NOT NULL,error TEXT,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS dead_letters(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL,error TEXT NOT NULL,state TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL,recovered_at REAL);
CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL,action TEXT NOT NULL,detail TEXT NOT NULL,created_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS audit_event_idx ON audit(event_id);"""

class Database:
    def __init__(self, url: str):
        self.postgres = url.startswith("postgres" + "ql://")
        if self.postgres:
            import psycopg
            self.conn = psycopg.connect(url)
        else:
            self.conn = sqlite3.connect(url, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")
        self.lock = threading.RLock()
        self.script(_schema(self.postgres)); self.commit()
    def execute(self, sql: str, params=()):
        return self.conn.execute(sql.replace("?", "%s") if self.postgres else sql, params)
    def script(self, sql: str):
        if self.postgres:
            for statement in sql.split(";"):
                if statement.strip(): self.conn.execute(statement)
        else: self.conn.executescript(sql)
    def commit(self): self.conn.commit()
    def rollback(self): self.conn.rollback()

class ControlPlane:
    def __init__(self, db_path=":memory:", secret=None, classifier=None, crm_sync=None, retry_sleep=time.sleep):
        self.db = Database(db_path); self.secret = secret; self.classifier = classifier
        self.crm_sync = crm_sync; self.retry_sleep = retry_sleep
        with self.db.lock:
            self.db.script(_schema(self.db.postgres)); self.db.commit()
        self.metrics = {"accepted": 0, "duplicates": 0, "rejected": 0}
    def ready(self):
        try: return self.db.execute("SELECT 1").fetchone()[0] == 1
        except Exception: return False
    def verify_signature(self, raw: bytes, signature: str) -> bool:
        if not self.secret or not signature: return False
        expected = hmac.new(self.secret.encode(), raw, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature.removeprefix("sha256="))
    @staticmethod
    def validate(data: dict[str, Any], correlation_id=None) -> IntakeEvent:
        if not isinstance(data, dict): raise ValueError("object payload required")
        req = ("event_id", "account_id", "email", "message")
        if any(not isinstance(data.get(k), str) or not data[k].strip() for k in req): raise ValueError("required strings missing")
        event_id, account, email, message = (data[k].strip() for k in req)
        if len(event_id)>120 or len(account)>120 or len(email)>320 or len(message)>10000: raise ValueError("field length limit exceeded")
        if "@" not in email or "." not in email.rsplit("@", 1)[-1]: raise ValueError("valid email required")
        try: amount = float(data.get("amount", 0))
        except (TypeError, ValueError) as exc: raise ValueError("amount must be numeric") from exc
        if amount < 0: raise ValueError("amount cannot be negative")
        source = data.get("source", "webhook")
        if not isinstance(source, str) or len(source) > 80: raise ValueError("invalid source")
        return IntakeEvent(event_id, account, email.lower(), message, source, amount, correlation_id or f"corr-{event_id}")
    def classify(self, event):
        if self.classifier:
            candidate = self.classifier(event.message)
            if candidate in ALLOWED_INTENTS: return candidate
        text = event.message.lower()
        if any(x in text for x in ("invoice", "charge", "refund", "payment")): return "billing"
        if any(x in text for x in ("error", "broken", "help", "issue")): return "support"
        if event.amount >= 1000 or any(x in text for x in ("quote", "demo", "buy", "pricing")): return "sales"
        return "unknown"
    def _audit(self, event_id, action, detail):
        self.db.execute("INSERT INTO audit(event_id,action,detail,created_at) VALUES(?,?,?,?)", (event_id, action, json.dumps(detail, sort_keys=True), time.time()))
    def _tx(self):
        class Tx:
            def __enter__(_, *a): self.db.lock.acquire(); return self.db
            def __exit__(_, typ, val, tb):
                try:
                    self.db.commit() if typ is None else self.db.rollback()
                finally: self.db.lock.release()
        return Tx()
    def ingest(self, data, correlation_id=None, authenticated=True):
        if not authenticated: self.metrics["rejected"] += 1; raise PermissionError("invalid webhook signature")
        event = self.validate(data, correlation_id); intent = self.classify(event)
        approval = intent in {"billing", "unknown"} or event.amount >= 5000
        action = "sync_crm" if intent in {"sales", "support"} else "queue_human_review"
        decision = Decision(event.event_id, "accepted", intent, action, approval, 0, event.correlation_id)
        now = time.time()
        try:
            with self._tx():
                self.db.execute("INSERT INTO events(event_id,payload,status,created_at,updated_at) VALUES(?,?,?,?,?)", (event.event_id, json.dumps(asdict(event)), "received", now, now))
                self.db.execute("INSERT INTO decisions(event_id,body,created_at,updated_at) VALUES(?,?,?,?)", (event.event_id, json.dumps(asdict(decision)), now, now))
                if approval: self.db.execute("INSERT INTO approvals(event_id,state,reviewer,created_at,updated_at) VALUES(?,?,?,?,?)", (event.event_id, "pending", None, now, now))
                self._audit(event.event_id, "decision", asdict(decision))
        except Exception as exc:
            if exc.__class__.__name__ not in {"IntegrityError", "UniqueViolation"}: raise
            self.metrics["duplicates"] += 1
            row = self.db.execute("SELECT body FROM decisions WHERE event_id=?", (event.event_id,)).fetchone()
            if row: return Decision(**json.loads(row[0]))
            raise RuntimeError("event is processing; retry later")
        if action == "sync_crm" and self.crm_sync: return self._deliver(event, decision, "delivery")
        with self._tx(): self.db.execute("UPDATE events SET status=?,updated_at=? WHERE event_id=?", (action, time.time(), event.event_id))
        self.metrics["accepted"] += 1; return decision
    def _deliver(self, event, decision, phase):
        last = None
        for attempt in range(1, 4):
            try:
                self.crm_sync(asdict(event), asdict(decision))
                result = Decision(**{**asdict(decision), "status": "completed" if phase == "recovery" else "accepted", "attempt": attempt})
                with self._tx():
                    self.db.execute("INSERT INTO retry_attempts(event_id,phase,attempt,outcome,error,created_at) VALUES(?,?,?,?,?,?)", (event.event_id, phase, attempt, "succeeded", None, time.time()))
                    self.db.execute("UPDATE events SET status=?,updated_at=? WHERE event_id=?", (result.status, time.time(), event.event_id))
                    self.db.execute("UPDATE decisions SET body=?,updated_at=? WHERE event_id=?", (json.dumps(asdict(result)), time.time(), event.event_id)); self._audit(event.event_id, "connector_succeeded", {"attempt": attempt, "phase": phase})
                self.metrics["accepted"] += 1; return result
            except TRANSIENT as exc:
                last = exc
                with self._tx():
                    self.db.execute("INSERT INTO retry_attempts(event_id,phase,attempt,outcome,error,created_at) VALUES(?,?,?,?,?,?)", (event.event_id, phase, attempt, "failed", str(exc), time.time())); self._audit(event.event_id, "connector_attempt_failed", {"attempt": attempt, "phase": phase, "error": str(exc)})
                if attempt < 3: self.retry_sleep(0.05 * 2 ** (attempt - 1))
        failed = Decision(**{**asdict(decision), "status": "dead_letter", "attempt": 3}); now = time.time()
        with self._tx():
            self.db.execute("INSERT INTO dead_letters(event_id,error,state,created_at,updated_at) VALUES(?,?,?,?,?)", (event.event_id, str(last), "active", now, now)); self.db.execute("UPDATE events SET status='dead_letter',updated_at=? WHERE event_id=?", (now, event.event_id)); self.db.execute("UPDATE decisions SET body=?,updated_at=? WHERE event_id=?", (json.dumps(asdict(failed)), now, event.event_id)); self._audit(event.event_id, "dead_letter", {"error": str(last)})
        self.metrics["accepted"] += 1; return failed
    def dead_letter(self, event_id, error):
        now = time.time()
        with self._tx():
            self.db.execute("INSERT INTO dead_letters(event_id,error,state,created_at,updated_at) VALUES(?,?,?,?,?)", (event_id, error, "active", now, now))
            self.db.execute("UPDATE events SET status='dead_letter',updated_at=? WHERE event_id=?", (now, event_id))
            self._audit(event_id, "dead_letter", {"error": error})
    def event_detail(self, event_id):
        row = self.db.execute("SELECT e.event_id,e.payload,e.status,e.created_at,e.updated_at,d.body FROM events e LEFT JOIN decisions d ON d.event_id=e.event_id WHERE e.event_id=?", (event_id,)).fetchone()
        if not row: raise KeyError("event not found")
        attempts = self.db.execute("SELECT id,phase,attempt,outcome,error,created_at FROM retry_attempts WHERE event_id=? ORDER BY id", (event_id,)).fetchall()
        dec = json.loads(row[5]) if row[5] else None
        return {"event_id": row[0], "payload": json.loads(row[1]), "status": row[2], "created_at": row[3], "updated_at": row[4], "decision": dec, "retry_attempts": [dict(zip(("id","phase","attempt","outcome","error","created_at"), r)) for r in attempts]}
    def audit_history(self, event_id): return [{"id":r[0],"action":r[1],"detail":json.loads(r[2]),"created_at":r[3]} for r in self.db.execute("SELECT id,action,detail,created_at FROM audit WHERE event_id=? ORDER BY id", (event_id,)).fetchall()]
    def dlq(self): return [dict(zip(("id","event_id","error","state","created_at","updated_at","recovered_at"),r)) for r in self.db.execute("SELECT id,event_id,error,state,created_at,updated_at,recovered_at FROM dead_letters ORDER BY id DESC").fetchall()]
    def dlq_detail(self, dlq_id):
        row = self.db.execute("SELECT id,event_id,error,state,created_at,updated_at,recovered_at FROM dead_letters WHERE id=?", (dlq_id,)).fetchone()
        if not row: raise KeyError("dead letter not found")
        item = dict(zip(("id","event_id","error","state","created_at","updated_at","recovered_at"), row)); item["event"] = self.event_detail(item["event_id"]); return item
    def retry_dead_letter(self, dlq_id):
        item = self.dlq_detail(dlq_id)
        if item["state"] != "active": raise ValueError("dead letter is not active")
        result = self._deliver(IntakeEvent(**item["event"]["payload"]), Decision(**item["event"]["decision"]), "recovery")
        if result.status == "dead_letter": return self.dlq_detail(dlq_id)
        with self._tx(): self.db.execute("UPDATE dead_letters SET state='recovered',updated_at=?,recovered_at=? WHERE id=?", (time.time(), time.time(), dlq_id)); self._audit(item["event_id"], "dead_letter_recovered", {"dead_letter_id": dlq_id})
        return self.dlq_detail(dlq_id)
    def approvals(self): return [{"event_id":r[0],"state":r[1],"reviewer":r[2]} for r in self.db.execute("SELECT event_id,state,reviewer FROM approvals WHERE state='pending'").fetchall()]
    def set_approval(self, event_id, state, reviewer):
        if state not in {"approved", "rejected"}: raise ValueError("state must be approved or rejected")
        with self._tx():
            cur = self.db.execute("UPDATE approvals SET state=?,reviewer=?,updated_at=? WHERE event_id=? AND state='pending'", (state, reviewer, time.time(), event_id))
            if not cur.rowcount: raise KeyError("pending approval not found")
            self._audit(event_id, "approval", {"state": state, "reviewer": reviewer})
        return {"event_id": event_id, "state": state, "reviewer": reviewer}
    def snapshot(self): return {**self.metrics, "approvals_pending": self.db.execute("SELECT COUNT(*) FROM approvals WHERE state='pending'").fetchone()[0], "dead_letters": self.db.execute("SELECT COUNT(*) FROM dead_letters WHERE state='active'").fetchone()[0], "audit_entries": self.db.execute("SELECT COUNT(*) FROM audit").fetchone()[0]}

def retry_with_backoff(operation, attempts=3, sleep=time.sleep):
    last = None
    for n in range(1, attempts + 1):
        try: return operation(), n
        except TRANSIENT as exc:
            last = exc
            if n < attempts: sleep(0.05 * 2 ** (n - 1))
    raise last

try:
    from fastapi import FastAPI, Header, HTTPException, Request, WebSocket
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    from fastapi import Depends
    def create_app(plane=None):
        env = os.getenv("ENVIRONMENT", "development"); url = os.getenv("DATABASE_URL", ":memory:")
        if env == "production" and not url.startswith("postgres" + "ql://"):
            raise RuntimeError("production requires PostgreSQL")
        if plane is None:
            plane = ControlPlane(url, secret=os.getenv("WEBHOOK_SECRET", "local-only-secret"))
        token = os.getenv("OPERATOR_TOKEN", "operator-test-token")
        app = FastAPI(title="AI Operations Control Plane", version="1.0.0")
        bearer = HTTPBearer(auto_error=False)
        def operator(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)):
            if credentials is None or not hmac.compare_digest(credentials.credentials, token):
                raise HTTPException(401, "operator authentication required")
            return True
        @app.get("/health")
        def health(): return {"status":"ok","service":"ai-ops-control-plane"}
        @app.get("/ready")
        def ready():
            if not plane.ready(): raise HTTPException(503, "database unavailable")
            return {"status":"ready"}
        @app.get("/ops/metrics", dependencies=[Depends(operator)])
        def metrics(): return plane.snapshot()
        @app.post("/v1/webhooks/operations")
        async def webhook(request: Request, x_webhook_signature: str | None = Header(None), x_correlation_id: str | None = Header(None)):
            raw = await request.body()
            if not plane.verify_signature(raw, x_webhook_signature or ""): raise HTTPException(401, "invalid webhook signature")
            try: return asdict(plane.ingest(json.loads(raw), x_correlation_id))
            except json.JSONDecodeError: raise HTTPException(400, "malformed JSON")
            except ValueError as exc: raise HTTPException(422, str(exc))
        @app.post("/v1/demo/failure")
        async def demo_failure(request: Request):
            body = await request.json()
            body.setdefault("source", "public-failure-demo")
            def fail(_event, _decision): raise TimeoutError("controlled synthetic dependency failure")
            plane.crm_sync = fail
            try:
                decision = plane.ingest(body, correlation_id=f"demo-{body['event_id']}")
            except Exception as exc:
                raise HTTPException(422, str(exc))
            item = plane.dlq()[0]
            return {"event": plane.event_detail(body["event_id"]), "dlq": item, "audit": plane.audit_history(body["event_id"]), "timeline": ["received", "validated", "persisted", "workflow_started", "dependency_failed", "retrying", "retry_exhausted", "dlq"]}
        @app.post("/v1/demo/recover/{dlq_id}")
        def demo_recover(dlq_id: int):
            plane.crm_sync = lambda _event, _decision: None
            try: item = plane.retry_dead_letter(dlq_id)
            except (KeyError, ValueError) as exc: raise HTTPException(409, str(exc))
            event_id = item["event_id"]
            return {"event": plane.event_detail(event_id), "dlq": item, "audit": plane.audit_history(event_id), "timeline": ["recovery_requested", "completed"]}
        @app.get("/v1/events/{event_id}", dependencies=[Depends(operator)])
        def event(event_id):
            try: return plane.event_detail(event_id)
            except KeyError as exc: raise HTTPException(404, str(exc))
        @app.get("/v1/events/{event_id}/audit", dependencies=[Depends(operator)])
        def audit(event_id):
            try: plane.event_detail(event_id); return {"items":plane.audit_history(event_id)}
            except KeyError as exc: raise HTTPException(404, str(exc))
        @app.get("/v1/approvals/queue", dependencies=[Depends(operator)])
        def approvals(): return {"items":plane.approvals()}
        @app.post("/v1/approvals/{event_id}", dependencies=[Depends(operator)])
        def approve(event_id, body: dict[str,str]):
            try: return plane.set_approval(event_id, body.get("state",""), body.get("reviewer","operator"))
            except (ValueError,KeyError) as exc: raise HTTPException(422, str(exc))
        @app.get("/v1/dlq", dependencies=[Depends(operator)])
        def dlq(): return {"items":plane.dlq()}
        @app.get("/v1/dlq/{dlq_id}", dependencies=[Depends(operator)])
        def dlq_item(dlq_id: int):
            try: return plane.dlq_detail(dlq_id)
            except KeyError as exc: raise HTTPException(404, str(exc))
        @app.post("/v1/dlq/{dlq_id}/retry", dependencies=[Depends(operator)])
        def dlq_retry(dlq_id: int):
            try: return plane.retry_dead_letter(dlq_id)
            except (KeyError,ValueError) as exc: raise HTTPException(409, str(exc))
        @app.websocket("/ws/ops")
        async def ws_ops(ws: WebSocket): await ws.accept(); await ws.send_json(plane.snapshot()); await ws.close()
        return app
    api = create_app()
except ImportError:
    api = None
