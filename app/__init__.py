"""AI Operations Control Plane — synthetic, production-minded reference implementation."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Callable

log = logging.getLogger("ai_ops")

ALLOWED_INTENTS = {"sales", "support", "billing", "unknown"}

@dataclass(frozen=True)
class IntakeEvent:
    event_id: str
    account_id: str
    email: str
    message: str
    source: str
    amount: float = 0.0
    correlation_id: str = ""

@dataclass(frozen=True)
class Decision:
    event_id: str
    status: str
    intent: str
    action: str
    requires_approval: bool
    attempt: int
    correlation_id: str

class ControlPlane:
    """Deterministic orchestration core; persistence can be SQLite locally or Postgres in deployment."""
    def __init__(self, db_path: str = ":memory:", secret: str = "synthetic-webhook-secret", classifier: Callable[[str], str] | None = None):
        self.db = sqlite3.connect(db_path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS decisions(event_id TEXT PRIMARY KEY, body TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS approvals(id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, state TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dead_letters(id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, error TEXT NOT NULL, created_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, action TEXT NOT NULL, detail TEXT NOT NULL, created_at REAL NOT NULL);
        """)
        self.db.commit(); self.secret = secret; self.classifier = classifier; self.metrics = {"accepted":0,"duplicates":0,"rejected":0,"dead_letters":0}

    def verify_signature(self, raw: bytes, signature: str) -> bool:
        expected = hmac.new(self.secret.encode(), raw, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature.removeprefix("sha256="))

    @staticmethod
    def validate(data: dict[str, Any], correlation_id: str | None = None) -> IntakeEvent:
        if not isinstance(data, dict): raise ValueError("object payload required")
        event_id, account, email, message = (str(data.get(k," ")).strip() for k in ("event_id","account_id","email","message"))
        if not event_id or event_id == " " or len(event_id)>120: raise ValueError("event_id required")
        if not account or account == " ": raise ValueError("account_id required")
        if "@" not in email or "." not in email.rsplit("@",1)[-1]: raise ValueError("valid email required")
        if not message or message == " " or len(message)>10000: raise ValueError("message required")
        try: amount=float(data.get("amount",0))
        except (TypeError,ValueError) as exc: raise ValueError("amount must be numeric") from exc
        if amount < 0: raise ValueError("amount cannot be negative")
        return IntakeEvent(event_id,account,email.lower(),message,str(data.get("source","webhook")),amount,correlation_id or f"corr-{event_id}")

    def classify(self, event: IntakeEvent) -> str:
        if self.classifier:
            candidate = self.classifier(event.message)
            if candidate in ALLOWED_INTENTS: return candidate
        text=event.message.lower()
        if any(x in text for x in ("invoice","charge","refund","payment")): return "billing"
        if any(x in text for x in ("error","broken","help","issue")): return "support"
        if event.amount >= 1000 or any(x in text for x in ("quote","demo","buy","pricing")): return "sales"
        return "unknown"

    def _audit(self,event_id:str,action:str,detail:dict[str,Any]):
        self.db.execute("INSERT INTO audit(event_id,action,detail,created_at) VALUES(?,?,?,?)",(event_id,action,json.dumps(detail,sort_keys=True),time.time()))
        self.db.commit(); log.info("audit event_id=%s action=%s detail=%s",event_id,action,detail)

    def ingest(self, data: dict[str,Any], correlation_id: str|None=None, authenticated: bool=True) -> Decision:
        if not authenticated: self.metrics["rejected"]+=1; raise PermissionError("invalid webhook signature")
        event=self.validate(data,correlation_id); payload=json.dumps(asdict(event),sort_keys=True)
        try:
            self.db.execute("INSERT INTO events VALUES(?,?,?,?)",(event.event_id,payload,"received",time.time())); self.db.commit()
        except sqlite3.IntegrityError:
            self.metrics["duplicates"]+=1; row=self.db.execute("SELECT body FROM decisions WHERE event_id=?",(event.event_id,)).fetchone()
            if row: return Decision(**json.loads(row[0]))
            return Decision(event.event_id,"duplicate","unknown","no-op",False,1,event.correlation_id)
        intent=self.classify(event); approval=intent in {"billing","unknown"} or event.amount>=5000
        action="sync_crm" if intent in {"sales","support"} else "queue_human_review"
        decision=Decision(event.event_id,"accepted",intent,action,approval,1,event.correlation_id)
        self.db.execute("INSERT INTO decisions VALUES(?,?,?)",(event.event_id,json.dumps(asdict(decision)),time.time()))
        if approval: self.db.execute("INSERT INTO approvals(event_id,state,created_at) VALUES(?,?,?)",(event.event_id,"pending",time.time()))
        self.db.execute("UPDATE events SET status=? WHERE event_id=?",(action,event.event_id)); self.db.commit()
        self.metrics["accepted"]+=1; self._audit(event.event_id,"decision",asdict(decision)); return decision

    def dead_letter(self,event_id:str,error:str):
        self.db.execute("INSERT INTO dead_letters(event_id,error,created_at) VALUES(?,?,?)",(event_id,error,time.time())); self.db.commit(); self.metrics["dead_letters"]+=1; self._audit(event_id,"dead_letter",{"error":error})

    def snapshot(self):
        return {**self.metrics,"approvals_pending":self.db.execute("SELECT COUNT(*) FROM approvals WHERE state='pending'").fetchone()[0],"dead_letters":self.db.execute("SELECT COUNT(*) FROM dead_letters").fetchone()[0],"audit_entries":self.db.execute("SELECT COUNT(*) FROM audit").fetchone()[0]}


def retry_with_backoff(operation: Callable[[], Any], attempts: int=3, sleep: Callable[[float],None]=time.sleep):
    last=None
    for n in range(1,attempts+1):
        try: return operation(), n
        except (TimeoutError,ConnectionError) as exc:
            last=exc
            if n<attempts: sleep(0.05*(2**(n-1)))
    raise last

try:
    from fastapi import FastAPI, Header, HTTPException, WebSocket
    api=FastAPI(title="AI Operations Control Plane",version="0.1.0",description="Synthetic B2B operations orchestration reference implementation")
    plane=ControlPlane()
    @api.get("/health")
    def health(): return {"status":"ok","service":"ai-ops-control-plane"}
    @api.get("/ops/metrics")
    def metrics(): return plane.snapshot()
    @api.post("/v1/webhooks/operations")
    def webhook(payload:dict[str,Any], x_webhook_signature:str|None=Header(default=None), x_correlation_id:str|None=Header(default=None)):
        try:
            auth=bool(x_webhook_signature) and plane.verify_signature(json.dumps(payload,separators=(",",":"),sort_keys=True).encode(),x_webhook_signature)
            return asdict(plane.ingest(payload,x_correlation_id,auth))
        except PermissionError as exc: raise HTTPException(401,str(exc))
        except ValueError as exc: raise HTTPException(422,str(exc))
    @api.websocket("/ws/ops")
    async def ws_status(ws:WebSocket):
        await ws.accept(); await ws.send_json(plane.snapshot()); await ws.close()
except ImportError:
    api=None
