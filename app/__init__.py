"""AI Operations Control Plane — synthetic, production-minded reference implementation."""
from __future__ import annotations
import hashlib,hmac,json,logging,os,sqlite3,threading,time
from dataclasses import asdict,dataclass
from typing import Any,Callable

log=logging.getLogger("ai_ops")
ALLOWED_INTENTS={"sales","support","billing","unknown"}

class _Postgres:
    """Small adapter keeping repository calls portable; production uses migrations."""
    def __init__(self,url):
        import psycopg
        self.conn=psycopg.connect(url)
    def execute(self,sql,params=()): return self.conn.execute(sql.replace("?","%s"),params)
    def commit(self): self.conn.commit()
    def executescript(self,sql):
        for statement in sql.split(";"):
            if statement.strip(): self.conn.execute(statement)

@dataclass(frozen=True)
class IntakeEvent:
    event_id:str; account_id:str; email:str; message:str; source:str; amount:float=0.0; correlation_id:str=""
@dataclass(frozen=True)
class Decision:
    event_id:str; status:str; intent:str; action:str; requires_approval:bool; attempt:int; correlation_id:str

class ControlPlane:
    def __init__(self,db_path=":memory:",secret=None,classifier=None,crm_sync=None):
        self.db=_Postgres(db_path) if db_path.startswith("postgresql") else sqlite3.connect(db_path,check_same_thread=False)
        self.lock=threading.RLock()
        if not db_path.startswith("postgresql"): self.db.execute("PRAGMA journal_mode=WAL")
        schema=("CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,payload TEXT NOT NULL,status TEXT NOT NULL,created_at DOUBLE PRECISION NOT NULL);CREATE TABLE IF NOT EXISTS decisions(event_id TEXT PRIMARY KEY,body TEXT NOT NULL,created_at DOUBLE PRECISION NOT NULL);CREATE TABLE IF NOT EXISTS approvals(id BIGSERIAL PRIMARY KEY,event_id TEXT NOT NULL,state TEXT NOT NULL,reviewer TEXT,created_at DOUBLE PRECISION NOT NULL);CREATE TABLE IF NOT EXISTS dead_letters(id BIGSERIAL PRIMARY KEY,event_id TEXT NOT NULL,error TEXT NOT NULL,created_at DOUBLE PRECISION NOT NULL);CREATE TABLE IF NOT EXISTS audit(id BIGSERIAL PRIMARY KEY,event_id TEXT NOT NULL,action TEXT NOT NULL,detail TEXT NOT NULL,created_at DOUBLE PRECISION NOT NULL);" if db_path.startswith("postgresql") else "CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,payload TEXT NOT NULL,status TEXT NOT NULL,created_at REAL NOT NULL);CREATE TABLE IF NOT EXISTS decisions(event_id TEXT PRIMARY KEY,body TEXT NOT NULL,created_at REAL NOT NULL);CREATE TABLE IF NOT EXISTS approvals(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL,state TEXT NOT NULL,reviewer TEXT,created_at REAL NOT NULL);CREATE TABLE IF NOT EXISTS dead_letters(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL,error TEXT NOT NULL,created_at REAL NOT NULL);CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY AUTOINCREMENT,event_id TEXT NOT NULL,action TEXT NOT NULL,detail TEXT NOT NULL,created_at REAL NOT NULL);")
        self.db.executescript(schema); self.db.commit()
        self.secret=secret or os.getenv("WEBHOOK_SECRET"); self.classifier=classifier; self.crm_sync=crm_sync; self.metrics={"accepted":0,"duplicates":0,"rejected":0,"dead_letters":0}
    def verify_signature(self,raw:bytes,signature:str)->bool:
        if not self.secret or not signature:return False
        expected=hmac.new(self.secret.encode(),raw,hashlib.sha256).hexdigest(); return hmac.compare_digest(expected,signature.removeprefix("sha256="))
    @staticmethod
    def validate(data:dict[str,Any],correlation_id=None)->IntakeEvent:
        if not isinstance(data,dict):raise ValueError("object payload required")
        required=("event_id","account_id","email","message")
        if any(not isinstance(data.get(k),str) or not data[k].strip() for k in required):raise ValueError("event_id, account_id, email and message are required strings")
        event_id,account,email,message=(data[k].strip() for k in required)
        if len(event_id)>120 or len(account)>120 or len(email)>320 or len(message)>10000:raise ValueError("field length limit exceeded")
        if "@" not in email or "." not in email.rsplit("@",1)[-1]:raise ValueError("valid email required")
        try:amount=float(data.get("amount",0))
        except (TypeError,ValueError) as exc:raise ValueError("amount must be numeric") from exc
        if amount<0:raise ValueError("amount cannot be negative")
        source=data.get("source","webhook")
        if not isinstance(source,str) or len(source)>80:raise ValueError("invalid source")
        return IntakeEvent(event_id,account,email.lower(),message,source,amount,correlation_id or f"corr-{event_id}")
    def classify(self,event):
        if self.classifier:
            candidate=self.classifier(event.message)
            if candidate in ALLOWED_INTENTS:return candidate
        text=event.message.lower()
        if any(x in text for x in ("invoice","charge","refund","payment")):return "billing"
        if any(x in text for x in ("error","broken","help","issue")):return "support"
        if event.amount>=1000 or any(x in text for x in ("quote","demo","buy","pricing")):return "sales"
        return "unknown"
    def _audit(self,event_id,action,detail):
        self.db.execute("INSERT INTO audit(event_id,action,detail,created_at) VALUES(?,?,?,?)",(event_id,action,json.dumps(detail,sort_keys=True),time.time()));self.db.commit();log.info("audit event_id=%s action=%s",event_id,action)
    def ingest(self,data,correlation_id=None,authenticated=True):
        if not authenticated:self.metrics["rejected"]+=1;raise PermissionError("invalid webhook signature")
        event=self.validate(data,correlation_id);payload=json.dumps(asdict(event),sort_keys=True)
        with self.lock:
            try:self.db.execute("INSERT INTO events VALUES(?,?,?,?)",(event.event_id,payload,"received",time.time()));self.db.commit()
            except Exception as exc:
                if not isinstance(exc,sqlite3.IntegrityError) and exc.__class__.__name__ not in {"UniqueViolation","IntegrityError"}: raise
                self.metrics["duplicates"]+=1;row=self.db.execute("SELECT body FROM decisions WHERE event_id=?",(event.event_id,)).fetchone()
                if row:return Decision(**json.loads(row[0]))
                raise RuntimeError("event is processing; retry later")
            intent=self.classify(event);approval=intent in {"billing","unknown"} or event.amount>=5000;action="sync_crm" if intent in {"sales","support"} else "queue_human_review"
            decision=Decision(event.event_id,"accepted",intent,action,approval,1,event.correlation_id)
            if action=="sync_crm" and self.crm_sync:
                try:retry_with_backoff(lambda:self.crm_sync(asdict(event),asdict(decision)))
                except (TimeoutError,ConnectionError) as exc:self.dead_letter(event.event_id,str(exc));return Decision(event.event_id,"dead_letter",intent,action,False,3,event.correlation_id)
            self.db.execute("INSERT INTO decisions VALUES(?,?,?)",(event.event_id,json.dumps(asdict(decision)),time.time()))
            if approval:self.db.execute("INSERT INTO approvals(event_id,state,reviewer,created_at) VALUES(?,?,?,?)",(event.event_id,"pending",None,time.time()))
            self.db.execute("UPDATE events SET status=? WHERE event_id=?",(action,event.event_id));self.db.commit();self.metrics["accepted"]+=1;self._audit(event.event_id,"decision",asdict(decision));return decision
    def dead_letter(self,event_id,error):
        self.db.execute("INSERT INTO dead_letters(event_id,error,created_at) VALUES(?,?,?)",(event_id,error,time.time()));self.db.execute("UPDATE events SET status=? WHERE event_id=?",("dead_letter",event_id));self.db.commit();self.metrics["dead_letters"]+=1;self._audit(event_id,"dead_letter",{"error":error})
    def approvals(self):return [{"event_id":r[0],"state":r[1],"reviewer":r[2]} for r in self.db.execute("SELECT event_id,state,reviewer FROM approvals WHERE state='pending'").fetchall()]
    def set_approval(self,event_id,state,reviewer):
        if state not in {"approved","rejected"}:raise ValueError("state must be approved or rejected")
        cur=self.db.execute("UPDATE approvals SET state=?,reviewer=? WHERE event_id=? AND state='pending'",(state,reviewer,event_id));self.db.commit()
        if not cur.rowcount:raise KeyError("pending approval not found")
        self._audit(event_id,"approval",{"state":state,"reviewer":reviewer});return {"event_id":event_id,"state":state,"reviewer":reviewer}
    def snapshot(self):return {**self.metrics,"approvals_pending":self.db.execute("SELECT COUNT(*) FROM approvals WHERE state='pending'").fetchone()[0],"dead_letters":self.db.execute("SELECT COUNT(*) FROM dead_letters").fetchone()[0],"audit_entries":self.db.execute("SELECT COUNT(*) FROM audit").fetchone()[0]}

def retry_with_backoff(operation,attempts=3,sleep=time.sleep):
    last=None
    for n in range(1,attempts+1):
        try:return operation(),n
        except (TimeoutError,ConnectionError) as exc:
            last=exc
            if n<attempts:sleep(0.05*(2**(n-1)))
    raise last

try:
 from fastapi import FastAPI,Header,HTTPException,Request,WebSocket
 api=FastAPI(title="AI Operations Control Plane",version="0.2.0",description="Synthetic B2B operations orchestration reference implementation")
 plane=ControlPlane(db_path=os.getenv("DATABASE_URL",":memory:"),secret=os.getenv("WEBHOOK_SECRET") or "local-only-secret")
 if os.getenv("ENVIRONMENT")=="production" and not os.getenv("WEBHOOK_SECRET"): raise RuntimeError("WEBHOOK_SECRET is required in production")
 @api.get("/health")
 def health():return {"status":"ok","service":"ai-ops-control-plane"}
 @api.get("/ops/metrics")
 def metrics():return plane.snapshot()
 @api.post("/v1/webhooks/operations")
 async def webhook(request:Request,x_webhook_signature:str|None=Header(default=None),x_correlation_id:str|None=Header(default=None)):
  raw=await request.body()
  if len(raw)>1_000_000:raise HTTPException(413,"payload too large")
  if not plane.verify_signature(raw,x_webhook_signature or ""):raise HTTPException(401,"invalid webhook signature")
  try:return asdict(plane.ingest(json.loads(raw),x_correlation_id,True))
  except json.JSONDecodeError:raise HTTPException(400,"malformed JSON")
  except ValueError as exc:raise HTTPException(422,str(exc))
  except RuntimeError as exc:raise HTTPException(409,str(exc))
 @api.get("/v1/approvals/queue")
 def approval_queue():return {"items":plane.approvals()}
 @api.post("/v1/approvals/{event_id}")
 def approve(event_id:str,body:dict[str,str]):
  try:return plane.set_approval(event_id,body.get("state",""),body.get("reviewer","operator"))
  except (ValueError,KeyError) as exc:raise HTTPException(422,str(exc))
 @api.websocket("/ws/ops")
 async def ws_status(ws:WebSocket):await ws.accept();await ws.send_json(plane.snapshot());await ws.close()
except ImportError:api=None
