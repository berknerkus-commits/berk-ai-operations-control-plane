import hashlib,hmac,json
from fastapi.testclient import TestClient
from app import api

client=TestClient(api)
def signed(body):
    raw=json.dumps(body,separators=(",",":"),sort_keys=True).encode()
    return raw,hmac.new(b"local-only-secret",raw,hashlib.sha256).hexdigest()

def test_http_webhook_and_duplicate():
    body={"event_id":"http-1","account_id":"acct","email":"x@example.com","message":"please send a quote","amount":100}
    raw,sig=signed(body)
    r=client.post("/v1/webhooks/operations",content=raw,headers={"X-Webhook-Signature":sig})
    assert r.status_code==200 and r.json()["intent"]=="sales"
    r2=client.post("/v1/webhooks/operations",content=raw,headers={"X-Webhook-Signature":sig})
    assert r2.status_code==200 and r2.json()["event_id"]=="http-1"

def test_http_rejects_bad_signature_and_malformed_json():
    assert client.post("/v1/webhooks/operations",content=b"{}",headers={"X-Webhook-Signature":"bad"}).status_code==401
    raw,sig=signed({"event_id":"bad-json"})
    assert client.post("/v1/webhooks/operations",content=b"{",headers={"X-Webhook-Signature":sig}).status_code==401

def test_approval_api():
    body={"event_id":"http-billing","account_id":"acct","email":"x@example.com","message":"refund invoice"}
    raw,sig=signed(body); assert client.post("/v1/webhooks/operations",content=raw,headers={"X-Webhook-Signature":sig}).status_code==200
    assert client.get("/v1/approvals/queue").json()["items"]
    assert client.post("/v1/approvals/http-billing",json={"state":"approved","reviewer":"synthetic-operator"}).status_code==200
