import hashlib, hmac, json
import pytest
from app import ControlPlane, retry_with_backoff


def payload(**overrides):
    data={"event_id":"evt-001","account_id":"acct-7","email":"buyer@example.com","message":"Please send a quote for the enterprise plan","source":"webhook","amount":1200}
    return {**data,**overrides}


def test_intake_classifies_and_persists_audit():
    p=ControlPlane()
    d=p.ingest(payload())
    assert (d.status,d.intent,d.action)==("accepted","sales","sync_crm")
    assert p.snapshot()["audit_entries"]==1


def test_signature_is_constant_time_compatible():
    p=ControlPlane(secret="s")
    raw=b'{"x":1}'; sig=hmac.new(b"s",raw,hashlib.sha256).hexdigest()
    assert p.verify_signature(raw,sig) and not p.verify_signature(raw,"bad")


def test_duplicate_cannot_create_second_decision():
    p=ControlPlane(); first=p.ingest(payload()); second=p.ingest(payload(message="refund this",amount=0))
    assert second == first and p.snapshot()["duplicates"]==1


def test_invalid_and_unauthenticated_boundaries():
    p=ControlPlane()
    with pytest.raises(PermissionError): p.ingest(payload(),authenticated=False)
    with pytest.raises(ValueError): p.ingest(payload(email="bad"))


def test_billing_and_unknown_go_to_human_approval():
    p=ControlPlane()
    assert p.ingest(payload(event_id="evt-2",message="I need a refund for an invoice")).requires_approval
    assert p.ingest(payload(event_id="evt-3",message="hello",amount=0)).action=="queue_human_review"
    assert p.snapshot()["approvals_pending"]==2


def test_llm_output_is_schema_bounded():
    p=ControlPlane(classifier=lambda _: "not-a-valid-intent")
    assert p.ingest(payload()).intent=="sales"
    p2=ControlPlane(classifier=lambda _: "support")
    assert p2.ingest(payload()).intent=="support"


def test_retry_and_dead_letter_after_exhaustion():
    calls=[]
    def fail(): calls.append(1); raise TimeoutError("synthetic downstream timeout")
    with pytest.raises(TimeoutError): retry_with_backoff(fail,attempts=3,sleep=lambda _:None)
    assert len(calls)==3
    p=ControlPlane(); p.dead_letter("evt-x","CRM unavailable")
    assert p.snapshot()["dead_letters"]==1
