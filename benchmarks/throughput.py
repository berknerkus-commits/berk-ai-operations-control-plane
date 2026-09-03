"""Synthetic local benchmark; output is not a production capacity claim."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ControlPlane

p=ControlPlane(); n=1000; start=time.perf_counter()
for i in range(n): p.ingest({"event_id":f"bench-{i}","account_id":"synthetic","email":"a@example.com","message":"request a quote","amount":100})
elapsed=time.perf_counter()-start
print(f"synthetic_events={n} elapsed_seconds={elapsed:.4f} events_per_second={n/elapsed:.1f}")
print("NOTE: in-memory SQLite, one process, synthetic workload; not a production benchmark")
