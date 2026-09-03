"""Run the same schema contract used by the production API."""
import os
from app import Database, _schema

url = os.environ.get("DATABASE_URL")
if not url:
    raise SystemExit("DATABASE_URL is required")
db = Database(url)
try:
    db.script(_schema(db.postgres))
    db.commit()
finally:
    db.conn.close()
