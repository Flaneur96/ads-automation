# db.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)

def test_connection() -> str:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 'ok' AS status;")
        row = cur.fetchone()
        return row["status"]
