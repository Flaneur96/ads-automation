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
import uuid
from psycopg2 import sql

def insert_client(payload: dict) -> str:
    """
    Zapisuje klienta do tabeli `clients`.
    Zwraca wygenerowany client_id.
    """
    client_id = str(uuid.uuid4())[:8]      # prosty, unikalny identyfikator
    cols = [
        "client_id", "client_name", "industry",
        "specialist_email", "google_ads_id", "meta_account_id",
        "tiktok_advertiser_id", "ga4_property_id", "gsc_property",
        "merchant_center_id"
    ]
    values = [client_id] + [payload.get(c) for c in cols[1:]]

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("INSERT INTO clients ({}) VALUES ({})")
               .format(
                   sql.SQL(",").join(map(sql.Identifier, cols)),
                   sql.SQL(",").join(sql.Placeholder() * len(cols))
               ),
            values,
        )
    return client_id
