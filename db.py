"""
db.py  –  pomocnicze funkcje do komunikacji z PostgreSQL (Railway)

Wymaga:
  • zmiennej środowiskowej DATABASE_URL (Railway link lub .env)
  • pakietu psycopg2-binary w requirements.txt
"""

import os
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql


# --- połączenie -------------------------------------------------------------


def get_conn():
    """
    Zwraca kontekstowe połączenie z bazą.
    Używaj:  with get_conn() as conn, conn.cursor() as cur: ...
    """
    return psycopg2.connect(os.environ["DATABASE_URL"],
                            cursor_factory=RealDictCursor)


def test_connection() -> str:
    """Proste zapytanie sprawdzające, czy baza odpowiada."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 'ok' AS status;")
        row = cur.fetchone()
        return row["status"]


# --- operacje na tabeli clients --------------------------------------------


_CLIENT_COLUMNS = [
    "client_id",
    "client_name",
    "industry",
    "specialist_email",
    "google_ads_id",
    "meta_account_id",
    "tiktok_advertiser_id",
    "ga4_property_id",
    "gsc_property",
    "merchant_center_id",
]


def insert_client(payload: dict) -> str:
    """
    Zapisuje klienta do tabeli `clients`.
    Pola wymagane: client_name, industry
    Zwraca wygenerowany client_id (8-znakowy).
    """
    client_id = str(uuid.uuid4())[:8]     # prosty unikalny ID

    values = [client_id] + [payload.get(col) for col in _CLIENT_COLUMNS[1:]]

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("INSERT INTO clients ({cols}) VALUES ({placeholders})")
               .format(
                   cols=sql.SQL(",").join(map(sql.Identifier, _CLIENT_COLUMNS)),
                   placeholders=sql.SQL(",").join(sql.Placeholder() *
                                                  len(_CLIENT_COLUMNS)),
               ),
            values,
        )

    return client_id
