"""
db.py - moduł obsługi bazy danych PostgreSQL
"""

import os
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
import logging
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# --- POŁĄCZENIE Z BAZĄ ---

def get_conn():
    """
    Tworzy i zwraca połączenie z bazą danych.
    Używa zmiennej środowiskowej DATABASE_URL z Railway.
    """
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL nie jest ustawiony!")
        
        return psycopg2.connect(database_url, cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def test_connection() -> str:
    """Testuje połączenie z bazą danych"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT version();")
            result = cur.fetchone()
            logger.info("Database connection successful")
            return "ok"
    except Exception as e:
        logger.error(f"Database test failed: {str(e)}")
        raise

# --- OPERACJE NA KLIENTACH ---

def insert_client(data: dict) -> str:
    """
    Dodaje nowego klienta do bazy danych.
    
    Args:
        data: Słownik z danymi klienta
        
    Returns:
        client_id: Wygenerowany identyfikator klienta
    """
    # Walidacja wymaganych pól
    if not data.get('client_name'):
        raise ValueError("Pole 'client_name' jest wymagane")
    if not data.get('industry'):
        raise ValueError("Pole 'industry' jest wymagane")
    
    # Generowanie unikalnego ID
    client_id = f"client_{uuid.uuid4().hex[:8]}"
    
    # Przygotowanie danych do wstawienia
    insert_data = {
        'client_id': client_id,
        'client_name': data.get('client_name'),
        'industry': data.get('industry'),
        'specialist_email': data.get('specialist_email'),
        'google_ads_id': data.get('google_ads_id'),
        'meta_account_id': data.get('meta_account_id'),
        'tiktok_advertiser_id': data.get('tiktok_advertiser_id'),
        'ga4_property_id': data.get('ga4_property_id'),
        'gsc_property': data.get('gsc_property'),
        'merchant_center_id': data.get('merchant_center_id'),
        'active': True
    }
    
    # Usuń pola z wartością None
    insert_data = {k: v for k, v in insert_data.items() if v is not None}
    
    columns = list(insert_data.keys())
    values = list(insert_data.values())
    
    try:
        with get_conn() as conn, conn.cursor() as cur:
            # Budowanie zapytania SQL
            query = sql.SQL("INSERT INTO clients ({}) VALUES ({}) RETURNING client_id").format(
                sql.SQL(',').join(map(sql.Identifier, columns)),
                sql.SQL(',').join(sql.Placeholder() * len(columns))
            )
            
            cur.execute(query, values)
            result = cur.fetchone()
            conn.commit()
            
            logger.info(f"Client {client_id} added successfully")
            return result['client_id']
            
    except psycopg2.IntegrityError as e:
        logger.error(f"Integrity error: {str(e)}")
        raise ValueError("Klient o takiej nazwie lub ID już istnieje")
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise

def get_all_clients() -> List[Dict]:
    """Pobiera listę wszystkich aktywnych klientów"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    client_id,
                    client_name,
                    industry,
                    specialist_email,
                    google_ads_id,       
                    meta_account_id,
                    tiktok_advertiser_id,
                    ga4_property_id,  
                    active,
                    created_at,
                    updated_at
                FROM clients
                WHERE active = TRUE
                ORDER BY created_at DESC
            """)
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Error fetching clients: {str(e)}")
        raise

def get_client_by_id(client_id: str) -> Optional[Dict]:
    """Pobiera dane pojedynczego klienta"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM clients WHERE client_id = %s",
                (client_id,)
            )
            return cur.fetchone()
    except Exception as e:
        logger.error(f"Error fetching client {client_id}: {str(e)}")
        raise

def update_client(client_id: str, data: dict) -> bool:
    """Aktualizuje dane klienta"""
    # Usuń pola których nie można aktualizować
    data.pop('client_id', None)
    data.pop('created_at', None)
    
    if not data:
        raise ValueError("Brak danych do aktualizacji")
    
    # Dodaj timestamp aktualizacji
    data['updated_at'] = datetime.now()
    
    columns = list(data.keys())
    values = list(data.values())
    values.append(client_id)  # dla WHERE clause
    
    try:
        with get_conn() as conn, conn.cursor() as cur:
            query = sql.SQL("UPDATE clients SET {} WHERE client_id = %s").format(
                sql.SQL(',').join(
                    sql.SQL("{} = {}").format(sql.Identifier(col), sql.Placeholder())
                    for col in columns
                )
            )
            
            cur.execute(query, values)
            conn.commit()
            
            return cur.rowcount > 0
    except Exception as e:
        logger.error(f"Error updating client {client_id}: {str(e)}")
        raise

# --- OPERACJE NA ALERTACH ---

def insert_alert(alert_data: dict) -> int:
    """Dodaje nowy alert do bazy"""
    required_fields = ['client_id', 'alert_type', 'severity', 'metric_name', 'recommendation']
    
    for field in required_fields:
        if not alert_data.get(field):
            raise ValueError(f"Pole '{field}' jest wymagane")
    
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alerts 
                (client_id, alert_type, severity, metric_name, 
                 current_value, threshold_value, recommendation)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                alert_data['client_id'],
                alert_data['alert_type'],
                alert_data['severity'],
                alert_data['metric_name'],
                alert_data.get('current_value'),
                alert_data.get('threshold_value'),
                alert_data['recommendation']
            ))
            
            result = cur.fetchone()
            conn.commit()
            return result['id']
    except Exception as e:
        logger.error(f"Error inserting alert: {str(e)}")
        raise

def get_unresolved_alerts() -> List[Dict]:
    """Pobiera wszystkie nierozwiązane alerty"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    a.*,
                    c.client_name,
                    c.specialist_email
                FROM alerts a
                JOIN clients c ON a.client_id = c.client_id
                WHERE a.resolved = FALSE
                ORDER BY 
                    CASE a.severity 
                        WHEN 'high' THEN 1 
                        WHEN 'medium' THEN 2 
                        ELSE 3 
                    END,
                    a.created_at DESC
            """)
            return cur.fetchall()
    except Exception as e:
        logger.error(f"Error fetching alerts: {str(e)}")
        raise

def mark_alert_resolved(alert_id: int) -> bool:
    """Oznacza alert jako rozwiązany"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                UPDATE alerts 
                SET resolved = TRUE, resolved_at = NOW()
                WHERE id = %s
            """, (alert_id,))
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        logger.error(f"Error resolving alert {alert_id}: {str(e)}")
        raise
