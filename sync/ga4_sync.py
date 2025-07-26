"""
GA4 Sync - kopiuje dane z różnych GA4 datasets do jednej tabeli
"""
import os
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery
import json
import db

logger = logging.getLogger(__name__)

class GA4Sync:
    def __init__(self):
        self.setup_client()
        
    def setup_client(self):
        """Konfiguracja BigQuery client"""
        bq_credentials = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if bq_credentials:
            credentials_info = json.loads(bq_credentials)
            self.bq_client = bigquery.Client.from_service_account_info(credentials_info)
        else:
            self.bq_client = bigquery.Client()
            
        self.project_id = os.environ.get('BQ_PROJECT_ID')
        self.dataset_id = os.environ.get('BQ_DATASET_ID', 'ads_data')
    
    def ensure_ga4_table_exists(self):
        """Tworzy unified tabelę GA4"""
        table_id = f"{self.project_id}.{self.dataset_id}.ga4_unified_performance"
        
        schema = [
            bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("client_id", "STRING"),
            bigquery.SchemaField("client_name", "STRING"),
            bigquery.SchemaField("ga4_property_id", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("medium", "STRING"),
            bigquery.SchemaField("campaign", "STRING"),
            bigquery.SchemaField("users", "INTEGER"),
            bigquery.SchemaField("sessions", "INTEGER"),
            bigquery.SchemaField("pageviews", "INTEGER"),
            bigquery.SchemaField("conversions", "INTEGER"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("add_to_carts", "INTEGER"),
            bigquery.SchemaField("checkouts", "INTEGER"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )
        
        try:
            table = self.bq_client.create_table(table)
            logger.info(f"Created unified GA4 table: {table_id}")
        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"GA4 table already exists: {table_id}")
    
    def sync_client_ga4_data(self, client_id: str, client_name: str, ga4_property_id: str, days_back: int = 7):
        """Synchronizuje GA4 dla jednego klienta"""
        logger.info(f"Syncing GA4 for {client_name} (property: {ga4_property_id})")
        
        # Query do GA4 dataset
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
        end_date = datetime.now().strftime('%Y%m%d')
        
        query = f"""
        SELECT
          PARSE_DATE('%Y%m%d', event_date) as date,
          '{client_id}' as client_id,
          '{client_name}' as client_name,
          '{ga4_property_id}' as ga4_property_id,
          
          traffic_source.source,
          traffic_source.medium,
          IFNULL(traffic_source.name, '(not set)') as campaign,
          
          COUNT(DISTINCT user_pseudo_id) as users,
          COUNT(DISTINCT CONCAT(user_pseudo_id, 
            CAST(EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) AS STRING))) as sessions,
          COUNTIF(event_name = 'page_view') as pageviews,
          COUNTIF(event_name = 'purchase') as conversions,
          SUM(CASE WHEN event_name = 'purchase' THEN ecommerce.purchase_revenue ELSE 0 END) as revenue,
          COUNTIF(event_name = 'add_to_cart') as add_to_carts,
          COUNTIF(event_name = 'begin_checkout') as checkouts
          
        FROM `{self.project_id}.analytics_{ga4_property_id}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
          AND traffic_source.source IS NOT NULL
        GROUP BY date, source, medium, campaign
        """
        
        try:
            # Usuń stare dane dla tego klienta i okresu
            delete_query = f"""
            DELETE FROM `{self.project_id}.{self.dataset_id}.ga4_unified_performance`
            WHERE client_id = '{client_id}'
              AND date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY) 
              AND CURRENT_DATE()
            """
            self.bq_client.query(delete_query).result()
            
            # Wstaw nowe dane
            insert_query = f"""
            INSERT INTO `{self.project_id}.{self.dataset_id}.ga4_unified_performance`
            (sync_timestamp, date, client_id, client_name, ga4_property_id, 
             source, medium, campaign, users, sessions, pageviews, 
             conversions, revenue, add_to_carts, checkouts)
            
            SELECT
              CURRENT_TIMESTAMP() as sync_timestamp,
              *
            FROM ({query})
            """
            
            job = self.bq_client.query(insert_query)
            job.result()
            
            # Sprawdź ile wierszy dodano
            check_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{self.project_id}.{self.dataset_id}.ga4_unified_performance`
            WHERE client_id = '{client_id}'
              AND DATE(sync_timestamp) = CURRENT_DATE()
            """
            result = list(self.bq_client.query(check_query))[0]
            row_count = result.row_count
            
            logger.info(f"Synced {row_count} GA4 rows for {client_name}")
            return row_count
            
        except Exception as e:
            logger.error(f"Error syncing GA4 for {client_name}: {str(e)}")
            raise

def sync_all_ga4_clients():
    """Synchronizuje GA4 dla wszystkich klientów którzy mają property_id"""
    try:
        sync = GA4Sync()
        sync.ensure_ga4_table_exists()
        
        # Pobierz klientów z GA4
        clients = db.get_all_clients()
        ga4_clients = [c for c in clients if c.get('active') and c.get('ga4_property_id')]
        
        logger.info(f"Found {len(ga4_clients)} clients with GA4")
        
        total_rows = 0
        for client in ga4_clients:
            try:
                rows = sync.sync_client_ga4_data(
                    client['client_id'],
                    client['client_name'],
                    client['ga4_property_id'],
                    days_back=30
                )
                total_rows += rows
            except Exception as e:
                logger.error(f"Failed GA4 sync for {client['client_name']}: {e}")
        
        return {"success": True, "total_rows": total_rows, "clients": len(ga4_clients)}
        
    except Exception as e:
        logger.error(f"GA4 sync failed: {e}")
        raise
