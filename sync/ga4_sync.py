"""
Google Analytics 4 to BigQuery sync
"""
import os
import logging
from datetime import datetime, timedelta
import json
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.cloud import bigquery
import pandas as pd

logger = logging.getLogger(__name__)

class GA4Sync:
    def __init__(self):
        self.setup_clients()
        
    def setup_clients(self):
        """Konfiguracja GA4 i BigQuery ze zmiennych środowiskowych"""
        try:
            # GA4 - używa tego samego service account co BigQuery
            credentials_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                self.ga4_client = BetaAnalyticsDataClient.from_service_account_info(
                    credentials_info
                )
            else:
                self.ga4_client = BetaAnalyticsDataClient()
            
            logger.info("GA4 client initialized")
            
            # BigQuery
            if credentials_json:
                self.bq_client = bigquery.Client.from_service_account_info(
                    json.loads(credentials_json)
                )
            else:
                self.bq_client = bigquery.Client()
                
            self.project_id = os.environ.get('BQ_PROJECT_ID')
            self.dataset_id = os.environ.get('BQ_DATASET_ID', 'ads_data')
            
            logger.info(f"BigQuery configured: {self.project_id}.{self.dataset_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            raise
    
    def ensure_table_exists(self):
        """Tworzy tabelę ga4_performance jeśli nie istnieje"""
        table_id = f"{self.project_id}.{self.dataset_id}.ga4_performance"
        
        schema = [
            bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("property_id", "STRING"),
            bigquery.SchemaField("property_name", "STRING"),
            # Źródła ruchu
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("medium", "STRING"),
            bigquery.SchemaField("campaign", "STRING"),
            # Metryki sesji
            bigquery.SchemaField("sessions", "INTEGER"),
            bigquery.SchemaField("users", "INTEGER"),
            bigquery.SchemaField("new_users", "INTEGER"),
            bigquery.SchemaField("pageviews", "INTEGER"),
            bigquery.SchemaField("bounce_rate", "FLOAT"),
            bigquery.SchemaField("avg_session_duration", "FLOAT"),
            bigquery.SchemaField("pages_per_session", "FLOAT"),
            # E-commerce
            bigquery.SchemaField("transactions", "INTEGER"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("ecommerce_conversion_rate", "FLOAT"),
            bigquery.SchemaField("avg_order_value", "FLOAT"),
            # Goals/Events
            bigquery.SchemaField("conversions", "INTEGER"),
            bigquery.SchemaField("conversion_rate", "FLOAT"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )
        
        try:
            table = self.bq_client.create_table(table)
            logger.info(f"Created table {table_id}")
        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Table {table_id} already exists")
    
    def get_property_data(self, property_id: str, property_name: str, days_back: int = 30):
        """Pobiera dane z GA4 dla jednej property"""
        
        logger.info(f"Fetching GA4 data for {property_name} ({property_id})")
        
        # Daty
        end_date = datetime.now() - timedelta(days=1)  # GA4 ma delay
        start_date = end_date - timedelta(days=days_back)
        
        # Request do GA4 API
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
                Dimension(name="sessionCampaignName"),
            ],
            metrics=[
                # Sesje
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="newUsers"),
                Metric(name="screenPageViews"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration"),
                # E-commerce
                Metric(name="ecommercePurchases"),
                Metric(name="purchaseRevenue"),
                # Events
                Metric(name="conversions"),
            ],
            date_ranges=[DateRange(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )],
        )
        
        # Wykonaj request
        response = self.ga4_client.run_report(request)
        
        # Przetwórz dane
        rows = []
        for row in response.rows:
            # Pobierz wartości
            dimensions = [d.value for d in row.dimension_values]
            metrics = [m.value for m in row.metric_values]
            
            # Obliczenia
            sessions = int(metrics[0])
            users = int(metrics[1])
            pageviews = int(metrics[3])
            transactions = int(metrics[6])
            revenue = float(metrics[7])
            conversions = int(metrics[8])
            
            rows.append({
                'sync_timestamp': datetime.now(),
                'date': datetime.strptime(dimensions[0], '%Y%m%d').date(),
                'property_id': property_id,
                'property_name': property_name,
                'source': dimensions[1],
                'medium': dimensions[2],
                'campaign': dimensions[3] if dimensions[3] != '(not set)' else None,
                'sessions': sessions,
                'users': users,
                'new_users': int(metrics[2]),
                'pageviews': pageviews,
                'bounce_rate': float(metrics[4]),
                'avg_session_duration': float(metrics[5]),
                'pages_per_session': pageviews / sessions if sessions > 0 else 0,
                'transactions': transactions,
                'revenue': round(revenue, 2),
                'ecommerce_conversion_rate': (transactions / sessions * 100) if sessions > 0 else 0,
                'avg_order_value': revenue / transactions if transactions > 0 else 0,
                'conversions': conversions,
                'conversion_rate': (conversions / sessions * 100) if sessions > 0 else 0,
            })
        
        return rows
    
    def sync_property_data(self, property_id: str, property_name: str, days_back: int = 30):
        """Synchronizuje dane jednej property GA4"""
        
        try:
            # Pobierz dane
            rows = self.get_property_data(property_id, property_name, days_back)
            
            if not rows:
                logger.warning(f"No data for {property_name}")
                return 0
            
            # Zapisz do BigQuery
            df = pd.DataFrame(rows)
            table_id = f"{self.project_id}.{self.dataset_id}.ga4_performance"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
            )
            
            job = self.bq_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()
            
            logger.info(f"Loaded {len(rows)} rows for {property_name}")
            return len(rows)
            
        except Exception as e:
            logger.error(f"Error syncing {property_name}: {str(e)}")
            raise

def sync_all_ga4_properties():
    """Synchronizuje wszystkie property GA4"""
    import db
    
    try:
        sync = GA4Sync()
        sync.ensure_table_exists()
        
        clients = db.get_all_clients()
        # Potrzebujemy nowego pola w bazie: ga4_property_id
        active_clients = [c for c in clients if c.get('active') and c.get('ga4_property_id')]
        
        logger.info(f"Starting GA4 sync for {len(active_clients)} clients")
        
        summary = {
            'total_clients': len(active_clients),
            'successful': 0,
            'failed': 0,
            'total_rows': 0
        }
        
        for client in active_clients:
            try:
                rows = sync.sync_property_data(
                    client['ga4_property_id'],
                    client['client_name'],
                    days_back=30
                )
                summary['successful'] += 1
                summary['total_rows'] += rows
            except Exception as e:
                summary['failed'] += 1
                logger.error(f"Failed {client['client_name']}: {e}")
        
        logger.info(f"GA4 sync completed: {summary}")
        return summary
        
    except Exception as e:
        logger.error(f"GA4 sync failed: {e}")
        raise
