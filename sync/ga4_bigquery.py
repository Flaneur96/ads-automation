"""
GA4 data processing - używa BigQuery Export zamiast GA4 API
"""
import os
import logging
from datetime import datetime, timedelta
import json
from google.cloud import bigquery
import pandas as pd

logger = logging.getLogger(__name__)

class GA4BigQuerySync:
    def __init__(self):
        self.setup_client()
        
    def setup_client(self):
        """Konfiguracja BigQuery"""
        try:
            bq_credentials = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
            if bq_credentials:
                credentials_info = json.loads(bq_credentials)
                self.bq_client = bigquery.Client.from_service_account_info(
                    credentials_info
                )
            else:
                self.bq_client = bigquery.Client()
                
            self.project_id = os.environ.get('BQ_PROJECT_ID')
            self.dataset_id = os.environ.get('BQ_DATASET_ID', 'ads_data')
            
            logger.info(f"BigQuery configured: {self.project_id}.{self.dataset_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            raise
    
    def find_ga4_datasets(self):
        """Znajduje wszystkie datasety GA4 w projekcie"""
        datasets = []
        
        for dataset in self.bq_client.list_datasets():
            dataset_id = dataset.dataset_id
            # GA4 datasets zaczynają się od 'analytics_'
            if dataset_id.startswith('analytics_'):
                datasets.append(dataset_id)
                
        logger.info(f"Found GA4 datasets: {datasets}")
        return datasets
    
    def get_ga4_data(self, ga4_property_id: str, property_name: str, days_back: int = 7):
        """Pobiera dane GA4 z BigQuery Export"""
        
        # Znajdź odpowiedni dataset dla tej property
        ga4_datasets = self.find_ga4_datasets()
        
        if not ga4_datasets:
            logger.warning("No GA4 datasets found - check if BigQuery Export is enabled")
            return []
        
        # Użyj pierwszego znalezionego datasetu (można rozszerzyć logic)
        ga4_dataset = ga4_datasets[0]
        
        # Zapytanie do GA4 data
        end_date = datetime.now() - timedelta(days=1)  # GA4 ma 1-day delay
        start_date = end_date - timedelta(days=days_back)
        
        query = f"""
        SELECT
          PARSE_DATE('%Y%m%d', event_date) as date,
          '{property_name}' as property_name,
          '{ga4_property_id}' as property_id,
          traffic_source.source as source,
          traffic_source.medium as medium,
          traffic_source.name as campaign,
          
          -- Sesje (przybliżone - GA4 nie ma bezpośrednio sessions)
          COUNT(DISTINCT CONCAT(user_pseudo_id, 
                               CAST(EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp)) AS STRING))) as sessions,
          
          -- Users
          COUNT(DISTINCT user_pseudo_id) as users,
          
          -- Events
          COUNT(*) as total_events,
          COUNTIF(event_name = 'page_view') as pageviews,
          COUNTIF(event_name = 'scroll') as scrolls,
          
          -- E-commerce
          COUNTIF(event_name = 'purchase') as purchases,
          SUM(CASE WHEN event_name = 'purchase' 
                   THEN ecommerce.purchase_revenue 
                   ELSE 0 END) as revenue,
          
          -- Custom events
          COUNTIF(event_name = 'form_submit') as form_submissions,
          COUNTIF(event_name = 'click') as clicks
          
        FROM `{self.project_id}.{ga4_dataset}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{start_date.strftime('%Y%m%d')}'
          AND '{end_date.strftime('%Y%m%d')}'
          AND traffic_source.source IS NOT NULL
        GROUP BY 1,2,3,4,5,6
        ORDER BY date DESC
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            rows = []
            for row in results:
                # Obliczenia
                sessions = row.sessions
                pageviews = row.pageviews
                revenue = row.revenue or 0
                purchases = row.purchases
                
                rows.append({
                    'sync_timestamp': datetime.now(),
                    'date': row.date,
                    'property_id': ga4_property_id,
                    'property_name': property_name,
                    'source': row.source or '(direct)',
                    'medium': row.medium or '(none)',
                    'campaign': row.campaign or '(not set)',
                    'sessions': sessions,
                    'users': row.users,
                    'pageviews': pageviews,
                    'total_events': row.total_events,
                    'purchases': purchases,
                    'revenue': round(revenue, 2),
                    'conversion_rate': (purchases / sessions * 100) if sessions > 0 else 0,
                    'pages_per_session': (pageviews / sessions) if sessions > 0 else 0,
                    'form_submissions': row.form_submissions,
                    'clicks': row.clicks
                })
            
            logger.info(f"Processed {len(rows)} rows from GA4 BigQuery export")
            return rows
            
        except Exception as e:
            logger.error(f"Error querying GA4 data: {str(e)}")
            raise
    
    def ensure_table_exists(self):
        """Tworzy tabelę ga4_performance jeśli nie istnieje"""
        table_id = f"{self.project_id}.{self.dataset_id}.ga4_performance"
        
        schema = [
            bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("property_id", "STRING"),
            bigquery.SchemaField("property_name", "STRING"),
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("medium", "STRING"),
            bigquery.SchemaField("campaign", "STRING"),
            bigquery.SchemaField("sessions", "INTEGER"),
            bigquery.SchemaField("users", "INTEGER"),
            bigquery.SchemaField("pageviews", "INTEGER"),
            bigquery.SchemaField("total_events", "INTEGER"),
            bigquery.SchemaField("purchases", "INTEGER"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("conversion_rate", "FLOAT"),
            bigquery.SchemaField("pages_per_session", "FLOAT"),
            bigquery.SchemaField("form_submissions", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
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
    
    def sync_property_data(self, property_id: str, property_name: str, days_back: int = 7):
        """Synchronizuje dane jednej property GA4"""
        
        try:
            # Pobierz dane z BigQuery Export
            rows = self.get_ga4_data(property_id, property_name, days_back)
            
            if not rows:
                logger.warning(f"No GA4 data found for {property_name}")
                return 0
            
            # Zapisz do naszej tabeli
            df = pd.DataFrame(rows)
            table_id = f"{self.project_id}.{self.dataset_id}.ga4_performance"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
            )
            
            job = self.bq_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()
            
            logger.info(f"Loaded {len(rows)} GA4 rows for {property_name}")
            return len(rows)
            
        except Exception as e:
            logger.error(f"Error syncing GA4 {property_name}: {str(e)}")
            raise

def sync_all_ga4_properties():
    """Synchronizuje wszystkie property GA4 używając BigQuery Export"""
    import db
    
    try:
        sync = GA4BigQuerySync()
        sync.ensure_table_exists()
        
        clients = db.get_all_clients()
        active_clients = [c for c in clients if c.get('active') and c.get('ga4_property_id')]
        
        logger.info(f"Starting GA4 BigQuery sync for {len(active_clients)} clients")
        
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
                    days_back=7
                )
                summary['successful'] += 1
                summary['total_rows'] += rows
            except Exception as e:
                summary['failed'] += 1
                logger.error(f"Failed {client['client_name']}: {e}")
        
        logger.info(f"GA4 BigQuery sync completed: {summary}")
        return summary
        
    except Exception as e:
        logger.error(f"GA4 BigQuery sync failed: {e}")
        raise
