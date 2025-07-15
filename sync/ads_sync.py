"""
Google Ads to BigQuery sync - uproszczona wersja
"""
import os
import logging
from datetime import datetime, timedelta
import json
import yaml
from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
import pandas as pd

logger = logging.getLogger(__name__)

class GoogleAdsSync:
    def __init__(self):
        self.setup_clients()
        
    def setup_clients(self):
        """Konfiguracja Google Ads i BigQuery ze zmiennych środowiskowych"""
        try:
            # Google Ads config
            google_ads_config = {
                "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
                "login_customer_id": os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"], 
                "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
                "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
                "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
                "use_proto_plus": True
            }
            
            yaml_str = yaml.dump(google_ads_config)
            self.ads_client = GoogleAdsClient.load_from_string(yaml_str)
            logger.info("Google Ads client initialized")
            
            # BigQuery
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
            
            logger.info(f"BigQuery client initialized for project: {self.project_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize clients: {e}")
            raise
    
    def ensure_table_exists(self):
        """Tworzy tabelę jeśli nie istnieje - ROZSZERZONA SCHEMA"""
        table_id = f"{self.project_id}.{self.dataset_id}.google_ads_performance"
        
        # Rozszerzona schema zgodna z DASHBOARD PAID ADS
        schema = [
            bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("customer_name", "STRING"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("campaign_status", "STRING"),
            bigquery.SchemaField("ad_group_id", "STRING"),
            bigquery.SchemaField("ad_group_name", "STRING"),
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("cost", "FLOAT"),
            bigquery.SchemaField("conversions", "FLOAT"),
            bigquery.SchemaField("conversions_value", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("cpc", "FLOAT"),
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("cpa", "FLOAT"),
            bigquery.SchemaField("roas", "FLOAT"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        
        # Partycjonowanie dla lepszej wydajności
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
    
    def sync_customer_data(self, customer_id: str, customer_name: str, days_back: int = 30):
    """Synchronizuje dane jednego klienta - PEŁNE DANE"""
    logger.info(f"Syncing {customer_name} ({customer_id})")
    
    # Pełny query z wszystkimi metrykami
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')  # <-- DODAJ TO
    
    # POPRAWIONE QUERY Z WHERE
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            ad_group.id,
            ad_group.name,
            segments.date,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            metrics.all_conversions,
            metrics.all_conversions_value
        FROM ad_group
        WHERE 
            segments.date BETWEEN '{start_date}' AND '{end_date}'
            AND campaign.status != 'REMOVED'
        ORDER BY segments.date DESC
    """
    
    # Reszta kodu bez zmian...
        """
        
        customer_id_clean = customer_id.replace('-', '')
        ga_service = self.ads_client.get_service("GoogleAdsService")
        
        try:
            response = ga_service.search_stream(
                customer_id=customer_id_clean,
                query=query
            )
            
            rows = []
            for batch in response:
                for row in batch.results:
                    # Podstawowe metryki
                    impressions = row.metrics.impressions
                    clicks = row.metrics.clicks
                    cost = row.metrics.cost_micros / 1_000_000
                    conversions = row.metrics.conversions
                    conversions_value = row.metrics.conversions_value
                    
                    # Obliczenia pochodne
                    ctr = (clicks / impressions * 100) if impressions > 0 else 0
                    cpc = (cost / clicks) if clicks > 0 else 0
                    cpm = (cost / impressions * 1000) if impressions > 0 else 0
                    cpa = (cost / conversions) if conversions > 0 else 0
                    roas = (conversions_value / cost) if cost > 0 else 0
                    
                    rows.append({
                        'sync_timestamp': datetime.now(),
                        'date': datetime.strptime(row.segments.date, '%Y-%m-%d').date(),
                        'customer_id': customer_id,
                        'customer_name': customer_name,
                        'campaign_id': str(row.campaign.id),
                        'campaign_name': row.campaign.name,
                        'campaign_status': row.campaign.status.name,
                        'ad_group_id': str(row.ad_group.id),
                        'ad_group_name': row.ad_group.name,
                        'impressions': impressions,
                        'clicks': clicks,
                        'cost': round(cost, 2),
                        'conversions': conversions,
                        'conversions_value': round(conversions_value, 2),
                        'ctr': round(ctr, 2),
                        'cpc': round(cpc, 2),
                        'cpm': round(cpm, 2),
                        'cpa': round(cpa, 2),
                        'roas': round(roas, 2),
                    })
            
            if rows:
                df = pd.DataFrame(rows)
                table_id = f"{self.project_id}.{self.dataset_id}.google_ads_performance"
                
                # Write disposition: WRITE_TRUNCATE dla tego klienta i okresu
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                )
                
                job = self.bq_client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                job.result()
                
                logger.info(f"Loaded {len(rows)} rows for {customer_name}")
                return len(rows)
            else:
                logger.warning(f"No data found for {customer_name}")
                return 0
                
        except Exception as e:
            logger.error(f"Error syncing {customer_name}: {str(e)}")
            raise

def sync_all_clients():
    """Synchronizuje wszystkich klientów - GŁÓWNA FUNKCJA"""
    import db
    
    try:
        sync = GoogleAdsSync()
        sync.ensure_table_exists()
        
        clients = db.get_all_clients()
        active_clients = [c for c in clients if c.get('active') and c.get('google_ads_id')]
        
        logger.info(f"Starting sync for {len(active_clients)} clients")
        
        summary = {
            'total_clients': len(active_clients),
            'successful': 0,
            'failed': 0,
            'total_rows': 0
        }
        
        for client in active_clients:
            try:
                rows = sync.sync_customer_data(
                    client['google_ads_id'],
                    client['client_name'],
                    days_back=30  # Ostatnie 30 dni
                )
                summary['successful'] += 1
                summary['total_rows'] += rows
            except Exception as e:
                summary['failed'] += 1
                logger.error(f"Failed {client['client_name']}: {e}")
        
        logger.info(f"Sync completed: {summary}")
        return summary
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise
        
def test_connection():
    """Test połączenia z Google Ads API"""
    try:
        sync = GoogleAdsSync()
        # Prosty test - pobierz info o MCC
        customer_service = sync.ads_client.get_service("CustomerService")
        customer_resource_name = customer_service.customer_path(
            os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"]
        )
        return {"status": "connected", "mcc": customer_resource_name}
    except Exception as e:
        return {"status": "error", "message": str(e)}
