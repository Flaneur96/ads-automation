"""
Google Ads to BigQuery sync - używa zmiennych środowiskowych
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
        """Tworzy tabelę jeśli nie istnieje"""
        table_id = f"{self.project_id}.{self.dataset_id}.google_ads_performance"
        
        schema = [
            bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("customer_name", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("cost", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        
        try:
            table = self.bq_client.create_table(table)
            logger.info(f"Created table {table_id}")
        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Table {table_id} already exists")
    
    def sync_customer_data(self, customer_id: str, customer_name: str):
        """Synchronizuje dane jednego klienta"""
        logger.info(f"Syncing {customer_name} ({customer_id})")
        
        # Prosty query na start
        query = """
            SELECT
                campaign.name,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros
            FROM campaign
            WHERE segments.date >= '2024-01-01'
            LIMIT 100
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
                    rows.append({
                        'sync_timestamp': datetime.now(),
                        'date': row.segments.date,
                        'customer_id': customer_id,
                        'customer_name': customer_name,
                        'campaign_name': row.campaign.name,
                        'impressions': row.metrics.impressions,
                        'clicks': row.metrics.clicks,
                        'cost': row.metrics.cost_micros / 1_000_000,
                        'ctr': (row.metrics.clicks / row.metrics.impressions * 100) if row.metrics.impressions > 0 else 0
                    })
            
            if rows:
                df = pd.DataFrame(rows)
                table_id = f"{self.project_id}.{self.dataset_id}.google_ads_performance"
                
                job = self.bq_client.load_table_from_dataframe(df, table_id)
                job.result()
                
                logger.info(f"Loaded {len(rows)} rows for {customer_name}")
                return len(rows)
                
        except Exception as e:
            logger.error(f"Error syncing {customer_name}: {str(e)}")
            raise

def test_sync_single_client(client_name, google_ads_id):
    """Test dla jednego klienta"""
    try:
        sync = GoogleAdsSync()
        sync.ensure_table_exists()
        
        rows = sync.sync_customer_data(google_ads_id, client_name)
        return {"success": True, "rows": rows}
    except Exception as e:
        return {"success": False, "error": str(e)}
