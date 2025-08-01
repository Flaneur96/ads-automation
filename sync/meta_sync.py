"""
Meta Ads (Facebook) to BigQuery sync - wersja dostosowana do raportu
"""
import os
import logging
from datetime import datetime, timedelta
import json
import requests
from google.cloud import bigquery
import pandas as pd

logger = logging.getLogger(__name__)

class MetaAdsSync:
    def __init__(self):
        self.setup_clients()
        
    def setup_clients(self):
        """Konfiguracja Meta i BigQuery ze zmiennych środowiskowych"""
        try:
            # Meta API config
            self.access_token = os.environ["META_ACCESS_TOKEN"]
            self.api_version = "v18.0"
            self.base_url = f"https://graph.facebook.com/{self.api_version}"
            
            logger.info("Meta API configured")
            
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
            
            logger.info(f"BigQuery configured: {self.project_id}.{self.dataset_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            raise
    
    def ensure_table_exists(self):
        """Tworzy tabelę meta_ads_performance - tylko potrzebne kolumny"""
        table_id = f"{self.project_id}.{self.dataset_id}.meta_ads_performance"
        
        schema = [
            bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("account_name", "STRING"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("adset_id", "STRING"),
            bigquery.SchemaField("adset_name", "STRING"),
            bigquery.SchemaField("ad_id", "STRING"),
            bigquery.SchemaField("ad_name", "STRING"),
            # Metryki z raportu
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("spend", "FLOAT"),
            bigquery.SchemaField("link_clicks", "INTEGER"),
            bigquery.SchemaField("landing_page_views", "INTEGER"),
            bigquery.SchemaField("add_to_cart", "INTEGER"),
            bigquery.SchemaField("add_to_cart_value", "FLOAT"),
            bigquery.SchemaField("purchases", "INTEGER"),
            bigquery.SchemaField("purchase_value", "FLOAT"),
            # Obliczone metryki
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("cpc", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
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
    
    def get_account_insights(self, account_id: str, start_date: str, end_date: str):
        """Pobiera dane z Meta Ads API - tylko potrzebne pola"""
        
        fields = [
            'campaign_id',
            'campaign_name',
            'adset_id',
            'adset_name',
            'ad_id',
            'ad_name',
            'impressions',
            'spend',
            'inline_link_clicks',  # link clicks
            'actions',  # tu są landing_page_view, add_to_cart, purchase
            'action_values',  # wartości konwersji
            'ctr',
            'cpc',
            'cpm'
        ]
        
        params = {
            'access_token': self.access_token,
            'fields': ','.join(fields),
            'level': 'ad',
            'time_range': json.dumps({
                'since': start_date,
                'until': end_date
            }),
            'time_increment': 1,
            'limit': 500
        }
        
        url = f"{self.base_url}/act_{account_id}/insights"
        
        all_data = []
        
        while url:
            response = requests.get(url, params=params)

            if response.status_code != 200:
                logger.error(f"Meta API error: {response.text}")
                raise Exception(f"Meta API error: {response.status_code}")
            
            data = response.json()
            all_data.extend(data.get('data', []))
            
            paging = data.get('paging', {})
            url = paging.get('next')
            params = {}
        
        return all_data
    
    def sync_account_data(self, account_id: str, account_name: str, days_back: int = 30):
        """Synchronizuje dane jednego konta Meta"""
        
        logger.info(f"Syncing Meta account {account_name} ({account_id})")
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        try:
            insights = self.get_account_insights(account_id, start_date, end_date)
            
            if not insights:
                logger.warning(f"No data for {account_name}")
                return 0
            
            rows = []
            for insight in insights:
                # Podstawowe metryki
                impressions = int(insight.get('impressions', 0))
                spend = float(insight.get('spend', 0))
                link_clicks = int(insight.get('inline_link_clicks', 0))
                
                # Przetwarzanie actions - tylko te z raportu
                actions = insight.get('actions', [])
                landing_page_views = 0
                add_to_cart = 0
                purchases = 0
                
                for action in actions:
                    action_type = action.get('action_type', '')
                    value = int(action.get('value', 0))
                    
                    if action_type == 'landing_page_view':
                        landing_page_views += value
                    elif action_type == 'add_to_cart':
                        add_to_cart += value
                    elif action_type == 'purchase':
                        purchases += value
                
                # Wartości konwersji
                action_values = insight.get('action_values', [])
                add_to_cart_value = 0
                purchase_value = 0
                
                for action_value in action_values:
                    action_type = action_value.get('action_type')
                    value = float(action_value.get('value', 0))
                    
                    if action_type == 'add_to_cart':
                        add_to_cart_value += value
                    elif action_type == 'purchase':
                        purchase_value += value
                
                rows.append({
                    'sync_timestamp': datetime.now(),
                    'date': datetime.strptime(insight['date_start'], '%Y-%m-%d').date(),
                    'account_id': account_id,
                    'account_name': account_name,
                    'campaign_id': insight.get('campaign_id'),
                    'campaign_name': insight.get('campaign_name'),
                    'adset_id': insight.get('adset_id'),
                    'adset_name': insight.get('adset_name'),
                    'ad_id': insight.get('ad_id'),
                    'ad_name': insight.get('ad_name'),
                    # Metryki z raportu
                    'impressions': impressions,
                    'spend': round(spend, 2),
                    'link_clicks': link_clicks,
                    'landing_page_views': landing_page_views,
                    'add_to_cart': add_to_cart,
                    'add_to_cart_value': round(add_to_cart_value, 2),
                    'purchases': purchases,
                    'purchase_value': round(purchase_value, 2),
                    # Obliczone
                    'cpm': round(float(insight.get('cpm', 0)), 2),
                    'cpc': round(float(insight.get('cpc', 0)), 2),
                    'ctr': round(float(insight.get('ctr', 0)), 2),
                })
            
            if rows:
                df = pd.DataFrame(rows)
                table_id = f"{self.project_id}.{self.dataset_id}.meta_ads_performance"
                
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                )
                
                job = self.bq_client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                job.result()
                
                logger.info(f"Loaded {len(rows)} rows for {account_name}")
                return len(rows)
                
        except Exception as e:
            logger.error(f"Error syncing {account_name}: {str(e)}")
            raise

def sync_all_meta_accounts():
    """Synchronizuje wszystkie konta Meta"""
    import db
    
    try:
        sync = MetaAdsSync()
        sync.ensure_table_exists()
        
        clients = db.get_all_clients()
        active_clients = [c for c in clients if c.get('active') and c.get('meta_account_id')]
        
        logger.info(f"Starting Meta sync for {len(active_clients)} clients")
        
        summary = {
            'total_clients': len(active_clients),
            'successful': 0,
            'failed': 0,
            'total_rows': 0
        }
        
        for client in active_clients:
            try:
                rows = sync.sync_account_data(
                    client['meta_account_id'],
                    client['client_name'],
                    days_back=30
                )
                summary['successful'] += 1
                summary['total_rows'] += rows
            except Exception as e:
                summary['failed'] += 1
                logger.error(f"Failed {client['client_name']}: {e}")
        
        logger.info(f"Meta sync completed: {summary}")
        return summary
        
    except Exception as e:
        logger.error(f"Meta sync failed: {e}")
        raise
