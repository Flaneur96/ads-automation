"""
Meta Ads (Facebook) to BigQuery sync - ROZSZERZONA WERSJA
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
        """Tworzy rozszerzoną tabelę meta_ads_performance"""
        table_id = f"{self.project_id}.{self.dataset_id}.meta_ads_performance_extended"
        
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
            
            # Podstawowe metryki
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("spend", "FLOAT"),
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("cpc", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
            
            # Link clicks
            bigquery.SchemaField("link_clicks", "INTEGER"),
            bigquery.SchemaField("cost_per_link_click", "FLOAT"),
            
            # Landing page views
            bigquery.SchemaField("landing_page_views", "INTEGER"),
            bigquery.SchemaField("cost_per_landing_page_view", "FLOAT"),
            
            # Add to cart
            bigquery.SchemaField("adds_to_cart", "INTEGER"),
            bigquery.SchemaField("cost_per_add_to_cart", "FLOAT"),
            bigquery.SchemaField("adds_to_cart_value", "FLOAT"),
            
            # Purchases
            bigquery.SchemaField("purchases", "INTEGER"),
            bigquery.SchemaField("cost_per_purchase", "FLOAT"),
            bigquery.SchemaField("purchase_value", "FLOAT"),
            
            # Wskaźniki
            bigquery.SchemaField("conversion_rate_landing_to_purchase", "FLOAT"),
            bigquery.SchemaField("roas", "FLOAT"),
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
        """Pobiera rozszerzone dane z Meta Ads API"""
        
        # Rozszerzone pola
        fields = [
            'campaign_id',
            'campaign_name',
            'adset_id',
            'adset_name',
            'ad_id',
            'ad_name',
            'impressions',
            'clicks',
            'spend',
            'cpm',
            'cpc',
            'ctr',
            'actions',
            'action_values',
            'cost_per_action_type',
            'website_ctr'
        ]
        
        # Parametry zapytania
        params = {
            'access_token': self.access_token,
            'fields': ','.join(fields),
            'level': 'ad',
            'breakdowns': 'age,gender',  # Opcjonalnie
            'action_breakdowns': 'action_type',
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
            
            # Następna strona
            paging = data.get('paging', {})
            url = paging.get('next')
            params = {}
        
        return all_data
    
    def sync_account_data(self, account_id: str, account_name: str, days_back: int = 30):
        """Synchronizuje rozszerzone dane jednego konta Meta"""
        
        logger.info(f"Syncing Meta account {account_name} ({account_id})")
        
        # Daty
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        try:
            # Pobierz dane z API
            insights = self.get_account_insights(account_id, start_date, end_date)
            
            if not insights:
                logger.warning(f"No data for {account_name}")
                return 0
            
            # Przetwórz dane
            rows = []
            for insight in insights:
                # Podstawowe metryki
                impressions = int(insight.get('impressions', 0))
                clicks = int(insight.get('clicks', 0))
                spend = float(insight.get('spend', 0))
                cpm = float(insight.get('cpm', 0))
                cpc = float(insight.get('cpc', 0))
                ctr = float(insight.get('ctr', 0))
                
                # Parsuj actions
                actions = insight.get('actions', [])
                action_values = insight.get('action_values', [])
                cost_per_action = insight.get('cost_per_action_type', [])
                
                # Inicjalizuj metryki
                link_clicks = 0
                landing_page_views = 0
                adds_to_cart = 0
                purchases = 0
                adds_to_cart_value = 0
                purchase_value = 0
                
                # Mapuj actions
                for action in actions:
                    action_type = action.get('action_type', '')
                    value = int(action.get('value', 0))
                    
                    if action_type == 'link_click':
                        link_clicks = value
                    elif action_type == 'landing_page_view':
                        landing_page_views = value
                    elif action_type == 'add_to_cart':
                        adds_to_cart = value
                    elif action_type in ['purchase', 'omni_purchase']:
                        purchases = value
                
                # Mapuj action values
                for action_value in action_values:
                    action_type = action_value.get('action_type', '')
                    value = float(action_value.get('value', 0))
                    
                    if action_type == 'add_to_cart':
                        adds_to_cart_value = value
                    elif action_type in ['purchase', 'omni_purchase']:
                        purchase_value = value
                
                # Mapuj costs per action
                cost_per_link_click = 0
                cost_per_landing_page_view = 0
                cost_per_add_to_cart = 0
                cost_per_purchase = 0
                
                for cost_action in cost_per_action:
                    action_type = cost_action.get('action_type', '')
                    cost = float(cost_action.get('value', 0))
                    
                    if action_type == 'link_click':
                        cost_per_link_click = cost
                    elif action_type == 'landing_page_view':
                        cost_per_landing_page_view = cost
                    elif action_type == 'add_to_cart':
                        cost_per_add_to_cart = cost
                    elif action_type in ['purchase', 'omni_purchase']:
                        cost_per_purchase = cost
                
                # Oblicz wskaźniki
                conversion_rate = (purchases / landing_page_views * 100) if landing_page_views > 0 else 0
                roas = (purchase_value / spend) if spend > 0 else 0
                
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
                    
                    # Podstawowe
                    'impressions': impressions,
                    'clicks': clicks,
                    'spend': round(spend, 2),
                    'cpm': round(cpm, 2),
                    'cpc': round(cpc, 2),
                    'ctr': round(ctr, 2),
                    
                    # Link clicks
                    'link_clicks': link_clicks,
                    'cost_per_link_click': round(cost_per_link_click, 2),
                    
                    # Landing page
                    'landing_page_views': landing_page_views,
                    'cost_per_landing_page_view': round(cost_per_landing_page_view, 2),
                    
                    # Add to cart
                    'adds_to_cart': adds_to_cart,
                    'cost_per_add_to_cart': round(cost_per_add_to_cart, 2),
                    'adds_to_cart_value': round(adds_to_cart_value, 2),
                    
                    # Purchases
                    'purchases': purchases,
                    'cost_per_purchase': round(cost_per_purchase, 2),
                    'purchase_value': round(purchase_value, 2),
                    
                    # Wskaźniki
                    'conversion_rate_landing_to_purchase': round(conversion_rate, 2),
                    'roas': round(roas, 2),
                })
            
            # Zapisz do BigQuery
            if rows:
                df = pd.DataFrame(rows)
                table_id = f"{self.project_id}.{self.dataset_id}.meta_ads_performance_extended"
                
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


# FUNKCJA POZA KLASĄ - NA KOŃCU PLIKU
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
                # Usuń prefix act_ jeśli jest
                account_id = client['meta_account_id']
                if account_id.startswith('act_'):
                    account_id = account_id[4:]
                    
                rows = sync.sync_account_data(
                    account_id,
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
