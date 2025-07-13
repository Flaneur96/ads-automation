"""
TikTok Ads to BigQuery sync
"""
import os
import logging
from datetime import datetime, timedelta
import json
import requests
import hashlib
import time
from google.cloud import bigquery
import pandas as pd

logger = logging.getLogger(__name__)

class TikTokAdsSync:
    def __init__(self):
        self.setup_clients()
        
    def setup_clients(self):
        """Konfiguracja TikTok i BigQuery ze zmiennych środowiskowych"""
        try:
            # TikTok API config
            self.access_token = os.environ["TIKTOK_ACCESS_TOKEN"]
            self.app_id = os.environ["TIKTOK_APP_ID"]
            self.secret = os.environ["TIKTOK_SECRET"]
            self.base_url = "https://business-api.tiktok.com/open_api/v1.3"
            
            logger.info("TikTok API configured")
            
            # BigQuery - ten sam co dla innych
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
        """Tworzy tabelę tiktok_ads_performance jeśli nie istnieje"""
        table_id = f"{self.project_id}.{self.dataset_id}.tiktok_ads_performance"
        
        schema = [
            bigquery.SchemaField("sync_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("advertiser_id", "STRING"),
            bigquery.SchemaField("advertiser_name", "STRING"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("adgroup_id", "STRING"),
            bigquery.SchemaField("adgroup_name", "STRING"),
            bigquery.SchemaField("ad_id", "STRING"),
            bigquery.SchemaField("ad_name", "STRING"),
            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("spend", "FLOAT"),
            bigquery.SchemaField("conversions", "INTEGER"),
            bigquery.SchemaField("conversion_value", "FLOAT"),
            bigquery.SchemaField("video_views", "INTEGER"),
            bigquery.SchemaField("video_views_25", "INTEGER"),
            bigquery.SchemaField("video_views_50", "INTEGER"),
            bigquery.SchemaField("video_views_75", "INTEGER"),
            bigquery.SchemaField("video_views_100", "INTEGER"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("cpc", "FLOAT"),
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("cpa", "FLOAT"),
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
    
    def get_advertiser_info(self, advertiser_id: str):
        """Pobiera informacje o koncie reklamowym"""
        url = f"{self.base_url}/advertiser/info/"
        
        headers = {
            "Access-Token": self.access_token,
            "Content-Type": "application/json"
        }
        
        params = {
            "advertiser_id": advertiser_id,
            "fields": ["name", "company", "status"]
        }
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("code") == 0:
                return data.get("data", {})
        
        logger.error(f"Failed to get advertiser info: {response.text}")
        return {"name": "Unknown"}
    
    def get_campaign_insights(self, advertiser_id: str, start_date: str, end_date: str):
        """Pobiera dane kampanii z TikTok Ads API"""
        url = f"{self.base_url}/report/integrated/get/"
        
        headers = {
            "Access-Token": self.access_token,
            "Content-Type": "application/json"
        }
        
        # TikTok wymaga specyficznego formatu
        body = {
            "advertiser_id": advertiser_id,
            "report_type": "BASIC",
            "dimensions": ["stat_time_day", "campaign_id", "adgroup_id", "ad_id"],
            "metrics": [
                "campaign_name",
                "adgroup_name", 
                "ad_name",
                "spend",
                "impressions",
                "clicks",
                "conversions",
                "total_purchase_value",
                "video_play_actions",
                "video_watched_2s",
                "video_watched_6s",
                "video_views_p25",
                "video_views_p50",
                "video_views_p75",
                "video_views_p100",
                "ctr",
                "cpc",
                "cpm"
            ],
            "filters": [
                {
                    "field_name": "stat_time_day",
                    "filter_type": "BETWEEN",
                    "filter_value": [start_date, end_date]
                }
            ],
            "page": 1,
            "page_size": 1000
        }
        
        response = requests.post(url, headers=headers, json=body)
        
        if response.status_code != 200:
            logger.error(f"TikTok API error: {response.text}")
            raise Exception(f"TikTok API error: {response.status_code}")
        
        data = response.json()
        
        if data.get("code") != 0:
            logger.error(f"TikTok API error: {data.get('message')}")
            raise Exception(f"TikTok API error: {data.get('message')}")
        
        return data.get("data", {}).get("list", [])
    
    def sync_advertiser_data(self, advertiser_id: str, advertiser_name: str, days_back: int = 30):
        """Synchronizuje dane jednego konta TikTok"""
        
        logger.info(f"Syncing TikTok advertiser {advertiser_name} ({advertiser_id})")
        
        # Daty w formacie YYYY-MM-DD
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        try:
            # Pobierz dane z API
            insights = self.get_campaign_insights(advertiser_id, start_date, end_date)
            
            if not insights:
                logger.warning(f"No data for {advertiser_name}")
                return 0
            
            # Przetwórz dane
            rows = []
            for insight in insights:
                metrics = insight.get("metrics", {})
                dimensions = insight.get("dimensions", {})
                
                # Podstawowe metryki
                impressions = int(metrics.get("impressions", 0))
                clicks = int(metrics.get("clicks", 0))
                spend = float(metrics.get("spend", 0))
                conversions = int(metrics.get("conversions", 0))
                conversion_value = float(metrics.get("total_purchase_value", 0))
                
                # Metryki video
                video_views = int(metrics.get("video_play_actions", 0))
                video_views_25 = int(metrics.get("video_views_p25", 0))
                video_views_50 = int(metrics.get("video_views_p50", 0))
                video_views_75 = int(metrics.get("video_views_p75", 0))
                video_views_100 = int(metrics.get("video_views_p100", 0))
                
                # Obliczenia
                ctr = float(metrics.get("ctr", 0))
                cpc = float(metrics.get("cpc", 0))
                cpm = float(metrics.get("cpm", 0))
                cpa = (spend / conversions) if conversions > 0 else 0
                
                rows.append({
                    'sync_timestamp': datetime.now(),
                    'date': datetime.strptime(dimensions.get("stat_time_day"), '%Y-%m-%d').date(),
                    'advertiser_id': advertiser_id,
                    'advertiser_name': advertiser_name,
                    'campaign_id': dimensions.get("campaign_id"),
                    'campaign_name': metrics.get("campaign_name"),
                    'adgroup_id': dimensions.get("adgroup_id"),
                    'adgroup_name': metrics.get("adgroup_name"),
                    'ad_id': dimensions.get("ad_id"),
                    'ad_name': metrics.get("ad_name"),
                    'impressions': impressions,
                    'clicks': clicks,
                    'spend': round(spend, 2),
                    'conversions': conversions,
                    'conversion_value': round(conversion_value, 2),
                    'video_views': video_views,
                    'video_views_25': video_views_25,
                    'video_views_50': video_views_50,
                    'video_views_75': video_views_75,
                    'video_views_100': video_views_100,
                    'ctr': round(ctr, 2),
                    'cpc': round(cpc, 2),
                    'cpm': round(cpm, 2),
                    'cpa': round(cpa, 2),
                })
            
            # Zapisz do BigQuery
            if rows:
                df = pd.DataFrame(rows)
                table_id = f"{self.project_id}.{self.dataset_id}.tiktok_ads_performance"
                
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND",
                )
                
                job = self.bq_client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                job.result()
                
                logger.info(f"Loaded {len(rows)} rows for {advertiser_name}")
                return len(rows)
                
        except Exception as e:
            logger.error(f"Error syncing {advertiser_name}: {str(e)}")
            raise

def sync_all_tiktok_accounts():
    """Synchronizuje wszystkie konta TikTok"""
    import db
    
    try:
        sync = TikTokAdsSync()
        sync.ensure_table_exists()
        
        clients = db.get_all_clients()
        active_clients = [c for c in clients if c.get('active') and c.get('tiktok_advertiser_id')]
        
        logger.info(f"Starting TikTok sync for {len(active_clients)} clients")
        
        summary = {
            'total_clients': len(active_clients),
            'successful': 0,
            'failed': 0,
            'total_rows': 0
        }
        
        for client in active_clients:
            try:
                rows = sync.sync_advertiser_data(
                    client['tiktok_advertiser_id'],
                    client['client_name'],
                    days_back=30
                )
                summary['successful'] += 1
                summary['total_rows'] += rows
            except Exception as e:
                summary['failed'] += 1
                logger.error(f"Failed {client['client_name']}: {e}")
        
        logger.info(f"TikTok sync completed: {summary}")
        return summary
        
    except Exception as e:
        logger.error(f"TikTok sync failed: {e}")
        raise
