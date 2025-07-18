"""
Google Ads → BigQuery sync – uproszczona wersja
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
        """Konfiguruje Google Ads i BigQuery z użyciem zmiennych środowiskowych."""
        try:
            # Konfiguracja Google Ads
            google_ads_config = {
                "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
                "login_customer_id": os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"],
                "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
                "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
                "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
                "use_proto_plus": True,
            }

            yaml_str = yaml.dump(google_ads_config)
            self.ads_client = GoogleAdsClient.load_from_string(yaml_str)
            logger.info("Zainicjalizowano klienta Google Ads")

            # BigQuery
            bq_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if bq_credentials:
                credentials_info = json.loads(bq_credentials)
                self.bq_client = bigquery.Client.from_service_account_info(credentials_info)
            else:
                self.bq_client = bigquery.Client()

            self.project_id = os.environ.get("BQ_PROJECT_ID")
            self.dataset_id = os.environ.get("BQ_DATASET_ID", "ads_data")

            logger.info(f"Zainicjalizowano klienta BigQuery dla projektu: {self.project_id}")

        except Exception as e:
            logger.error(f"Nie udało się zainicjalizować klientów: {e}")
            raise

    def ensure_table_exists(self):
        """Tworzy tabelę, jeśli nie istnieje – rozszerzony schemat."""
        table_id = f"{self.project_id}.{self.dataset_id}.google_ads_performance"

        # Rozszerzony schemat zgodny z panelem Dashboard Paid Ads
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
            type_=bigquery.TimePartitioningType.DAY, field="date"
        )

        try:
            self.bq_client.create_table(table)
            logger.info(f"Utworzono tabelę {table_id}")
        except Exception as e:
            if "Already Exists" in str(e):
                logger.info(f"Tabela {table_id} już istnieje")

    def sync_customer_data(self, customer_id: str, customer_name: str, days_back: int = 30):
        """Synchronizuje dane jednego klienta – pełny zakres danych."""
        logger.info(f"Synchronizuję {customer_name} ({customer_id})")

        # Pełne zapytanie ze wszystkimi metrykami
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        # Zapytanie z klauzulą WHERE
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

        customer_id_clean = customer_id.replace("-", "")
        ga_service = self.ads_client.get_service("GoogleAdsService")

        try:
            response = ga_service.search_stream(customer_id=customer_id_clean, query=query)

            rows = []
            for batch in response:
                for row in batch.results:
                    # Metryki podstawowe
                    impressions = row.metrics.impressions
                    clicks = row.metrics.clicks
                    cost = row.metrics.cost_micros / 1_000_000
                    conversions = row.metrics.conversions
                    conversions_value = row.metrics.conversions_value

                    # Metryki pochodne
                    ctr = (clicks / impressions * 100) if impressions > 0 else 0
                    cpc = (cost / clicks) if clicks > 0 else 0
                    cpm = (cost / impressions * 1000) if impressions > 0 else 0
                    cpa = (cost / conversions) if conversions > 0 else 0
                    roas = (conversions_value / cost) if cost > 0 else 0

                    rows.append(
                        {
                            "sync_timestamp": datetime.now(),
                            "date": datetime.strptime(row.segments.date, "%Y-%m-%d").date(),
                            "customer_id": customer_id,
                            "customer_name": customer_name,
                            "campaign_id": str(row.campaign.id),
                            "campaign_name": row.campaign.name,
                            "campaign_status": row.campaign.status.name,
                            "ad_group_id": str(row.ad_group.id),
                            "ad_group_name": row.ad_group.name,
                            "impressions": impressions,
                            "clicks": clicks,
                            "cost": round(cost, 2),
                            "conversions": conversions,
                            "conversions_value": round(conversions_value, 2),
                            "ctr": round(ctr, 2),
                            "cpc": round(cpc, 2),
                            "cpm": round(cpm, 2),
                            "cpa": round(cpa, 2),
                            "roas": round(roas, 2),
                        }
                    )

            if rows:
                df = pd.DataFrame(rows)
                table_id = f"{self.project_id}.{self.dataset_id}.google_ads_performance"

                # Zapis danych: WRITE_APPEND – dopisuje wiersze
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

                job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()

                logger.info(f"Załadowano {len(rows)} wierszy dla {customer_name}")
                return len(rows)
            else:
                logger.warning(f"Brak danych dla {customer_name}")
                return 0

        except Exception as e:
            logger.error(f"Błąd synchronizacji {customer_name}: {str(e)}")
            raise


def sync_all_clients():
    """Synchronizuje wszystkich klientów – funkcja główna."""
    import db

    try:
        sync = GoogleAdsSync()
        sync.ensure_table_exists()

        clients = db.get_all_clients()
        active_clients = [c for c in clients if c.get("active") and c.get("google_ads_id")]

        logger.info(f"Rozpoczynam synchronizację dla {len(active_clients)} klientów")

        summary = {"total_clients": len(active_clients), "successful": 0, "failed": 0, "total_rows": 0}

        for client in active_clients:
            try:
                rows = sync.sync_customer_data(
                    client["google_ads_id"],
                    client["client_name"],
                    days_back=30,  # ostatnie 30 dni
                )
                summary["successful"] += 1
                summary["total_rows"] += rows
            except Exception as e:
                summary["failed"] += 1
                logger.error(f"Niepowodzenie {client['client_name']}: {e}")

        logger.info(f"Synchronizacja zakończona: {summary}")
        return summary

    except Exception as e:
        logger.error(f"Synchronizacja nie powiodła się: {e}")
        raise


def test_connection():
    """Testuje połączenie z Google Ads API."""
    try:
        sync = GoogleAdsSync()
        # Prosty test – pobranie informacji o MCC
        customer_service = sync.ads_client.get_service("CustomerService")
        customer_resource_name = customer_service.customer_path(os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"])
        return {"status": "connected", "mcc": customer_resource_name}
    except Exception as e:
        return {"status": "error", "message": str(e)}
