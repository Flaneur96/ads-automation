"""
app.py - główny serwer Flask dla automatyzacji kampanii reklamowych
"""

from flask import Flask, jsonify, request
from datetime import datetime
import os
import logging
import db

app = Flask(__name__)

# Konfiguracja logowania
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = app.logger

# --- PODSTAWOWE ENDPOINTY ---

@app.route("/")
def home():
    """Podstawowy endpoint sprawdzający czy aplikacja działa"""
    return "🎉 Twoja automatyzacja działa!"

@app.route("/status")
def status():
    """Zwraca status aplikacji z timestampem"""
    return jsonify({
        "status": "OK",
        "message": "System działa poprawnie",
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "1.0"
    })

@app.route("/test-db")
def test_db():
    """Testuje połączenie z bazą danych"""
    try:
        result = db.test_connection()
        return jsonify({
            "status": "success",
            "message": f"Baza danych podłączona ({result})!",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Błąd połączenia z bazą: {str(e)}"
        }), 500

@app.route("/health")
def health():
    """Health check endpoint dla monitoringu"""
    try:
        # Sprawdź bazę danych
        db_status = "healthy"
        try:
            db.test_connection()
        except:
            db_status = "unhealthy"
        
        return jsonify({
            "status": "healthy",
            "database": db_status,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500

# W app.py (dodaj importy jeśli potrzeba: from flask import jsonify)
@app.route("/test-ga4-list-properties")
def test_ga4_list_properties():
    """Test: Lista properties do których service account ma dostęp"""
    try:
        from ga4_sync import GA4Sync  # Zaimportuj twoją klasę
        sync = GA4Sync()  # Inicjalizuj z scopes (już masz w kodzie)
        
        from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient
        admin_client = AnalyticsAdminServiceClient(credentials=sync.ga4_client.credentials)
        
        response = admin_client.list_properties()
        
        properties = []
        for property in response:
            properties.append({
                "display_name": property.display_name,
                "property_id": property.name.split('/')[-1],
                "full_name": property.name
            })
        
        return jsonify({
            "status": "success",
            "count": len(properties),
            "properties": properties
        })
    
    except Exception as e:
        return jsonify({
            "error": str(e),
            "type": type(e).__name__
        }), 500
        
@app.route("/debug-service-account")
def debug_service_account():
    try:
        import json
        creds_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if creds_json:
            creds = json.loads(creds_json)
            return jsonify({
                "service_account_email": creds.get('client_email'),
                "project_id": creds.get('project_id')
            })
        return jsonify({"error": "No credentials found"})
    except Exception as e:
        return jsonify({"error": str(e)})
        
@app.route("/sync/debug-google-ads")
def debug_google_ads():
    """Debug Google Ads configuration"""
    try:
        from sync.ads_sync import GoogleAdsSync
        sync = GoogleAdsSync()
        
        # Test podstawowej konfiguracji
        return jsonify({
            "status": "configured",
            "project_id": sync.project_id,
            "dataset_id": sync.dataset_id,
            "ads_client": "initialized" if sync.ads_client else "failed"
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }), 500

# --- KLIENCI ---

@app.route("/add-client", methods=["POST"])
def add_client():
    """Dodaje nowego klienta do bazy danych"""
    logger.info(f"Received add-client request from {request.remote_addr}")
    
    # Obsługa różnych formatów danych
    if request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict()
    
    logger.info(f"Request data: {data}")
    
    # Walidacja
    if not data:
        logger.warning("No data provided in request")
        return jsonify({
            "status": "error",
            "message": "Brak danych w żądaniu"
        }), 400
    
    # Sprawdzenie wymaganych pól
    required_fields = ["client_name", "industry"]
    missing_fields = [field for field in required_fields if not data.get(field)]
    
    if missing_fields:
        logger.warning(f"Missing required fields: {missing_fields}")
        return jsonify({
            "status": "error",
            "message": f"Brakuje wymaganych pól: {', '.join(missing_fields)}"
        }), 400
    
    try:
        # Dodanie klienta do bazy
        client_id = db.insert_client(data)
        logger.info(f"Successfully added client with ID: {client_id}")
        
        return jsonify({
            "status": "success",
            "client_id": client_id,
            "message": "Klient został dodany pomyślnie"
        }), 201
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 400
        
    except Exception as e:
        logger.error(f"Error adding client: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Wystąpił błąd podczas dodawania klienta"
        }), 500

@app.route("/clients", methods=["GET"])
def get_clients():
    """Zwraca listę wszystkich klientów"""
    try:
        clients = db.get_all_clients()
        return jsonify({
            "status": "success",
            "count": len(clients),
            "clients": clients
        })
    except Exception as e:
        logger.error(f"Error fetching clients: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Błąd pobierania listy klientów"
        }), 500

# --- GOOGLE ADS SYNC ---

@app.route("/sync/check-config")
def check_sync_config():
    """Sprawdza czy wszystkie zmienne są ustawione"""
    required = [
        "GOOGLE_ADS_DEVELOPER_TOKEN",
        "GOOGLE_ADS_LOGIN_CUSTOMER_ID",
        "GOOGLE_ADS_CLIENT_ID",
        "GOOGLE_ADS_CLIENT_SECRET",
        "GOOGLE_ADS_REFRESH_TOKEN",
        "BQ_PROJECT_ID"
    ]
    
    missing = []
    for var in required:
        if not os.environ.get(var):
            missing.append(var)
    
    return jsonify({
        "configured": len(missing) == 0,
        "missing": missing
    })

@app.route("/sync/test-google-ads", methods=["POST"])
def test_google_ads_sync():
    """Test synchronizacji Google Ads"""
    data = request.get_json()
    
    if not data or not data.get('client_name') or not data.get('google_ads_id'):
        return jsonify({"error": "Wymagane: client_name i google_ads_id"}), 400
    
    try:
        from sync.ads_sync import GoogleAdsSync
        sync = GoogleAdsSync()
        sync.ensure_table_exists()
        
        rows = sync.sync_customer_data(
            data['google_ads_id'],
            data['client_name'],
            days_back=data.get('days_back', 30)
        )
        
        return jsonify({
            "success": True,
            "rows_synced": rows,
            "client": data['client_name']
        })
    except Exception as e:
        logger.error(f"Google Ads sync failed: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/sync/all-google-ads", methods=["POST"])
def sync_all_google_ads():
    """Synchronizuje wszystkie konta Google Ads"""
    try:
        from sync.ads_sync import sync_all_clients
        result = sync_all_clients()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Sync all failed: {str(e)}")
        return jsonify({"error": str(e)}), 500

# --- META ADS SYNC ---

@app.route("/sync/test-meta-ads", methods=["POST"])
def test_meta_ads_sync():
    """Test synchronizacji Meta Ads"""
    data = request.get_json()
    
    if not data or not data.get('account_id'):
        return jsonify({"error": "Wymagane: account_id"}), 400
    
    try:
        from sync.meta_sync import MetaAdsSync
        sync = MetaAdsSync()
        sync.ensure_table_exists()
        
        rows = sync.sync_account_data(
            data['account_id'],
            data.get('account_name', 'Test Account'),
            days_back=data.get('days_back', 30)
        )
        
        return jsonify({
            "success": True,
            "rows_synced": rows,
            "account": data.get('account_name', data['account_id'])
        })
    except Exception as e:
        logger.error(f"Meta sync failed: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/sync/all-meta", methods=["POST"])
def sync_all_meta():
    """Synchronizuje wszystkie konta Meta"""
    try:
        from sync.meta_sync import sync_all_meta_accounts
        result = sync_all_meta_accounts()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Meta sync all failed: {str(e)}")
        return jsonify({"error": str(e)}), 500

# --- TIKTOK ADS SYNC ---

@app.route("/sync/test-tiktok-ads", methods=["POST"])
def test_tiktok_ads_sync():
    """Test synchronizacji TikTok Ads"""
    data = request.get_json()
    
    if not data or not data.get('advertiser_id'):
        return jsonify({"error": "Wymagane: advertiser_id"}), 400
    
    try:
        from sync.tiktok_sync import TikTokAdsSync
        sync = TikTokAdsSync()
        sync.ensure_table_exists()
        
        rows = sync.sync_advertiser_data(
            data['advertiser_id'],
            data.get('advertiser_name', 'Test Account'),
            days_back=data.get('days_back', 30)
        )
        
        return jsonify({
            "success": True,
            "rows_synced": rows,
            "advertiser": data.get('advertiser_name', data['advertiser_id'])
        })
    except Exception as e:
        logger.error(f"TikTok sync failed: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/sync/all-tiktok", methods=["POST"])
def sync_all_tiktok():
    """Synchronizuje wszystkie konta TikTok"""
    try:
        from sync.tiktok_sync import sync_all_tiktok_accounts
        result = sync_all_tiktok_accounts()
        return jsonify(result)
    except Exception as e:
        logger.error(f"TikTok sync all failed: {str(e)}")
        return jsonify({"error": str(e)}), 500

# --- GA4 SYNC ---

@app.route("/sync/test-ga4", methods=["POST"])
def test_ga4_sync():
    """Test synchronizacji GA4"""
    data = request.get_json()
    
    if not data or not data.get('property_id'):
        return jsonify({"error": "Wymagane: property_id"}), 400
    
    try:
        from sync.ga4_sync import GA4Sync
        sync = GA4Sync()
        sync.ensure_table_exists()
        
        rows = sync.sync_property_data(
            data['property_id'],
            data.get('property_name', 'Test Property'),
            days_back=data.get('days_back', 30)
        )
        
        return jsonify({
            "success": True,
            "rows_synced": rows,
            "property": data.get('property_name', data['property_id'])
        })
    except Exception as e:
        logger.error(f"GA4 sync failed: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route("/sync/all-ga4", methods=["POST"])
def sync_all_ga4():
    """Synchronizuje wszystkie property GA4"""
    try:
        from sync.ga4_sync import sync_all_ga4_properties
        result = sync_all_ga4_properties()
        return jsonify(result)
    except Exception as e:
        logger.error(f"GA4 sync all failed: {str(e)}")
        return jsonify({"error": str(e)}), 500

# --- SYNC STATUS ---

@app.route("/sync/status")
def sync_status():
    """Pokazuje status wszystkich integracji"""
    integrations = {
        "google_ads": {
            "configured": all([
                os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN"),
                os.environ.get("GOOGLE_ADS_REFRESH_TOKEN")
            ]),
            "table": "google_ads_performance"
        },
        "meta_ads": {
            "configured": bool(os.environ.get("META_ACCESS_TOKEN")),
            "table": "meta_ads_performance"
        },
        "bigquery": {
            "configured": all([
                os.environ.get("BQ_PROJECT_ID"),
                os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            ]),
            "project": os.environ.get("BQ_PROJECT_ID"),
            "dataset": os.environ.get("BQ_DATASET_ID", "ads_data")
        }
    }
    
    return jsonify(integrations)

# --- OBSŁUGA BŁĘDÓW ---

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "message": "Endpoint nie został znaleziony"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({
        "status": "error",
        "message": "Wewnętrzny błąd serwera"
    }), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
