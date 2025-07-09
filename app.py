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

# --- ENDPOINTY ---

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

# Obsługa błędów
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
