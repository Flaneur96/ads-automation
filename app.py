"""
app.py  –  główny serwer Flask uruchamiany przez Gunicorna
Endpointy:
  /           – prosty test „żyję”
  /status     – JSON z timestampem
  /test-db    – test realnego połączenia z DB
  /add-client – POST JSON, zapisuje klienta w DB
"""

from flask import Flask, jsonify, request, abort
from datetime import datetime
import os
import logging

import db   # lokalny moduł db.py

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


# --- endpointy --------------------------------------------------------------


@app.route("/")
def home():
    return "🎉 Twoja automatyzacja działa!"


@app.route("/status")
def status():
    return jsonify(
        status="OK",
        message="System działa",
        time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


@app.route("/test-db")
def test_db():
    try:
        result = db.test_connection()
        return f"Baza danych podłączona ({result})!"
    except Exception as exc:
        app.logger.exception("DB test failed")
        return f"❌ Błąd połączenia z DB: {exc}", 500


@app.route("/add-client", methods=["POST"])
def add_client():
    required = {"client_name", "industry"}
    data = request.get_json(silent=True) or {}

    missing = required - data.keys()
    if missing:
        abort(400, f"Brakuje pól: {', '.join(missing)}")

    try:
        client_id = db.insert_client(data)
        return jsonify(status="ok", client_id=client_id), 201
    except Exception as exc:
        app.logger.exception("Insert client failed")
        abort(500, str(exc))


# --- local run (opcjonalny) -------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
