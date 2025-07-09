from flask import Flask, jsonify
import os
from datetime import datetime
import db   # ⬅️ nowy import

app = Flask(__name__)

@app.route("/")
def home():
    return "🎉 Twoja automatyzacja działa!"

@app.route("/status")
def status():
    return jsonify(
        status="OK",
        message="System działa",
        time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

@app.route("/test-db")
def test_db():
    try:
        result = db.test_connection()          # ⬅️ realne zapytanie
        return f"Baza danych podłączona ({result})!"
    except Exception as e:
        return f"❌ Błąd połączenia z DB: {e}", 500
