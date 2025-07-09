from flask import Flask, jsonify
import os
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def home():
    return "🎉 Twoja automatyzacja działa!"

@app.route('/status')
def status():
    return jsonify({
        "status": "OK",
        "message": "System działa",
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route('/test-db')
def test_db():
    # Na razie tylko test
    return "Baza danych podłączona!"

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)