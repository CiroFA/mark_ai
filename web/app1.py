import os
import sys
from flask import Flask, request, jsonify, send_from_directory

# Aggiunge ../scripts al PYTHONPATH per importare ask_mark
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from ask_mark import ask_mark

# Flask servirà i file dalla cartella corrente (dove c'è index1.html)
app = Flask(__name__, static_folder=".")

@app.route("/")
def index():
    return send_from_directory(".", "index1.html")

@app.route("/ask", methods=["POST"])
def ask():
    data = request.get_json()
    question = data.get("question", "")
    answer = ask_mark(question)
    return jsonify({"answer": answer})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)