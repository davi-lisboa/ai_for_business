# app.py
from flask import Flask, request, jsonify, render_template
import csv
import os
from datetime import datetime

app = Flask(__name__)

CSV_FILE = "cadastros.csv"


def ensure_csv_header():
    """Cria o arquivo CSV com cabeçalho caso ainda não exista."""
    if not os.path.isfile(CSV_FILE):
        with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["data_hora", "nome", "email", "commodities"])


@app.route("/")
def index():
    """Renderiza a página HTML com o formulário."""
    return render_template("index.html")


@app.route("/salvar", methods=["POST"])
def salvar():
    """Recebe JSON do front‑end, grava no CSV e devolve resposta JSON."""
    # 1️⃣ Recebe e decodifica o JSON
    data = request.get_json()
    if not data:
        return jsonify(success=False, message="JSON inválido ou ausente."), 400

    # 2️⃣ Validação mínima
    nome = data.get("nome", "").strip()
    email = data.get("email", "").strip()
    commodities = data.get("commodities", [])

    if not nome or not email or not commodities:
        return (
            jsonify(
                success=False,
                message="Todos os campos (nome, e‑mail e commodities) são obrigatórios.",
            ),
            400,
        )

    # 3️⃣ Formata a linha a ser gravada
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # commodities são guardadas como "Ouro|Prata|Café"
    commodities_str = "|".join(commodities)

    linha = [timestamp, nome, email, commodities_str]

    # 4️⃣ Garante cabeçalho e grava (append) no CSV
    ensure_csv_header()
    try:
        with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(linha)
    except Exception as exc:
        return jsonify(success=False, message=str(exc)), 500

    # 5️⃣ Responde ao cliente
    return jsonify(success=True, message="Cadastro salvo com sucesso.")


@app.route("/listagem")
def listagem():
    """Página simples que exibe todos os cadastros já gravados."""
    if not os.path.isfile(CSV_FILE):
        registros = []
    else:
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            registros = list(reader)

    return render_template("listagem.html", registros=registros)


if __name__ == "__main__":
    # modo debug = True apenas para desenvolvimento local
    app.run(host="0.0.0.0", port=5000, debug=True)
