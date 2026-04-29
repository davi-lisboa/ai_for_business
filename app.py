from flask import Flask, request, jsonify, render_template
import os
from datetime import datetime
import duckdb

app = Flask(__name__)

DB_FILE = os.environ.get("DB_FILE", "/tmp/cadastros.duckdb")


def get_conn():
    """Abre conexão com DuckDB e garante schema da tabela."""
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = duckdb.connect(DB_FILE)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cadastros (
            data_hora TIMESTAMP,
            nome VARCHAR,
            email VARCHAR,
            commodities VARCHAR
        )
        """
    )
    return conn


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/salvar", methods=["POST"])
def salvar():
    data = request.get_json()
    if not data:
        return jsonify(success=False, message="JSON inválido ou ausente."), 400

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

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commodities_str = "|".join(commodities)

    try:
        conn = get_conn()
        conn.execute(
            "INSERT INTO cadastros (data_hora, nome, email, commodities) VALUES (?, ?, ?, ?)",
            [timestamp, nome, email, commodities_str],
        )
        conn.close()
    except Exception as exc:
        return jsonify(success=False, message=str(exc)), 500

    return jsonify(success=True, message="Cadastro salvo com sucesso.")


@app.route("/listagem")
def listagem():
    try:
        conn = get_conn()
        rows = conn.execute(
            "SELECT data_hora, nome, email, commodities FROM cadastros ORDER BY data_hora DESC"
        ).fetchall()
        conn.close()
        registros = [
            {
                "data_hora": str(r[0]),
                "nome": r[1],
                "email": r[2],
                "commodities": r[3],
            }
            for r in rows
        ]
    except Exception:
        registros = []

    return render_template("listagem.html", registros=registros)


@app.errorhandler(Exception)
def handle_unexpected_error(exc):
    """Garante resposta JSON para erros inesperados na API /salvar."""
    if request.path == "/salvar":
        return jsonify(success=False, message=f"Erro interno: {exc}"), 500
    raise exc


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
