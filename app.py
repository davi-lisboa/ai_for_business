from flask import Flask, request, jsonify, render_template
import os
import duckdb
from datetime import datetime

app = Flask(__name__)

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
DB_FILE = os.path.join(DATA_DIR, "cadastros.duckdb")
USERS_CSV = os.path.join(DATA_DIR, "usuarios.csv")
COMMODITIES_MAP_CSV = os.path.join(DATA_DIR, "commodities_investing_map.csv")

COMMODITIES_INVESTING_MAP = [
    ("🥇 Ouro", "XAU/USD"),
    ("🥈 Prata", "XAG/USD"),
    ("🟠 Cobre", "HG"),
    ("☕ Café", "KC"),
    ("🌱 Soja", "ZS"),
    ("🌾 Trigo", "ZW"),
    ("🧵 Algodão", "CT"),
    ("🛢️ Petróleo", "CL"),
]


def get_conn():
    os.makedirs(DATA_DIR, exist_ok=True)
    return duckdb.connect(DB_FILE)


def ensure_data_store():
    conn = get_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usuarios (
            id VARCHAR PRIMARY KEY,
            nome VARCHAR NOT NULL,
            email VARCHAR NOT NULL,
            commodities VARCHAR NOT NULL,
            data_cadastro TIMESTAMP NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS commodities_map (
            commodity VARCHAR PRIMARY KEY,
            ticker_investing VARCHAR NOT NULL
        )
        """
    )

    conn.executemany(
        """
        INSERT INTO commodities_map (commodity, ticker_investing)
        VALUES (?, ?)
        ON CONFLICT (commodity) DO UPDATE SET ticker_investing = EXCLUDED.ticker_investing
        """,
        COMMODITIES_INVESTING_MAP,
    )

    sync_tables_to_csv(conn)
    conn.close()


def sync_tables_to_csv(conn):
    conn.execute(
        f"""
        COPY (
            SELECT id, nome, email, commodities, strftime(data_cadastro, '%Y-%m-%d %H:%M:%S') AS data_cadastro
            FROM usuarios
            ORDER BY data_cadastro DESC
        ) TO '{USERS_CSV}' (HEADER, DELIMITER ',')
        """
    )

    conn.execute(
        f"""
        COPY (
            SELECT commodity, ticker_investing
            FROM commodities_map
            ORDER BY commodity
        ) TO '{COMMODITIES_MAP_CSV}' (HEADER, DELIMITER ',')
        """
    )


def append_user(nome: str, email: str, commodities: list[str]):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO usuarios (id, nome, email, commodities, data_cadastro)
        VALUES (uuid(), ?, ?, ?, ?)
        """,
        [nome, email, "|".join(commodities), datetime.now()],
    )
    sync_tables_to_csv(conn)
    conn.close()


def load_users():
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            id,
            nome,
            email,
            replace(commodities, '|', ', ') AS commodities,
            strftime(data_cadastro, '%Y-%m-%d %H:%M:%S') AS data_cadastro
        FROM usuarios
        ORDER BY data_cadastro DESC
        """
    ).fetchall()
    conn.close()

    return [
        {
            "id": row[0],
            "nome": row[1],
            "email": row[2],
            "commodities": row[3],
            "data_cadastro": row[4],
        }
        for row in rows
    ]

ensure_data_store()


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

    try:
        append_user(nome=nome, email=email, commodities=commodities)
    except Exception as exc:
        return jsonify(success=False, message=str(exc)), 500

    return jsonify(success=True, message="Cadastro salvo com sucesso.")


@app.route("/listagem")
def listagem():
    try:
        registros = load_users()
    except Exception:
        registros = []

    return render_template("listagem.html", registros=registros)


@app.errorhandler(Exception)
def handle_unexpected_error(exc):
    if request.path == "/salvar":
        return jsonify(success=False, message=f"Erro interno: {exc}"), 500
    raise exc


if __name__ == "__main__":
    ensure_data_store()
    app.run(host="0.0.0.0", port=5000, debug=True)
