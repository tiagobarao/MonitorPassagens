"""
Monitor de Preços de Passagens Aéreas
Usa a API Aviasales/TravelPayouts + alertas via Telegram.
"""

import json
import os
import sqlite3
import logging
import time
import requests
from datetime import datetime, date, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

# ---------------------------------------------------------------------------
# CONFIGURAÇÃO — lida de config.json
# ---------------------------------------------------------------------------

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

def _carregar_config():
    # Permite passar o config como variável de ambiente (ex: deploy no Fly.io)
    config_env = os.getenv("CONFIG_JSON")
    if config_env:
        return json.loads(config_env)
    if not os.path.exists(_CONFIG_PATH):
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado: {_CONFIG_PATH}\n"
            "Defina a variável de ambiente CONFIG_JSON ou crie o arquivo config.json."
        )
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)

_cfg = _carregar_config()

AVIASALES_TOKEN    = _cfg["aviasales_token"]
TELEGRAM_BOT_TOKEN = _cfg.get("telegram_bot_token", "")
TELEGRAM_CHAT_ID   = _cfg.get("telegram_chat_id",   "")

DB_PATH        = _cfg.get("db_path",        "passagens.db")
LOG_PATH       = _cfg.get("log_path",        "monitor.log")
TOP_DATAS      = _cfg.get("top_datas",       5)
SCAN_HORIZONTE = _cfg.get("scan_horizonte",  4)   # meses à frente
PAUSA_CHAMADAS = _cfg.get("pausa_chamadas",  2)   # segundos entre chamadas

ROTAS = _cfg["rotas"]

# Mapa de códigos IATA de companhias aéreas comuns
_AIRLINES = {
    "JL": "Japan Airlines", "NH": "ANA", "LA": "LATAM", "G3": "GOL",
    "AD": "Azul",           "AA": "American Airlines", "UA": "United",
    "DL": "Delta",          "AF": "Air France", "KL": "KLM",
    "IB": "Iberia",         "TP": "TAP",        "LH": "Lufthansa",
    "BA": "British Airways","EK": "Emirates",   "QR": "Qatar Airways",
    "TK": "Turkish Airlines","CX": "Cathay Pacific","SQ": "Singapore Airlines",
}

def _nome_companhia(codigo: str) -> str:
    return _AIRLINES.get(codigo.upper(), codigo.upper()) if codigo else "—"

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BANCO DE DADOS
# ---------------------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS precos (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            capturado_em TEXT NOT NULL,
            origem       TEXT NOT NULL,
            destino      TEXT NOT NULL,
            data_partida TEXT NOT NULL,
            preco_usd    REAL NOT NULL,
            companhia    TEXT NOT NULL DEFAULT '',
            tipo         TEXT NOT NULL
        )
    """)
    try:
        conn.execute("ALTER TABLE precos ADD COLUMN companhia TEXT NOT NULL DEFAULT ''")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()
    log.info("Banco de dados inicializado: %s", DB_PATH)


def salvar_preco(origem, destino, data_partida, preco_usd, companhia, tipo):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO precos (capturado_em, origem, destino, data_partida, preco_usd, companhia, tipo) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (datetime.now().isoformat(timespec="seconds"), origem, destino,
         data_partida, preco_usd, companhia, tipo),
    )
    conn.commit()
    conn.close()


def listar_datas_monitoradas():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT DISTINCT origem, destino, data_partida
        FROM (
            SELECT origem, destino, data_partida, preco_usd,
                   ROW_NUMBER() OVER (
                       PARTITION BY origem, destino
                       ORDER BY preco_usd ASC
                   ) AS rn
            FROM precos
            WHERE tipo = 'estimado'
        )
        WHERE rn <= ?
    """, (TOP_DATAS,)).fetchall()
    conn.close()
    return rows

# ---------------------------------------------------------------------------
# TELEGRAM
# ---------------------------------------------------------------------------

def enviar_telegram(mensagem: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       mensagem,
            "parse_mode": "HTML",
        }, timeout=10)
        resp.raise_for_status()
        log.info("Mensagem Telegram enviada.")
    except Exception as exc:
        log.warning("Falha ao enviar Telegram: %s", exc)

# ---------------------------------------------------------------------------
# AVIASALES — helpers
# ---------------------------------------------------------------------------

_BASE_URL  = "https://api.travelpayouts.com"
_CABINE_MAP = {"economy": 0, "premium_economy": 0, "business": 1, "first": 2}


def _headers():
    return {"X-Access-Token": AVIASALES_TOKEN}


def _chamar_api(endpoint: str, params: dict) -> dict | None:
    """Faz uma chamada GET à API, trata 429. Retorna JSON ou None."""
    url = _BASE_URL + endpoint
    for tentativa in range(3):
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=15)
            if resp.status_code == 429:
                espera = int(resp.headers.get("Retry-After", 60))
                log.warning("429 — aguardando %ds… (tentativa %d/3)", espera, tentativa + 1)
                time.sleep(espera)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.error("Erro HTTP Aviasales (%s): %s", endpoint, exc)
            return None
    return None


def _buscar_calendario(origem: str, destino: str, mes: str,
                       trip_class: int = 0, somente_direto: bool = False,
                       ida_e_volta: bool = True, dias_retorno: int = 7) -> list[dict]:
    """
    Busca preços para todos os dias de um mês.
    mes = 'YYYY-MM'
    Retorna lista de {"data", "preco", "companhia"} ordenada por preço.
    """
    params = {
        "origin":      origem,
        "destination": destino,
        "depart_date": mes,
        "currency":    "usd",
        "trip_class":  trip_class,
        "token":       AVIASALES_TOKEN,
    }
    if ida_e_volta:
        # Calcula o mês de retorno com base nos dias_retorno
        ano, m = int(mes[:4]), int(mes[5:7])
        mes_retorno = date(ano, m, 1) + timedelta(days=dias_retorno + 15)
        params["return_date"] = mes_retorno.strftime("%Y-%m")
    else:
        params["one_way"] = "true"
    if somente_direto:
        params["direct"] = "true"
    body = _chamar_api("/v1/prices/calendar", params)
    if not body or not body.get("success") or not body.get("data"):
        return []

    resultados = []
    for data_str, info in body["data"].items():
        try:
            preco = float(info["price"])
        except (KeyError, TypeError, ValueError):
            continue
        # Filtra datas passadas
        if date.fromisoformat(data_str[:10]) <= date.today():
            continue
        resultados.append({
            "data":       data_str[:10],
            "preco":      preco,
            "companhia":  _nome_companhia(info.get("airline", "")),
        })

    resultados.sort(key=lambda x: x["preco"])
    return resultados


def _buscar_preco_data(origem: str, destino: str, data_partida: str,
                       trip_class: int = 0, somente_direto: bool = False,
                       ida_e_volta: bool = True, dias_retorno: int = 7) -> dict | None:
    """
    Busca o menor preço disponível para uma data específica.
    Retorna {"data", "preco", "companhia"} ou None.
    """
    mes = data_partida[:7]   # YYYY-MM
    resultados = _buscar_calendario(origem, destino, mes, trip_class,
                                    somente_direto, ida_e_volta, dias_retorno)
    # Filtra apenas a data exata
    for r in resultados:
        if r["data"] == data_partida:
            return r
    return None

# ---------------------------------------------------------------------------
# VARREDURA DIÁRIA
# ---------------------------------------------------------------------------

def varredura_diaria():
    """
    Para cada rota, consulta o calendário de preços dos próximos
    SCAN_HORIZONTE meses e salva as TOP_DATAS datas mais baratas.
    """
    log.info("=== Iniciando varredura diária ===")

    # Gera lista de meses a consultar (ex: ["2026-04", "2026-05", ...])
    meses = []
    for i in range(SCAN_HORIZONTE):
        d = date.today().replace(day=1) + timedelta(days=32 * i)
        meses.append(d.strftime("%Y-%m"))

    for rota in ROTAS:
        origem  = rota["origem"]
        destino = rota["destino"]
        trip_class     = _CABINE_MAP.get(rota.get("cabine", "economy"), 0)
        somente_direto = rota.get("somente_direto", False)
        ida_e_volta    = rota.get("ida_e_volta", True)
        dias_retorno   = rota.get("dias_retorno", 7)
        log.info("Varrendo %s → %s (%d meses, cabine: %s, %s, direto: %s)",
                 origem, destino, len(meses), rota.get("cabine", "economy"),
                 "ida e volta" if ida_e_volta else "só ida", somente_direto)

        todos = []
        for i, mes in enumerate(meses, 1):
            log.info("  [%d/%d] %s → %s | %s…", i, len(meses), origem, destino, mes)
            resultados = _buscar_calendario(origem, destino, mes, trip_class,
                                            somente_direto, ida_e_volta, dias_retorno)
            todos.extend(resultados)
            time.sleep(PAUSA_CHAMADAS)

        if not todos:
            log.warning("  Nenhum resultado para %s → %s. "
                        "Verifique o token e se a rota tem voos disponíveis.", origem, destino)
            continue

        todos.sort(key=lambda x: x["preco"])
        top = todos[:TOP_DATAS]
        log.info("  %d datas com preço encontradas.", len(todos))
        for r in top:
            salvar_preco(origem, destino, r["data"], r["preco"], r["companhia"], "estimado")
            log.info("  %s → %s | %s | USD %.2f | %s (estimado)",
                     origem, destino, r["data"], r["preco"], r["companhia"])

    log.info("=== Varredura diária concluída ===")

# ---------------------------------------------------------------------------
# MONITORAMENTO HORÁRIO
# ---------------------------------------------------------------------------

def monitoramento_horario():
    """Reconsulta as datas salvas e envia resumo completo via Telegram."""
    log.info("--- Monitoramento horário ---")
    datas = listar_datas_monitoradas()

    if not datas:
        log.warning("Nenhuma data no banco. Verifique se a varredura diária rodou com sucesso.")
        return

    alertas = {(r["origem"], r["destino"]): r["alerta_preco"] for r in ROTAS}
    agora   = datetime.now().strftime("%d/%m/%Y %H:%M")
    por_rota: dict[str, list] = {}

    for origem, destino, data_partida in datas:
        if date.fromisoformat(data_partida) <= date.today():
            continue

        rota_config    = next((r for r in ROTAS if r["origem"] == origem
                               and r["destino"] == destino), None)
        trip_class     = _CABINE_MAP.get(
            rota_config.get("cabine", "economy") if rota_config else "economy", 0)
        somente_direto = rota_config.get("somente_direto", False) if rota_config else False
        ida_e_volta    = rota_config.get("ida_e_volta",    True)  if rota_config else True
        dias_retorno   = rota_config.get("dias_retorno",   7)     if rota_config else 7
        log.info("  Consultando %s → %s | %s…", origem, destino, data_partida)
        voo = _buscar_preco_data(origem, destino, data_partida, trip_class,
                                 somente_direto, ida_e_volta, dias_retorno)
        if voo is None:
            log.info("  Sem preço disponível para %s → %s | %s.", origem, destino, data_partida)
            continue

        salvar_preco(origem, destino, data_partida, voo["preco"], voo["companhia"], "real")
        log.info("  %s → %s | %s | USD %.2f | %s (real)",
                 origem, destino, data_partida, voo["preco"], voo["companhia"])

        chave = f"{origem}→{destino}"
        por_rota.setdefault(chave, []).append({
            "data":      data_partida,
            "preco":     voo["preco"],
            "companhia": voo["companhia"],
            "alerta":    voo["preco"] < alertas.get((origem, destino), float("inf")),
        })
        time.sleep(PAUSA_CHAMADAS)

    if not por_rota:
        log.info("Sem resultados para enviar ao Telegram.")
        return

    linhas = [f"✈️ <b>Monitoramento — {agora}</b>\n"]
    for rota, voos in por_rota.items():
        linhas.append(f"<b>{rota}</b>")
        for v in sorted(voos, key=lambda x: x["preco"]):
            icone = "🔴" if v["alerta"] else "🔵"
            linhas.append(f"{icone} {v['data']}  |  USD {v['preco']:.2f}  |  {v['companhia']}")
        linhas.append("")

    linhas.append("🔴 = abaixo do limite de alerta")
    enviar_telegram("\n".join(linhas))
    log.info("--- Monitoramento horário concluído ---")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    init_db()

    log.info("Iniciando monitor — primeira varredura agora.")
    varredura_diaria()
    monitoramento_horario()

    scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(varredura_diaria,      "cron", hour=7,   minute=0,  id="varredura_diaria")
    scheduler.add_job(monitoramento_horario, "cron", minute=5,             id="monitoramento_horario")

    scheduler.start()
    log.info("Agendador iniciado. Pressione Ctrl+C para encerrar.")
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Monitor encerrado pelo usuário.")


if __name__ == "__main__":
    main()
