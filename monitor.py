"""
Monitor de Preços de Passagens Aéreas
Usa a SerpAPI (Google Flights) + alertas via Telegram.
"""

import json
import os
import sqlite3
import logging
import time
import requests
import serpapi
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

SERPAPI_KEY        = _cfg["serpapi_key"]
TELEGRAM_BOT_TOKEN = _cfg.get("telegram_bot_token", "")
TELEGRAM_CHAT_ID   = _cfg.get("telegram_chat_id",   "")

DB_PATH        = _cfg.get("db_path",       "passagens.db")
LOG_PATH       = _cfg.get("log_path",      "monitor.log")
TOP_DATAS      = _cfg.get("top_datas",     5)
SCAN_HORIZONTE = _cfg.get("scan_horizonte", 4)   # meses à frente

ROTAS = _cfg["rotas"]

# SerpAPI: travel_class values
_CABINE_MAP = {"economy": 1, "premium_economy": 2, "business": 3, "first": 4}

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
    # Detecta schema antigo (coluna preco_usd) e recria a tabela
    cols = {row[1] for row in conn.execute("PRAGMA table_info(precos)")}
    if cols and "preco_usd" in cols:
        log.warning("Schema antigo detectado (preco_usd). Recriando tabela precos…")
        conn.execute("DROP TABLE precos")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS precos (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            capturado_em TEXT NOT NULL,
            origem       TEXT NOT NULL,
            destino      TEXT NOT NULL,
            data_partida TEXT NOT NULL,
            preco_brl    REAL NOT NULL,
            companhia    TEXT NOT NULL DEFAULT '',
            paradas      INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    log.info("Banco de dados inicializado: %s", DB_PATH)


def salvar_preco(origem, destino, data_partida, preco_brl, companhia, paradas):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO precos (capturado_em, origem, destino, data_partida, preco_brl, companhia, paradas) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (datetime.now().isoformat(timespec="seconds"), origem, destino,
         data_partida, preco_brl, companhia, paradas),
    )
    conn.commit()
    conn.close()

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
    except BaseException as exc:
        log.warning("Falha ao enviar Telegram: %s", exc)

# ---------------------------------------------------------------------------
# SERPAPI — Google Flights
# ---------------------------------------------------------------------------

def _sabados_do_mes(primeiro_dia: date) -> list[date]:
    """Retorna os 4 primeiros sábados do mês."""
    sabados = []
    d = primeiro_dia
    while d.weekday() != 5:   # 5 = sábado
        d += timedelta(days=1)
    for _ in range(4):
        if d.month == primeiro_dia.month:
            sabados.append(d)
        d += timedelta(days=7)
    return sabados


def _buscar_economy_mes(origem: str, destino: str, mes_inicio: date, mes_fim: date,
                        ida_e_volta: bool = True) -> list[dict]:
    """
    Economy: usa google_travel_explore (1 chamada por mês).
    Retorna lista de {"mes", "preco_brl", "companhia", "paradas", "link"}.
    """
    params = {
        "engine":        "google_travel_explore",
        "departure_id":  origem,
        "arrival_id":    destino,
        "outbound_date": mes_inicio.strftime("%Y-%m-%d"),
        "return_date":   mes_fim.strftime("%Y-%m-%d"),
        "travel_class":  1,
        "currency":      "BRL",
        "hl":            "pt-BR",
        "api_key":       SERPAPI_KEY,
    }
    if not ida_e_volta:
        params["type"] = 2

    try:
        results = serpapi.search(params)
    except Exception as exc:
        log.error("Erro SerpAPI economy (%s→%s) %s: %s",
                  origem, destino, mes_inicio.strftime("%Y-%m"), exc)
        return []

    google_link = results.get("search_metadata", {}).get("google_travel_explore_url", "")
    resultados = []
    for v in results.get("flights", []):
        try:
            preco_brl = float(v["price"])
        except (KeyError, TypeError, ValueError):
            continue
        resultados.append({
            "mes":       mes_inicio.strftime("%Y-%m"),
            "preco_brl": preco_brl,
            "companhia": v.get("airline", "—") or "—",
            "paradas":   int(v.get("number_of_stops", 0)),
            "link":      google_link,
        })

    resultados.sort(key=lambda x: x["preco_brl"])
    return resultados


def _buscar_business_data(origem: str, destino: str, data_ida: date, travel_class: int,
                          ida_e_volta: bool = True, dias_retorno: int = 7) -> list[dict]:
    """
    Business/First: usa google_flights em uma data específica.
    Retorna lista de {"mes", "preco_brl", "companhia", "paradas", "link"}.
    """
    params = {
        "engine":        "google_flights",
        "departure_id":  origem,
        "arrival_id":    destino,
        "outbound_date": data_ida.strftime("%Y-%m-%d"),
        "travel_class":  travel_class,
        "currency":      "BRL",
        "hl":            "pt-BR",
        "type":          1 if ida_e_volta else 2,
        "api_key":       SERPAPI_KEY,
    }
    if ida_e_volta:
        params["return_date"] = (data_ida + timedelta(days=dias_retorno)).strftime("%Y-%m-%d")

    try:
        results = serpapi.search(params)
    except Exception as exc:
        log.error("Erro SerpAPI business (%s→%s) %s: %s",
                  origem, destino, data_ida, exc)
        return []

    google_link = results.get("search_metadata", {}).get("google_flights_url", "")
    voos_raw = results.get("best_flights", []) + results.get("other_flights", [])

    resultados = []
    for v in voos_raw:
        try:
            preco_brl = float(v["price"])
        except (KeyError, TypeError, ValueError):
            continue
        flights   = v.get("flights", [{}])
        companhia = flights[0].get("airline", "—") if flights else "—"
        paradas   = len(v.get("layovers", []))
        resultados.append({
            "mes":       data_ida.strftime("%Y-%m"),
            "preco_brl": preco_brl,
            "companhia": companhia,
            "paradas":   paradas,
            "link":      google_link,
        })

    resultados.sort(key=lambda x: x["preco_brl"])
    return resultados


def _buscar_voos(origem: str, destino: str, travel_class: int = 1,
                 ida_e_volta: bool = True, dias_retorno: int = 7) -> list[dict]:
    """
    Economy → google_travel_explore (1 chamada/mês).
    Business/First → google_flights nos 4 sábados de cada mês.
    Retorna lista combinada ordenada por preço.
    """
    hoje = date.today()
    todos = []

    for i in range(SCAN_HORIZONTE):
        primeiro = (hoje.replace(day=1) + timedelta(days=32 * i)).replace(day=1)
        mes_str  = primeiro.strftime("%Y-%m")

        if travel_class == 1:   # economy
            if primeiro.month == 12:
                ultimo = primeiro.replace(year=primeiro.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                ultimo = primeiro.replace(month=primeiro.month + 1, day=1) - timedelta(days=1)
            # Garante que outbound_date não seja data passada
            inicio_efetivo = max(primeiro, hoje + timedelta(days=1))
            if inicio_efetivo > ultimo:
                continue
            log.info("  %s → %s | %s (economy)…", origem, destino, mes_str)
            todos.extend(_buscar_economy_mes(origem, destino, inicio_efetivo, ultimo, ida_e_volta))
        else:                   # business / first
            for sabado in _sabados_do_mes(primeiro):
                if sabado <= hoje:
                    continue
                log.info("  %s → %s | %s business…", origem, destino, sabado)
                todos.extend(_buscar_business_data(
                    origem, destino, sabado, travel_class, ida_e_volta=ida_e_volta,
                    dias_retorno=dias_retorno))

    todos.sort(key=lambda x: x["preco_brl"])
    return todos

# ---------------------------------------------------------------------------
# VERIFICAÇÃO DE ROTAS (roda a cada 6h)
# ---------------------------------------------------------------------------

def verificar_rotas():
    """Consulta todas as rotas e envia resumo via Telegram. Roda a cada 6h."""
    log.info("=== Verificando rotas ===")
    agora  = datetime.now().strftime("%d/%m/%Y %H:%M")
    linhas = [f"✈️ <b>Monitor de Passagens — {agora}</b>\n"]
    encontrou = False

    for rota in ROTAS:
        origem, destino = rota["origem"], rota["destino"]
        alerta_brl   = rota.get("alerta_preco", float("inf"))
        travel_class = _CABINE_MAP.get(rota.get("cabine", "economy"), 1)
        ida_e_volta  = rota.get("ida_e_volta", True)
        log.info("  Buscando %s → %s (cabine: %s, %s)…",
                 origem, destino, rota.get("cabine", "economy"),
                 "ida e volta" if ida_e_volta else "só ida")

        todos = _buscar_voos(origem, destino, travel_class, ida_e_volta)

        if not todos:
            log.warning("Sem resultados para %s → %s.", origem, destino)
            continue

        top = todos[:TOP_DATAS]

        linhas.append(f"<b>{origem} → {destino}</b>")
        for v in top:
            salvar_preco(origem, destino, v["mes"], v["preco_brl"],
                         v["companhia"], v["paradas"])
            icone   = "🔴" if v["preco_brl"] < alerta_brl else "🔵"
            paradas = "direto" if v["paradas"] == 0 else f"{v['paradas']} parada(s)"
            link    = v["link"]
            linhas.append(
                f'{icone} {v["mes"]}  |  R$ {v["preco_brl"]:,.0f}  |  '
                f'{v["companhia"]} ({paradas})  '
                f'<a href="{link}">🛒 comprar</a>'
            )
            log.info("%s → %s | %s | R$ %.2f | %s (%d paradas)",
                     origem, destino, v["mes"], v["preco_brl"],
                     v["companhia"], v["paradas"])
        linhas.append("")
        encontrou = True

    if encontrou:
        linhas.append("🔴 = abaixo do limite de alerta")
        enviar_telegram("\n".join(linhas))
    else:
        log.info("Sem resultados para enviar ao Telegram.")

    log.info("=== Verificação concluída ===")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    init_db()

    log.info("Iniciando monitor — primeira verificação agora.")
    verificar_rotas()

    scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(verificar_rotas, "interval", hours=6, id="verificar_rotas")

    scheduler.start()
    log.info("Agendador iniciado (a cada 6h). Pressione Ctrl+C para encerrar.")
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Monitor encerrado pelo usuário.")


if __name__ == "__main__":
    main()
