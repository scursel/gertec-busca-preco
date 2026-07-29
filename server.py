#!/usr/bin/env python3
"""
Gertec Busca Preço Server
=========================
Servidor TCP + HTTP para terminais de consulta Gertec G2/G2S/G2E.
Substitui o app Java que roda no caixa — consulta preços direto do
ERP WebPosto via API de integração, sem depender de caixa aberto.

Protocolo TCP (porta 6500):
  Handshake:  servidor envia #ok → terminal responde #bpg2s|... ou #bpg2e|...
  Keep-alive: #alwayslive / #live? / #live
  Consulta:   terminal envia #<barcode> → servidor responde #NOME|PRECO ou #nfound
  GIF (G2S):  #gif + header binário + dados (max 124KB, 320×240)
  Mensagem:   #mesg + 2 linhas (somente G2S)

Sync com WebPosto:
  Catálogo:   GRUPO → PRODUTO por grupo (bypass do limite de 2000 itens)
  Preços:     PRODUTO_EMPRESA bulk (limite=2000) + lookup sob demanda
  Intervalos: preços a cada 5 min, catálogo a cada 30 min

Dashboard HTTP (porta 8650):
  Estatísticas, terminais conectados, log de consultas,
  upload de GIFs, editor de mensagens G2E, busca de barcode.
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

import aiohttp
from aiohttp import web

# ─── Configuração ────────────────────────────────────────────────────────────
TCP_PORT = int(os.environ.get("GERT_TCP_PORT", "6500"))
DASH_PORT = int(os.environ.get("GERT_DASH_PORT", "8650"))
SERVER_IP = os.environ.get("GERT_SERVER_IP", "0.0.0.0")

WEBPOSTO_BASE = os.environ.get(
    "WEBPOSTO_BASE_URL", "https://web.qualityautomacao.com.br/INTEGRACAO"
)

# Empresas (códigos numéricos). Cada uma precisa de WEBPOSTO_TOKEN_<codigo> no env.
EMPRESAS = json.loads(os.environ.get("GERT_EMPRESAS", "[1]"))

SYNC_PRICES_INTERVAL = int(os.environ.get("GERT_SYNC_PRICES_SEC", "300"))
SYNC_CATALOG_INTERVAL = int(os.environ.get("GERT_SYNC_CATALOG_SEC", "1800"))
GIF_ROTATION_INTERVAL = int(os.environ.get("GERT_GIF_ROTATION_SEC", "30"))

# Mensagem de boas-vindas para terminais G2S (2 linhas, max 20 chars cada)
WELCOME_LINE1 = os.environ.get("GERT_WELCOME_LINE1", "CONSULTE AQUI!")
WELCOME_LINE2 = os.environ.get("GERT_WELCOME_LINE2", "BEM-VINDO!")

LOG_DIR = Path(os.environ.get("GERT_LOG_DIR", Path(__file__).parent / "logs"))
GIF_DIR = Path(os.environ.get("GERT_GIF_DIR", Path(__file__).parent / "gifs"))

# Credenciais da interface web do terminal G2E (para proxy de mensagens)
G2E_ADMIN_USER = os.environ.get("G2E_ADMIN_USER", "admin")
G2E_ADMIN_PASS = os.environ.get("G2E_ADMIN_PASS", "admin")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "gertec-server.log"),
    ],
)
log = logging.getLogger("gertec")


# ─── Estado ──────────────────────────────────────────────────────────────────
class ServerState:
    def __init__(self):
        self.products = {}       # barcode → {nome, preco, produtoCodigo, empresa, updated_at}
        self.by_codigo = {}      # produtoCodigo → {nome, preco, barcode, barcodes}
        self.last_price_sync = 0
        self.last_catalog_sync = 0
        self.sync_errors = []
        self.http_session = None  # sessão aiohttp compartilhada p/ lookups sob demanda
        self.stats = {
            "total_queries": 0,
            "hits": 0,
            "misses": 0,
            "connections": 0,
            "active_connections": 0,
            "gif_sends": 0,
            "mesg_sends": 0,
            "on_demand_lookups": 0,
            "started_at": time.time(),
        }
        self.query_log = []
        self.connected_terminals = {}
        self.terminal_writers = {}


state = ServerState()


# ─── WebPosto Sync ───────────────────────────────────────────────────────────
def get_token(empresa):
    return os.environ.get(f"WEBPOSTO_TOKEN_{empresa}")


async def sync_catalog(session):
    """Catálogo completo: GRUPO → PRODUTO por grupo.

    A API WebPosto limita PRODUTO a 2000 itens por chamada e ignora o
    parâmetro `codigo` para paginação. Porém, o filtro `grupoCodigo`
    funciona — então buscamos todos os grupos e iteramos sobre eles.
    """
    for empresa in EMPRESAS:
        token = get_token(empresa)
        if not token:
            log.warning(f"Sem token para empresa {empresa}")
            continue
        try:
            # Passo 1: listar grupos
            params = {"CHAVE": token, "empresaCodigo": empresa, "limite": 100}
            async with session.get(
                f"{WEBPOSTO_BASE}/GRUPO",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
            groups = data.get("resultados", []) if isinstance(data, dict) else data
            if not groups:
                log.warning(f"Nenhum grupo encontrado (empresa {empresa})")
                continue

            # Passo 2: buscar produtos por grupo
            all_items = []
            for g in groups:
                gc = g.get("codigo")
                if not gc:
                    continue
                params = {
                    "CHAVE": token,
                    "empresaCodigo": empresa,
                    "limite": 2000,
                    "grupoCodigo": gc,
                }
                async with session.get(
                    f"{WEBPOSTO_BASE}/PRODUTO",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as resp:
                    data = await resp.json()
                if isinstance(data, list) and data and isinstance(data[0], str):
                    log.error(f"Erro PRODUTO (grupo {gc}): {data[0]}")
                    continue
                items = data.get("resultados", []) if isinstance(data, dict) else data
                all_items.extend(items)

            # Deduplicar por produtoCodigo
            seen = set()
            unique_items = []
            for item in all_items:
                pc = item.get("produtoCodigo") or item.get("codigo")
                if pc and pc not in seen:
                    seen.add(pc)
                    unique_items.append(item)

            count = 0
            for item in unique_items:
                barras = item.get("produtoCodigoBarra", [])
                nome = item.get("nome", "PRODUTO")
                pc = item.get("produtoCodigo") or item.get("codigo")
                if barras:
                    for b in barras:
                        bc = b.get("codigoBarra", "")
                        if bc:
                            state.products[bc] = {
                                "nome": nome,
                                "produtoCodigo": pc,
                                "empresa": empresa,
                                "preco": state.products.get(bc, {}).get("preco"),
                                "updated_at": time.time(),
                            }
                            count += 1
                if pc:
                    all_bcs = [
                        b.get("codigoBarra", "")
                        for b in barras
                        if b.get("codigoBarra")
                    ]
                    state.by_codigo[pc] = {
                        "nome": nome,
                        "barcode": all_bcs[0] if all_bcs else None,
                        "barcodes": all_bcs,
                        "preco": state.by_codigo.get(pc, {}).get("preco"),
                    }
            state.last_catalog_sync = time.time()
            log.info(
                f"Sync catálogo: {count} barcodes, {len(unique_items)} produtos, "
                f"{len(groups)} grupos (empresa {empresa})"
            )
        except Exception as e:
            msg = f"Erro sync catálogo (empresa {empresa}): {e}"
            log.error(msg)
            state.sync_errors.append(f"{datetime.now().isoformat()} {msg}")


async def sync_prices(session):
    """Sync de preços via PRODUTO_EMPRESA (bulk, max 2000 itens).

    PRODUTO_EMPRESA também ignora paginação por `codigo`, mas aceita
    `limite=2000`. Para produtos fora desse bulk, o lookup sob demanda
    (fetch_price_on_demand) resolve na hora da consulta.
    """
    for empresa in EMPRESAS:
        token = get_token(empresa)
        if not token:
            continue
        try:
            params = {
                "CHAVE": token,
                "empresaCodigo": empresa,
                "limite": 2000,
                "codigo": 0,
            }
            async with session.get(
                f"{WEBPOSTO_BASE}/PRODUTO_EMPRESA",
                params=params,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                data = await resp.json()
            if isinstance(data, list) and data and isinstance(data[0], str):
                log.error(f"Erro PRODUTO_EMPRESA: {data[0]}")
            else:
                items = data.get("resultados", []) if isinstance(data, dict) else data
                updated = 0
                for item in items:
                    pc = item.get("produtoCodigo")
                    pv = item.get("precoVenda", 0)
                    if not pc or not pv or pv <= 0:
                        continue
                    price = float(pv)
                    if pc in state.by_codigo:
                        state.by_codigo[pc]["preco"] = price
                        for bc in state.by_codigo[pc].get("barcodes", []):
                            if bc and bc in state.products:
                                state.products[bc]["preco"] = price
                                updated += 1
                        bc = state.by_codigo[pc].get("barcode")
                        if (
                            bc
                            and bc in state.products
                            and state.products[bc].get("preco") != price
                        ):
                            state.products[bc]["preco"] = price
                            updated += 1
                    else:
                        state.by_codigo[pc] = {
                            "nome": None,
                            "barcode": None,
                            "preco": price,
                        }
                state.last_price_sync = time.time()
                log.info(
                    f"Sync preços: {len(items)} produtos, "
                    f"{updated} barcodes atualizados (empresa {empresa})"
                )
        except Exception as e:
            msg = f"Erro sync preços (empresa {empresa}): {e}"
            log.error(msg)
            state.sync_errors.append(f"{datetime.now().isoformat()} {msg}")


async def fetch_price_on_demand(produto_codigo, empresa):
    """Busca preço de um produto específico via PRODUTO_EMPRESA?produtoCodigo=X.

    Chamado quando o terminal bipa um produto que está no catálogo mas
    ainda não tem preço no cache (fora do bulk de 2000). Cacheia o
    resultado para consultas subsequentes.
    """
    token = get_token(empresa)
    if not token or not state.http_session:
        return None
    try:
        params = {
            "CHAVE": token,
            "empresaCodigo": empresa,
            "limite": 1,
            "produtoCodigo": produto_codigo,
        }
        async with state.http_session.get(
            f"{WEBPOSTO_BASE}/PRODUTO_EMPRESA",
            params=params,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json()
        items = data.get("resultados", []) if isinstance(data, dict) else data
        if items:
            pv = items[0].get("precoVenda", 0)
            if pv and pv > 0:
                price = float(pv)
                if produto_codigo in state.by_codigo:
                    state.by_codigo[produto_codigo]["preco"] = price
                    for bc in state.by_codigo[produto_codigo].get("barcodes", []):
                        if bc and bc in state.products:
                            state.products[bc]["preco"] = price
                state.stats["on_demand_lookups"] += 1
                log.info(f"Preço sob demanda: produtoCodigo={produto_codigo} → R${price:.2f}")
                return price
    except Exception as e:
        log.warning(f"Lookup sob demanda falhou ({produto_codigo}): {e}")
    return None


async def sync_loop():
    async with aiohttp.ClientSession() as session:
        state.http_session = session
        await sync_catalog(session)
        await sync_prices(session)
        price_counter = 0
        while True:
            await asyncio.sleep(SYNC_PRICES_INTERVAL)
            price_counter += SYNC_PRICES_INTERVAL
            await sync_prices(session)
            if price_counter >= SYNC_CATALOG_INTERVAL:
                await sync_catalog(session)
                price_counter = 0


# ─── Protocolo Gertec ────────────────────────────────────────────────────────
def build_mesg(line1, line2, tempo=5):
    """Monta comando #mesg (2 linhas de texto, somente G2S)."""
    cmd = "#mesg"
    cmd += chr(len(line1) + 48) + line1
    cmd += chr(len(line2) + 48) + line2
    cmd += chr(tempo + 48)
    cmd += chr(48)  # reservado
    return cmd.encode("ascii", errors="replace")


def build_gif_command(gif_data, index=0, loops=0, tempo=10):
    """Monta comando #gif com header binário (somente G2S)."""
    header = b"#gif"
    header += f"{index:02X}".encode()
    header += f"{loops:02X}".encode()
    header += f"{tempo:02X}".encode()
    header += f"{len(gif_data):06X}".encode()
    header += b"0000"
    header += b"\x17"
    return header + gif_data


def format_price_response(nome, preco):
    if preco is not None:
        nome_short = nome[:20] if len(nome) <= 20 else nome[:17] + "..."
        resp = f"#{nome_short}|{preco:.2f}"
    else:
        resp = f"#{nome[:20]}|SEM PRECO"
    return resp.encode("ascii", errors="replace")


async def send_gif_to_terminal(writer, gif_path):
    try:
        gif_data = gif_path.read_bytes()
        if len(gif_data) > 124 * 1024:
            return False
        cmd = build_gif_command(gif_data, index=0, loops=1, tempo=15)
        writer.write(cmd)
        await writer.drain()
        state.stats["gif_sends"] += 1
        return True
    except Exception:
        return False


async def handle_terminal(reader, writer):
    addr = writer.get_extra_info("peername")
    addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"
    state.stats["connections"] += 1
    state.stats["active_connections"] += 1
    state.connected_terminals[addr_str] = {
        "connected_at": datetime.now().isoformat(),
        "queries": 0,
        "model": "unknown",
    }
    state.terminal_writers[addr_str] = writer
    log.info(f"Terminal conectado: {addr_str}")

    try:
        # Handshake
        writer.write(b"#ok")
        await writer.drain()
        await asyncio.sleep(0.5)
        data = await asyncio.wait_for(reader.read(255), timeout=5)
        response = data.decode("ascii", errors="replace").strip("\x00")
        log.info(f"Handshake {addr_str}: {response}")
        state.connected_terminals[addr_str]["model"] = response

        # Keep-alive permanente
        writer.write(b"#alwayslive")
        await writer.drain()
        await asyncio.sleep(0.3)
        data = await asyncio.wait_for(reader.read(255), timeout=5)
        log.info(
            f"Alwayslive {addr_str}: "
            f"{data.decode('ascii', errors='replace').strip(chr(0))}"
        )

        # Detectar modelo: G2E não suporta #mesg/#gif via TCP
        is_g2e = "bpg2e" in response.lower()
        state.connected_terminals[addr_str]["is_g2e"] = is_g2e

        if not is_g2e:
            writer.write(build_mesg(WELCOME_LINE1, WELCOME_LINE2, 5))
            await writer.drain()
            state.stats["mesg_sends"] += 1
            gif_files = sorted(GIF_DIR.glob("*.gif"))
            if gif_files:
                await send_gif_to_terminal(writer, gif_files[0])
        else:
            log.info(f"Terminal {addr_str} é G2E — pulando #mesg/#gif (usar config web)")

        # Loop principal
        while True:
            try:
                data = await asyncio.wait_for(reader.read(255), timeout=120)
            except asyncio.TimeoutError:
                writer.write(b"#live?")
                await writer.drain()
                continue

            if not data:
                log.info(f"Terminal {addr_str} desconectou")
                break

            msg = data.decode("ascii", errors="replace").strip("\x00").strip()
            if not msg:
                continue

            if msg.startswith("#") and not msg.startswith("#live"):
                # Respostas de protocolo — não são consultas
                if msg.startswith(
                    ("#gif_ok", "#img_error", "#mesg_error",
                     "#alwayslive", "#tc406", "#tc502", "#bpg2e")
                ):
                    log.info(f"Resposta protocolo {addr_str}: {msg}")
                    continue

                barcode = msg[1:].strip()
                state.stats["total_queries"] += 1
                state.connected_terminals[addr_str]["queries"] += 1

                product = state.products.get(barcode)
                if product and product.get("preco") is not None:
                    state.stats["hits"] += 1
                    resp = format_price_response(product["nome"], product["preco"])
                    log.info(f"HIT: {barcode} → {product['nome']} R${product['preco']:.2f}")
                elif product:
                    # Produto conhecido mas sem preço — buscar sob demanda
                    pc = product.get("produtoCodigo")
                    emp = product.get("empresa", EMPRESAS[0] if EMPRESAS else 1)
                    price = None
                    if pc:
                        price = await fetch_price_on_demand(pc, emp)
                    if price is not None:
                        product["preco"] = price
                        state.stats["hits"] += 1
                        resp = format_price_response(product["nome"], price)
                        log.info(f"HIT (sob demanda): {barcode} → {product['nome']} R${price:.2f}")
                    else:
                        state.stats["hits"] += 1
                        resp = format_price_response(product["nome"], None)
                        log.info(f"HIT (sem preço): {barcode} → {product['nome']}")
                else:
                    state.stats["misses"] += 1
                    resp = b"#nfound"
                    log.info(f"MISS: {barcode}")

                state.query_log.append({
                    "time": datetime.now().isoformat(),
                    "barcode": barcode,
                    "terminal": addr_str,
                    "hit": product is not None,
                    "nome": product["nome"] if product else None,
                    "preco": product.get("preco") if product else None,
                })
                if len(state.query_log) > 500:
                    state.query_log = state.query_log[-200:]

                writer.write(resp)
                await writer.drain()

            elif msg == "#live":
                pass
            else:
                log.debug(f"Terminal {addr_str} enviou: {msg}")

    except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
        log.info(f"Conexão perdida: {addr_str}")
    except Exception as e:
        log.error(f"Erro terminal {addr_str}: {e}")
    finally:
        state.stats["active_connections"] -= 1
        state.connected_terminals.pop(addr_str, None)
        state.terminal_writers.pop(addr_str, None)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


# ─── Rotação de GIFs (propaganda) ───────────────────────────────────────────
async def gif_rotation_loop():
    log.info("Loop de propaganda iniciado (scaneia gifs/ a cada ciclo)")
    gif_index = 0
    while True:
        await asyncio.sleep(GIF_ROTATION_INTERVAL)
        if not state.terminal_writers:
            continue
        gif_files = sorted(GIF_DIR.glob("*.gif"))
        if not gif_files:
            continue
        gif_path = gif_files[gif_index % len(gif_files)]
        gif_index += 1
        for addr_str, w in list(state.terminal_writers.items()):
            info = state.connected_terminals.get(addr_str, {})
            if info.get("is_g2e"):
                continue  # G2E não suporta #gif via TCP
            try:
                await send_gif_to_terminal(w, gif_path)
            except Exception:
                pass


# ─── Dashboard HTTP ──────────────────────────────────────────────────────────
async def dashboard_index(request):
    uptime = time.time() - state.stats["started_at"]
    hours = int(uptime // 3600)
    mins = int((uptime % 3600) // 60)
    server_ip = SERVER_IP if SERVER_IP != "0.0.0.0" else "localhost"

    terminals_html = ""
    for addr, info in state.connected_terminals.items():
        terminals_html += (
            f"<tr><td>{addr}</td><td>{info['model']}</td>"
            f"<td>{info['connected_at']}</td><td>{info['queries']}</td></tr>"
        )

    queries_html = ""
    for q in reversed(state.query_log[-50:]):
        status = "HIT" if q["hit"] else "MISS"
        preco = f"R$ {q['preco']:.2f}" if q.get("preco") else "-"
        queries_html += (
            f"<tr><td>{q['time'][11:19]}</td><td>{q['barcode']}</td>"
            f"<td>{q.get('nome') or '-'}</td><td>{preco}</td>"
            f"<td>{status}</td><td>{q['terminal']}</td></tr>"
        )

    errors_html = ""
    for e in state.sync_errors[-10:]:
        errors_html += f"<li>{e}</li>"

    last_price = (
        datetime.fromtimestamp(state.last_price_sync).strftime("%H:%M:%S")
        if state.last_price_sync
        else "nunca"
    )
    last_catalog = (
        datetime.fromtimestamp(state.last_catalog_sync).strftime("%H:%M:%S")
        if state.last_catalog_sync
        else "nunca"
    )

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Gertec Busca Preço — Dashboard</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'JetBrains Mono', 'Fira Code', monospace; background: #0d1117; color: #c9d1d9; padding: 20px; }}
h1 {{ color: #58a6ff; margin-bottom: 5px; font-size: 1.4em; }}
.subtitle {{ color: #8b949e; margin-bottom: 20px; font-size: 0.85em; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin-bottom: 25px; }}
.card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; }}
.card .label {{ color: #8b949e; font-size: 0.75em; text-transform: uppercase; }}
.card .value {{ color: #f0f6fc; font-size: 1.8em; font-weight: bold; margin-top: 5px; }}
.card .value.green {{ color: #3fb950; }}
.card .value.red {{ color: #f85149; }}
.card .value.blue {{ color: #58a6ff; }}
table {{ width: 100%; border-collapse: collapse; background: #161b22; border-radius: 8px; overflow: hidden; margin-bottom: 25px; }}
th {{ background: #21262d; color: #8b949e; text-align: left; padding: 10px 12px; font-size: 0.75em; text-transform: uppercase; }}
td {{ padding: 8px 12px; border-top: 1px solid #21262d; font-size: 0.85em; }}
tr:hover td {{ background: #1c2128; }}
h2 {{ color: #c9d1d9; margin: 20px 0 10px; font-size: 1.1em; }}
.panel {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; margin-bottom: 25px; }}
.panel li {{ font-size: 0.8em; margin: 5px 0; list-style: none; }}
.setup {{ background: #111d2e; border: 1px solid #1f6feb; border-radius: 8px; padding: 18px; margin: 0 0 25px; }}
.setup h2 {{ color: #79c0ff; margin: 0 0 8px; }}
.setup-intro {{ color: #c9d1d9; font-size: 0.85em; margin-bottom: 14px; }}
.setup-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 10px; margin-bottom: 14px; }}
.setup-item {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 10px 12px; }}
.setup-item .label {{ color: #8b949e; display: block; font-size: 0.72em; margin-bottom: 5px; text-transform: uppercase; }}
.setup code {{ color: #f0f6fc; font-size: 1.05em; }}
.setup-warning {{ background: #3d2b0b; border-left: 3px solid #d29922; color: #f2cc60; padding: 10px 12px; font-size: 0.82em; margin-bottom: 10px; }}
.setup-note {{ color: #8b949e; font-size: 0.78em; }}
</style>
</head>
<body>
<h1>Gertec Busca Preço — Server</h1>
<p class="subtitle">Uptime: {hours}h {mins}m | TCP {TCP_PORT} / HTTP {DASH_PORT}</p>

<section class="setup">
<h2>Como configurar o terminal Gertec</h2>
<p class="setup-intro">No menu de rede do terminal, configure o servidor de consulta:</p>
<div class="setup-grid">
<div class="setup-item"><span class="label">IP do servidor</span><code>{server_ip}</code></div>
<div class="setup-item"><span class="label">Porta TCP</span><code>{TCP_PORT}</code></div>
<div class="setup-item"><span class="label">Endereço completo</span><code>{server_ip}:{TCP_PORT}</code></div>
</div>
<div class="setup-warning"><strong>Importante:</strong> não use a porta {DASH_PORT} no terminal. Ela é somente o dashboard HTTP. O terminal usa TCP puro na porta {TCP_PORT}.</div>
<p class="setup-note">Dashboard no navegador: <code>http://{server_ip}:{DASH_PORT}</code></p>
</section>

<div class="grid">
<div class="card"><div class="label">Consultas</div><div class="value blue">{state.stats['total_queries']}</div></div>
<div class="card"><div class="label">Hits</div><div class="value green">{state.stats['hits']}</div></div>
<div class="card"><div class="label">Misses</div><div class="value red">{state.stats['misses']}</div></div>
<div class="card"><div class="label">Terminais Ativos</div><div class="value">{state.stats['active_connections']}</div></div>
<div class="card"><div class="label">Barcodes em Cache</div><div class="value">{len(state.products)}</div></div>
<div class="card"><div class="label">Com Preço</div><div class="value">{sum(1 for v in state.by_codigo.values() if v.get('preco') and v['preco'] > 0)}</div></div>
<div class="card"><div class="label">Último Sync Preço</div><div class="value" style="font-size:1.2em">{last_price}</div></div>
<div class="card"><div class="label">Lookups Sob Demanda</div><div class="value">{state.stats['on_demand_lookups']}</div></div>
</div>

<h2>Terminais Conectados</h2>
<table><tr><th>Endereço</th><th>Modelo</th><th>Conectado</th><th>Consultas</th></tr>
{terminals_html or '<tr><td colspan="4" style="color:#8b949e">Nenhum terminal conectado</td></tr>'}
</table>

<h2>Últimas Consultas</h2>
<table><tr><th>Hora</th><th>Barcode</th><th>Produto</th><th>Preço</th><th>Status</th><th>Terminal</th></tr>
{queries_html or '<tr><td colspan="6" style="color:#8b949e">Nenhuma consulta ainda</td></tr>'}
</table>

<h2>Sync / Erros</h2>
<div class="panel"><ul>
<li style="color:#3fb950">Catálogo: {last_catalog} ({len(state.products)} barcodes)</li>
<li style="color:#3fb950">Preços: {last_price}</li>
{errors_html or '<li style="color:#8b949e">Sem erros</li>'}
</ul></div>

<h2>Propaganda (GIFs) — somente G2S</h2>
<div class="panel" id="gif-panel">
<p style="color:#8b949e;font-size:0.8em;margin-bottom:10px">GIFs são exibidos em terminais <strong>G2S</strong> quando ociosos. O G2E não suporta GIF via TCP. Máx 124KB (320×240).</p>
<div id="gif-list" style="margin-bottom:10px"></div>
<form id="gif-upload" style="display:flex;gap:10px;align-items:center">
<input type="file" name="gif" accept=".gif" style="font-size:0.8em">
<button type="submit" style="background:#238636;color:#fff;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:0.8em">Upload</button>
</form>
</div>

<h2>Mensagens Idle — G2E</h2>
<div class="panel" id="g2e-panel">
<p style="color:#8b949e;font-size:0.8em;margin-bottom:10px">4 linhas de texto exibidas no G2E quando ocioso. Enviado via interface web do terminal.</p>
<div id="g2e-form" style="display:grid;gap:8px;max-width:400px">
<div><label style="color:#8b949e;font-size:0.75em">Linha 1 (máx 20)</label><input id="g2e-l1" maxlength="20" style="width:100%;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px"></div>
<div><label style="color:#8b949e;font-size:0.75em">Linha 2</label><input id="g2e-l2" maxlength="20" style="width:100%;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px"></div>
<div><label style="color:#8b949e;font-size:0.75em">Linha 3</label><input id="g2e-l3" maxlength="20" style="width:100%;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px"></div>
<div><label style="color:#8b949e;font-size:0.75em">Linha 4</label><input id="g2e-l4" maxlength="20" style="width:100%;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px"></div>
<div style="display:flex;gap:10px;align-items:center">
<label style="color:#8b949e;font-size:0.75em">Tempo (s)</label><input id="g2e-tempo" type="number" min="1" max="99" value="3" style="width:60px;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px">
<label style="color:#8b949e;font-size:0.75em"><input id="g2e-logo" type="checkbox" checked> Logo</label>
</div>
<button onclick="saveG2E()" style="background:#1f6feb;color:#fff;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;font-size:0.85em">Salvar no Terminal</button>
<span id="g2e-status" style="font-size:0.8em"></span>
</div>
</div>

<h2>Diagnóstico — Buscar Barcode</h2>
<div class="panel">
<div style="display:flex;gap:10px;align-items:center">
<input id="lookup-bc" placeholder="Código de barras" style="flex:1;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px">
<button onclick="doLookup()" style="background:#8957e5;color:#fff;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:0.8em">Buscar</button>
</div>
<pre id="lookup-result" style="color:#8b949e;font-size:0.8em;margin-top:10px;white-space:pre-wrap"></pre>
</div>

<script>
setTimeout(() => location.reload(), 10000);
async function loadGifs() {{
  const r = await fetch('/gifs');
  const d = await r.json();
  const el = document.getElementById('gif-list');
  if (!d.gifs.length) {{ el.innerHTML = '<span style="color:#8b949e;font-size:0.8em">Nenhum GIF. Coloque arquivos em gifs/ ou use o upload.</span>'; return; }}
  el.innerHTML = d.gifs.map(g => `<span style="display:inline-block;background:#21262d;padding:4px 10px;border-radius:4px;margin:3px;font-size:0.8em">${{g.name}} (${{(g.size/1024).toFixed(1)}}KB) <a href="#" onclick="delGif('${{g.name}}');return false" style="color:#f85149;margin-left:6px">x</a></span>`).join('');
}}
async function delGif(name) {{
  await fetch('/gifs/' + name, {{method:'DELETE'}});
  loadGifs();
}}
document.getElementById('gif-upload').onsubmit = async (e) => {{
  e.preventDefault();
  const fd = new FormData();
  const f = e.target.querySelector('input').files[0];
  if (!f) return;
  fd.append('gif', f);
  const r = await fetch('/upload-gif', {{method:'POST', body:fd}});
  const d = await r.json();
  if (d.error) alert(d.error);
  else {{ e.target.reset(); loadGifs(); }}
}};
loadGifs();
async function loadG2E() {{
  try {{
    const r = await fetch('/g2e/messages');
    if (!r.ok) return;
    const d = await r.json();
    if (d.error) {{ document.getElementById('g2e-status').textContent = 'Erro: ' + d.error; return; }}
    document.getElementById('g2e-l1').value = d.linha1 || '';
    document.getElementById('g2e-l2').value = d.linha2 || '';
    document.getElementById('g2e-l3').value = d.linha3 || '';
    document.getElementById('g2e-l4').value = d.linha4 || '';
    document.getElementById('g2e-tempo').value = d.tempo || 3;
    document.getElementById('g2e-logo').checked = d.logo !== false;
  }} catch(e) {{}}
}}
async function saveG2E() {{
  const st = document.getElementById('g2e-status');
  st.textContent = 'Salvando...';
  st.style.color = '#d29922';
  try {{
    const r = await fetch('/g2e/messages', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{
        linha1: document.getElementById('g2e-l1').value,
        linha2: document.getElementById('g2e-l2').value,
        linha3: document.getElementById('g2e-l3').value,
        linha4: document.getElementById('g2e-l4').value,
        tempo: parseInt(document.getElementById('g2e-tempo').value) || 3,
        logo: document.getElementById('g2e-logo').checked,
      }})
    }});
    const d = await r.json();
    if (d.ok) {{ st.textContent = 'Salvo no terminal ' + (d.terminal||''); st.style.color = '#3fb950'; }}
    else {{ st.textContent = 'Erro: ' + (d.error||'desconhecido'); st.style.color = '#f85149'; }}
  }} catch(e) {{ st.textContent = 'Erro: ' + e; st.style.color = '#f85149'; }}
}}
loadG2E();
async function doLookup() {{
  const bc = document.getElementById('lookup-bc').value.trim();
  if (!bc) return;
  const r = await fetch('/lookup?barcode=' + encodeURIComponent(bc));
  const d = await r.json();
  document.getElementById('lookup-result').textContent = JSON.stringify(d, null, 2);
}}
</script>
</body></html>"""
    return web.Response(text=html, content_type="text/html")


async def dashboard_api(request):
    return web.json_response({
        "stats": state.stats,
        "cache": {
            "products": len(state.products),
            "by_codigo": len(state.by_codigo),
            "with_price": sum(
                1 for v in state.by_codigo.values() if v.get("preco") and v["preco"] > 0
            ),
            "last_price_sync": state.last_price_sync,
            "last_catalog_sync": state.last_catalog_sync,
        },
        "terminals": state.connected_terminals,
        "recent_queries": state.query_log[-20:],
        "errors": state.sync_errors[-5:],
    })


async def dashboard_upload_gif(request):
    reader = await request.multipart()
    field = await reader.next()
    if field is None or field.name != "gif":
        return web.json_response({"error": "Campo 'gif' obrigatório"}, status=400)
    filename = field.filename or "propaganda.gif"
    if not filename.lower().endswith(".gif"):
        return web.json_response({"error": "Apenas arquivos .gif"}, status=400)
    data = await field.read(decode=False)
    if len(data) > 124 * 1024:
        return web.json_response(
            {"error": f"GIF muito grande ({len(data)} bytes). Máximo: 124KB"},
            status=400,
        )
    safe_name = "".join(c for c in filename if c.isalnum() or c in "._-")
    dest = GIF_DIR / safe_name
    dest.write_bytes(data)
    log.info(f"GIF upload: {safe_name} ({len(data)} bytes)")
    return web.json_response({"ok": True, "file": safe_name, "size": len(data)})


async def dashboard_list_gifs(request):
    gifs = [{"name": f.name, "size": f.stat().st_size} for f in sorted(GIF_DIR.glob("*.gif"))]
    return web.json_response({"gifs": gifs, "dir": str(GIF_DIR)})


async def dashboard_delete_gif(request):
    name = request.match_info.get("name", "")
    safe_name = "".join(c for c in name if c.isalnum() or c in "._-")
    path = GIF_DIR / safe_name
    if path.exists() and path.suffix == ".gif":
        path.unlink()
        log.info(f"GIF removido: {safe_name}")
        return web.json_response({"ok": True})
    return web.json_response({"error": "Não encontrado"}, status=404)


# ─── Proxy de mensagens G2E ─────────────────────────────────────────────────
async def g2e_get_messages(request):
    """Lê mensagens idle da interface web de um terminal G2E."""
    terminal_ip = request.query.get("ip", "")
    if not terminal_ip:
        for addr, info in state.connected_terminals.items():
            if info.get("is_g2e"):
                terminal_ip = addr.split(":")[0]
                break
    if not terminal_ip:
        return web.json_response({"error": "Nenhum terminal G2E conectado"}, status=404)
    try:
        async with aiohttp.ClientSession() as session:
            auth = aiohttp.BasicAuth(G2E_ADMIN_USER, G2E_ADMIN_PASS)
            async with session.get(
                f"http://{terminal_ip}/mensagens",
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                html = await resp.text()
        lines = {}
        for i in range(1, 5):
            m = re.search(rf'name="Linha{i}"\s+value="([^"]*)"', html)
            lines[f"linha{i}"] = m.group(1) if m else ""
        m = re.search(r'name="Texib"[^>]*value="(\d+)"', html)
        lines["tempo"] = int(m.group(1)) if m else 3
        m = re.search(r'name="Logo"\s+value="1"\s+checked', html)
        lines["logo"] = bool(m)
        lines["terminal_ip"] = terminal_ip
        return web.json_response(lines)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)


async def g2e_set_messages(request):
    """Salva mensagens idle em um terminal G2E via interface web."""
    data = await request.json()
    terminal_ip = data.get("ip", "")
    if not terminal_ip:
        for addr, info in state.connected_terminals.items():
            if info.get("is_g2e"):
                terminal_ip = addr.split(":")[0]
                break
    if not terminal_ip:
        return web.json_response({"error": "Nenhum terminal G2E conectado"}, status=404)
    form = {
        "Linha1": data.get("linha1", "")[:20],
        "Linha2": data.get("linha2", "")[:20],
        "Linha3": data.get("linha3", "")[:20],
        "Linha4": data.get("linha4", "")[:20],
        "Texib": str(data.get("tempo", 3)),
        "Logo": "1" if data.get("logo", True) else "0",
    }
    try:
        async with aiohttp.ClientSession() as session:
            auth = aiohttp.BasicAuth(G2E_ADMIN_USER, G2E_ADMIN_PASS)
            async with session.post(
                f"http://{terminal_ip}/salvamensagens",
                data=form,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                status = resp.status
        log.info(f"Mensagens G2E salvas em {terminal_ip}: {form}")
        return web.json_response({"ok": True, "terminal": terminal_ip, "http_status": status})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)


async def dashboard_lookup(request):
    barcode = request.query.get("barcode", "").strip()
    if not barcode:
        return web.json_response({"error": "Parâmetro 'barcode' obrigatório"}, status=400)
    product = state.products.get(barcode)
    if product:
        return web.json_response({"found": True, "barcode": barcode, **product})
    by_code = state.by_codigo.get(barcode)
    if by_code:
        return web.json_response({"found": True, "type": "by_codigo", "codigo": barcode, **by_code})
    return web.json_response({
        "found": False,
        "barcode": barcode,
        "total_products": len(state.products),
        "total_by_codigo": len(state.by_codigo),
    })


# ─── Main ────────────────────────────────────────────────────────────────────
async def main():
    LOG_DIR.mkdir(exist_ok=True)
    GIF_DIR.mkdir(exist_ok=True)

    log.info(f"Iniciando Gertec Busca Preço Server — TCP:{TCP_PORT} HTTP:{DASH_PORT}")
    log.info(f"Empresas: {EMPRESAS}")

    tcp_server = await asyncio.start_server(handle_terminal, "0.0.0.0", TCP_PORT)
    log.info(f"TCP server em 0.0.0.0:{TCP_PORT}")

    asyncio.create_task(sync_loop())
    asyncio.create_task(gif_rotation_loop())

    app = web.Application()
    app.router.add_get("/", dashboard_index)
    app.router.add_get("/api", dashboard_api)
    app.router.add_post("/upload-gif", dashboard_upload_gif)
    app.router.add_get("/gifs", dashboard_list_gifs)
    app.router.add_delete("/gifs/{name}", dashboard_delete_gif)
    app.router.add_get("/g2e/messages", g2e_get_messages)
    app.router.add_post("/g2e/messages", g2e_set_messages)
    app.router.add_get("/lookup", dashboard_lookup)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", DASH_PORT)
    await site.start()
    log.info(f"Dashboard em http://0.0.0.0:{DASH_PORT}")

    async with tcp_server:
        await tcp_server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
