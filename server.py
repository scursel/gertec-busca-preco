#!/usr/bin/env python3
"""
Gertec Busca Preco G2S Server
Servidor TCP (porta 6500) para terminais de consulta Gertec G2/G2S.
Substitui o app Java que roda no caixa.
"""

import asyncio
import ipaddress
import json
import logging
import os
import re
import sys
import textwrap
import time
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
from aiohttp import web

# === CONFIG ===
TCP_PORT = 6500
DASH_PORT = int(os.environ.get("GERT_DASH_PORT", "8650"))
SERVER_IP = os.environ.get("GERT_SERVER_IP", "192.168.1.113")
WEBPOSTO_BASE = "https://web.qualityautomacao.com.br/INTEGRACAO"
SYNC_PRICES_INTERVAL = 300
SYNC_CATALOG_INTERVAL = 1800
GIF_ROTATION_INTERVAL = 30
QUERY_LOG_HISTORY_LIMIT = 500
LOG_DIR = Path(__file__).parent / "logs"
GIF_DIR = Path(__file__).parent / "gifs"
EMPRESAS = json.loads(os.environ.get("GERT_EMPRESAS", "[7806]"))  # universo permitido de empresas
DEFAULT_LEITORES_CONFIG = Path(__file__).parent / "config" / "leitores.json"
PRICE_TTL = int(os.environ.get("GERT_PRICE_TTL", "3600"))  # idade a partir da qual o preço é revalidado
PRICE_STALE_MAX = int(os.environ.get("GERT_PRICE_STALE_MAX", "21600"))  # idade máxima de preço stale servido com API fora
NO_PRICE_CACHE_TTL = 3600  # cache negativo por (produtoCodigo, empresa)
PROGRESSIVE_DELAY = 0.1  # pausa entre chamadas do sync progressivo
TERMINAL_NAO_CONFIGURADO = "TERMINAL NAO CONFIGURADO"
CATALOGO_INDISPONIVEL = "CATALOGO INDISPONIVEL"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "gertec-server.log"),
    ],
)
log = logging.getLogger("gertec")


_LOG_LINE_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) "
    r"\[INFO\] (?P<message>.*)$"
)
_HIT_QUERY_RE = re.compile(
    r"^HIT(?: \([^)]*\))?: (?P<barcode>\S+) -> "
    r"(?P<nome>.*?)(?: R\$(?P<preco>\d+(?:\.\d+)?))?$"
)
_TERMINAL_END_RE = re.compile(
    r"^Terminal (?P<terminal>.+?) (?:disconnected|connection lost)$"
)


def _parse_query_log_line(line, terminal="histórico"):
    """Convert one persisted HIT/MISS line into a dashboard query entry."""
    match = _LOG_LINE_RE.match(line.rstrip("\n"))
    if not match:
        return None

    timestamp = datetime.strptime(
        match.group("timestamp"), "%Y-%m-%d %H:%M:%S,%f"
    ).isoformat(timespec="milliseconds")
    message = match.group("message")

    if message.startswith("MISS: "):
        return {
            "time": timestamp,
            "barcode": message[6:].strip(),
            "terminal": terminal,
            "hit": False,
            "nome": None,
            "preco": None,
        }

    hit = _HIT_QUERY_RE.match(message)
    if not hit:
        return None

    preco = hit.group("preco")
    return {
        "time": timestamp,
        "barcode": hit.group("barcode"),
        "terminal": terminal,
        "hit": True,
        "nome": hit.group("nome").strip(),
        "preco": float(preco) if preco else None,
    }


def load_query_history():
    """Load query events already written to the text log after a restart."""
    log_path = LOG_DIR / "gertec-server.log"
    if not log_path.exists():
        return []

    history = []
    active_terminals = []
    try:
        with log_path.open(encoding="utf-8", errors="replace") as logfile:
            for line in logfile:
                parsed_line = _LOG_LINE_RE.match(line.rstrip("\n"))
                if not parsed_line:
                    continue
                message = parsed_line.group("message")

                if message.startswith("Terminal connected: "):
                    terminal = message.split(": ", 1)[1]
                    if terminal not in active_terminals:
                        active_terminals.append(terminal)
                    continue

                ended = _TERMINAL_END_RE.match(message)
                if ended:
                    terminal = ended.group("terminal")
                    if terminal in active_terminals:
                        active_terminals.remove(terminal)
                    continue

                terminal = active_terminals[-1] if active_terminals else "histórico"
                query = _parse_query_log_line(line, terminal)
                if query:
                    history.append(query)
    except OSError as exc:
        log.warning(f"Could not load query history: {exc}")

    return history


# === LEITOR CONFIG ===
class ConfigInvalidaError(Exception):
    """Configuração de leitores (IP -> empresa) inválida; o servidor não deve subir."""

    def __init__(self, path, erros):
        self.path = str(path)
        self.erros = list(erros)
        super().__init__(f"Config de leitores inválida ({self.path}): " + "; ".join(self.erros))


_LEITOR_KEYS = {"empresa", "nome"}


def leitores_config_path():
    return Path(os.environ.get("GERT_LEITORES_CONFIG") or DEFAULT_LEITORES_CONFIG)


def _empresas_permitidas():
    permitidas = set()
    for empresa in EMPRESAS:
        try:
            permitidas.add(int(empresa))
        except (TypeError, ValueError):
            continue
    return permitidas


def _validar_leitor_config(path, environ=None):
    """Valida a config de leitores.

    Retorna (config, erros). ``config`` traz os leitores que puderam ser lidos
    (útil para o resumo do --check-config) mesmo quando há erros.
    """
    environ = os.environ if environ is None else environ
    erros = []
    leitores = {}
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"versao": None, "leitores": {}}, [f"arquivo não encontrado: {path}"]
    except (OSError, UnicodeDecodeError) as exc:
        return {"versao": None, "leitores": {}}, [f"não foi possível ler {path}: {exc}"]
    except json.JSONDecodeError as exc:
        return {"versao": None, "leitores": {}}, [f"JSON inválido: {exc}"]

    if not isinstance(raw, dict):
        return {"versao": None, "leitores": {}}, ["raiz do JSON precisa ser um objeto"]

    versao = raw.get("versao", 1)
    if versao != 1:
        erros.append(f"versao não suportada: {versao!r} (esperado 1)")

    raw_leitores = raw.get("leitores")
    if not isinstance(raw_leitores, dict):
        erros.append("campo 'leitores' ausente ou não é objeto")
        raw_leitores = {}
    elif not raw_leitores:
        erros.append("campo 'leitores' vazio: nenhum leitor configurado")

    permitidas = _empresas_permitidas()
    for ip, entry in raw_leitores.items():
        try:
            ip_obj = ipaddress.ip_address(ip)
        except ValueError:
            erros.append(f"leitor {ip!r}: chave não é um IPv4 válido")
            continue
        if ip_obj.version != 4:
            erros.append(f"leitor {ip!r}: só IPv4 é aceito")
            continue
        if not isinstance(entry, dict):
            erros.append(f"leitor {ip}: valor precisa ser objeto com 'empresa'")
            continue
        desconhecidas = sorted(set(entry) - _LEITOR_KEYS)
        if desconhecidas:
            erros.append(f"leitor {ip}: chave(s) desconhecida(s) {desconhecidas}")
        empresa = entry.get("empresa")
        if isinstance(empresa, bool) or not isinstance(empresa, int) or empresa <= 0:
            erros.append(f"leitor {ip}: 'empresa' precisa ser inteiro > 0 (recebido {empresa!r})")
            continue
        nome = entry.get("nome")
        if nome is not None and (not isinstance(nome, str) or not nome.strip()):
            erros.append(f"leitor {ip}: 'nome' precisa ser string não vazia")
            nome = None
        if empresa not in permitidas:
            erros.append(f"leitor {ip}: empresa {empresa} não está em GERT_EMPRESAS {sorted(permitidas)}")
        if not environ.get(f"WEBPOSTO_TOKEN_{empresa}"):
            erros.append(f"leitor {ip}: empresa {empresa} sem token WEBPOSTO_TOKEN_{empresa} no ambiente")
        leitores[str(ip_obj)] = {"empresa": empresa, "nome": nome.strip() if nome else None}

    return {"versao": versao, "leitores": leitores}, erros


def load_leitor_config(path=None, environ=None):
    """Carrega e valida a config IP do leitor -> empresa. Levanta ConfigInvalidaError."""
    path = Path(path) if path else leitores_config_path()
    config, erros = _validar_leitor_config(path, environ)
    if erros:
        raise ConfigInvalidaError(path, erros)
    config["path"] = str(path)
    return config


# === STATE ===
class EmpresaCache:
    """Catálogo e preços de UMA empresa. Identidade de produto (barcode -> produto) é por filial."""

    def __init__(self, empresa):
        self.empresa = empresa
        self.products = {}        # barcode -> {"nome", "produtoCodigo", "barcodes": [...], "updated_at"}
        self.by_codigo = {}       # produtoCodigo -> {"nome", "barcode", "barcodes": [...], "updated_at"}
        self.prices = {}          # produtoCodigo -> {"preco": float, "updated_at": ts}
        self.barcode_preco = {}   # barcode -> {"preco": float, "updated_at": ts}
        self.last_price_sync = 0
        self.progressive_cursor = 0


class ServerState:
    def __init__(self):
        self.empresas = {}       # empresa -> EmpresaCache (catálogo + preços da empresa)
        self.config = None       # resultado de load_leitor_config
        self.config_errors = []
        self.last_price_sync = 0
        self.last_catalog_sync = 0
        self.sync_errors = []
        self.http_session = None  # shared aiohttp session for on-demand lookups
        self.no_price_confirmed = {}  # "produtoCodigo_empresa" -> timestamp (confirmed precoVenda=0 or inactive)
        self.stats = {
            "total_queries": 0,
            "hits": 0,
            "misses": 0,
            "connections": 0,
            "active_connections": 0,
            "gif_sends": 0,
            "mesg_sends": 0,
            "on_demand_lookups": 0,
            "on_demand_no_price": 0,
            "on_demand_errors": 0,
            "progressive_synced": 0,
            "unmapped_queries": 0,
            "stale_served": 0,
            "started_at": time.time(),
        }
        historical_queries = load_query_history()
        self.query_log = historical_queries[-QUERY_LOG_HISTORY_LIMIT:]
        self.stats["total_queries"] = len(historical_queries)
        self.stats["hits"] = sum(1 for query in historical_queries if query["hit"])
        self.stats["misses"] = sum(1 for query in historical_queries if not query["hit"])
        self.connected_terminals = {}
        self.terminal_writers = {}  # addr_str -> writer (for GIF push)

    def leitores(self):
        return (self.config or {}).get("leitores", {})

    def config_empresas(self):
        """Empresas efetivamente sincronizadas: as citadas por algum leitor."""
        return sorted({leitor["empresa"] for leitor in self.leitores().values()})


state = ServerState()


def empresa_cache(state, empresa):
    cache = state.empresas.get(empresa)
    if cache is None:
        cache = state.empresas[empresa] = EmpresaCache(empresa)
    return cache


def empresa_para_ip(state, ip):
    leitor = state.leitores().get(ip)
    return leitor["empresa"] if leitor else None


def catalogo_totais(state):
    """(barcodes, produtos) distintos somando os catálogos de todas as empresas (só contagem)."""
    barcodes, produtos = set(), set()
    for cache in state.empresas.values():
        barcodes.update(cache.products)
        produtos.update(cache.by_codigo)
    return len(barcodes), len(produtos)


def nome_filial(state, empresa):
    for leitor in state.leitores().values():
        if leitor["empresa"] == empresa and leitor.get("nome"):
            return leitor["nome"]
    return None


# === WEBPOSTO SYNC ===
def get_token(empresa):
    return os.environ.get(f"WEBPOSTO_TOKEN_{empresa}")


def find_product_by_barcode(products, barcode):
    """Find an exact barcode or its equivalent UPC-A/EAN-13 representation."""
    product = products.get(barcode)
    if product is not None:
        return product

    if not barcode.isdigit():
        return None
    if len(barcode) == 13 and barcode.startswith("0"):
        return products.get(barcode[1:])
    if len(barcode) == 12:
        return products.get(f"0{barcode}")
    return None


async def api_get_json(session, endpoint, params, timeout):
    """Único ponto de acesso à API WebPosto (seam substituível nos testes)."""
    async with session.get(
        f"{WEBPOSTO_BASE}/{endpoint}",
        params=params,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        return await resp.json()


def _empresas_para_sync(empresas=None):
    """Empresas a sincronizar: sempre restritas às citadas pela config de leitores."""
    configuradas = state.config_empresas()
    if empresas is None:
        return configuradas
    return [e for e in empresas if e in configuradas]


def _item_ativo(item):
    ativo = item.get("ativo", True)
    if isinstance(ativo, str):
        ativo = ativo.strip().lower() not in ("", "false", "0", "n", "nao", "não")
    return bool(ativo)


def _item_preco(item):
    """Preço servível de um item PRODUTO_EMPRESA: só item ativo com precoVenda > 0."""
    if not _item_ativo(item):
        return None
    try:
        pv = float(item.get("precoVenda") or 0)
    except (TypeError, ValueError):
        return None
    return pv if pv > 0 else None


def _store_price(empresa, produto_codigo, price, now=None):
    cache = empresa_cache(state, empresa)
    entry = {"preco": price, "updated_at": time.time() if now is None else now}
    cache.prices[produto_codigo] = entry
    for bc in cache.by_codigo.get(produto_codigo, {}).get("barcodes", []):
        cache.barcode_preco[bc] = entry
    state.no_price_confirmed.pop(f"{produto_codigo}_{empresa}", None)


def _drop_price(empresa, produto_codigo):
    """Produto sem preço (inativo/zero) na empresa: remove preço e marca cache negativo."""
    cache = empresa_cache(state, empresa)
    cache.prices.pop(produto_codigo, None)
    for bc in cache.by_codigo.get(produto_codigo, {}).get("barcodes", []):
        cache.barcode_preco.pop(bc, None)
    state.no_price_confirmed[f"{produto_codigo}_{empresa}"] = time.time()


def _relink_barcode_prices(empresa):
    """Após o catálogo da empresa, reaponta barcode -> preço dela (sem apagar preço)."""
    cache = empresa_cache(state, empresa)
    for pc, entry in cache.prices.items():
        for bc in cache.by_codigo.get(pc, {}).get("barcodes", []):
            cache.barcode_preco[bc] = entry


def _no_price_cached(produto_codigo, empresa):
    cached_at = state.no_price_confirmed.get(f"{produto_codigo}_{empresa}")
    return bool(cached_at and (time.time() - cached_at) < NO_PRICE_CACHE_TTL)


async def sync_catalog(session, empresas=None):
    """Full catalog sync: GRUPO → PRODUTO per group (bypasses 2000-item limit).

    O catálogo é POR EMPRESA (state.empresas[E].products/by_codigo): o mesmo barcode
    pode apontar para produtoCodigo diferente em cada filial.
    """
    for empresa in _empresas_para_sync(empresas):
        token = get_token(empresa)
        if not token:
            log.warning(f"No token for empresa {empresa}")
            continue
        try:
            # Step 1: get all groups
            params = {"CHAVE": token, "empresaCodigo": empresa, "limite": 100}
            data = await api_get_json(session, "GRUPO", params, 30)
            groups = data.get("resultados", []) if isinstance(data, dict) else data
            if not groups:
                log.warning(f"No groups found for empresa {empresa}")
                continue

            # Step 2: fetch products per group (grupoCodigo filter works!)
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
                data = await api_get_json(session, "PRODUTO", params, 60)
                if isinstance(data, list) and data and isinstance(data[0], str):
                    log.error(f"PRODUTO error (grupo {gc}): {data[0]}")
                    continue
                items = data.get("resultados", []) if isinstance(data, dict) else data
                all_items.extend(items)

            # Deduplicate by produtoCodigo
            seen = set()
            unique_items = []
            for item in all_items:
                pc = item.get("produtoCodigo") or item.get("codigo")
                if pc and pc not in seen:
                    seen.add(pc)
                    unique_items.append(item)

            cache = empresa_cache(state, empresa)
            count = 0
            now = time.time()
            for item in unique_items:
                barras = item.get("produtoCodigoBarra", []) or []
                nome = item.get("nome", "PRODUTO")
                pc = item.get("produtoCodigo") or item.get("codigo")
                all_bcs = [b.get("codigoBarra", "") for b in barras if b.get("codigoBarra")]
                for bc in all_bcs:
                    cache.products[bc] = {
                        "nome": nome,
                        "produtoCodigo": pc,
                        "barcodes": all_bcs,
                        "updated_at": now,
                    }
                    count += 1
                if pc:
                    cache.by_codigo[pc] = {
                        "nome": nome,
                        "barcode": all_bcs[0] if all_bcs else None,
                        "barcodes": all_bcs,
                        "updated_at": now,
                    }
            _relink_barcode_prices(empresa)
            state.last_catalog_sync = time.time()
            log.info(f"Catalog sync: {count} barcodes, {len(unique_items)} products, {len(groups)} groups (empresa {empresa})")
        except Exception as e:
            msg = f"Catalog sync error (empresa {empresa}): {e}"
            log.error(msg)
            state.sync_errors.append(f"{datetime.now().isoformat()} {msg}")


async def sync_prices(session, empresas=None):
    """Sync prices from PRODUTO_EMPRESA — bulk fetch (max 2000), gravado só na própria empresa."""
    for empresa in _empresas_para_sync(empresas):
        token = get_token(empresa)
        if not token:
            continue
        try:
            # Bulk fetch: PRODUTO_EMPRESA limite=2000 (max allowed)
            params = {
                "CHAVE": token,
                "empresaCodigo": empresa,
                "limite": 2000,
                "codigo": 0,
            }
            data = await api_get_json(session, "PRODUTO_EMPRESA", params, 60)
            if isinstance(data, list) and data and isinstance(data[0], str):
                log.error(f"PRODUTO_EMPRESA error: {data[0]}")
            else:
                items = data.get("resultados", []) if isinstance(data, dict) else data

                updated = 0
                no_price = 0
                now = time.time()
                for item in items:
                    pc = item.get("produtoCodigo")
                    if not pc:
                        continue
                    price = _item_preco(item)
                    if price is None:
                        # Inativo ou precoVenda <= 0 nesta empresa: nunca servir preço.
                        _drop_price(empresa, pc)
                        no_price += 1
                        continue
                    _store_price(empresa, pc, price, now)
                    updated += 1

                cache = empresa_cache(state, empresa)
                cache.last_price_sync = time.time()
                state.last_price_sync = cache.last_price_sync
                log.info(
                    f"Price sync (PRODUTO_EMPRESA): {len(items)} products, {updated} priced, "
                    f"{no_price} inactive/no-price (empresa {empresa})"
                )
        except Exception as e:
            msg = f"Price sync error (empresa {empresa}): {e}"
            log.error(msg)
            state.sync_errors.append(f"{datetime.now().isoformat()} {msg}")


async def _fetch_produto_empresa(session, token, produto_codigo, empresa):
    params = {
        "CHAVE": token,
        "empresaCodigo": empresa,
        "limite": 1,
        "produtoCodigo": produto_codigo,
    }
    data = await api_get_json(session, "PRODUTO_EMPRESA", params, 10)
    return data.get("resultados", []) if isinstance(data, dict) else data


async def fetch_price_on_demand(produto_codigo, empresa):
    """Fetch price for a single product via PRODUTO_EMPRESA?produtoCodigo=X, só na empresa dada.

    Returns:
        float > 0  — price found and cached (state.empresas[empresa])
        0.0        — API confirmed no price (precoVenda=0 or inactive product)
        None       — API call failed (network error, timeout, etc.)
    """
    token = get_token(empresa)
    if not token or not state.http_session:
        return None

    # Negative cache: skip if we already confirmed no price for THIS empresa within the last hour
    if _no_price_cached(produto_codigo, empresa):
        return 0.0

    try:
        items = await _fetch_produto_empresa(state.http_session, token, produto_codigo, empresa)
        if isinstance(items, list) and items and isinstance(items[0], str):
            # API returned an error string
            log.warning(f"On-demand API error for {produto_codigo} (empresa {empresa}): {items[0]}")
            state.stats["on_demand_errors"] += 1
            return None
        if items:
            item = items[0]
            price = _item_preco(item)
            if price is not None:
                _store_price(empresa, produto_codigo, price)
                state.stats["on_demand_lookups"] += 1
                log.info(f"On-demand price: produtoCodigo={produto_codigo} empresa={empresa} -> R${price:.2f}")
                return price
            # Product exists in ERP but has no sale price here (inactive or zero price)
            _drop_price(empresa, produto_codigo)
            state.stats["on_demand_no_price"] += 1
            log.info(
                f"On-demand: produtoCodigo={produto_codigo} has no price in empresa {empresa} "
                f"(precoVenda={item.get('precoVenda')}, ativo={item.get('ativo', True)})"
            )
            return 0.0
        # Product not registered in PRODUTO_EMPRESA for this empresa
        _drop_price(empresa, produto_codigo)
        state.stats["on_demand_no_price"] += 1
        log.info(f"On-demand: produtoCodigo={produto_codigo} not found in PRODUTO_EMPRESA (empresa {empresa})")
        return 0.0
    except Exception as e:
        state.stats["on_demand_errors"] += 1
        log.warning(f"On-demand price lookup failed for {produto_codigo} (empresa {empresa}): {e}")
    return None


async def sync_prices_progressive(session, empresas=None):
    """Progressively fetch prices for products NOT covered by the bulk 2000.

    Por empresa: cobre produtos sem preço nessa empresa e revalida preços cujo
    updated_at passou de PRICE_TTL. Runs inside sync_loop after each bulk sync.
    """
    BATCH_SIZE = 200  # products per progressive cycle (~4h for full coverage)
    MAX_API_ERRORS = 5  # stop batch if too many consecutive errors

    for empresa in _empresas_para_sync(empresas):
        token = get_token(empresa)
        if not token:
            continue
        cache = empresa_cache(state, empresa)

        now = time.time()
        pending = []
        for pc in cache.by_codigo:
            entry = cache.prices.get(pc)
            if entry is not None and (now - entry["updated_at"]) < PRICE_TTL:
                continue
            # Skip recently confirmed no-price (negative cache)
            if entry is None and _no_price_cached(pc, empresa):
                continue
            pending.append(pc)

        if not pending:
            continue

        # Take a batch starting from the cursor
        cursor = cache.progressive_cursor % max(len(pending), 1)
        batch = pending[cursor:cursor + BATCH_SIZE]
        if not batch:
            batch = pending[:BATCH_SIZE]
        cache.progressive_cursor = cursor + len(batch)

        fetched = 0
        no_price = 0
        errors = 0
        consecutive_errors = 0

        for pc in batch:
            if consecutive_errors >= MAX_API_ERRORS:
                log.warning(f"Progressive sync: stopping early after {consecutive_errors} consecutive errors (empresa {empresa})")
                break
            try:
                items = await _fetch_produto_empresa(session, token, pc, empresa)
                if isinstance(items, list) and items and isinstance(items[0], str):
                    errors += 1
                    consecutive_errors += 1
                    continue
                price = _item_preco(items[0]) if items else None
                if price is not None:
                    _store_price(empresa, pc, price)
                    fetched += 1
                else:
                    _drop_price(empresa, pc)
                    no_price += 1
                consecutive_errors = 0
            except Exception as e:
                errors += 1
                consecutive_errors += 1
                log.debug(f"Progressive sync error for {pc} (empresa {empresa}): {e}")

            # Small delay to be gentle on the API
            await asyncio.sleep(PROGRESSIVE_DELAY)

        state.stats["progressive_synced"] += fetched
        if fetched or no_price or errors:
            log.info(
                f"Progressive price sync: {fetched} priced, {no_price} no-price, "
                f"{errors} errors, {len(pending)} still pending (empresa {empresa})"
            )


async def sync_loop():
    async with aiohttp.ClientSession() as session:
        state.http_session = session  # share for on-demand lookups
        await sync_catalog(session)
        await sync_prices(session)
        await sync_prices_progressive(session)
        price_counter = 0
        while True:
            await asyncio.sleep(SYNC_PRICES_INTERVAL)
            price_counter += SYNC_PRICES_INTERVAL
            await sync_prices(session)
            await sync_prices_progressive(session)
            if price_counter >= SYNC_CATALOG_INTERVAL:
                await sync_catalog(session)
                price_counter = 0


# === GERTEC PROTOCOL ===
PRODUCT_NAME_LINE_WIDTH = 20
PRODUCT_NAME_LINE_COUNT = 4
PRODUCT_NAME_MAX_LENGTH = PRODUCT_NAME_LINE_WIDTH * PRODUCT_NAME_LINE_COUNT
PRICE_MAX_LENGTH = 20


def _format_product_name(nome):
    """Fit a product name in the Gertec's 4 lines of 20 columns.

    The price-query protocol does not use a literal newline. The terminal
    renders the product field in 20-column chunks, so the field is padded to
    80 bytes before the ``|`` separator.
    """
    text = " ".join(str(nome or "PRODUTO").replace("\r", " ").replace("\n", " ").split())
    lines = textwrap.wrap(
        text,
        width=PRODUCT_NAME_LINE_WIDTH,
        max_lines=PRODUCT_NAME_LINE_COUNT,
        placeholder="...",
    )
    return "".join(line.ljust(PRODUCT_NAME_LINE_WIDTH) for line in lines).ljust(PRODUCT_NAME_MAX_LENGTH)


def build_mesg(line1, line2, tempo=5):
    cmd = "#mesg"
    cmd += chr(len(line1) + 48) + line1
    cmd += chr(len(line2) + 48) + line2
    cmd += chr(tempo + 48)
    cmd += chr(48)  # reserved
    return cmd.encode("ascii", errors="replace")


def build_gif_command(gif_data, index=0, loops=0, tempo=10):
    header = b"#gif"
    header += f"{index:02X}".encode()
    header += f"{loops:02X}".encode()
    header += f"{tempo:02X}".encode()
    header += f"{len(gif_data):06X}".encode()
    header += b"0000"
    header += b"\x17"
    return header + gif_data


def format_price_response(nome, preco):
    product_name = _format_product_name(nome)
    if preco is not None:
        # The terminal receives the price as text; include the currency
        # symbol explicitly and use the Brazilian decimal separator.
        price = f"R$ {preco:.2f}".replace(".", ",")
    else:
        price = "SEM PRECO"
    return f"#{product_name}|{price[:PRICE_MAX_LENGTH]}".encode("ascii", errors="replace")


async def send_gif_to_terminal(writer, gif_path):
    """Send a GIF propaganda to a specific terminal."""
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


async def consulta_preco(ip, barcode):
    """Resolve uma consulta do leitor ``ip`` usando SOMENTE a empresa vinculada a ele.

    Retorna dict com: resp (bytes do protocolo), empresa, product, preco, status.
    status ∈ {"unmapped", "miss", "hit", "hit_ondemand", "hit_revalidated",
              "hit_stale", "no_price", "lookup_failed"}.
    """
    empresa = empresa_para_ip(state, ip)
    if empresa is None:
        # Falha fechada: nenhuma empresa é consultada, nenhum preço é exibido.
        log.warning(f"Leitor sem vínculo de filial: ip={ip} barcode={barcode} — respondendo sem preço")
        return {
            "resp": format_price_response(TERMINAL_NAO_CONFIGURADO, None),
            "empresa": None, "product": None, "nome": None, "preco": None, "status": "unmapped",
        }

    # Identidade do produto vem SÓ do catálogo da empresa do leitor (nunca de outra filial).
    cache = empresa_cache(state, empresa)
    if not cache.products:
        # Catálogo desta empresa ainda não sincronizou: não dá para afirmar "não encontrado"
        # nem usar o catálogo de outra filial. Sem preço, sem nome, sem chamada de API.
        log.warning(f"Catalogo da empresa {empresa} indisponivel: ip={ip} barcode={barcode} — respondendo sem preço")
        return {
            "resp": format_price_response(CATALOGO_INDISPONIVEL, None),
            "empresa": empresa, "product": None, "nome": None, "preco": None, "status": "lookup_failed",
        }
    product = find_product_by_barcode(cache.products, barcode)
    if product is None:
        return {"resp": b"#nfound", "empresa": empresa, "product": None, "nome": None,
                "preco": None, "status": "miss"}

    nome = product["nome"]
    pc = product.get("produtoCodigo")
    entry = cache.prices.get(pc) if pc else None
    if entry is None:
        entry = cache.barcode_preco.get(barcode)

    def result(preco, status):
        return {
            "resp": format_price_response(nome, preco),
            "empresa": empresa, "product": product, "nome": nome, "preco": preco, "status": status,
        }

    if entry is not None:
        age = time.time() - entry["updated_at"]
        if age < PRICE_TTL:
            return result(entry["preco"], "hit")
        price = await fetch_price_on_demand(pc, empresa) if pc else None
        if price is not None and price > 0:
            return result(price, "hit_revalidated")
        if price == 0.0:
            return result(None, "no_price")
        # API fora: stale da MESMA empresa, só dentro da janela
        if age < PRICE_STALE_MAX:
            state.stats["stale_served"] += 1
            log.warning(f"Serving stale price (age {int(age)}s, empresa {empresa}) for {barcode}")
            return result(entry["preco"], "hit_stale")
        return result(None, "lookup_failed")

    price = await fetch_price_on_demand(pc, empresa) if pc else None
    if price is not None and price > 0:
        return result(price, "hit_ondemand")
    if price == 0.0:
        return result(None, "no_price")
    return result(None, "lookup_failed")


async def handle_terminal(reader, writer):
    addr = writer.get_extra_info("peername")
    addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"
    ip = addr[0] if addr else None
    empresa = empresa_para_ip(state, ip)
    state.stats["connections"] += 1
    state.stats["active_connections"] += 1
    state.connected_terminals[addr_str] = {
        "connected_at": datetime.now().isoformat(),
        "queries": 0,
        "model": "unknown",
        "ip": ip,
        "empresa": empresa,
        "filial": nome_filial(state, empresa) if empresa else None,
    }
    state.terminal_writers[addr_str] = writer
    log.info(f"Terminal connected: {addr_str}")
    if empresa is None:
        log.warning(f"Terminal {addr_str} sem vínculo em config de leitores — consultas sem preço")

    try:
        # Handshake
        writer.write(b"#ok")
        await writer.drain()
        await asyncio.sleep(0.5)
        data = await asyncio.wait_for(reader.read(255), timeout=5)
        response = data.decode("ascii", errors="replace").strip("\x00")
        log.info(f"Terminal {addr_str} handshake: {response}")
        state.connected_terminals[addr_str]["model"] = response

        # Alwayslive
        writer.write(b"#alwayslive")
        await writer.drain()
        await asyncio.sleep(0.3)
        data = await asyncio.wait_for(reader.read(255), timeout=5)
        log.info(f"Terminal {addr_str} alwayslive: {data.decode('ascii', errors='replace').strip(chr(0))}")

        # Detect model: G2E (#bpg2e) does NOT support #mesg/#gif via TCP
        is_g2e = "bpg2e" in response.lower()
        state.connected_terminals[addr_str]["is_g2e"] = is_g2e

        if not is_g2e:
            # G2S: send welcome message + initial GIF
            writer.write(build_mesg("POSTO SCURSEL", "CONSULTE AQUI!", 5))
            await writer.drain()
            state.stats["mesg_sends"] += 1
            gif_files = sorted(GIF_DIR.glob("*.gif"))
            if gif_files:
                await send_gif_to_terminal(writer, gif_files[0])
        else:
            log.info(f"Terminal {addr_str} is G2E — skipping #mesg/#gif (use web config)")

        # Main loop
        while True:
            try:
                data = await asyncio.wait_for(reader.read(255), timeout=120)
            except asyncio.TimeoutError:
                writer.write(b"#live?")
                await writer.drain()
                continue

            if not data:
                log.info(f"Terminal {addr_str} disconnected")
                break

            msg = data.decode("ascii", errors="replace").strip("\x00").strip()
            if not msg:
                continue

            if msg.startswith("#") and not msg.startswith("#live"):
                # Terminal protocol responses — not queries
                if msg.startswith(("#gif_ok", "#img_error", "#mesg_error",
                                   "#alwayslive", "#tc406", "#tc502", "#bpg2e")):
                    log.info(f"Terminal {addr_str} response: {msg}")
                    continue
                barcode = msg[1:].strip()
                state.stats["total_queries"] += 1
                state.connected_terminals[addr_str]["queries"] += 1

                result = await consulta_preco(ip, barcode)
                product = result["product"]
                status = result["status"]
                resp = result["resp"]
                emp = result["empresa"]
                if status == "unmapped":
                    state.stats["misses"] += 1
                    state.stats["unmapped_queries"] += 1
                elif status == "miss":
                    state.stats["misses"] += 1
                    log.info(f"MISS: {barcode}")
                elif product is None:
                    state.stats["misses"] += 1
                    log.warning(f"SEM CATALOGO (emp={emp}): {barcode} -> sem preço")
                else:
                    state.stats["hits"] += 1
                    if result["preco"] is not None:
                        tag = {"hit": "", "hit_ondemand": "on-demand ",
                               "hit_revalidated": "revalidated ", "hit_stale": "stale "}[status]
                        log.info(f"HIT ({tag}emp={emp}): {barcode} -> {product['nome']} R${result['preco']:.2f}")
                    elif status == "no_price":
                        log.info(f"HIT (no price emp={emp}): {barcode} -> {product['nome']}")
                    else:
                        # API call failed — show SEM PRECO but will retry on next bip
                        log.warning(f"HIT (lookup failed, will retry emp={emp}): {barcode} -> {product['nome']}")

                state.query_log.append({
                    "time": datetime.now().isoformat(),
                    "barcode": barcode,
                    "terminal": addr_str,
                    "hit": product is not None,
                    "nome": product["nome"] if product else None,
                    "preco": result["preco"],
                    "empresa": emp,
                })
                if len(state.query_log) > 500:
                    state.query_log = state.query_log[-200:]

                writer.write(resp)
                await writer.drain()

            elif msg == "#live":
                pass
            else:
                log.debug(f"Terminal {addr_str} sent: {msg}")

    except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
        log.info(f"Terminal {addr_str} connection lost")
    except Exception as e:
        log.error(f"Terminal {addr_str} error: {e}")
    finally:
        state.stats["active_connections"] -= 1
        state.connected_terminals.pop(addr_str, None)
        state.terminal_writers.pop(addr_str, None)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


# === GIF ROTATION ===
async def gif_rotation_loop():
    log.info("Propaganda loop started (scans gifs/ every cycle)")
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
            # Skip G2E terminals — they don't support #gif via TCP
            info = state.connected_terminals.get(addr_str, {})
            if info.get("is_g2e"):
                continue
            try:
                await send_gif_to_terminal(w, gif_path)
            except Exception:
                pass


# === DASHBOARD ===
async def dashboard_index(request):
    uptime = time.time() - state.stats["started_at"]
    hours = int(uptime // 3600)
    mins = int((uptime % 3600) // 60)
    server_ip = SERVER_IP

    terminals_html = ""
    for addr, info in state.connected_terminals.items():
        terminals_html += f"<tr><td>{addr}</td><td>{info['model']}</td><td>{info['connected_at']}</td><td>{info['queries']}</td></tr>"

    queries_html = ""
    for q in reversed(state.query_log[-50:]):
        status = "HIT" if q["hit"] else "MISS"
        preco = f"R$ {q['preco']:.2f}" if q.get("preco") else "-"
        query_time = datetime.fromisoformat(q["time"]).strftime("%d/%m/%Y %H:%M:%S")
        queries_html += f"<tr><td>{query_time}</td><td>{q['barcode']}</td><td>{q.get('nome') or '-'}</td><td>{preco}</td><td>{status}</td><td>{q['terminal']}</td></tr>"

    leitores_txt = ", ".join(
        f"{ip} → {l['empresa']}" + (f" ({l['nome']})" if l.get("nome") else "")
        for ip, l in state.leitores().items()
    ) or "nenhum"

    errors_html = ""
    for e in state.sync_errors[-10:]:
        errors_html += f"<li>{e}</li>"

    last_price = datetime.fromtimestamp(state.last_price_sync).strftime("%H:%M:%S") if state.last_price_sync else "nunca"
    total_barcodes, _ = catalogo_totais(state)
    last_catalog = datetime.fromtimestamp(state.last_catalog_sync).strftime("%H:%M:%S") if state.last_catalog_sync else "nunca"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Gertec Server - Dashboard</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'JetBrains Mono', monospace; background: #0d1117; color: #c9d1d9; padding: 20px; }}
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
.errors {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; }}
.errors li {{ font-size: 0.8em; margin: 5px 0; list-style: none; }}
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
<h1>Gertec Busca Preco - Server</h1>
<p class="subtitle">Posto Scursel | Uptime: {hours}h {mins}m | TCP {TCP_PORT} / HTTP {DASH_PORT}</p>
<section class="setup">
<h2>Como configurar o terminal Gertec</h2>
<p class="setup-intro">No menu de rede do Gertec, configure o servidor de consulta usando TCP:</p>
<div class="setup-grid">
<div class="setup-item"><span class="label">IP do servidor Hermes</span><code>{server_ip}</code></div>
<div class="setup-item"><span class="label">Porta TCP do Gertec</span><code>{TCP_PORT}</code></div>
<div class="setup-item"><span class="label">Endereco completo</span><code>{server_ip}:{TCP_PORT}</code></div>
</div>
<div class="setup-warning"><strong>Importante:</strong> nao use a porta {DASH_PORT} no Gertec. Ela e somente o dashboard HTTP. O terminal deve usar a porta TCP {TCP_PORT}; nao informe <code>http://</code>.</div>
<p class="setup-note">Para abrir este dashboard no navegador: <code>http://{server_ip}:{DASH_PORT}</code></p>
</section>
<div class="grid">
<div class="card"><div class="label">Consultas Totais</div><div class="value blue">{state.stats['total_queries']}</div></div>
<div class="card"><div class="label">Hits</div><div class="value green">{state.stats['hits']}</div></div>
<div class="card"><div class="label">Misses</div><div class="value red">{state.stats['misses']}</div></div>
<div class="card"><div class="label">Terminais Ativos</div><div class="value">{state.stats['active_connections']}</div></div>
<div class="card"><div class="label">Produtos em Cache</div><div class="value">{total_barcodes}</div></div>
<div class="card"><div class="label">Ultimo Sync Preco</div><div class="value" style="font-size:1.2em">{last_price}</div></div>
</div>
<h2>Terminais Conectados</h2>
<table><tr><th>Endereco</th><th>Modelo</th><th>Conectado</th><th>Consultas</th></tr>
{terminals_html or '<tr><td colspan="4" style="color:#8b949e">Nenhum terminal conectado</td></tr>'}
</table>
<h2>Ultimas Consultas</h2>
<table><tr><th>Data/Hora</th><th>Barcode</th><th>Produto</th><th>Preco</th><th>Status</th><th>Terminal</th></tr>
{queries_html or '<tr><td colspan="6" style="color:#8b949e">Nenhuma consulta ainda</td></tr>'}
</table>
<h2>Sync / Erros</h2>
<div class="errors"><ul>
<li style="color:#3fb950">Catalogo: {last_catalog} ({total_barcodes} barcodes)</li>
<li style="color:#3fb950">Precos: {last_price}</li>
<li style="color:#3fb950">Leitores: {leitores_txt}</li>
{errors_html or '<li style="color:#8b949e">Sem erros</li>'}
</ul></div>
<h2>Propaganda (GIFs) — somente G2S</h2>
<div class="errors" id="gif-panel">
<p style="color:#8b949e;font-size:0.8em;margin-bottom:10px">GIFs sao exibidos em terminais <strong>G2S</strong> quando ociosos. O G2E nao suporta GIF via TCP — use o painel de mensagens abaixo. Max 124KB (320x240).</p>
<div id="gif-list" style="margin-bottom:10px"></div>
<form id="gif-upload" style="display:flex;gap:10px;align-items:center">
<input type="file" name="gif" accept=".gif" style="font-size:0.8em">
<button type="submit" style="background:#238636;color:#fff;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:0.8em">Upload</button>
</form>
</div>
<h2>Mensagens Idle — G2E</h2>
<div class="errors" id="g2e-panel">
<p style="color:#8b949e;font-size:0.8em;margin-bottom:10px">Configura as 4 linhas de texto exibidas no G2E quando ocioso. Enviado via interface web do terminal (admin/admin).</p>
<div id="g2e-form" style="display:grid;gap:8px;max-width:400px">
<div><label style="color:#8b949e;font-size:0.75em">Linha 1 (max 20)</label><input id="g2e-l1" maxlength="20" style="width:100%;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px"></div>
<div><label style="color:#8b949e;font-size:0.75em">Linha 2</label><input id="g2e-l2" maxlength="20" style="width:100%;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px"></div>
<div><label style="color:#8b949e;font-size:0.75em">Linha 3</label><input id="g2e-l3" maxlength="20" style="width:100%;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px"></div>
<div><label style="color:#8b949e;font-size:0.75em">Linha 4</label><input id="g2e-l4" maxlength="20" style="width:100%;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px"></div>
<div style="display:flex;gap:10px;align-items:center">
<label style="color:#8b949e;font-size:0.75em">Tempo (s)</label><input id="g2e-tempo" type="number" min="1" max="99" value="3" style="width:60px;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px">
<label style="color:#8b949e;font-size:0.75em"><input id="g2e-logo" type="checkbox" checked> Logo Gertec</label>
</div>
<button onclick="saveG2E()" style="background:#1f6feb;color:#fff;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;font-size:0.85em">Salvar no Terminal</button>
<span id="g2e-status" style="font-size:0.8em"></span>
</div>
</div>
<h2>Diagnostico — Buscar Barcode</h2>
<div class="errors">
<div style="display:flex;gap:10px;align-items:center">
<input id="lookup-bc" placeholder="Codigo de barras" style="flex:1;background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:6px;border-radius:4px">
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
// G2E messages
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
// Barcode lookup
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
    priced = set()
    empresas = {}
    for codigo in sorted(set(state.config_empresas()) | set(state.empresas)):
        cache = empresa_cache(state, codigo)
        priced.update(cache.prices)
        empresas[str(codigo)] = {
            "catalogo_barcodes": len(cache.products),
            "catalogo_produtos": len(cache.by_codigo),
            "produtos": len(cache.prices),
            "precos": len(cache.barcode_preco),
            "last_price_sync": cache.last_price_sync,
        }
    total_barcodes, total_produtos = catalogo_totais(state)
    return web.json_response({
        "stats": state.stats,
        "cache": {
            "products": total_barcodes,
            "by_codigo": total_produtos,
            "with_price": len(priced),
            "last_price_sync": state.last_price_sync,
            "last_catalog_sync": state.last_catalog_sync,
        },
        "empresas": empresas,
        "leitores": state.leitores(),
        "terminals": state.connected_terminals,
        "recent_queries": state.query_log[-20:],
        "errors": state.sync_errors[-5:],
    })



async def dashboard_upload_gif(request):
    """Upload a GIF for propaganda rotation."""
    reader = await request.multipart()
    field = await reader.next()
    if field is None or field.name != "gif":
        return web.json_response({"error": "Campo 'gif' obrigatorio"}, status=400)

    filename = field.filename or "propaganda.gif"
    if not filename.lower().endswith(".gif"):
        return web.json_response({"error": "Apenas arquivos .gif"}, status=400)

    data = await field.read(decode=False)
    if len(data) > 124 * 1024:
        return web.json_response({"error": f"GIF muito grande ({len(data)} bytes). Maximo: 124KB (memoria compartilhada com audio)"}, status=400)

    # Save
    safe_name = "".join(c for c in filename if c.isalnum() or c in "._-")
    dest = GIF_DIR / safe_name
    dest.write_bytes(data)
    log.info(f"GIF uploaded: {safe_name} ({len(data)} bytes)")
    return web.json_response({"ok": True, "file": safe_name, "size": len(data)})


async def dashboard_list_gifs(request):
    """List uploaded GIFs."""
    gifs = []
    for f in sorted(GIF_DIR.glob("*.gif")):
        gifs.append({"name": f.name, "size": f.stat().st_size})
    return web.json_response({"gifs": gifs, "dir": str(GIF_DIR)})


async def dashboard_delete_gif(request):
    """Delete a GIF."""
    name = request.match_info.get("name", "")
    safe_name = "".join(c for c in name if c.isalnum() or c in "._-")
    path = GIF_DIR / safe_name
    if path.exists() and path.suffix == ".gif":
        path.unlink()
        log.info(f"GIF deleted: {safe_name}")
        return web.json_response({"ok": True})
    return web.json_response({"error": "Nao encontrado"}, status=404)

# === G2E WEB CONFIG PROXY ===
G2E_ADMIN_USER = os.environ.get("G2E_ADMIN_USER", "admin")
G2E_ADMIN_PASS = os.environ.get("G2E_ADMIN_PASS", "admin")


async def g2e_get_messages(request):
    """Read current idle messages from a G2E terminal's web interface."""
    terminal_ip = request.query.get("ip", "")
    if not terminal_ip:
        # Auto-detect from connected terminals
        for addr, info in state.connected_terminals.items():
            if info.get("is_g2e"):
                terminal_ip = addr.split(":")[0]
                break
    if not terminal_ip:
        return web.json_response({"error": "Nenhum terminal G2E conectado"}, status=404)

    try:
        async with aiohttp.ClientSession() as session:
            auth = aiohttp.BasicAuth(G2E_ADMIN_USER, G2E_ADMIN_PASS)
            async with session.get(f"http://{terminal_ip}/mensagens", auth=auth,
                                   timeout=aiohttp.ClientTimeout(total=5)) as resp:
                html = await resp.text()
        # Parse current values from HTML form
        import re
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
    """Save idle messages to a G2E terminal via its web interface."""
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
            async with session.post(f"http://{terminal_ip}/salvamensagens",
                                    data=form, auth=auth,
                                    timeout=aiohttp.ClientTimeout(total=5)) as resp:
                status = resp.status
        log.info(f"G2E messages saved to {terminal_ip}: {form}")
        return web.json_response({"ok": True, "terminal": terminal_ip, "http_status": status})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=502)


def _lookup_empresa(query):
    """Resolve a empresa do /lookup. Retorna (empresa, filial, erro_response)."""
    leitores = state.leitores()
    opcoes = [{"ip": ip, "empresa": l["empresa"], "nome": l.get("nome")} for ip, l in leitores.items()]
    ip = query.get("ip", "").strip()
    empresa_raw = query.get("empresa", "").strip()
    if ip:
        leitor = leitores.get(ip)
        if not leitor:
            return None, None, web.json_response(
                {"error": f"IP {ip} sem vinculo de filial", "opcoes": opcoes}, status=404)
        return leitor["empresa"], leitor.get("nome"), None
    if empresa_raw:
        try:
            empresa = int(empresa_raw)
        except ValueError:
            empresa = None
        if empresa not in state.config_empresas():
            return None, None, web.json_response(
                {"error": f"Empresa {empresa_raw} nao configurada", "opcoes": opcoes}, status=400)
        return empresa, nome_filial(state, empresa), None
    if len(leitores) == 1:
        leitor = next(iter(leitores.values()))
        return leitor["empresa"], leitor.get("nome"), None
    return None, None, web.json_response(
        {"error": "Informe 'empresa' ou 'ip' (mais de um leitor configurado)" if leitores
         else "Nenhum leitor configurado", "opcoes": opcoes}, status=400)


def _lookup_preco(empresa, produto_codigo, barcode=None):
    cache = empresa_cache(state, empresa)
    entry = cache.prices.get(produto_codigo) if produto_codigo else None
    if entry is None and barcode:
        entry = cache.barcode_preco.get(barcode)
    if entry is None:
        return {"preco": None, "preco_idade_s": None}
    return {"preco": entry["preco"], "preco_idade_s": int(time.time() - entry["updated_at"])}


async def dashboard_lookup(request):
    """Diagnostic: look up a barcode in the cache of ONE empresa."""
    barcode = request.query.get("barcode", "").strip()
    if not barcode:
        return web.json_response({"error": "Parametro 'barcode' obrigatorio"}, status=400)
    empresa, filial, error = _lookup_empresa(request.query)
    if error is not None:
        return error
    scope = {"empresa": empresa, "filial": filial}
    # Catálogo e preço SÓ da empresa escolhida: nunca nome/preço de outra filial.
    cache = empresa_cache(state, empresa)
    product = find_product_by_barcode(cache.products, barcode)
    if product:
        return web.json_response({"found": True, "barcode": barcode, **product, **scope,
                                  **_lookup_preco(empresa, product.get("produtoCodigo"), barcode)})
    # Try by_codigo
    pc = int(barcode) if barcode.isdigit() and int(barcode) in cache.by_codigo else barcode
    by_code = cache.by_codigo.get(pc)
    if by_code:
        return web.json_response({"found": True, "type": "by_codigo", "codigo": barcode, **by_code, **scope,
                                  **_lookup_preco(empresa, pc)})
    return web.json_response({"found": False, "barcode": barcode, **scope,
                              "total_products": len(cache.products),
                              "total_by_codigo": len(cache.by_codigo)})


# === MAIN ===
def check_config():
    """--check-config: valida config de leitores + tokens sem abrir TCP/dashboard."""
    path = leitores_config_path()
    config, erros = _validar_leitor_config(path)
    print(f"Config de leitores: {path}")
    print(f"GERT_EMPRESAS (permitidas): {sorted(_empresas_permitidas())}")
    for ip, leitor in config["leitores"].items():
        token = "presente" if get_token(leitor["empresa"]) else "AUSENTE"
        print(f"  {ip} -> empresa {leitor['empresa']} -> {leitor.get('nome') or '-'} (token: {token})")
    empresas = sorted({l["empresa"] for l in config["leitores"].values()})
    print(f"Empresas sincronizadas: {empresas}")
    if erros:
        for erro in erros:
            print(f"ERRO: {erro}")
        print("RESULTADO: INVALIDA — o servidor NAO inicia com esta config")
        return 1
    print("RESULTADO: OK")
    return 0


async def main():
    LOG_DIR.mkdir(exist_ok=True)
    GIF_DIR.mkdir(exist_ok=True)

    try:
        state.config = load_leitor_config()
    except ConfigInvalidaError as exc:
        state.config_errors = exc.erros
        log.error(f"Config de leitores INVALIDA em {exc.path}: {'; '.join(exc.erros)} — servidor NAO iniciado")
        raise SystemExit(2)

    log.info(f"Starting Gertec Server - TCP:{TCP_PORT} HTTP:{DASH_PORT}")
    log.info(f"Empresas permitidas (GERT_EMPRESAS): {EMPRESAS}")
    log.info(f"Config de leitores: {state.config['path']}")
    for ip, leitor in state.leitores().items():
        log.info(f"Leitor {ip} -> empresa {leitor['empresa']} ({leitor.get('nome') or '-'})")
    log.info(f"Empresas sincronizadas: {state.config_empresas()}")

    tcp_server = await asyncio.start_server(handle_terminal, "0.0.0.0", TCP_PORT)
    log.info(f"TCP server listening on 0.0.0.0:{TCP_PORT}")

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
    log.info(f"Dashboard at http://0.0.0.0:{DASH_PORT}")

    async with tcp_server:
        await tcp_server.serve_forever()


if __name__ == "__main__":
    if "--check-config" in sys.argv[1:]:
        sys.exit(check_config())
    asyncio.run(main())
