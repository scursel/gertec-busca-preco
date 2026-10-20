"""Isolamento de preço por filial/leitor.

Nenhum teste sai para a rede: o seam ``server.api_get_json`` é substituído por
uma API fake com respostas sintéticas por (endpoint, empresa, produtoCodigo).
Os tokens usados são strings fake só no ambiente do teste.
"""

import asyncio
import json
import logging
import os
import random
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from aiohttp.test_utils import make_mocked_request

import server

PYTHON = sys.executable
SERVER_PY = Path(server.__file__).resolve()
TODAS = [7806, 7818, 7820, 7823]
FAKE_ENV = {f"WEBPOSTO_TOKEN_{e}": "fake-token-teste" for e in TODAS}

# Produto real do incidente (tabela Zen 2026-10-20) — usado só como fixture.
PC_REAL = 1585906
EAN_REAL = "7898921425506"
# Produto sintético ativo nas duas filiais com preços diferentes.
PC_DIFF = 2000001
EAN_DIFF = "7890000000017"
# Produto sintético dentro do bulk.
PC_BULK = 100
EAN_BULK = "7890000000024"

CATALOGO = [
    {"produtoCodigo": PC_REAL, "nome": "ORELHA DE GATO DOCE TIMBE",
     "produtoCodigoBarra": [{"codigoBarra": EAN_REAL}]},
    {"produtoCodigo": PC_DIFF, "nome": "PRODUTO TESTE DIFF",
     "produtoCodigoBarra": [{"codigoBarra": EAN_DIFF}]},
    {"produtoCodigo": PC_BULK, "nome": "PRODUTO TESTE BULK",
     "produtoCodigoBarra": [{"codigoBarra": EAN_BULK}]},
]


def precos_reais():
    return {
        (7806, PC_REAL): {"precoVenda": 9.99, "ativo": False},
        (7818, PC_REAL): {"precoVenda": 12.99, "ativo": True},
        (7820, PC_REAL): {"precoVenda": 6.00, "ativo": False},
        (7823, PC_REAL): {"precoVenda": 6.00, "ativo": False},
        (7818, PC_DIFF): {"precoVenda": 5.49, "ativo": True},
        (7823, PC_DIFF): {"precoVenda": 7.25, "ativo": True},
        (7806, PC_DIFF): {"precoVenda": 3.33, "ativo": True},
        (7818, PC_BULK): {"precoVenda": 2.50, "ativo": True},
        (7823, PC_BULK): {"precoVenda": 4.75, "ativo": True},
    }


class FakeApi:
    """Fake do seam api_get_json. Registra todas as chamadas."""

    def __init__(self, precos=None, bulk=None, jitter=False):
        self.precos = precos_reais() if precos is None else precos
        # pcs presentes no bulk PRODUTO_EMPRESA de cada empresa (o produto real fica fora)
        self.bulk = {e: {PC_DIFF, PC_BULK} for e in TODAS} if bulk is None else bulk
        self.jitter = jitter
        self.down = set()   # empresas com API fora (exceção de rede)
        self.catalogos = {}  # empresa -> catálogo próprio (default: CATALOGO para todas)
        self.calls = []

    async def __call__(self, session, endpoint, params, timeout):
        empresa = params["empresaCodigo"]
        pc = params.get("produtoCodigo")
        self.calls.append((endpoint, empresa, pc))
        if self.jitter:
            await asyncio.sleep(random.random() / 200)
        if empresa in self.down:
            raise OSError("API indisponivel (fake)")
        if endpoint == "GRUPO":
            return {"resultados": [{"codigo": 1}]}
        if endpoint == "PRODUTO":
            return {"resultados": json.loads(json.dumps(self.catalogos.get(empresa, CATALOGO)))}
        if endpoint == "PRODUTO_EMPRESA":
            if pc is not None:
                item = self.precos.get((empresa, pc))
                return {"resultados": [dict(item, produtoCodigo=pc)] if item else []}
            return {"resultados": [
                dict(item, produtoCodigo=p) for (e, p), item in self.precos.items()
                if e == empresa and p in self.bulk.get(empresa, set())
            ]}
        raise AssertionError(f"endpoint inesperado {endpoint}")

    def empresas_chamadas(self, endpoint="PRODUTO_EMPRESA"):
        return {e for (ep, e, _pc) in self.calls if ep == endpoint}


def write_config(dirpath, leitores, raw=None):
    path = Path(dirpath) / "leitores.json"
    path.write_text(raw if raw is not None else json.dumps({"versao": 1, "leitores": leitores}))
    return path


def preco_resp(nome, preco):
    return server.format_price_response(nome, preco)


_NULL = logging.NullHandler()


def setUpModule():
    server.log.propagate = False
    server.log.addHandler(_NULL)


def tearDownModule():
    server.log.removeHandler(_NULL)
    server.log.propagate = True


class FilialTestCase(unittest.IsolatedAsyncioTestCase):
    LEITORES = {
        "192.168.1.197": {"empresa": 7818, "nome": "Posto Waldemiro"},
        "192.168.1.198": {"empresa": 7823, "nome": "Filial Teste"},
    }

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patches = [
            mock.patch.dict(os.environ, FAKE_ENV),
            mock.patch.object(server, "EMPRESAS", list(TODAS)),
            mock.patch.object(server, "PROGRESSIVE_DELAY", 0),
            mock.patch.object(server, "PRICE_TTL", 3600),
            mock.patch.object(server, "PRICE_STALE_MAX", 21600),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.api = FakeApi()
        p = mock.patch.object(server, "api_get_json", self.api)
        p.start()
        self.addCleanup(p.stop)
        self.reset_state(self.LEITORES)

    def reset_state(self, leitores):
        server.state = server.ServerState()
        server.state.http_session = object()  # sessão fake; o seam ignora
        server.state.config = server.load_leitor_config(write_config(self.tmp.name, leitores))
        return server.state

    async def full_sync(self, empresas=None):
        s = server.state.http_session
        await server.sync_catalog(s, empresas)
        await server.sync_prices(s, empresas)
        await server.sync_prices_progressive(s, empresas)

    async def consulta(self, ip, barcode):
        return await server.consulta_preco(ip, barcode)


# --------------------------------------------------------------------------
class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for p in (mock.patch.dict(os.environ, FAKE_ENV), mock.patch.object(server, "EMPRESAS", list(TODAS))):
            p.start()
            self.addCleanup(p.stop)

    def load(self, leitores=None, raw=None):
        return server.load_leitor_config(write_config(self.tmp.name, leitores, raw))

    def assertInvalida(self, leitores=None, raw=None, contains=None):
        with self.assertRaises(server.ConfigInvalidaError) as ctx:
            self.load(leitores, raw)
        if contains:
            self.assertIn(contains, str(ctx.exception))

    def test_config_valida(self):
        cfg = self.load({"192.168.1.197": {"empresa": 7818, "nome": "Posto Waldemiro"}})
        self.assertEqual(cfg["leitores"], {"192.168.1.197": {"empresa": 7818, "nome": "Posto Waldemiro"}})

    def test_config_real_proposta(self):
        cfg = self.load({"192.168.1.197": {"empresa": 7818, "nome": "Posto Waldemiro"}})
        self.assertEqual(cfg["leitores"], {"192.168.1.197": {"empresa": 7818, "nome": "Posto Waldemiro"}})

    def test_config_real_coerente_com_mitigacao(self):
        with mock.patch.object(server, "EMPRESAS", [7818]):
            cfg = self.load({"192.168.1.197": {"empresa": 7818, "nome": "Posto Waldemiro"}})
        self.assertEqual(sorted({l["empresa"] for l in cfg["leitores"].values()}), [7818])

    def test_json_quebrado(self):
        self.assertInvalida(raw='{"versao": 1, "leitores": {', contains="JSON")

    def test_arquivo_ausente(self):
        with self.assertRaises(server.ConfigInvalidaError):
            server.load_leitor_config(Path(self.tmp.name) / "nao-existe.json")

    def test_leitores_vazio(self):
        self.assertInvalida({}, contains="vazio")

    def test_leitores_ausente_ou_nao_objeto(self):
        self.assertInvalida(raw='{"versao": 1}')
        self.assertInvalida(raw='{"versao": 1, "leitores": []}')
        self.assertInvalida(raw='[]')

    def test_ip_invalido(self):
        for ip in ("192.168.1.300", "leitor-1", "", "::1"):
            with self.subTest(ip=ip):
                self.assertInvalida({ip: {"empresa": 7818}})

    def test_empresa_fora_de_gert_empresas(self):
        self.assertInvalida({"192.168.1.197": {"empresa": 7819}}, contains="GERT_EMPRESAS")
        with mock.patch.object(server, "EMPRESAS", [7818]):
            self.assertInvalida({"192.168.1.197": {"empresa": 7806}}, contains="GERT_EMPRESAS")

    def test_empresa_tipo_invalido(self):
        for emp in ("7818", 0, -1, True, None, 78.18):
            with self.subTest(empresa=emp):
                self.assertInvalida({"192.168.1.197": {"empresa": emp}})

    def test_chave_desconhecida(self):
        self.assertInvalida({"192.168.1.197": {"empresa": 7818, "nme": "typo"}}, contains="desconhecida")
        self.assertInvalida({"192.168.1.197": {"empressa": 7818}})

    def test_nome_vazio(self):
        self.assertInvalida({"192.168.1.197": {"empresa": 7818, "nome": "  "}})

    def test_empresa_sem_token(self):
        env = {k: v for k, v in os.environ.items() if k != "WEBPOSTO_TOKEN_7818"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertInvalida({"192.168.1.197": {"empresa": 7818}}, contains="WEBPOSTO_TOKEN_7818")

    def test_startup_nao_sobe_com_config_invalida(self):
        bad = write_config(self.tmp.name, None, raw="{quebrado")
        start_server = mock.AsyncMock()
        tcp_site = mock.MagicMock()
        server.state = server.ServerState()
        with mock.patch.dict(os.environ, {"GERT_LEITORES_CONFIG": str(bad)}), \
                mock.patch.object(server.asyncio, "start_server", start_server), \
                mock.patch.object(server.web, "TCPSite", tcp_site), \
                mock.patch.object(server, "sync_loop") as sync_loop, \
                self.assertLogs("gertec", "ERROR") as logs:
            with self.assertRaises(SystemExit) as ctx:
                asyncio.run(server.main())
        self.assertNotEqual(ctx.exception.code, 0)
        start_server.assert_not_called()
        tcp_site.assert_not_called()
        sync_loop.assert_not_called()
        self.assertIsNone(server.state.config)
        self.assertTrue(any(str(bad) in line for line in logs.output))

    def test_startup_nao_sobe_sem_arquivo(self):
        start_server = mock.AsyncMock()
        with mock.patch.dict(os.environ, {"GERT_LEITORES_CONFIG": str(Path(self.tmp.name) / "x.json")}), \
                mock.patch.object(server.asyncio, "start_server", start_server), \
                self.assertLogs("gertec", "ERROR"):
            with self.assertRaises(SystemExit):
                asyncio.run(server.main())
        start_server.assert_not_called()


class CheckConfigCliTests(unittest.TestCase):
    """python server.py --check-config — subprocess real, sem rede, tokens fake."""

    def run_check(self, leitores, empresas, tokens, raw=None):
        with tempfile.TemporaryDirectory() as tmp:
            path = write_config(tmp, leitores, raw)
            env = {"PATH": os.environ.get("PATH", ""), "GERT_LEITORES_CONFIG": str(path),
                   "GERT_EMPRESAS": json.dumps(empresas)}
            env.update({f"WEBPOSTO_TOKEN_{e}": "fake-cli-token" for e in tokens})
            return subprocess.run([PYTHON, str(SERVER_PY), "--check-config"], env=env,
                                  capture_output=True, text=True, timeout=30)

    def test_ok(self):
        r = self.run_check({"192.168.1.197": {"empresa": 7818, "nome": "Posto Waldemiro"}}, [7806, 7818], [7806, 7818])
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("192.168.1.197 -> empresa 7818 -> Posto Waldemiro (token: presente)", r.stdout)
        self.assertNotIn("fake-cli-token", r.stdout + r.stderr)

    def test_token_ausente(self):
        r = self.run_check({"192.168.1.197": {"empresa": 7818}}, [7818], [])
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("token: AUSENTE", r.stdout)

    def test_empresa_fora_do_universo(self):
        r = self.run_check({"192.168.1.197": {"empresa": 7823}}, [7818], [7823])
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("GERT_EMPRESAS", r.stdout)


# --------------------------------------------------------------------------
class IsolamentoTests(FilialTestCase):
    async def test_mesmo_ean_precos_diferentes_por_leitor(self):
        await self.full_sync()
        r197 = await self.consulta("192.168.1.197", EAN_DIFF)
        r198 = await self.consulta("192.168.1.198", EAN_DIFF)
        self.assertEqual((r197["empresa"], r197["preco"]), (7818, 5.49))
        self.assertEqual((r198["empresa"], r198["preco"]), (7823, 7.25))
        self.assertEqual(r197["resp"], preco_resp("PRODUTO TESTE DIFF", 5.49))
        self.assertEqual(r198["resp"], preco_resp("PRODUTO TESTE DIFF", 7.25))
        # bulk também isolado
        self.assertEqual((await self.consulta("192.168.1.197", EAN_BULK))["preco"], 2.50)
        self.assertEqual((await self.consulta("192.168.1.198", EAN_BULK))["preco"], 4.75)

    async def test_produto_real_waldemiro_1299_e_7823_sem_preco(self):
        await self.full_sync()
        r197 = await self.consulta("192.168.1.197", EAN_REAL)
        r198 = await self.consulta("192.168.1.198", EAN_REAL)
        self.assertEqual(r197["preco"], 12.99)
        self.assertIn(b"R$ 12,99", r197["resp"])
        self.assertIsNone(r198["preco"])
        self.assertTrue(r198["resp"].endswith(b"|SEM PRECO"))

    async def test_catalogo_por_empresa_nao_guarda_preco_nem_empresa(self):
        await self.full_sync()
        self.assertFalse(hasattr(server.state, "products"))   # sem catálogo global
        self.assertFalse(hasattr(server.state, "by_codigo"))
        for empresa in (7818, 7823):
            cache = server.empresa_cache(server.state, empresa)
            self.assertEqual(set(cache.products), {EAN_REAL, EAN_DIFF, EAN_BULK})
            for bc, prod in cache.products.items():
                self.assertNotIn("preco", prod)
                self.assertNotIn("empresa", prod)
            for pc, info in cache.by_codigo.items():
                self.assertNotIn("preco", info)
                self.assertNotIn("empresa", info)
        # Catálogo re-sincronizado depois dos preços não apaga preço.
        await server.sync_catalog(server.state.http_session)
        self.assertEqual((await self.consulta("192.168.1.197", EAN_DIFF))["preco"], 5.49)

    async def test_sync_so_empresas_configuradas(self):
        await self.full_sync()
        self.assertEqual(self.api.empresas_chamadas("PRODUTO_EMPRESA"), {7818, 7823})
        self.assertEqual(self.api.empresas_chamadas("PRODUTO"), {7818, 7823})
        self.assertNotIn(7806, server.state.empresas)
        self.assertNotIn(7820, server.state.empresas)
        # pedido explícito de empresa não configurada é ignorado
        self.api.calls.clear()
        await server.sync_prices(server.state.http_session, [7806, 7820])
        self.assertEqual(self.api.calls, [])

    async def esperado(self):
        return {
            ip: [(await self.consulta(ip, bc))["preco"] for bc in (EAN_REAL, EAN_DIFF, EAN_BULK)]
            for ip in ("192.168.1.197", "192.168.1.198")
        }

    async def test_ordem_de_sync_nao_altera_resultado(self):
        resultados = []
        for ordem in ([7823, 7818], [7818, 7823]):
            with self.subTest(ordem=ordem):
                self.reset_state(self.LEITORES)
                await self.full_sync(ordem)
                resultados.append(await self.esperado())
        self.assertEqual(resultados[0], resultados[1])
        self.assertEqual(resultados[0], {
            "192.168.1.197": [12.99, 5.49, 2.50],
            "192.168.1.198": [None, 7.25, 4.75],
        })

    async def test_sync_concorrente_nao_contamina(self):
        self.api.jitter = True
        for rodada in range(5):
            self.reset_state(self.LEITORES)
            s = server.state.http_session
            await asyncio.gather(server.sync_catalog(s, [7823]), server.sync_catalog(s, [7818]))
            await asyncio.gather(server.sync_prices(s, [7823]), server.sync_prices(s, [7818]),
                                 server.sync_prices(s, [7818]), server.sync_prices(s, [7823]))
            await asyncio.gather(server.sync_prices_progressive(s, [7818]),
                                 server.sync_prices_progressive(s, [7823]))
            got = await self.esperado()
            self.assertEqual(got, {
                "192.168.1.197": [12.99, 5.49, 2.50],
                "192.168.1.198": [None, 7.25, 4.75],
            }, f"rodada {rodada}")

    async def test_consultas_concorrentes_on_demand(self):
        self.api.jitter = True
        await server.sync_catalog(server.state.http_session)
        resps = await asyncio.gather(*[
            self.consulta(ip, EAN_DIFF) for ip in ["192.168.1.197", "192.168.1.198"] * 10
        ])
        for r in resps:
            self.assertEqual(r["preco"], {7818: 5.49, 7823: 7.25}[r["empresa"]])

    async def test_on_demand_so_na_empresa_do_leitor(self):
        await server.sync_catalog(server.state.http_session)
        self.api.calls.clear()
        r = await self.consulta("192.168.1.198", EAN_REAL)  # 7823 inativo
        self.assertIsNone(r["preco"])
        self.assertEqual(self.api.calls, [("PRODUTO_EMPRESA", 7823, PC_REAL)])
        self.assertEqual(r["status"], "no_price")
        self.assertIn(f"{PC_REAL}_7823", server.state.no_price_confirmed)

    async def test_miss_nao_consulta_api(self):
        await server.sync_catalog(server.state.http_session)
        self.api.calls.clear()
        r = await self.consulta("192.168.1.197", "0000000000000")
        self.assertEqual(r["resp"], b"#nfound")
        self.assertEqual(self.api.calls, [])


# Barcode reatribuído: o MESMO EAN aponta para produtoCodigo diferente em cada filial.
EAN_REAT = "7890000000031"
PC_REAT_A = 3000001   # produto do EAN na 7818
PC_REAT_B = 3000002   # produto do EAN na 7823
# Barcode que só existe no catálogo da 7823.
EAN_SO_B = "7890000000048"
PC_SO_B = 3000003


class CatalogoPorEmpresaTests(FilialTestCase):
    """Regressão: catálogo global sobrescrito pelo último sync trocava a identidade do produto."""

    def setUp(self):
        super().setUp()
        self.api.catalogos = {
            7818: CATALOGO + [
                {"produtoCodigo": PC_REAT_A, "nome": "PRODUTO DA 7818",
                 "produtoCodigoBarra": [{"codigoBarra": EAN_REAT}]},
            ],
            7823: CATALOGO + [
                {"produtoCodigo": PC_REAT_B, "nome": "PRODUTO DA 7823",
                 "produtoCodigoBarra": [{"codigoBarra": EAN_REAT}]},
                {"produtoCodigo": PC_SO_B, "nome": "SO NA 7823",
                 "produtoCodigoBarra": [{"codigoBarra": EAN_SO_B}]},
            ],
        }
        # Preços ativos cruzados: contaminação de identidade viraria preço ERRADO, não só "SEM PRECO".
        self.api.precos.update({
            (7818, PC_REAT_A): {"precoVenda": 8.10, "ativo": True},
            (7823, PC_REAT_B): {"precoVenda": 9.20, "ativo": True},
            (7818, PC_REAT_B): {"precoVenda": 1.11, "ativo": True},
            (7823, PC_REAT_A): {"precoVenda": 2.22, "ativo": True},
            (7823, PC_SO_B): {"precoVenda": 3.30, "ativo": True},
            (7818, PC_SO_B): {"precoVenda": 4.40, "ativo": True},
        })

    async def assertReatribuidoIsolado(self):
        r197 = await self.consulta("192.168.1.197", EAN_REAT)
        r198 = await self.consulta("192.168.1.198", EAN_REAT)
        self.assertEqual((r197["empresa"], r197["nome"], r197["preco"]), (7818, "PRODUTO DA 7818", 8.10))
        self.assertEqual((r198["empresa"], r198["nome"], r198["preco"]), (7823, "PRODUTO DA 7823", 9.20))
        self.assertEqual(r197["product"]["produtoCodigo"], PC_REAT_A)
        self.assertEqual(r198["product"]["produtoCodigo"], PC_REAT_B)
        self.assertEqual(r197["resp"], preco_resp("PRODUTO DA 7818", 8.10))
        self.assertEqual(r198["resp"], preco_resp("PRODUTO DA 7823", 9.20))
        # Produto real continua certo nas duas filiais.
        self.assertEqual((await self.consulta("192.168.1.197", EAN_REAL))["preco"], 12.99)
        self.assertIsNone((await self.consulta("192.168.1.198", EAN_REAL))["preco"])

    async def test_barcode_reatribuido_nas_duas_ordens_de_sync(self):
        for ordem in ([7818, 7823], [7823, 7818]):
            with self.subTest(ordem=ordem):
                self.reset_state(self.LEITORES)
                await self.full_sync(ordem)
                await self.assertReatribuidoIsolado()

    async def test_barcode_reatribuido_sync_concorrente(self):
        self.api.jitter = True
        for rodada in range(5):
            with self.subTest(rodada=rodada):
                self.reset_state(self.LEITORES)
                s = server.state.http_session
                await asyncio.gather(server.sync_catalog(s, [7823]), server.sync_catalog(s, [7818]),
                                     server.sync_prices(s, [7818]), server.sync_prices(s, [7823]))
                await asyncio.gather(server.sync_prices_progressive(s, [7818]),
                                     server.sync_prices_progressive(s, [7823]))
                await self.assertReatribuidoIsolado()

    async def test_barcode_reatribuido_on_demand_sem_progressivo(self):
        s = server.state.http_session
        await server.sync_catalog(s, [7818, 7823])
        self.api.calls.clear()
        await self.assertReatribuidoIsolado()
        on_demand = {(e, pc) for (ep, e, pc) in self.api.calls if ep == "PRODUTO_EMPRESA"}
        self.assertIn((7818, PC_REAT_A), on_demand)
        self.assertIn((7823, PC_REAT_B), on_demand)
        self.assertNotIn((7818, PC_REAT_B), on_demand)
        self.assertNotIn((7823, PC_REAT_A), on_demand)

    async def test_progressivo_itera_catalogo_da_propria_empresa(self):
        await self.full_sync()
        consultados = {(e, pc) for (ep, e, pc) in self.api.calls if ep == "PRODUTO_EMPRESA" and pc}
        self.assertNotIn((7818, PC_REAT_B), consultados)
        self.assertNotIn((7818, PC_SO_B), consultados)
        self.assertNotIn((7823, PC_REAT_A), consultados)
        self.assertNotIn(PC_SO_B, server.empresa_cache(server.state, 7818).prices)
        self.assertNotIn(PC_REAT_B, server.empresa_cache(server.state, 7818).prices)

    async def test_barcode_so_no_catalogo_de_b_nao_vaza_para_a(self):
        await self.full_sync()
        self.api.calls.clear()
        r197 = await self.consulta("192.168.1.197", EAN_SO_B)
        self.assertEqual((r197["status"], r197["resp"]), ("miss", b"#nfound"))
        self.assertIsNone(r197["preco"])
        self.assertIsNone(r197["nome"])
        self.assertIsNone(r197["product"])
        self.assertNotIn(b"SO NA 7823", r197["resp"])
        self.assertEqual(self.api.calls, [])  # nenhuma API (nem da 7823, nem da 7818)
        # A filial dona do barcode continua servindo o seu.
        r198 = await self.consulta("192.168.1.198", EAN_SO_B)
        self.assertEqual((r198["nome"], r198["preco"]), ("SO NA 7823", 3.30))

    async def test_catalogo_de_a_nao_sincronizado_fica_sem_preco(self):
        await self.full_sync([7823])   # só a 7823 sincronizou
        self.api.calls.clear()
        for bc in (EAN_REAT, EAN_SO_B, EAN_REAL):
            r197 = await self.consulta("192.168.1.197", bc)
            self.assertEqual(r197["status"], "lookup_failed")
            self.assertIsNone(r197["preco"])
            self.assertIsNone(r197["nome"])
            self.assertEqual(r197["resp"], preco_resp(server.CATALOGO_INDISPONIVEL, None))
            self.assertNotIn(b"7823", r197["resp"])
        self.assertEqual(self.api.calls, [])

    async def test_drop_price_de_uma_filial_nao_remove_barcode_da_outra(self):
        await self.full_sync()
        server._drop_price(7823, PC_REAT_B)
        server._drop_price(7823, PC_REAT_A)   # PC_REAT_A não existe no catálogo da 7823
        cache_a = server.empresa_cache(server.state, 7818)
        self.assertEqual(cache_a.barcode_preco[EAN_REAT]["preco"], 8.10)
        self.assertEqual(cache_a.prices[PC_REAT_A]["preco"], 8.10)
        self.assertNotIn(EAN_REAT, server.empresa_cache(server.state, 7823).barcode_preco)
        self.assertEqual((await self.consulta("192.168.1.197", EAN_REAT))["preco"], 8.10)

    async def test_store_price_casa_barcode_no_catalogo_da_propria_empresa(self):
        await server.sync_catalog(server.state.http_session)
        server._store_price(7823, PC_REAT_A, 2.22)   # pc de outra filial: não liga barcode na 7823
        self.assertNotIn(EAN_REAT, server.empresa_cache(server.state, 7823).barcode_preco)
        server._store_price(7823, PC_REAT_B, 9.20)
        self.assertEqual(server.empresa_cache(server.state, 7823).barcode_preco[EAN_REAT]["preco"], 9.20)
        self.assertNotIn(EAN_REAT, server.empresa_cache(server.state, 7818).barcode_preco)

    async def test_revalidacao_ttl_fora_do_bulk_serve_preco_novo(self):
        for ordem in ([7818, 7823], [7823, 7818]):
            with self.subTest(ordem=ordem):
                self.reset_state(self.LEITORES)
                self.api.precos[(7818, PC_REAT_A)] = {"precoVenda": 8.10, "ativo": True}
                await self.full_sync(ordem)
                cache = server.empresa_cache(server.state, 7818)
                self.assertEqual(cache.prices[PC_REAT_A]["preco"], 8.10)   # veio do progressivo
                cache.prices[PC_REAT_A]["updated_at"] = time.time() - 99999
                self.api.precos[(7818, PC_REAT_A)] = {"precoVenda": 8.55, "ativo": True}
                self.api.calls.clear()
                r = await self.consulta("192.168.1.197", EAN_REAT)
                self.assertEqual((r["preco"], r["status"], r["nome"]), (8.55, "hit_revalidated", "PRODUTO DA 7818"))
                self.assertEqual(self.api.calls, [("PRODUTO_EMPRESA", 7818, PC_REAT_A)])

    async def test_lookup_dashboard_resolve_no_catalogo_da_empresa(self):
        await self.full_sync([7823, 7818])
        for qs, esperado in ((f"barcode={EAN_REAT}&empresa=7818", (7818, "PRODUTO DA 7818", PC_REAT_A, 8.10)),
                             (f"barcode={EAN_REAT}&ip=192.168.1.198", (7823, "PRODUTO DA 7823", PC_REAT_B, 9.20))):
            req = make_mocked_request("GET", f"/lookup?{qs}")
            body = json.loads((await server.dashboard_lookup(req)).body)
            self.assertEqual((body["empresa"], body["nome"], body["produtoCodigo"], body["preco"]), esperado)
        req = make_mocked_request("GET", f"/lookup?barcode={EAN_SO_B}&empresa=7818")
        body = json.loads((await server.dashboard_lookup(req)).body)
        self.assertFalse(body["found"])
        self.assertEqual(body["empresa"], 7818)
        self.assertNotIn("nome", body)
        self.assertNotIn("preco", body)
        # código de produto da outra filial também não resolve
        req = make_mocked_request("GET", f"/lookup?barcode={PC_SO_B}&empresa=7818")
        self.assertFalse(json.loads((await server.dashboard_lookup(req)).body)["found"])

    async def test_api_conta_catalogo_por_empresa(self):
        await self.full_sync()
        body = json.loads((await server.dashboard_api(make_mocked_request("GET", "/api"))).body)
        self.assertEqual(body["empresas"]["7818"]["catalogo_barcodes"], 4)
        self.assertEqual(body["empresas"]["7823"]["catalogo_barcodes"], 5)
        self.assertEqual(body["cache"]["products"], 5)    # barcodes distintos (união)
        self.assertEqual(body["cache"]["by_codigo"], 6)   # produtoCodigo distintos (união)


class LeitorNaoMapeadoTests(FilialTestCase):
    async def test_ip_sem_vinculo_sem_preco_e_sem_api(self):
        await self.full_sync()
        # Todos os caches com preço para o produto, inclusive empresas não usadas.
        for emp, preco in ((7806, 9.99), (7818, 12.99), (7820, 6.00), (7823, 6.00)):
            server._store_price(emp, PC_REAL, preco)
        self.api.calls.clear()
        with self.assertLogs("gertec", "WARNING") as logs:
            r = await self.consulta("10.0.0.50", EAN_REAL)
        self.assertEqual(r["resp"], preco_resp("TERMINAL NAO CONFIGURADO", None))
        self.assertIsNone(r["preco"])
        self.assertIsNone(r["empresa"])
        for valor in (b"9,99", b"12,99", b"6,00", b"R$"):
            self.assertNotIn(valor, r["resp"])
        self.assertEqual(self.api.calls, [])
        self.assertTrue(any("10.0.0.50" in line for line in logs.output))

    async def test_sem_config_carregada_falha_fechado(self):
        server.state.config = None
        server._store_price(7818, PC_REAL, 12.99)
        r = await self.consulta("192.168.1.197", EAN_REAL)
        self.assertEqual(r["status"], "unmapped")
        self.assertIsNone(r["preco"])
        self.assertEqual(self.api.calls, [])


class AtivoTests(FilialTestCase):
    LEITORES = {
        "192.168.1.196": {"empresa": 7806},
        "192.168.1.197": {"empresa": 7818},
        "192.168.1.199": {"empresa": 7820},
        "192.168.1.198": {"empresa": 7823},
    }
    ESPERADO = {"192.168.1.196": None, "192.168.1.197": 12.99, "192.168.1.199": None, "192.168.1.198": None}
    EMPRESA = {"192.168.1.196": 7806, "192.168.1.197": 7818, "192.168.1.199": 7820, "192.168.1.198": 7823}

    def assertSemPrecoInativo(self):
        for emp in (7806, 7820, 7823):
            cache = server.empresa_cache(server.state, emp)
            self.assertNotIn(PC_REAL, cache.prices, f"preço inativo gravado em {emp}")
            self.assertNotIn(EAN_REAL, cache.barcode_preco)

    async def check_respostas(self):
        for ip, esperado in self.ESPERADO.items():
            r = await self.consulta(ip, EAN_REAL)
            self.assertEqual(r["empresa"], self.EMPRESA[ip])
            self.assertEqual(r["preco"], esperado, ip)
            if esperado is None:
                self.assertTrue(r["resp"].endswith(b"|SEM PRECO"), r["resp"])
            else:
                self.assertIn(b"R$ 12,99", r["resp"])

    async def test_on_demand_respeita_ativo(self):
        await server.sync_catalog(server.state.http_session)
        await self.check_respostas()
        self.assertSemPrecoInativo()
        self.assertEqual(server.empresa_cache(server.state, 7818).prices[PC_REAL]["preco"], 12.99)

    async def test_bulk_respeita_ativo(self):
        self.api.bulk = {e: {PC_REAL} for e in TODAS}
        s = server.state.http_session
        await server.sync_catalog(s)
        await server.sync_prices(s)
        self.assertSemPrecoInativo()
        self.api.calls.clear()
        await self.check_respostas()
        # 7818 servido do cache do bulk; inativos já em cache negativo -> nenhuma chamada
        self.assertEqual(self.api.calls, [])

    async def test_progressivo_respeita_ativo(self):
        s = server.state.http_session
        await server.sync_catalog(s)
        await server.sync_prices_progressive(s)
        self.assertSemPrecoInativo()
        self.api.calls.clear()
        await self.check_respostas()
        self.assertEqual(self.api.calls, [])

    async def test_preco_em_cache_some_quando_item_fica_inativo(self):
        s = server.state.http_session
        await server.sync_catalog(s)
        server._store_price(7818, PC_REAL, 12.99, now=time.time() - 4000)  # TTL vencido
        self.api.precos[(7818, PC_REAL)] = {"precoVenda": 12.99, "ativo": False}
        r = await self.consulta("192.168.1.197", EAN_REAL)
        self.assertIsNone(r["preco"])
        self.assertNotIn(PC_REAL, server.empresa_cache(server.state, 7818).prices)

    async def test_ativo_ausente_e_ativo_zero(self):
        s = server.state.http_session
        await server.sync_catalog(s)
        self.api.precos[(7818, PC_REAL)] = {"precoVenda": 12.99}  # sem campo ativo -> ativo
        self.api.precos[(7823, PC_REAL)] = {"precoVenda": 6.00, "ativo": 0}
        self.assertEqual((await self.consulta("192.168.1.197", EAN_REAL))["preco"], 12.99)
        self.assertIsNone((await self.consulta("192.168.1.198", EAN_REAL))["preco"])

    async def test_preco_zero_ativo_sem_preco(self):
        s = server.state.http_session
        await server.sync_catalog(s)
        self.api.precos[(7818, PC_REAL)] = {"precoVenda": 0, "ativo": True}
        self.assertIsNone((await self.consulta("192.168.1.197", EAN_REAL))["preco"])


class RevalidacaoTests(FilialTestCase):
    async def test_preco_fora_do_bulk_e_revalidado_pelo_progressivo(self):
        s = server.state.http_session
        await server.sync_catalog(s)
        antigo = time.time() - 4000  # > PRICE_TTL
        server._store_price(7818, PC_REAL, 9.99, now=antigo)
        await server.sync_prices(s)  # bulk não cobre PC_REAL
        self.assertEqual(server.empresa_cache(server.state, 7818).prices[PC_REAL]["preco"], 9.99)
        await server.sync_prices_progressive(s)
        entry = server.empresa_cache(server.state, 7818).prices[PC_REAL]
        self.assertEqual(entry["preco"], 12.99)
        self.assertGreater(entry["updated_at"], antigo + 3000)
        self.assertEqual(server.empresa_cache(server.state, 7818).barcode_preco[EAN_REAL]["preco"], 12.99)

    async def test_progressivo_nao_revalida_preco_fresco(self):
        s = server.state.http_session
        await server.sync_catalog(s)
        server._store_price(7818, PC_REAL, 11.11)
        self.api.calls.clear()
        await server.sync_prices_progressive(s, [7818])
        self.assertNotIn(("PRODUTO_EMPRESA", 7818, PC_REAL), self.api.calls)
        self.assertEqual(server.empresa_cache(server.state, 7818).prices[PC_REAL]["preco"], 11.11)

    async def test_consulta_com_ttl_vencido_revalida_e_serve_novo(self):
        await server.sync_catalog(server.state.http_session)
        server._store_price(7818, PC_REAL, 9.99, now=time.time() - 4000)
        self.api.calls.clear()
        r = await self.consulta("192.168.1.197", EAN_REAL)
        self.assertEqual((r["preco"], r["status"]), (12.99, "hit_revalidated"))
        self.assertEqual(self.api.calls, [("PRODUTO_EMPRESA", 7818, PC_REAL)])

    async def test_consulta_com_preco_fresco_nao_chama_api(self):
        await server.sync_catalog(server.state.http_session)
        server._store_price(7818, PC_REAL, 9.99)
        self.api.calls.clear()
        r = await self.consulta("192.168.1.197", EAN_REAL)
        self.assertEqual(r["preco"], 9.99)
        self.assertEqual(self.api.calls, [])


class ApiIndisponivelTests(FilialTestCase):
    async def asyncSetUp(self):
        await server.sync_catalog(server.state.http_session)
        # A outra filial tem preço em cache: nunca pode vazar.
        server._store_price(7823, PC_REAL, 7.77)
        self.api.down = set(TODAS)
        self.api.calls.clear()

    async def test_stale_dentro_da_janela_mesma_empresa(self):
        server._store_price(7818, PC_REAL, 11.50, now=time.time() - 7200)  # TTL < idade < STALE_MAX
        r = await self.consulta("192.168.1.197", EAN_REAL)
        self.assertEqual((r["preco"], r["status"], r["empresa"]), (11.50, "hit_stale", 7818))
        self.assertEqual(self.api.empresas_chamadas(), {7818})

    async def test_stale_fora_da_janela_sem_preco(self):
        server._store_price(7818, PC_REAL, 11.50, now=time.time() - 30000)  # > STALE_MAX
        r = await self.consulta("192.168.1.197", EAN_REAL)
        self.assertIsNone(r["preco"])
        self.assertTrue(r["resp"].endswith(b"|SEM PRECO"))
        self.assertNotIn(b"7,77", r["resp"])
        self.assertEqual(self.api.empresas_chamadas(), {7818})

    async def test_sem_cache_sem_preco_e_nao_tenta_outra_filial(self):
        r = await self.consulta("192.168.1.197", EAN_REAL)
        self.assertEqual((r["preco"], r["status"]), (None, "lookup_failed"))
        self.assertNotIn(b"7,77", r["resp"])
        self.assertEqual(self.api.calls, [("PRODUTO_EMPRESA", 7818, PC_REAL)])
        self.assertNotIn(f"{PC_REAL}_7818", server.state.no_price_confirmed)

    async def test_erro_string_da_api_tratado_como_falha(self):
        async def erro(session, endpoint, params, timeout):
            self.api.calls.append((endpoint, params["empresaCodigo"], params.get("produtoCodigo")))
            return ["Chave invalida"]
        with mock.patch.object(server, "api_get_json", erro):
            r = await self.consulta("192.168.1.197", EAN_REAL)
        self.assertEqual(r["status"], "lookup_failed")
        self.assertEqual(self.api.empresas_chamadas(), {7818})

    async def test_progressivo_com_api_fora_mantem_stale(self):
        server._store_price(7818, PC_REAL, 11.50, now=time.time() - 7200)
        await server.sync_prices_progressive(server.state.http_session, [7818])
        self.assertEqual(server.empresa_cache(server.state, 7818).prices[PC_REAL]["preco"], 11.50)


class DashboardTests(FilialTestCase):
    async def lookup(self, qs):
        req = make_mocked_request("GET", f"/lookup?{qs}")
        resp = await server.dashboard_lookup(req)
        return resp.status, json.loads(resp.body)

    async def test_lookup_multi_leitor_exige_parametro(self):
        await self.full_sync()
        status, body = await self.lookup(f"barcode={EAN_DIFF}")
        self.assertEqual(status, 400)
        self.assertEqual({o["empresa"] for o in body["opcoes"]}, {7818, 7823})

    async def test_lookup_por_ip_e_por_empresa(self):
        await self.full_sync()
        status, body = await self.lookup(f"barcode={EAN_DIFF}&ip=192.168.1.197")
        self.assertEqual((status, body["empresa"], body["preco"], body["filial"]), (200, 7818, 5.49, "Posto Waldemiro"))
        status, body = await self.lookup(f"barcode={EAN_DIFF}&empresa=7823")
        self.assertEqual((status, body["empresa"], body["preco"]), (200, 7823, 7.25))
        status, _ = await self.lookup(f"barcode={EAN_DIFF}&empresa=7806")
        self.assertEqual(status, 400)
        status, _ = await self.lookup(f"barcode={EAN_DIFF}&ip=10.0.0.1")
        self.assertEqual(status, 404)

    async def test_lookup_leitor_unico_infere_empresa(self):
        self.reset_state({"192.168.1.197": {"empresa": 7818, "nome": "Posto Waldemiro"}})
        await self.full_sync()
        server._store_price(7823, PC_REAL, 7.77)  # cache de outra empresa não deve aparecer
        status, body = await self.lookup(f"barcode={EAN_REAL}")
        self.assertEqual((status, body["found"], body["empresa"], body["preco"]), (200, True, 7818, 12.99))
        status, body = await self.lookup("barcode=0000000000000")
        self.assertEqual((status, body["found"], body["empresa"]), (200, False, 7818))

    async def test_api_contagem_por_empresa(self):
        await self.full_sync()
        resp = await server.dashboard_api(make_mocked_request("GET", "/api"))
        body = json.loads(resp.body)
        self.assertEqual(set(body["empresas"]), {"7818", "7823"})
        self.assertEqual(body["empresas"]["7818"]["produtos"], 3)  # REAL, DIFF, BULK
        self.assertEqual(body["empresas"]["7823"]["produtos"], 2)  # REAL inativo
        for chave in ("stats", "cache", "terminals", "recent_queries", "errors"):
            self.assertIn(chave, body)

    async def test_index_renderiza(self):
        resp = await server.dashboard_index(make_mocked_request("GET", "/"))
        self.assertIn("192.168.1.197 → 7818 (Posto Waldemiro)", resp.text)


class HistoricoLogTests(unittest.TestCase):
    """As novas linhas HIT (com emp=) continuam legíveis pelo histórico pós-restart."""

    def test_linhas_novas_parseiam(self):
        for tag in ("", "on-demand ", "revalidated ", "stale "):
            with self.subTest(tag=tag):
                q = server._parse_query_log_line(
                    f"2026-10-20 15:00:00,000 [INFO] HIT ({tag}emp=7818): {EAN_REAL} -> ORELHA R$12.99")
                self.assertEqual((q["barcode"], q["nome"], q["preco"], q["hit"]), (EAN_REAL, "ORELHA", 12.99, True))
        q = server._parse_query_log_line(f"2026-10-20 15:00:00,000 [INFO] HIT (no price emp=7823): {EAN_REAL} -> ORELHA")
        self.assertEqual((q["nome"], q["preco"]), ("ORELHA", None))


class TcpEndToEndTests(FilialTestCase):
    """Protocolo real via TCP local (127.0.0.1), com handshake G2E."""

    async def bip(self, barcode):
        srv = await asyncio.start_server(server.handle_terminal, "127.0.0.1", 0)
        port = srv.sockets[0].getsockname()[1]
        async with srv:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            self.assertEqual(await reader.readexactly(3), b"#ok")
            writer.write(b"#bpg2e|4.3.2 S")
            await writer.drain()
            self.assertEqual(await reader.readexactly(11), b"#alwayslive")
            writer.write(b"#alwayslive_ok")
            await writer.drain()
            await asyncio.sleep(0.5)
            writer.write(f"#{barcode}".encode())
            await writer.drain()
            resp = await asyncio.wait_for(reader.read(255), timeout=5)
            writer.close()
            await writer.wait_closed()
            await asyncio.sleep(0.05)
        return resp

    async def test_tcp_leitor_mapeado(self):
        self.reset_state({"127.0.0.1": {"empresa": 7818, "nome": "Teste local"}})
        await self.full_sync()
        resp = await self.bip(EAN_REAL)
        self.assertEqual(resp, preco_resp("ORELHA DE GATO DOCE TIMBE", 12.99))
        self.assertEqual(server.state.query_log[-1]["empresa"], 7818)
        self.assertEqual(server.state.query_log[-1]["preco"], 12.99)

    async def test_tcp_leitor_nao_mapeado(self):
        await self.full_sync()  # config só com 192.168.1.197/198
        server._store_price(7818, PC_REAL, 12.99)
        self.api.calls.clear()
        resp = await self.bip(EAN_REAL)
        self.assertEqual(resp, preco_resp("TERMINAL NAO CONFIGURADO", None))
        self.assertEqual(self.api.calls, [])
        self.assertEqual(server.state.stats["unmapped_queries"], 1)

    async def test_tcp_catalogo_da_filial_indisponivel(self):
        self.reset_state({"127.0.0.1": {"empresa": 7818, "nome": "Teste local"}})
        self.api.down.add(7818)
        await self.full_sync()  # catálogo da 7818 não carrega
        resp = await self.bip(EAN_REAL)
        self.assertEqual(resp, preco_resp(server.CATALOGO_INDISPONIVEL, None))
        self.assertEqual(server.state.query_log[-1]["empresa"], 7818)
        self.assertFalse(server.state.query_log[-1]["hit"])
        self.assertEqual(server.state.stats["misses"], 1)


if __name__ == "__main__":
    unittest.main()
