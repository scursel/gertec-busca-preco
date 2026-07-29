# Gertec Busca Preço Server

Servidor Linux que substitui o app Java dos terminais de consulta **Gertec G2 / G2S / G2E**. Consulta preços direto do ERP **WebPosto** via API de integração — sem depender de caixa aberto.

## Como funciona

```
┌──────────────┐    TCP 6500     ┌──────────────────┐    HTTPS     ┌──────────┐
│  Terminal     │◄──────────────►│  gertec-server    │◄────────────►│ WebPosto │
│  Gertec G2x   │  #barcode →    │  (Python/asyncio) │  API REST    │   ERP    │
│               │  ← #NOME|PRECO │                   │              │          │
└──────────────┘                 │  + Dashboard HTTP │              └──────────┘
                                 │    porta 8650     │
                                 └──────────────────┘
```

1. **Sync catálogo** — busca `GRUPO` → `PRODUTO` por grupo na API WebPosto (bypass do limite de 2.000 itens por chamada)
2. **Sync preços** — `PRODUTO_EMPRESA` bulk (2.000 mais recentes) a cada 5 min
3. **Lookup sob demanda** — quando o terminal bipa um produto sem preço no cache, o servidor busca na API em tempo real (~1s) e cacheia
4. **Protocolo TCP** — handshake `#ok` → `#bpg2s`/`#bpg2e`, keep-alive `#alwayslive`, consulta `#<barcode>` → `#NOME|PRECO` ou `#nfound`
5. **Propaganda** — GIFs rotativos em terminais G2S (via TCP `#gif`); mensagens de texto em G2E (via interface web do terminal)

## Modelos suportados

| Modelo | Handshake | GIF via TCP | Mensagem via TCP | Config propaganda |
|--------|-----------|-------------|------------------|-------------------|
| **G2S** | `#bpg2s\|...` | ✅ | ✅ | Dashboard (upload GIF) |
| **G2E** | `#bpg2e\|...` | ❌ | ❌ | Interface web do terminal (proxy no dashboard) |

O servidor detecta o modelo automaticamente no handshake e adapta o comportamento.

## Requisitos

- Python 3.10+
- `aiohttp`
- Token de integração WebPosto (obtido no ERP: Configurações → Integração)
- Terminal Gertec na mesma rede (ou com rota TCP até o servidor)

## Instalação

```bash
git clone https://github.com/scursel/gertec-busca-preco.git
cd gertec-busca-preco
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configurar variáveis de ambiente
cp .env.example .env
# Editar .env com seu token WebPosto e código de empresa
```

### Variáveis de ambiente

| Variável | Default | Descrição |
|----------|---------|-----------|
| `WEBPOSTO_TOKEN_<EMPRESA>` | — | Token de integração (ex: `WEBPOSTO_TOKEN_1=*** |
| `GERT_EMPRESAS` | `[1]` | Lista JSON de códigos de empresa |
| `GERT_TCP_PORT` | `6500` | Porta TCP do protocolo Gertec |
| `GERT_DASH_PORT` | `8650` | Porta HTTP do dashboard |
| `GERT_SERVER_IP` | `0.0.0.0` | IP exibido nas instruções do dashboard |
| `GERT_SYNC_PRICES_SEC` | `300` | Intervalo de sync de preços (segundos) |
| `GERT_SYNC_CATALOG_SEC` | `1800` | Intervalo de sync de catálogo (segundos) |
| `GERT_GIF_ROTATION_SEC` | `30` | Intervalo de rotação de GIFs (segundos) |
| `GERT_WELCOME_LINE1` | `CONSULTE AQUI!` | Mensagem de boas-vindas G2S (linha 1) |
| `GERT_WELCOME_LINE2` | `BEM-VINDO!` | Mensagem de boas-vindas G2S (linha 2) |
| `GERT_LOG_DIR` | `./logs` | Diretório de logs |
| `GERT_GIF_DIR` | `./gifs` | Diretório de GIFs de propaganda |
| `G2E_ADMIN_USER` | `admin` | Usuário da interface web do G2E |
| `G2E_ADMIN_PASS` | `admin` | Senha da interface web do G2E |
| `WEBPOSTO_BASE_URL` | `https://web.qualityautomacao.com.br/INTEGRACAO` | URL base da API |

### Rodar

```bash
source .env
python3 server.py
```

### systemd (recomendado para produção)

```bash
cp gertec-server.service ~/.config/systemd/user/
# Editar o .env path no arquivo se necessário
systemctl --user daemon-reload
systemctl --user enable --now gertec-server.service
```

## Configurar o terminal Gertec

No menu de rede do terminal:

1. **Servidor de consulta**: IP do servidor + porta TCP `6500`
2. **Não** usar `http://` — é TCP puro
3. **Não** usar a porta `8650` (é o dashboard)

## Dashboard

Acesse `http://<ip-servidor>:8650`:

- **Estatísticas** — consultas, hits, misses, terminais ativos, cache
- **Terminais** — modelo, endereço, consultas por terminal
- **Log de consultas** — últimas 50 consultas com barcode, produto, preço
- **Upload de GIFs** — propaganda para terminais G2S (máx 124KB, 320×240)
- **Mensagens G2E** — editor das 4 linhas de texto idle (proxy para interface web do terminal)
- **Diagnóstico** — busca de barcode no cache

## API do dashboard

| Endpoint | Método | Descrição |
|----------|--------|-----------|
| `/` | GET | Dashboard HTML |
| `/api` | GET | JSON com stats, cache, terminais, consultas |
| `/lookup?barcode=X` | GET | Buscar barcode no cache |
| `/gifs` | GET | Listar GIFs |
| `/upload-gif` | POST | Upload de GIF (multipart) |
| `/gifs/{name}` | DELETE | Remover GIF |
| `/g2e/messages` | GET | Ler mensagens idle do G2E |
| `/g2e/messages` | POST | Salvar mensagens idle no G2E |

## Limitações conhecidas da API WebPosto

- `PRODUTO` retorna no máximo **2.000 itens** por chamada; o parâmetro `codigo` para paginação é **ignorado**. Solução: filtrar por `grupoCodigo` (funciona).
- `PRODUTO_EMPRESA` também ignora paginação por `codigo`; `limite=2000` é o máximo aceito (2.500 → HTTP 400). O filtro `grupoCodigo` retorna HTTP 400.
- `PRODUTO_EMPRESA?produtoCodigo=X` funciona como filtro exato (1 resultado) — usado para lookup sob demanda.
- A busca por nome (`?nome=TERMO`) não é confiável — retorna sempre os mesmos resultados.

## Protocolo TCP Gertec

### Handshake

```
Servidor → Terminal:  #ok
Terminal → Servidor:  #bpg2s|4.3.2 S   (ou #bpg2e|4.3.2 S)
Servidor → Terminal:  #alwayslive
Terminal → Servidor:  #alwayslive_ok
```

### Keep-alive

```
Servidor → Terminal:  #live?     (após 120s sem dados)
Terminal → Servidor:  #live
```

### Consulta

```
Terminal → Servidor:  #7891234567890
Servidor → Terminal:  #PRODUTO EXEMPLO|12.90
                  ou  #PRODUTO EXEMPLO|SEM PRECO
                  ou  #nfound
```

### GIF (somente G2S)

```
Header: #gif + index(2) + loops(2) + tempo(2) + tamanho(6) + 0000 + \x17
Dados:  bytes do GIF (máx 124KB, 320×240)
```

### Mensagem (somente G2S)

```
#mesg + chr(len1+48) + linha1 + chr(len2+48) + linha2 + chr(tempo+48) + chr(48)
```

## Licença

MIT — veja [LICENSE](LICENSE).
