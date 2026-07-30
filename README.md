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

## Para agentes de código — instruções de implementação

Se você é um agente AI ou desenvolvedor implementando este projeto, siga estas instruções na ordem:

### 1. Setup do ambiente

```bash
git clone https://github.com/scursel/gertec-busca-preco.git
cd gertec-busca-preco
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### 2. Variáveis obrigatórias no `.env`

Pelo menos uma empresa deve ter token configurado:

```env
WEBPOSTO_TOKEN_1=seu-token-aqui
GERT_EMPRESAS=[1]
```

Tokens são obtidos no ERP WebPosto: Configurações → Integração. Para múltiplas empresas, configure um token por empresa:

```env
WEBPOSTO_TOKEN_1=token-empresa-1
WEBPOSTO_TOKEN_2=token-empresa-2
GERT_EMPRESAS=[1,2]
```

### 3. Executar localmente

```bash
source .env
python3 server.py
```

O servidor escuta:
- **TCP porta 6500** — protocolo Gertec (terminais físicos)
- **HTTP porta 8650** — dashboard web

### 4. Arquitetura

```
server.py
├── sync_catalog()        # Catálogo PRODUTO via GRUPO (bypass limite 2000)
├── sync_prices_bulk()    # Preços PRODUTO_EMPRESA (2000 mais recentes)
├── sync_prices_progressive()  # Preços dos demais (~12K produtos, 4h cobertura)
├── fetch_price_on_demand()    # Lookup em tempo real por empresa
└── handle_terminal()     # Protocolo TCP Gertec
```

### 5. Endpoint PRODUTO_EMPRESA — documentação

⚠️ `PRODUTO_EMPRESA` **não está no inventário oficial** de endpoints da API WebPosto (37 endpoints documentados). É um endpoint funcional mas não documentado pela Quality Automação. Se a API parar de responder, esteja preparado para fallback.

**Funcionalidade:** retorna preço de venda, custo, estoque e status (ativo/inativo) por produto e empresa.

**Parâmetros:**
```
GET /INTEGRACAO/PRODUTO_EMPRESA?CHAVE=<token>&empresaCodigo=<emp>&limite=1&produtoCodigo=<cod>
```

**Resposta típica:**
```json
{
  "resultados": [{
    "produtoCodigo": 1878759,
    "precoVenda": 21.99,
    "precoCusto": 10.93,
    "estoqueQtde": 1.0,
    "ativo": true,
    "ultimaAlteracao": "2026-06-23T10:45:44"
  }]
}
```

### 6. Pitfalls críticos

| Problema | Causa | Solução |
|----------|-------|---------|
| Produto retorna SEM PRECO no terminal | Cache negativo chaveado sem empresa | Usar `produtoCodigo_empresa` como chave (v1.1+) |
| Barcode indexado na empresa errada | Sync de catálogo usa a última empresa do loop | On-demand tenta TODAS as empresas (v1.1+) |
| Progressive sync não cobre produto | Filtro por empresa no sync progressivo | Remover filtro, verificar todas as empresas (v1.1+) |
| PRODUTO_EMPRESA ignora `produtoCodigoBarra` | API só aceita `produtoCodigo` | Mapear barcode → código localmente |
| `PRODUTO` ignora filtro `codigo` | Parâmetro não funciona | Buscar por `grupoCodigo` ou varrer catálogo |
| `PRODUTO` ignora filtro `nome` | Busca retorna sempre os mesmos 3 resultados | Filtrar localmente após sync |
| Limite 2000 em PRODUTO_EMPRESA | HTTP 400 se limite > 2000 | Usar `limite=2000` |

### 7. Multi-empresa: como funciona

1. **Sync de catálogo** (`PRODUTO`): busca por GRUPO, indexa `produtoCodigo → barcode(s)`. O catálogo é compartilhado — mesmo código em todas as empresas.

2. **Sync de preços** (`PRODUTO_EMPRESA`): bulk (2000 por empresa) + progressive sync (200 por ciclo, 5 min). Cada empresa tem seus próprios preços/estoque/status.

3. **Lookup sob demanda** (TCP `#barcode`): quando o terminal bipa um produto sem preço:
   - Busca empresa primária (a indexada no cache)
   - Se retorna `preco=0` ou `ativo=False`, tenta **todas as outras empresas**
   - Primeiro preço > 0 encontrado é cacheado e retornado
   - Se nenhuma empresa tem preço, marca como `confirmado_sem_preco` (não re-consulta por 1h)

4. **Cache negativo**: chaveado por `produtoCodigo_empresa` (não apenas `produtoCodigo`). Produto inativo na empresa A não bloqueia consultas na empresa B.

### 8. Protocolo TCP Gertec

```python
# Handshake (servidor inicia)
server → terminal:  b"#ok"
terminal → server:  b"#bpg2s|4.3.2 S"  # G2S ou #bpg2e para G2E
server → terminal:  b"#alwayslive" + dados do GIF (G2S) ou vazio (G2E)

# Consulta
terminal → server:  b"#7891234567890"  # barcode
server → terminal:  b"#PRODUTO EXEMPLO                                                |R$ 12,90"  # encontrado com preço
                  ou b"#PRODUTO EXEMPLO|SEM PRECO"  # encontrado sem preço
                  ou b"#nfound"  # barcode não cadastrado

O campo do nome ocupa até **4 linhas de 20 colunas (80 bytes)**. O terminal
faz a divisão visual em blocos de 20 colunas; não é necessário enviar `\\n`.
O campo do preço ocupa uma linha de até 20 caracteres.

# Keep-alive (após 120s sem dados)
server → terminal:  b"#live?"
terminal → server:  b"#live"
```

### 9. Dashboard HTTP (porta 8650)

| Endpoint | Método | Descrição |
|----------|--------|-----------|
| `/` | GET | Dashboard HTML |
| `/api` | GET | JSON com stats, cache, terminais, consultas |
| `/lookup?barcode=X` | GET | Buscar barcode no cache (para debug) |
| `/gifs` | GET | Listar GIFs de propaganda |
| `/upload-gif` | POST | Upload de GIF (multipart) |

### 10. systemd (produção)

```bash
cp gertec-server.service ~/.config/systemd/user/
# Editar EnvironmentFile se necessário
systemctl --user daemon-reload
systemctl --user enable --now gertec-server.service
```

O watchdog `watchdog_gertec_server.py` pode ser configurado como cron a cada 5 min para healthcheck automático.

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
| `WEBPOSTO_TOKEN_<EMPRESA>` | — | Token de integração. Crie uma variável por empresa (ex: `WEBPOSTO_TOKEN_1`, `WEBPOSTO_TOKEN_2`) |
| `GERT_EMPRESAS` | `[1]` | Lista JSON de códigos de empresa. Ex: `[1,2]` para 2 empresas |
| `GERT_TCP_PORT` | `6500` | Porta TCP do protocolo Gertec |
| `GERT_DASH_PORT` | `8650` | Porta HTTP do dashboard |
| `GERT_SERVER_IP` | `0.0.0.0` | IP de escuta do servidor |
| `GERT_SYNC_PRICES_SEC` | `300` | Intervalo de sync de preços (segundos) |
| `GERT_SYNC_CATALOG_SEC` | `1800` | Intervalo de sync de catálogo (segundos) |
| `GERT_GIF_ROTATION_SEC` | `30` | Intervalo de rotação de GIFs (segundos) |
| `GERT_WELCOME_LINE1` | `CONSULTE AQUI!` | Mensagem de boas-vindas G2S (linha 1) |
| `GERT_WELCOME_LINE2` | `BEM-VINDO!` | Mensagem de boas-vindas G2S (linha 2) |
| `GERT_LOG_DIR` | `./logs` | Diretório de logs |
| `GERT_GIF_DIR` | `./gifs` | Diretório de GIFs de propaganda |
| `G2E_ADMIN_USER` | `admin` | Usuário da interface web do G2E |
| `G2E_ADMIN_PASS` | `admin` | Senha da interface web do G2E (mude em produção) |
| `WEBPOSTO_BASE_URL` | `https://web.qualityautomacao.com.br/INTEGRACAO` | URL base da API WebPosto |

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

## Multi-empresa: pitfalls conhecidos

### 1. Produto ativo em uma empresa, inativo em outra

Um mesmo produto (mesmo `produtoCodigo`) pode estar **ativo com preço em uma empresa** e **inativo em outra**. O catálogo `PRODUTO` é compartilhado entre empresas, mas `PRODUTO_EMPRESA` tem dados independentes por empresa.

**Sintoma:** um barcode consultado retorna "SEM PRECO" mesmo com preço cadastrado no ERP.

**Causa:** o servidor indexa o barcode na última empresa onde o produto foi encontrado durante o sync de catálogo. Se a empresa indexada tem o produto inativo, o lookup retorna preço zero.

**Solução implementada:** o lookup sob demanda (`fetch_price_on_demand`) agora tenta **todas as empresas configuradas** quando a empresa primária retorna preço zero. O primeiro preço > 0 encontrado é usado e cacheado.

### 2. Cache negativo por empresa (⚠️ crítico — bug corrigido na v1.1)

**Problema original:** o cache negativo (`no_price_confirmed`) era chaveado apenas por `produtoCodigo`, sem incluir a empresa. Quando a empresa A retornava `precoVenda=0, ativo=False`, o cache bloqueava consultas em **todas as outras empresas** por 1 hora — mesmo que o produto estivesse ativo com preço na empresa B.

**Log de sintoma:**
```
On-demand: produtoCodigo=1878759 has no price (precoVenda=0, ativo=False)
HIT (confirmed no price in all empresas): 7896054900341 -> XAROPE GROSELHA WILSON 900ML
```

**Correção (commit `505e10b`):** a chave do cache negativo mudou de `produtoCodigo` para `produtoCodigo_empresa`. Produto inativo na empresa A não bloqueia mais consultas na empresa B.

**Se você usa versão anterior a `505e10b`, atualize:**
```bash
git pull origin main
systemctl --user restart gertec-server.service
```

### 3. Progressive sync sem filtro de empresa

O sync progressivo de preços também foi corrigido: antes só verificava produtos sem preço na empresa indexada. Agora verifica **todos os produtos sem preço em todas as empresas**, garantindo cobertura mesmo se o produto foi indexado à empresa errada.

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
Servidor → Terminal:  #PRODUTO EXEMPLO                                                |R$ 12,90
                  ou  #PRODUTO EXEMPLO|SEM PRECO
                  ou  #nfound
```

O nome ocupa até 4 linhas de 20 colunas (80 bytes); o terminal faz a quebra
visual em blocos de 20 colunas. O preço ocupa uma linha de até 20 caracteres.

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
