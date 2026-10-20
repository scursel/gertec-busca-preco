# Filial por leitor

Copie `config/leitores.example.json` para `config/leitores.json`. Substitua o IP de documentação por um IP fixo/reservado do leitor e `empresa` pelo código real da filial. Configure `GERT_EMPRESAS` e `WEBPOSTO_TOKEN_<empresa>` no ambiente do serviço. Nunca versione tokens ou o cadastro operacional.

Execute `python server.py --check-config` com o mesmo ambiente do serviço antes de reiniciar. Configuração inválida impede inicialização. Para adicionar filial, inclua a empresa no universo permitido e o leitor no JSON.

## Garantias

- IP de origem TCP determina a filial. Leitor desconhecido recebe `TERMINAL NAO CONFIGURADO|SEM PRECO`.
- Catálogo e preços são isolados por empresa, inclusive para um mesmo código de barras.
- Produtos inativos/sem preço naquela empresa não usam preço de outra filial.
- `GERT_PRICE_TTL` (padrão 3600 segundos) força revalidação de preços preenchidos na consulta e no sync progressivo. O bulk de 2000 registros não representa os produtos mais recentes.
- Em indisponibilidade da API, um preço da mesma filial pode ser usado até `GERT_PRICE_STALE_MAX` (padrão 21600 segundos); depois, SEM PRECO. Não há fallback entre empresas.
- Após restart, o catálogo é reconstruído; durante o aquecimento o leitor pode responder CATALOGO INDISPONIVEL. Produtos fora do bulk são consultados sob demanda.

## Diagnóstico

`GET /lookup?barcode=EAN&ip=IP_DO_LEITOR` ou `&empresa=CODIGO` consulta o cache da filial. Sem filtro, só infere filial quando existe exatamente um leitor. Não força atualização e pode mostrar preço nulo antes da primeira consulta/progressivo. `/api` informa leitores, filial dos terminais e contagens por empresa.

## Testes

```sh
python -m unittest discover -s tests
python -m unittest test_server
```

Os testes usam fixtures e não exigem cadastro operacional nem tokens reais. Faça backup antes de aplicar código/configuração e confirme o preço na tela física após o deploy.
