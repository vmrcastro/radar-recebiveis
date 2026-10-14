# Radar de Vazamento de Recebíveis — Pedregulho Pagamentos 🪨

> Protótipo end-to-end em Databricks para uma credenciadora de pagamentos focada em micro, pequenos e médios lojistas.
> **Pedregulho Pagamentos é uma empresa fictícia; todos os dados são sintéticos.**

## O problema de negócio

Credenciadoras brasileiras crescem em **capital de giro para lojistas**, pago por uma **trava de % dos recebíveis de cartão**.
Quando o lojista começa a passar o cartão na maquininha do concorrente — ou migra as vendas para Pix — a credenciadora
perde **duas vezes**: a receita de MDR (churn) **e** a garantia do empréstimo (inadimplência). Normalmente, risco de
crédito e retenção comercial olham para esse mesmo sinal em silos, e tarde demais.

O **Radar** junta o TPV diário, a agenda da registradora (que mostra os recebíveis do lojista em *todas* as
credenciadoras), a carteira de crédito e o texto dos atendimentos para:

1. **Detectar cedo** quem está vazando TPV e quem vai atrasar o crédito (ML, validação out-of-time);
2. **Explicar e recomendar** a próxima melhor ação para o agente de campo (GenAI);
3. **Servir a fila** no app do hub, com latência de milissegundos e registro transacional das ações (Lakebase);
4. **Responder em linguagem natural** a gestores de risco e comercial (Genie), tudo **governado** no Unity Catalog.

## Arquitetura

```
 Volume UC (landing)         Lakeflow Declarative Pipeline (serverless)                    Lakeflow Job
 ───────────────────         ─────────────────────────────────────────────────            ───────────────
 cadastro_lojistas.csv  ─┐   bronze_*  ──►  silver_* (expectations, máscaras PII)  ──►  ML: modelo_vazamento,
 vendas_diarias.parquet ─┤   (Auto Loader)  silver_atendimentos_ia (ai_query/LLM)       modelo_inadimplencia
 registradora.csv       ─┼─►                     │                                      (MLflow + UC Models)
 contratos_credito.csv  ─┤                       ▼                                           │
 atendimentos.json      ─┘               gold_lojista_semana, gold_carteira_credito,          ▼
                                         gold_concorrencia_semana, dim_lojistas ──────► scores_lojistas
                                                                                             │ GenAI (ai_query)
                                                                                             ▼
                         Genie space  ◄──── tabelas gold + alertas (UC) ◄──────────── alertas_lojistas (Delta, CDF)
                              ▲                                                              │ synced table
                              │                                                              ▼
                     Databricks App "Radar do Hub" ◄──────── leitura ms / write-back ──── Lakebase (Postgres)
                     (FastAPI)                                                          alertas + app.acoes_campo
```

| Etapa obrigatória | Onde está |
|---|---|
| **Lakeflow** (ingestão) | `src/pipeline/*.sql` — Auto Loader (`read_files`) em CSV/Parquet/JSON, expectations, MVs gold; job em `resources/03_jobs.yml` |
| **Unity Catalog** (governança) | máscaras de PII como funções UC (`src/governance/01_*`) aplicadas no pipeline, tags/comentários/grants (`02_*`), linhagem automática, modelos no UC |
| **Lakebase** (serving) | projeto Autoscaling + synced table `alertas_lojistas_lakebase` + tabela transacional `app.acoes_campo` |
| **ML / GenAI** | `src/ml/treinar_e_pontuar.py` (2 modelos, backtest de negócio), `ai_query` no pipeline (atendimentos) e em `src/genai/gerar_alertas.py` |
| **Genie** | `genie_spaces.radar_genie` em `resources/05_serving.yml` (instruções, SQLs de exemplo) |
| **Databricks App** | `app/` — Visão executiva, Fila do Hub (Lakebase), Pergunte ao Radar (Genie) |

## Deploy (qualquer workspace)

Pré-requisitos: Databricks CLI com suporte a `postgres_*`/`genie_spaces` em bundles, workspace com serverless,
Lakebase e Foundation Model APIs, e um catálogo onde você possa criar schemas.

```bash
# workspace atual (target dev → perfil fevm-vini, catálogo vini_catalog)
./scripts/deploy.sh dev

# outro workspace: edite o target "demo" em databricks.yml (host/catalog) ou passe variáveis
./scripts/deploy.sh demo --var="catalog=main" --var="empresa=Pedregulho Pagamentos"
```

O deploy é feito em fases porque Genie e synced table exigem que as tabelas já existam:

1. `bundle deploy --select …` da infraestrutura de dados → `bundle run radar_jornada --only …` até os alertas
2. `bundle deploy` completo (synced table, Genie, App)
3. governança/grants + publicação no Lakebase → `bundle run radar_app`

Depois do primeiro deploy, basta `databricks bundle run radar_jornada -t dev` para regenerar tudo.

### Variáveis principais (`databricks.yml`)

| Variável | Padrão | Uso |
|---|---|---|
| `catalog` / `schema` | `vini_catalog` / `radar_recebiveis` | destino no UC |
| `empresa` | `Pedregulho Pagamentos` | nome exibido no App, Genie e textos de GenAI |
| `warehouse_id` | lookup `Serverless Starter Warehouse` | Genie, App |
| `lakebase_project` | `radar-recebiveis` | projeto Lakebase Autoscaling |
| `llm_endpoint` / `llm_endpoint_extracao` | Claude Sonnet / Haiku | alertas / atendimentos |
| `grupo_pii` | `radar_pii_leitores` | quem vê PII sem máscara |
| `n_lojistas`, `data_referencia` | `15000`, `2026-10-08` | tamanho e "hoje" da simulação |

## Evidências de execução

A pasta [`evidencias/`](evidencias/README.md) traz, em texto, a saída real de uma execução completa da jornada (job run `10454867535961`, 7/7 tarefas com SUCCESS):
- os notebooks executados, com as saídas das células;
- o event log do pipeline e as métricas de qualidade;
- consultas de governança no UC;
- métricas e backtest dos modelos e as respostas do LLM;
- consultas no Lakebase;
- conversas com o Genie e respostas da API do App.

Para regerar: `python scripts/coletar_evidencias.py --target dev`.

## Roteiro de demonstração

Ver [`docs/roteiro_apresentacao.md`](docs/roteiro_apresentacao.md).
