# Evidências de execução

Tudo nesta pasta é **saída real** da jornada rodando no workspace Databricks (`fevm-vini`), em texto: notebooks com o
resultado de cada célula, logs, resultados de consultas e respostas do LLM, do Genie e do App. Nenhum arquivo
aqui é imagem ou foi digitado à mão. Os arquivos foram gerados por [`scripts/coletar_evidencias.py`](../scripts/coletar_evidencias.py),
que lê as APIs do workspace depois de uma execução completa do job.

## Resumo da execução

- **Job `radar_jornada`, run `10454867535961`**: as 7 tarefas terminaram com **SUCCESS** na mesma execução. Rodou em 14/10/2026, das 13:30 às 14:05 UTC (35 min), em computação serverless.
- **Lakeflow**:
  - 5 fontes brutas com 42 arquivos ingeridos pelo Auto Loader: 4.964.914 vendas diárias, 1.784.300 linhas da registradora, 21.513 atendimentos, 15.000 lojistas e 5.346 contratos.
  - 9 *expectations* avaliadas, com 0 registros reprovados.
  - 6.332 atendimentos classificados pelo LLM dentro do pipeline.
- **Unity Catalog**:
  - CNPJ, CPF, nome, telefone e e-mail aparecem mascarados para quem está fora do grupo `radar_pii_leitores`.
  - Tags de PII e de camada aplicadas.
  - Grants apenas na camada gold/ML.
  - Lineage do bronze até `alertas_lojistas`.
- **ML**: 2 modelos registrados no UC com alias `champion`.

  | Modelo | AUC fora do tempo | Recall antecipado | % do valor sinalizado | Antecedência mediana |
  |---|---|---|---|---|
  | Inadimplência | 0,971 | 90% | 92% do saldo | 91 dias |
  | Vazamento | 0,924 | 88% | 87% da receita de MDR | 35 dias |

- **GenAI**: 440 alertas com resumo, ação, oferta e roteiro gerados pelo `ai_query` (Claude Sonnet 4.6, saída estruturada).
- **Lakebase**:
  - 440 alertas sincronizados do Delta para o Postgres 17 por synced table.
  - Consulta da fila em 9,3 ms e gravação do write-back em 5,6 ms, medidos no servidor do App.
- **Genie**: 3 perguntas em português (incluindo um follow-up) responderam com SQL gerado sobre as tabelas gold.
- **App**: todos os endpoints responderam HTTP 200, incluindo KPIs (R$ 7,1 mi em risco, 245 críticos), fila, detalhe do lojista, write-back e Genie.

> Os dados são 100% sintéticos (empresa fictícia "Pedregulho Pagamentos").
> Nesta coleta, o script registrou uma ação de teste via `POST /api/acoes` (agente `coleta-de-evidencias`, resultado "Sem contato"), para provar o write-back no Lakebase.

## Índice

| Arquivo | O que prova | Estágio obrigatório |
|---|---|---|
| [`00_bundle_validate_summary.txt`](00_bundle_validate_summary.txt) | `bundle validate` OK e todos os recursos que o Asset Bundle criou no workspace | Portabilidade (DAB) |
| [`01_execucao_job.md`](01_execucao_job.md) | As 7 tarefas do DAG com estado, horário e duração, todas na **mesma** execução | Orquestração |
| [`notebooks/`](notebooks/) | Cada notebook executado pelo job, **com as saídas das células** (`.ipynb` + transcrição `.md`) | Todos |
| [`02_lakeflow_pipeline.md`](02_lakeflow_pipeline.md) | Update do pipeline, linhas por tabela bronze/silver/gold, métricas das *expectations*, arquivos ingeridos pelo Auto Loader | **Lakeflow** |
| [`03_unity_catalog.md`](03_unity_catalog.md) | Máscaras de PII em ação, funções de máscara, tags, grants e lineage automático | **Unity Catalog** |
| [`04_ml_modelos.md`](04_ml_modelos.md) | Modelos registrados no UC (alias `champion`), métricas MLflow, backtest de negócio e importância das variáveis | **ML** |
| [`05_genai_alertas.md`](05_genai_alertas.md) | Saídas reais do LLM (`ai_query`): classificação do atendimento no pipeline e alertas com resumo, ação e roteiro | **GenAI** |
| [`08_app_api.md`](08_app_api.md) | Respostas HTTP reais do Databricks App (KPIs, fila, detalhe, write-back, Genie) | **Databricks App** |
| [`06_lakebase.md`](06_lakebase.md) | Synced table Delta → Postgres, consultas no Lakebase com latência, write-back do App em `app.acoes_campo` | **Lakebase** |
| [`07_genie.md`](07_genie.md) | 3 perguntas em português (com follow-up) → interpretação, SQL gerado e resultado | **Genie** |

## Como regerar

```bash
./scripts/deploy.sh dev                                   # ou: databricks bundle run -t dev radar_jornada
pip install "databricks-sdk>=0.105.0" "psycopg[binary]>=3.2" requests
DATABRICKS_CONFIG_PROFILE=fevm-vini python scripts/coletar_evidencias.py --target dev
```
