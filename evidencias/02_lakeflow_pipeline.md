# Lakeflow Declarative Pipeline — execução

> Coletado automaticamente por `scripts/coletar_evidencias.py` em 2026-10-14 14:05 UTC · workspace `https://fevm-vini.cloud.databricks.com`

- Pipeline: **Radar de Recebíveis — Lakeflow (dev)** (`af3d815c-1a11-4957-9d1e-c8f038763622`) · serverless=True · canal CURRENT
- Update: `381fd0d0-5ec3-4184-beb4-fd177e9408e0` · estado **COMPLETED** · full_refresh=True · disparado pela tarefa `lakeflow_pipeline` do job

### Linhas escritas por tabela (event log, flow_progress COMPLETED)

Quantas linhas cada tabela bronze/silver/gold escreveu neste update.

```sql
SELECT origin.flow_name AS tabela,
               -- materialized views reportam o total no evento COMPLETED; streaming tables, por micro-batch (RUNNING)
               coalesce(max(CASE WHEN details:flow_progress.status = 'COMPLETED' THEN details:flow_progress.metrics.num_output_rows::bigint END),
                        sum(CASE WHEN details:flow_progress.status = 'RUNNING' THEN details:flow_progress.metrics.num_output_rows::bigint END)) AS linhas_escritas,
               coalesce(sum(details:flow_progress.data_quality.dropped_records::bigint), 0) AS registros_descartados
        FROM event_log('af3d815c-1a11-4957-9d1e-c8f038763622')
        WHERE event_type = 'flow_progress' AND origin.flow_name LIKE '%.%' AND origin.update_id = '381fd0d0-5ec3-4184-beb4-fd177e9408e0'
        GROUP BY 1 ORDER BY 1
```

| tabela | linhas_escritas | registros_descartados |
|---|---|---|
| pipelines.flowTimeMetrics.missingFlowName |  | 0 |
| vini_catalog.radar_recebiveis.bronze_atendimentos | 21513 | 0 |
| vini_catalog.radar_recebiveis.bronze_cadastro_lojistas | 15000 | 0 |
| vini_catalog.radar_recebiveis.bronze_contratos_credito | 5346 | 0 |
| vini_catalog.radar_recebiveis.bronze_registradora | 1784300 | 0 |
| vini_catalog.radar_recebiveis.bronze_vendas_diarias | 4964914 | 0 |
| vini_catalog.radar_recebiveis.dim_lojistas | 15000 | 0 |
| vini_catalog.radar_recebiveis.gold_atendimentos_sinais | 6332 | 0 |
| vini_catalog.radar_recebiveis.gold_carteira_credito | 5346 | 0 |
| vini_catalog.radar_recebiveis.gold_concorrencia_semana | 206494 | 0 |
| vini_catalog.radar_recebiveis.gold_contrato_dia | 1047309 | 0 |
| vini_catalog.radar_recebiveis.gold_lojista_semana | 757746 | 0 |
| vini_catalog.radar_recebiveis.silver_atendimentos_ia | 6332 | 0 |
| vini_catalog.radar_recebiveis.silver_contratos_credito | 5346 | 0 |
| vini_catalog.radar_recebiveis.silver_lojistas | 15000 | 0 |
| vini_catalog.radar_recebiveis.silver_registradora | 1784300 | 0 |
| vini_catalog.radar_recebiveis.silver_vendas_diarias | 4964914 | 0 |

### Qualidade de dados — expectations avaliadas

Métricas das `CONSTRAINT ... EXPECT` declaradas nas camadas silver.

```sql
SELECT origin.flow_name AS tabela, e.name AS expectation, e.passed_records::bigint AS aprovados,
               e.failed_records::bigint AS reprovados
        FROM (SELECT origin, explode(from_json(details:flow_progress.data_quality.expectations,
                 'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
              FROM event_log('af3d815c-1a11-4957-9d1e-c8f038763622')
              WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL AND origin.update_id = '381fd0d0-5ec3-4184-beb4-fd177e9408e0')
        ORDER BY 1, 2
```

| tabela | expectation | aprovados | reprovados |
|---|---|---|---|
| vini_catalog.radar_recebiveis.silver_contratos_credito | contrato_valido | 5346 | 0 |
| vini_catalog.radar_recebiveis.silver_contratos_credito | retencao_razoavel | 5346 | 0 |
| vini_catalog.radar_recebiveis.silver_lojistas | cnpj_formato_valido | 15000 | 0 |
| vini_catalog.radar_recebiveis.silver_lojistas | id_lojista_presente | 15000 | 0 |
| vini_catalog.radar_recebiveis.silver_lojistas | porte_conhecido | 15000 | 0 |
| vini_catalog.radar_recebiveis.silver_registradora | lojista_identificado | 1784300 | 0 |
| vini_catalog.radar_recebiveis.silver_registradora | valor_nao_negativo | 1784300 | 0 |
| vini_catalog.radar_recebiveis.silver_vendas_diarias | lojista_presente | 4964914 | 0 |
| vini_catalog.radar_recebiveis.silver_vendas_diarias | valores_nao_negativos | 4964914 | 0 |

### Linha do tempo do update



```sql
SELECT timestamp, event_type, level, message
        FROM event_log('af3d815c-1a11-4957-9d1e-c8f038763622')
        WHERE event_type IN ('create_update', 'update_progress') AND origin.update_id = '381fd0d0-5ec3-4184-beb4-fd177e9408e0'
        ORDER BY timestamp
```

| timestamp | event_type | level | message |
|---|---|---|---|
| 2026-10-14T13:31:33.798Z | create_update | INFO | Update 381fd0 started by JOB_TASK. |
| 2026-10-14T13:31:35.954Z | update_progress | INFO | Update 381fd0 is WAITING_FOR_RESOURCES. |
| 2026-10-14T13:31:45.566Z | update_progress | INFO | Update 381fd0 is INITIALIZING. |
| 2026-10-14T13:32:06.710Z | update_progress | INFO | Update 381fd0 is RESETTING. |
| 2026-10-14T13:32:09.727Z | update_progress | INFO | Update 381fd0 is SETTING_UP_TABLES. |
| 2026-10-14T13:32:18.545Z | update_progress | INFO | Update 381fd0 is RUNNING. |
| 2026-10-14T13:40:40.382Z | update_progress | INFO | Update 381fd0 is COMPLETED. |

### Arquivos brutos ingeridos pelo Auto Loader (bronze)



```sql
SELECT 'bronze_cadastro_lojistas' AS tabela, count(DISTINCT _arquivo_origem) AS arquivos, count(*) AS linhas FROM vini_catalog.radar_recebiveis.bronze_cadastro_lojistas
        UNION ALL SELECT 'bronze_vendas_diarias', count(DISTINCT _arquivo_origem), count(*) FROM vini_catalog.radar_recebiveis.bronze_vendas_diarias
        UNION ALL SELECT 'bronze_registradora', count(DISTINCT _arquivo_origem), count(*) FROM vini_catalog.radar_recebiveis.bronze_registradora
        UNION ALL SELECT 'bronze_contratos_credito', count(DISTINCT _arquivo_origem), count(*) FROM vini_catalog.radar_recebiveis.bronze_contratos_credito
        UNION ALL SELECT 'bronze_atendimentos', count(DISTINCT _arquivo_origem), count(*) FROM vini_catalog.radar_recebiveis.bronze_atendimentos
```

| tabela | arquivos | linhas |
|---|---|---|
| bronze_cadastro_lojistas | 1 | 15000 |
| bronze_vendas_diarias | 13 | 4964914 |
| bronze_registradora | 14 | 1784300 |
| bronze_contratos_credito | 1 | 5346 |
| bronze_atendimentos | 13 | 21513 |

