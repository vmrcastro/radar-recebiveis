# Lakebase (Postgres) — serving operacional

> Coletado automaticamente por `scripts/coletar_evidencias.py` em 2026-10-14 14:08 UTC · workspace `https://fevm-vini.cloud.databricks.com`

- Projeto: `radar-recebiveis` · branch `projects/radar-recebiveis/branches/production` · endpoint `projects/radar-recebiveis/branches/production/endpoints/primary`
- Host: `ep-delicate-frog-d26wczc8.database.us-east-1.cloud.databricks.com` · database `databricks_postgres`

Synced table (Delta → Postgres):

```json
{
 "name": "synced_tables/vini_catalog.radar_recebiveis.alertas_lojistas_lakebase",
 "status": {
  "detailed_state": "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE",
  "last_sync": {
   "delta_table_sync_info": {
    "delta_commit_time": "2026-10-14T14:04:10Z",
    "delta_commit_version": 6
   },
   "sync_end_time": "2026-10-14T14:05:03.349420Z",
   "sync_start_time": "2026-10-14T14:05:02.321679Z"
  },
  "message": "Synced table creation succeeded using Delta Live Tables: https://fevm-vini.cloud.databricks.com#joblist/pipelines/1e637dc4-9323-45e9-91d7-797f16408a53/updates/c6b87ceb-981f-438e-9f3b-18b2acb17f73.",
  "pipeline_id": "1e637dc4-9323-45e9-91d7-797f16408a53",
  "project": "projects/radar-recebiveis",
  "unity_catalog_provisioning_state": "ACTIVE"
 },
 "synced_table_id": "vini_catalog.radar_recebiveis.alertas_lojistas_lakebase",
 "uid": "24bf0fa3-59cd-4c78-b5a3-041a5280b2a0"
}
```

### Versão do Postgres

```sql
SELECT version()
```

| version |
|---|
| PostgreSQL 17.11 (fcae950) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit |

_tempo de consulta (cliente → Lakebase, incl. rede): 137.2 ms_

### Tabelas no Lakebase

```sql
SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema IN ('radar_recebiveis', 'app') ORDER BY 1, 2
```

| table_schema | table_name | table_type |
|---|---|---|
| app | acoes_campo | BASE TABLE |
| radar_recebiveis | alertas_lojistas_lakebase | BASE TABLE |

_tempo de consulta (cliente → Lakebase, incl. rede): 226.5 ms_

### Linhas sincronizadas do lakehouse

```sql
SELECT count(*) AS alertas, count(*) FILTER (WHERE faixa_risco = 'Crítico') AS criticos, round(sum(valor_em_risco)::numeric, 0) AS valor_em_risco FROM "radar_recebiveis".alertas_lojistas_lakebase
```

| alertas | criticos | valor_em_risco |
|---|---|---|
| 440 | 245 | 7147497 |

_tempo de consulta (cliente → Lakebase, incl. rede): 134.6 ms_

### Fila priorizada (o que o App lê)

```sql
SELECT prioridade, id_lojista, nome_fantasia, hub, faixa_risco, tipo_sinal, round(valor_em_risco::numeric, 0) AS valor_em_risco FROM "radar_recebiveis".alertas_lojistas_lakebase ORDER BY prioridade LIMIT 10
```

| prioridade | id_lojista | nome_fantasia | hub | faixa_risco | tipo_sinal | valor_em_risco |
|---|---|---|---|---|---|---|
| 1 | LJ008915 | Drogaria Saúde Já | Hub Campinas | Crítico | Desvio para concorrente | 336295 |
| 2 | LJ005236 | Mecânica Pit Stop | Hub Porto Alegre | Alto | Desvio para concorrente | 250292 |
| 3 | LJ003304 | Eletro Lima | Hub BH Contagem | Crítico | Desvio para concorrente | 206751 |
| 4 | LJ004470 | Empório Bom Preço | Hub Ribeirão Preto | Crítico | Desvio para concorrente | 178134 |
| 5 | LJ002383 | Espetinho do Sérgio | Hub Brasília | Crítico | Desvio para concorrente | 165668 |
| 6 | LJ012359 | Posto Rota Sul | Hub SP Centro | Crítico | Desvio para concorrente | 159397 |
| 7 | LJ007604 | Barbearia Navalha de Ouro | Hub Florianópolis | Crítico | Desvio para concorrente | 154861 |
| 8 | LJ014065 | Drogaria Marques | Hub RJ Centro | Crítico | Sinais combinados | 141594 |
| 9 | LJ008301 | Depósito Bom Tijolo | Hub RJ Barra | Crítico | Desvio para concorrente | 128130 |
| 10 | LJ009002 | Empório Bom Preço | Hub Curitiba | Crítico | Desvio para concorrente | 124865 |

_tempo de consulta (cliente → Lakebase, incl. rede): 136.2 ms_

### Write-back do agente (tabela transacional app.acoes_campo)

```sql
SELECT id_acao, id_lojista, agente, acao, resultado, registrado_em FROM app.acoes_campo ORDER BY registrado_em DESC LIMIT 10
```

| id_acao | id_lojista | agente | acao | resultado | registrado_em |
|---|---|---|---|---|---|
| 4 | LJ008915 | coleta-de-evidencias | Ligação | Sem contato | 2026-10-14 14:08:11.765648+00:00 |
| 3 | LJ008915 | coleta-de-evidencias | Ligação | Sem contato | 2026-10-14 13:28:29.944640+00:00 |
| 2 | LJ008915 | coleta-de-evidencias | Ligação | Sem contato | 2026-10-14 12:46:35.367282+00:00 |

_tempo de consulta (cliente → Lakebase, incl. rede): 219.7 ms_

