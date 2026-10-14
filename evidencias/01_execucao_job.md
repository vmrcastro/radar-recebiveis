# Execução do job — jornada completa

> Coletado automaticamente por `scripts/coletar_evidencias.py` em 2026-10-14 14:05 UTC · workspace `https://fevm-vini.cloud.databricks.com`

- Job: **Radar de Recebíveis — Jornada completa (dev)** (job id `883752161988103`)
- Run id: `10454867535961` · https://fevm-vini.cloud.databricks.com/?o=7474658470734499#job/883752161988103/run/10454867535961
- Resultado: **SUCCESS** · início 2026-10-14 13:30:08 UTC · fim 2026-10-14 14:05:25 UTC · duração 35.3 min
- Todas as tarefas abaixo rodaram nesta **mesma execução**, na ordem de dependência do DAG.

## Tarefas

| tarefa | tipo | resultado | início | fim | duração | task run id |
|---|---|---|---|---|---|---|
| governanca_funcoes | notebook | SUCCESS | 2026-10-14 13:30:08 UTC | 2026-10-14 13:30:40 UTC | 0.5 min | 1105572102679200 |
| gerar_dados_sinteticos | notebook | SUCCESS | 2026-10-14 13:30:40 UTC | 2026-10-14 13:31:31 UTC | 0.9 min | 626813594111959 |
| lakeflow_pipeline | pipeline | SUCCESS | 2026-10-14 13:31:32 UTC | 2026-10-14 13:41:47 UTC | 10.3 min | 946716853169923 |
| ml_treinar_e_pontuar | notebook | SUCCESS | 2026-10-14 13:41:48 UTC | 2026-10-14 14:01:05 UTC | 19.3 min | 796528513733310 |
| genai_alertas | notebook | SUCCESS | 2026-10-14 14:01:05 UTC | 2026-10-14 14:04:15 UTC | 3.2 min | 711259691843804 |
| governanca_tags_e_acessos | notebook | SUCCESS | 2026-10-14 14:04:15 UTC | 2026-10-14 14:05:08 UTC | 0.9 min | 674819863956250 |
| lakebase_publicar | notebook | SUCCESS | 2026-10-14 14:04:15 UTC | 2026-10-14 14:05:24 UTC | 1.2 min | 72809108180172 |

## Parâmetros da execução

| parâmetro | valor |
|---|---|
| catalog | vini_catalog |
| schema | radar_recebiveis |
| empresa | Pedregulho Pagamentos |
| grupo_pii | radar_pii_leitores |
| n_lojistas | 15000 |
| data_referencia | 2026-10-08 |
| llm_endpoint | databricks-claude-sonnet-4-6 |
| lakebase_project | radar-recebiveis |
| lakebase_database | databricks_postgres |
| app_name | radar-recebiveis-dev |
| experimento | /Workspace/Users/vinicius.castro@databricks.com/radar_recebiveis_dev |
