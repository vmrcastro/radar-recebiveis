# Unity Catalog — governança

> Coletado automaticamente por `scripts/coletar_evidencias.py` em 2026-10-14 14:06 UTC · workspace `https://fevm-vini.cloud.databricks.com`

### Tabelas do schema



```sql
SELECT table_name, table_type, comment FROM vini_catalog.information_schema.tables
        WHERE table_schema = 'radar_recebiveis' AND table_name NOT LIKE '\_\_%' AND table_name NOT LIKE 'event_log_%'
        ORDER BY table_name
```

| table_name | table_type | comment |
|---|---|---|
| alertas_lojistas | MANAGED | Fila de alertas do Radar (faixas Crítico, Alto e Médio; quem já passou de 60 dias de atraso vai para cobrança e não entra), priorizada por valor em risco, com … |
| alertas_lojistas_lakebase | FOREIGN | Database table synced from another UC table |
| backtest_radar | MANAGED |  |
| bronze_atendimentos | STREAMING_TABLE | Atendimentos em texto livre (chat, WhatsApp, telefone, visita) exportados da plataforma de atendimento. |
| bronze_cadastro_lojistas | STREAMING_TABLE | Cadastro bruto de lojistas (CSV do CRM/onboarding). Contém PII — acesso restrito à engenharia. |
| bronze_contratos_credito | STREAMING_TABLE | Contratos de capital de giro (pagos via trava de % dos recebíveis de cartão). |
| bronze_registradora | STREAMING_TABLE | Agenda de recebíveis de cartão por CNPJ e credenciadora, arquivo semanal da registradora (CSV ; com decimal em vírgula). |
| bronze_vendas_diarias | STREAMING_TABLE | Vendas diárias por lojista (crédito, débito e Pix recebido na conta), arquivos Parquet do autorizador. |
| dim_lojistas | MATERIALIZED_VIEW | Dimensão de lojistas SEM PII, segura para consumo analítico (Genie, App, BI). Uma linha por lojista. |
| gold_atendimentos_sinais | MATERIALIZED_VIEW | Atendimentos recentes com sinais extraídos por IA (motivo, concorrente citado, risco de cancelamento), sem PII. |
| gold_carteira_credito | MATERIALIZED_VIEW | Fotografia atual (data de referência) da carteira de capital de giro: um registro por contrato com saldo, atraso e faixa de risco. |
| gold_concorrencia_semana | MATERIALIZED_VIEW | Recebíveis de cartão por credenciadora, semana, segmento e UF (visão da registradora). Base para medir ganho/perda de share por concorrente. |
| gold_contrato_dia | MATERIALIZED_VIEW | Evolução diária de cada contrato de capital de giro: retenção do dia, acumulado pago vs esperado, saldo devedor e dias de atraso. |
| gold_lojista_semana | MATERIALIZED_VIEW | Uma linha por lojista e semana (segunda-feira): TPV na Pedregulho, recebíveis em concorrentes (registradora), receita e situação do crédito. |
| importancia_variaveis | MANAGED |  |
| scores_lojistas | MANAGED | Pontuação semanal do Radar: probabilidade de vazamento de TPV (8 semanas) e de inadimplência >60 dias (13 semanas), valor em risco em R$ e sinais explicáveis. … |
| silver_atendimentos_ia | STREAMING_TABLE | Atendimentos dos últimos 90 dias enriquecidos por LLM (ai_query): motivo, menção a concorrente, risco de cancelamento, sentimento. |
| silver_contratos_credito | MATERIALIZED_VIEW | Contratos de capital de giro. Amortização = pct_retencao_recebiveis × recebíveis de cartão que passam na Pedregulho. |
| silver_lojistas | MATERIALIZED_VIEW | Cadastro de lojistas, uma linha por lojista (última versão recebida). PII mascarada via Unity Catalog. |
| silver_registradora | MATERIALIZED_VIEW | Agenda semanal de recebíveis de cartão por lojista e credenciadora (inclui concorrentes), já associada ao id do lojista. |
| silver_vendas_diarias | STREAMING_TABLE | Vendas diárias por lojista. valor_cartao = crédito + débito (é o que pode ser travado como garantia do crédito). |

### Máscaras de coluna em ação (usuário fora do grupo PII)

A coleta roda com um usuário que **não** pertence ao grupo de PII: CNPJ, CPF, nome, e-mail e telefone voltam mascarados pelas funções UC aplicadas no `MASK` das colunas.

```sql
SELECT id_lojista, cnpj, razao_social, nome_titular, cpf_titular, telefone, email, nome_fantasia
        FROM vini_catalog.radar_recebiveis.silver_lojistas ORDER BY id_lojista LIMIT 5
```

| id_lojista | cnpj | razao_social | nome_titular | cpf_titular | telefone | email | nome_fantasia |
|---|---|---|---|---|---|---|---|
| LJ000001 | **.***.***/****-07 | P•••••• | H•••••• | ***.***.***-40 | (**) *****-**76 | h•••••• | Padaria Pão Dourado |
| LJ000002 | **.***.***/****-47 | D•••••• | R•••••• | ***.***.***-93 | (**) *****-**42 | r•••••• | Drogaria Saúde Já |
| LJ000003 | **.***.***/****-44 | B•••••• | A•••••• | ***.***.***-38 | (**) *****-**03 | a•••••• | Bicho Mimado |
| LJ000004 | **.***.***/****-80 | C•••••• | E•••••• | ***.***.***-60 | (**) *****-**45 | e•••••• | Constrular |
| LJ000005 | **.***.***/****-78 | B•••••• | J•••••• | ***.***.***-08 | (**) *****-**96 | j•••••• | Boteco do Souza |

### Funções de máscara registradas no UC



```sql
SELECT routine_name, routine_definition FROM vini_catalog.information_schema.routines
        WHERE routine_schema = 'radar_recebiveis' ORDER BY 1
```

| routine_name | routine_definition |
|---|---|
| mascara_documento | CASE   WHEN is_account_group_member('radar_pii_leitores') THEN valor   WHEN valor IS NULL THEN NULL   ELSE concat(regexp_replace(substr(valor, 1, length(valor)… |
| mascara_texto | CASE   WHEN is_account_group_member('radar_pii_leitores') THEN valor   WHEN valor IS NULL THEN NULL   ELSE concat(left(valor, 1), '••••••') END |

### Tags de coluna (PII)



```sql
SELECT table_name, column_name, tag_name, tag_value FROM vini_catalog.information_schema.column_tags
        WHERE schema_name = 'radar_recebiveis' ORDER BY 1, 2
```

| table_name | column_name | tag_name | tag_value |
|---|---|---|---|
| silver_lojistas | cnpj | classificacao | pii |
| silver_lojistas | cnpj | tipo_pii | cnpj |
| silver_lojistas | cpf_titular | classificacao | pii |
| silver_lojistas | cpf_titular | tipo_pii | cpf |
| silver_lojistas | email | classificacao | pii |
| silver_lojistas | email | tipo_pii | email |
| silver_lojistas | nome_titular | tipo_pii | nome |
| silver_lojistas | nome_titular | classificacao | pii |
| silver_lojistas | razao_social | tipo_pii | nome |
| silver_lojistas | razao_social | classificacao | pii |
| silver_lojistas | telefone | classificacao | pii |
| silver_lojistas | telefone | tipo_pii | telefone |

### Tags de tabela



```sql
SELECT table_name, tag_name, tag_value FROM vini_catalog.information_schema.table_tags
        WHERE schema_name = 'radar_recebiveis' ORDER BY 1, 2
```

| table_name | tag_name | tag_value |
|---|---|---|
| alertas_lojistas | area_negocio | risco_e_retencao |
| alertas_lojistas | camada | gold |
| alertas_lojistas | contem_pii | nao |
| alertas_lojistas | natureza | saida_de_ml |
| backtest_radar | area_negocio | risco_e_retencao |
| backtest_radar | camada | gold |
| backtest_radar | contem_pii | nao |
| backtest_radar | natureza | saida_de_ml |
| bronze_atendimentos | area_negocio | risco_e_retencao |
| bronze_atendimentos | camada | bronze |
| bronze_atendimentos | contem_pii | sim |
| bronze_atendimentos | natureza | dados |
| bronze_cadastro_lojistas | area_negocio | risco_e_retencao |
| bronze_cadastro_lojistas | camada | bronze |
| bronze_cadastro_lojistas | contem_pii | sim |
| bronze_cadastro_lojistas | natureza | dados |
| bronze_contratos_credito | area_negocio | risco_e_retencao |
| bronze_contratos_credito | camada | bronze |
| bronze_contratos_credito | contem_pii | sim |
| bronze_contratos_credito | natureza | dados |
| bronze_registradora | area_negocio | risco_e_retencao |
| bronze_registradora | camada | bronze |
| bronze_registradora | contem_pii | sim |
| bronze_registradora | natureza | dados |
| bronze_vendas_diarias | area_negocio | risco_e_retencao |
| bronze_vendas_diarias | camada | bronze |
| bronze_vendas_diarias | contem_pii | sim |
| bronze_vendas_diarias | natureza | dados |
| dim_lojistas | area_negocio | risco_e_retencao |
| dim_lojistas | camada | gold |
| dim_lojistas | contem_pii | nao |
| dim_lojistas | natureza | dados |
| gold_atendimentos_sinais | area_negocio | risco_e_retencao |
| gold_atendimentos_sinais | camada | gold |
| gold_atendimentos_sinais | contem_pii | nao |
| gold_atendimentos_sinais | natureza | dados |
| gold_carteira_credito | area_negocio | risco_e_retencao |
| gold_carteira_credito | camada | gold |
| gold_carteira_credito | contem_pii | nao |
| gold_carteira_credito | natureza | dados |
| gold_concorrencia_semana | area_negocio | risco_e_retencao |
| gold_concorrencia_semana | camada | gold |
| gold_concorrencia_semana | contem_pii | nao |
| gold_concorrencia_semana | natureza | dados |
| gold_contrato_dia | area_negocio | risco_e_retencao |
| gold_contrato_dia | camada | gold |
| gold_contrato_dia | contem_pii | nao |
| gold_contrato_dia | natureza | dados |
| gold_lojista_semana | area_negocio | risco_e_retencao |
| gold_lojista_semana | camada | gold |
| gold_lojista_semana | contem_pii | nao |
| gold_lojista_semana | natureza | dados |
| importancia_variaveis | area_negocio | risco_e_retencao |
| importancia_variaveis | camada | gold |
| importancia_variaveis | contem_pii | nao |
| importancia_variaveis | natureza | saida_de_ml |
| scores_lojistas | area_negocio | risco_e_retencao |
| scores_lojistas | camada | gold |
| scores_lojistas | contem_pii | nao |
| scores_lojistas | natureza | saida_de_ml |
| silver_atendimentos_ia | area_negocio | risco_e_retencao |
| silver_atendimentos_ia | camada | silver |
| silver_atendimentos_ia | contem_pii | nao |
| silver_atendimentos_ia | natureza | dados |
| silver_contratos_credito | area_negocio | risco_e_retencao |
| silver_contratos_credito | camada | silver |
| silver_contratos_credito | contem_pii | nao |
| silver_contratos_credito | natureza | dados |
| silver_lojistas | area_negocio | risco_e_retencao |
| silver_lojistas | camada | silver |
| silver_lojistas | contem_pii | sim |
| silver_lojistas | natureza | dados |
| silver_registradora | area_negocio | risco_e_retencao |
| silver_registradora | camada | silver |
| silver_registradora | contem_pii | nao |
| silver_registradora | natureza | dados |
| silver_vendas_diarias | area_negocio | risco_e_retencao |
| silver_vendas_diarias | camada | silver |
| silver_vendas_diarias | contem_pii | nao |
| silver_vendas_diarias | natureza | dados |

### Grants concedidos pela jornada (consumidores e service principal do App)

Least privilege: consumidores (`account users`) e o App (service principal `21dcec58-1189-4458-a39e-e93f6845d8bf`) só leem gold/ML; bronze e silver (com PII) não recebem grant.

```sql
SELECT grantee, 'SCHEMA' AS objeto, 'radar_recebiveis' AS nome, privilege_type
        FROM vini_catalog.information_schema.schema_privileges
        WHERE schema_name = 'radar_recebiveis' AND grantee IN ('account users', '21dcec58-1189-4458-a39e-e93f6845d8bf')
        UNION ALL
        SELECT grantee, 'TABLE', table_name, privilege_type FROM vini_catalog.information_schema.table_privileges
        WHERE table_schema = 'radar_recebiveis' AND grantee IN ('account users', '21dcec58-1189-4458-a39e-e93f6845d8bf') AND table_name NOT LIKE '\_\_%'
        ORDER BY 1, 2, 3
```

| grantee | objeto | nome | privilege_type |
|---|---|---|---|
| 21dcec58-1189-4458-a39e-e93f6845d8bf | SCHEMA | radar_recebiveis | USE_SCHEMA |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | alertas_lojistas | SELECT |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | backtest_radar | SELECT |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | dim_lojistas | SELECT |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | gold_atendimentos_sinais | SELECT |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | gold_carteira_credito | SELECT |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | gold_concorrencia_semana | SELECT |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | gold_contrato_dia | SELECT |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | gold_lojista_semana | SELECT |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | importancia_variaveis | SELECT |
| 21dcec58-1189-4458-a39e-e93f6845d8bf | TABLE | scores_lojistas | SELECT |
| account users | SCHEMA | radar_recebiveis | USE_SCHEMA |
| account users | TABLE | alertas_lojistas | SELECT |
| account users | TABLE | backtest_radar | SELECT |
| account users | TABLE | dim_lojistas | SELECT |
| account users | TABLE | gold_atendimentos_sinais | SELECT |
| account users | TABLE | gold_carteira_credito | SELECT |
| account users | TABLE | gold_concorrencia_semana | SELECT |
| account users | TABLE | gold_contrato_dia | SELECT |
| account users | TABLE | gold_lojista_semana | SELECT |
| account users | TABLE | importancia_variaveis | SELECT |
| account users | TABLE | scores_lojistas | SELECT |

### Lineage capturado automaticamente (system.access.table_lineage)

Do arquivo bruto até a tabela de alertas consumida pelo App/Lakebase.

```sql
SELECT source_table_name AS origem, target_table_name AS destino, entity_type AS executado_por,
               max(event_time) AS ultimo_evento
        FROM system.access.table_lineage
        WHERE target_table_schema = 'radar_recebiveis' AND target_table_catalog = 'vini_catalog'
          AND source_table_name IS NOT NULL AND event_date >= current_date() - INTERVAL 7 DAYS
        GROUP BY 1, 2, 3 ORDER BY 2, 1
```

| origem | destino | executado_por | ultimo_evento |
|---|---|---|---|
| dim_lojistas | _alertas_lojistas_staging | JOB | 2026-10-14T13:24:16.945Z |
| scores_lojistas | _alertas_lojistas_staging | JOB | 2026-10-14T13:24:16.945Z |
| _alertas_lojistas_staging | alertas_lojistas | JOB | 2026-10-14T13:24:26.380Z |
| dim_lojistas | alertas_lojistas | JOB | 2026-10-13T23:12:37.201Z |
| scores_lojistas | alertas_lojistas | JOB | 2026-10-13T23:12:37.201Z |
| silver_lojistas | dim_lojistas | PIPELINE | 2026-10-14T13:32:45.459Z |
| dim_lojistas | gold_atendimentos_sinais | PIPELINE | 2026-10-14T13:40:40.347Z |
| silver_atendimentos_ia | gold_atendimentos_sinais | PIPELINE | 2026-10-14T13:40:40.347Z |
| dim_lojistas | gold_carteira_credito | PIPELINE | 2026-10-14T13:33:00.053Z |
| gold_contrato_dia | gold_carteira_credito | PIPELINE | 2026-10-14T13:33:00.053Z |
| dim_lojistas | gold_concorrencia_semana | PIPELINE | 2026-10-14T13:32:50.779Z |
| silver_registradora | gold_concorrencia_semana | PIPELINE | 2026-10-14T13:32:50.779Z |
| silver_contratos_credito | gold_contrato_dia | PIPELINE | 2026-10-14T13:32:55.241Z |
| silver_vendas_diarias | gold_contrato_dia | PIPELINE | 2026-10-14T13:32:55.241Z |
| dim_lojistas | gold_lojista_semana | PIPELINE | 2026-10-14T13:33:01.775Z |
| gold_contrato_dia | gold_lojista_semana | PIPELINE | 2026-10-14T13:33:01.775Z |
| silver_registradora | gold_lojista_semana | PIPELINE | 2026-10-14T13:33:01.775Z |
| silver_vendas_diarias | gold_lojista_semana | PIPELINE | 2026-10-14T13:33:01.775Z |
| bronze_atendimentos | silver_atendimentos_ia | PIPELINE | 2026-10-14T13:40:37.162Z |
| bronze_contratos_credito | silver_contratos_credito | PIPELINE | 2026-10-14T13:32:41.421Z |
| bronze_cadastro_lojistas | silver_lojistas | PIPELINE | 2026-10-14T13:32:41.517Z |
| bronze_cadastro_lojistas | silver_registradora | PIPELINE | 2026-10-14T13:32:45.514Z |
| bronze_registradora | silver_registradora | PIPELINE | 2026-10-14T13:32:45.514Z |
| bronze_vendas_diarias | silver_vendas_diarias | PIPELINE | 2026-10-14T13:32:48.212Z |

