# Databricks notebook source
# MAGIC %md
# MAGIC # Alertas com GenAI — do score à ação do agente de campo
# MAGIC
# MAGIC O modelo diz **quem** e **quanto**; o LLM traduz os sinais em **por quê** e **o que fazer**:
# MAGIC um resumo para o gestor, a ação recomendada e um roteiro de abordagem para o agente do hub.
# MAGIC Tudo via `ai_query` em SQL, governado e auditável no Unity Catalog.

# COMMAND ----------

dbutils.widgets.text("catalog", "vini_catalog")
dbutils.widgets.text("schema", "radar_recebiveis")
dbutils.widgets.text("empresa", "Pedregulho Pagamentos")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6")
dbutils.widgets.text("max_alertas_llm", "800")

FQ = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
EMPRESA = dbutils.widgets.get("empresa")
LLM = dbutils.widgets.get("llm_endpoint")
MAX_LLM = int(dbutils.widgets.get("max_alertas_llm"))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW candidatos AS
SELECT s.*, d.nome_fantasia, d.segmento, d.uf, d.cidade, d.hub, d.agente_responsavel, d.porte, d.plano,
       row_number() OVER (ORDER BY s.valor_em_risco DESC) AS prioridade
FROM {FQ}.scores_lojistas s
JOIN {FQ}.dim_lojistas d USING (id_lojista)
WHERE s.faixa_risco IN ('Crítico', 'Alto', 'Médio')
""")
print(spark.sql("SELECT count(*) FROM candidatos").first()[0], "alertas")

# COMMAND ----------

PROMPT = f"""Você é o copiloto de retenção e risco da {EMPRESA}, credenciadora que atende micro, pequenos e médios lojistas no Brasil.
O crédito de capital de giro é pago com uma trava de % dos recebíveis de cartão: se o lojista leva as vendas de cartão para outra
credenciadora ou migra para Pix, a {EMPRESA} perde a receita de MDR E a garantia do empréstimo.

Escreva em português do Brasil, tom profissional e direto, sem inventar números além dos fornecidos.
- resumo: 1-2 frases para o gestor explicando o risco e o valor em jogo.
- acao_recomendada: exatamente uma de: "Visita presencial em 48h", "Oferta de retenção (taxa)", "Renegociar contrato", "Ligação de relacionamento", "Monitorar".
- oferta_sugerida: oferta concreta e plausível (ex.: redução de MDR no crédito, antecipação D+0 sem custo por 3 meses, carência/alongamento do contrato, migração para Pix na conta com cashback). Máx. 20 palavras.
- roteiro: 3 frases curtas, numeradas, que o agente pode falar ao lojista (sem jargão interno, sem citar "modelo" ou "score").
"""

# As respostas do LLM vão para uma tabela de staging (ai_query roda uma única vez)
spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}._alertas_lojistas_staging AS
WITH llm AS (
  SELECT c.*,
    ai_query(
      '{LLM}',
      concat(
        {PROMPT!r},
        '\\n\\nLojista: ', nome_fantasia, ' (', segmento, ', ', porte, ', ', cidade, '/', uf, ', ', hub, ')',
        '\\nTipo de sinal: ', tipo_sinal,
        '\\nProbabilidade de vazamento do TPV em 8 semanas: ', format_number(prob_vazamento * 100, 0), '%',
        '\\nProbabilidade de inadimplência >60 dias em 13 semanas: ', coalesce(format_number(prob_inadimplencia * 100, 0) || '%', 'sem crédito ativo'),
        '\\nSaldo devedor: R$ ', format_number(saldo_devedor, 0), ' | Receita anual de MDR: R$ ', format_number(receita_mdr_anual, 0),
        '\\nValor em risco estimado: R$ ', format_number(valor_em_risco, 0),
        '\\nSinais observados: ', array_join(sinais, '; '),
        '\\nÚltimo atendimento: ', coalesce(ultimo_atendimento_resumo, 'nenhum nos últimos 60 dias')
      ),
      responseFormat => 'STRUCT<alerta: STRUCT<resumo: STRING, acao_recomendada: STRING, oferta_sugerida: STRING, roteiro: STRING>>',
      failOnError => false
    ) AS resposta
  FROM candidatos c
  WHERE prioridade <= {MAX_LLM}
),
parse AS (
  SELECT * EXCEPT (resposta),
    from_json(resposta.result, 'resumo STRING, acao_recomendada STRING, oferta_sugerida STRING, roteiro STRING') AS ia
  FROM llm
)
SELECT
  id_lojista, prioridade, semana_referencia, nome_fantasia, segmento, porte, plano, uf, cidade, hub, agente_responsavel,
  faixa_risco, tipo_sinal, round(prob_vazamento, 4) AS prob_vazamento, round(prob_inadimplencia, 4) AS prob_inadimplencia,
  round(valor_em_risco, 2) AS valor_em_risco, round(valor_risco_credito, 2) AS valor_risco_credito, round(valor_risco_receita, 2) AS valor_risco_receita,
  round(saldo_devedor, 2) AS saldo_devedor, round(receita_mdr_anual, 2) AS receita_mdr_anual, CAST(dias_atraso_atual AS INT) AS dias_atraso,
  round(razao_tpv_cartao_4s_12s - 1, 4) AS variacao_tpv_cartao, round(tpv_cartao_media_12s, 2) AS tpv_cartao_media_semanal,
  principal_concorrente, concorrente_citado_atendimento, ultimo_atendimento_resumo,
  array_join(sinais, ' | ') AS sinais,
  coalesce(ia.resumo, concat(tipo_sinal, ': ', sinais[0])) AS resumo_ia,
  coalesce(ia.acao_recomendada, CASE WHEN prob_inadimplencia > 0.5 THEN 'Renegociar contrato' ELSE 'Ligação de relacionamento' END) AS acao_recomendada,
  ia.oferta_sugerida,
  ia.roteiro AS roteiro_abordagem,
  current_timestamp() AS gerado_em
FROM parse
UNION ALL
SELECT
  id_lojista, prioridade, semana_referencia, nome_fantasia, segmento, porte, plano, uf, cidade, hub, agente_responsavel,
  faixa_risco, tipo_sinal, round(prob_vazamento, 4), round(prob_inadimplencia, 4),
  round(valor_em_risco, 2), round(valor_risco_credito, 2), round(valor_risco_receita, 2),
  round(saldo_devedor, 2), round(receita_mdr_anual, 2), CAST(dias_atraso_atual AS INT),
  round(razao_tpv_cartao_4s_12s - 1, 4), round(tpv_cartao_media_12s, 2),
  principal_concorrente, concorrente_citado_atendimento, ultimo_atendimento_resumo,
  array_join(sinais, ' | '),
  concat(tipo_sinal, ': ', sinais[0]),
  CASE WHEN prob_inadimplencia > 0.5 THEN 'Renegociar contrato' ELSE 'Ligação de relacionamento' END,
  NULL, NULL, current_timestamp()
FROM candidatos
WHERE prioridade > {MAX_LLM}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC A tabela final é atualizada com **MERGE**, nunca recriada: `CREATE OR REPLACE` gera um novo id de tabela Delta e
# MAGIC quebra o stream (CDF) da synced table do Lakebase. Com MERGE, o Lakebase recebe só inserções, atualizações e remoções.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.alertas_lojistas
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Fila de alertas do Radar (faixas Crítico, Alto e Médio; quem já passou de 60 dias de atraso vai para cobrança e não entra), priorizada por valor em risco, com explicação, ação recomendada e roteiro gerados por GenAI. Fonte da tabela sincronizada no Lakebase.'
AS SELECT * FROM {FQ}._alertas_lojistas_staging
""")
display(spark.sql(f"""
MERGE INTO {FQ}.alertas_lojistas t
USING {FQ}._alertas_lojistas_staging s ON t.id_lojista = s.id_lojista
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
"""))
spark.sql(f"DROP TABLE {FQ}._alertas_lojistas_staging")

display(spark.sql(f"SELECT prioridade, nome_fantasia, tipo_sinal, valor_em_risco, acao_recomendada, resumo_ia, roteiro_abordagem FROM {FQ}.alertas_lojistas ORDER BY prioridade LIMIT 10"))
