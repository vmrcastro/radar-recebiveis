# Execução do notebook `genai_alertas`

- Task run id: `711259691843804` · início 2026-10-14 14:01:05 UTC · fim 2026-10-14 14:04:15 UTC · resultado **SUCCESS**
- URL: https://fevm-vini.cloud.databricks.com/?o=7474658470734499#job/883752161988103/run/711259691843804

> Transcrição em texto do notebook executado pelo job (código + saída de cada célula), extraída da exportação da execução (`jobs export-run`).

# Alertas com GenAI — do score à ação do agente de campo

O modelo diz **quem** e **quanto**; o LLM traduz os sinais em **por quê** e **o que fazer**:
um resumo para o gestor, a ação recomendada e um roteiro de abordagem para o agente do hub.
Tudo via `ai_query` em SQL, governado e auditável no Unity Catalog.

**Célula [1]**

```python
dbutils.widgets.text("catalog", "vini_catalog")
dbutils.widgets.text("schema", "radar_recebiveis")
dbutils.widgets.text("empresa", "Pedregulho Pagamentos")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6")
dbutils.widgets.text("max_alertas_llm", "800")

FQ = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
EMPRESA = dbutils.widgets.get("empresa")
LLM = dbutils.widgets.get("llm_endpoint")
MAX_LLM = int(dbutils.widgets.get("max_alertas_llm"))
```

**Célula [2]**

```python
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW candidatos AS
SELECT s.*, d.nome_fantasia, d.segmento, d.uf, d.cidade, d.hub, d.agente_responsavel, d.porte, d.plano,
       row_number() OVER (ORDER BY s.valor_em_risco DESC) AS prioridade
FROM {FQ}.scores_lojistas s
JOIN {FQ}.dim_lojistas d USING (id_lojista)
WHERE s.faixa_risco IN ('Crítico', 'Alto', 'Médio')
""")
print(spark.sql("SELECT count(*) FROM candidatos").first()[0], "alertas")
```

Saída:

```
440 alertas
```

**Célula [3]**

```python
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
```

Saída:

```
DataFrame[num_affected_rows: bigint, num_inserted_rows: bigint]
```

A tabela final é atualizada com **MERGE**, nunca recriada: `CREATE OR REPLACE` gera um novo id de tabela Delta e
quebra o stream (CDF) da synced table do Lakebase. Com MERGE, o Lakebase recebe só inserções, atualizações e remoções.

**Célula [4]**

```python
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
```

Saída (tabela):

| num_affected_rows | num_updated_rows | num_deleted_rows | num_inserted_rows |
|---|---|---|---|
| 440 | 440 | 0 | 0 |

Saída (tabela):

| prioridade | nome_fantasia | tipo_sinal | valor_em_risco | acao_recomendada | resumo_ia | roteiro_abordagem |
|---|---|---|---|---|---|---|
| 1 | Drogaria Saúde Já | Desvio para concorrente | 336295.24 | Visita presencial em 48h | A Drogaria Saúde Já está desviando recebíveis para a NuvemPay em ritmo acelerado — share da Pedregulho caiu 14 p.p. e o contrato já acumula atraso —, colocando… | 1. "Notamos que seu movimento de cartão conosco reduziu nas últimas semanas e queremos entender como podemos melhorar sua experiência antes que isso gere qualq… |
| 2 | Mecânica Pit Stop | Desvio para concorrente | 250292.35 | Visita presencial em 48h | A Mecânica Pit Stop apresenta sinais combinados de desvio de recebíveis e deterioração da garantia do crédito: a trava cobre apenas 48% da parcela esperada, o … | 1. "Notamos que suas vendas no cartão diminuíram conosco nas últimas semanas e queremos entender se há algo que possamos melhorar para facilitar o dia a dia da… |
| 3 | Eletro Lima | Desvio para concorrente | 206750.93 | Visita presencial em 48h | A Eletro Lima apresenta forte sinal de migração para a PagaLogo, com 87% de probabilidade de desvio do TPV em 8 semanas e R$ 59.112/semana já registrados no co… | 1. "Lima, ficamos sabendo que você recebeu uma proposta com antecipação no mesmo dia sem custo — queremos garantir que a Pedregulho Pagamentos seja pelo menos … |
| 4 | Empório Bom Preço | Desvio para concorrente | 178134.15 | Visita presencial em 48h | O Empório Bom Preço está desviando volume expressivo de recebíveis para a NuvemPay — o share da Pedregulho caiu 33 p.p. e a trava já cobre apenas 64% da parcel… | 1. "Notamos uma queda no movimento de cartão aqui na loja e gostaríamos de entender como podemos melhorar o que oferecemos a você." 2. "Temos uma proposta espe… |
| 5 | Espetinho do Sérgio | Desvio para concorrente | 165668.41 | Visita presencial em 48h | O Espetinho do Sérgio está migrando ativamente para a PagaLogo: o TPV na Pedregulho caiu 28% em 4 semanas, o share de recebíveis despencou 25 p.p. e a trava já… | 1. "Sérgio, ficamos sabendo que você recebeu uma proposta com antecipação no mesmo dia sem custo — e queremos te oferecer exatamente isso, agora, como cliente … |
| 6 | Posto Rota Sul | Desvio para concorrente | 159396.5 | Visita presencial em 48h | O Posto Rota Sul apresenta sinais claros de migração para a NuvemPay: os recebíveis registrados no concorrente subiram 142% e o share da Pedregulho caiu 16 p.p… | 1. "Posto Rota Sul, sabemos que você recebeu uma proposta competitiva e queremos garantir que a Pedregulho continue sendo a melhor opção para o seu negócio." 2… |
| 7 | Barbearia Navalha de Ouro | Desvio para concorrente | 154861.32 | Visita presencial em 48h | A Barbearia Navalha de Ouro está desviando ativamente o TPV para a MercaPix — a trava de recebíveis já cobre apenas 37% da parcela e o contrato acumula 11 dias… | 1. "Notamos uma queda nas suas vendas conosco e queremos entender o que aconteceu — sua satisfação é prioridade para nós." 2. "Podemos oferecer liquidação no m… |
| 8 | Drogaria Marques | Sinais combinados | 141594.33 | Visita presencial em 48h | A Drogaria Marques apresenta risco crítico de migração para a NuvemPay, com 76% de probabilidade de vazamento de TPV em 8 semanas e 54% de risco de inadimplênc… | 1. "Drogaria Marques, identificamos uma oportunidade de revisar suas condições conosco e queremos garantir que você tenha a proposta mais competitiva do mercad… |
| 9 | Depósito Bom Tijolo | Desvio para concorrente | 128130.28 | Visita presencial em 48h | O Depósito Bom Tijolo está desviando ativamente recebíveis para o Banco Laranja PJ e migrando volume para Pix, com a trava cobrindo apenas 25% da parcela esper… | 1. "Notamos uma queda no seu movimento conosco e queremos entender o que está acontecendo para ajudar da melhor forma." 2. "Sabemos que recebeu uma proposta co… |
| 10 | Empório Bom Preço | Desvio para concorrente | 124865.36 | Visita presencial em 48h | O Empório Bom Preço está migrando ativamente seu volume de vendas para a Maquininha Azul, com queda de 19 p.p. no share da Pedregulho e trava de recebíveis já … | 1. "Empório Bom Preço, percebemos que seu movimento no cartão conosco reduziu bastante nas últimas semanas — gostaríamos de entender o que está acontecendo e v… |
