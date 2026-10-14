-- =====================================================================
-- GOLD — produtos de dados de negócio: carteira de crédito, visão semanal
-- do lojista (Pedregulho × concorrentes) e dimensões sem PII para Genie/App.
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_lojistas
COMMENT 'Dimensão de lojistas SEM PII, segura para consumo analítico (Genie, App, BI). Uma linha por lojista.'
TBLPROPERTIES ('camada' = 'gold')
AS SELECT
  id_lojista, nome_fantasia, segmento, uf, cidade, regiao, hub, agente_responsavel, porte, plano,
  data_credenciamento, usa_conta_digital
FROM silver_lojistas;

-- Amortização diária de cada contrato: a trava retém pct_retencao × recebíveis de cartão do dia.
-- Se o lojista desvia o cartão para outra credenciadora ou migra para Pix, a garantia some.
CREATE OR REFRESH MATERIALIZED VIEW gold_contrato_dia
COMMENT 'Evolução diária de cada contrato de capital de giro: retenção do dia, acumulado pago vs esperado, saldo devedor e dias de atraso.'
TBLPROPERTIES ('camada' = 'gold')
AS
WITH calendario AS (
  SELECT c.*, explode(sequence(c.data_contratacao, DATE'${data_referencia}')) AS data
  FROM silver_contratos_credito c
),
diario AS (
  SELECT
    cal.id_contrato, cal.id_lojista, cal.data, cal.data_contratacao, cal.valor_principal, cal.valor_total_devido,
    cal.prazo_meses, cal.pct_retencao_recebiveis,
    coalesce(v.valor_cartao, 0) * cal.pct_retencao_recebiveis AS retencao_bruta_dia,
    cal.valor_total_devido / (cal.prazo_meses * 30) AS parcela_diaria_esperada,
    least(cal.valor_total_devido, cal.valor_total_devido * (datediff(cal.data, cal.data_contratacao) + 1) / (cal.prazo_meses * 30)) AS esperado_acumulado
  FROM calendario cal
  LEFT JOIN silver_vendas_diarias v ON v.id_lojista = cal.id_lojista AND v.data_venda = cal.data
),
acum AS (
  SELECT *,
    least(valor_total_devido, sum(retencao_bruta_dia) OVER (PARTITION BY id_contrato ORDER BY data)) AS pago_acumulado
  FROM diario
)
SELECT
  id_contrato, id_lojista, data, data_contratacao, valor_principal, valor_total_devido, prazo_meses, pct_retencao_recebiveis,
  parcela_diaria_esperada,
  pago_acumulado - coalesce(lag(pago_acumulado) OVER (PARTITION BY id_contrato ORDER BY data), 0) AS retencao_dia,
  esperado_acumulado, pago_acumulado,
  valor_total_devido - pago_acumulado AS saldo_devedor,
  greatest(0, esperado_acumulado - pago_acumulado) AS valor_em_atraso,
  CAST(greatest(0, floor((esperado_acumulado - pago_acumulado) / parcela_diaria_esperada)) AS INT) AS dias_atraso
FROM acum;

CREATE OR REFRESH MATERIALIZED VIEW gold_carteira_credito
COMMENT 'Fotografia atual (data de referência) da carteira de capital de giro: um registro por contrato com saldo, atraso e faixa de risco.'
TBLPROPERTIES ('camada' = 'gold')
AS SELECT
  c.id_contrato, c.id_lojista, d.segmento, d.uf, d.regiao, d.hub, d.porte,
  c.data_contratacao, c.valor_principal, c.valor_total_devido, c.prazo_meses, c.pct_retencao_recebiveis,
  c.pago_acumulado, c.saldo_devedor, c.valor_em_atraso, c.dias_atraso,
  round(c.pago_acumulado / c.valor_total_devido, 4) AS pct_pago,
  CASE
    WHEN c.saldo_devedor <= 0.01 THEN 'Quitado'
    WHEN c.dias_atraso = 0 THEN 'Em dia'
    WHEN c.dias_atraso <= 30 THEN '1-30 dias'
    WHEN c.dias_atraso <= 60 THEN '31-60 dias'
    WHEN c.dias_atraso <= 90 THEN '61-90 dias'
    ELSE '90+ dias'
  END AS faixa_atraso,
  c.dias_atraso > 60 AS inadimplente_60
FROM gold_contrato_dia c
JOIN dim_lojistas d USING (id_lojista)
WHERE c.data = DATE'${data_referencia}';

-- Visão semanal do lojista: o que passa na Pedregulho × o que a registradora mostra nos concorrentes.
CREATE OR REFRESH MATERIALIZED VIEW gold_lojista_semana
COMMENT 'Uma linha por lojista e semana (segunda-feira): TPV na Pedregulho, recebíveis em concorrentes (registradora), receita e situação do crédito.'
TBLPROPERTIES ('camada' = 'gold')
AS
WITH vendas AS (
  SELECT id_lojista, date_trunc('WEEK', data_venda) AS semana,
    sum(valor_cartao) AS tpv_cartao, sum(valor_pix) AS tpv_pix, sum(tpv_total) AS tpv_total,
    sum(receita_mdr) AS receita_mdr, sum(qtd_transacoes) AS qtd_transacoes, count(*) AS dias_com_venda
  FROM silver_vendas_diarias GROUP BY ALL
),
reg AS (
  SELECT id_lojista, CAST(semana_referencia AS TIMESTAMP) AS semana,
    sum(CASE WHEN e_pedregulho THEN valor_recebiveis ELSE 0 END) AS recebiveis_pedregulho,
    sum(CASE WHEN NOT e_pedregulho THEN valor_recebiveis ELSE 0 END) AS recebiveis_concorrentes,
    max_by(CASE WHEN NOT e_pedregulho THEN credenciadora END, CASE WHEN NOT e_pedregulho THEN valor_recebiveis END) AS principal_concorrente
  FROM silver_registradora GROUP BY ALL
),
credito AS (
  SELECT id_lojista, date_trunc('WEEK', data) AS semana,
    sum(saldo_devedor) FILTER (WHERE dayofweek(data) = 1 OR data = DATE'${data_referencia}') AS saldo_devedor,
    max(dias_atraso) AS max_dias_atraso,
    sum(retencao_dia) AS retencao_semana,
    sum(parcela_diaria_esperada) AS parcela_esperada_semana
  FROM gold_contrato_dia GROUP BY ALL
),
semanas AS (
  SELECT id_lojista, semana FROM vendas UNION SELECT id_lojista, semana FROM reg
)
SELECT
  s.id_lojista, CAST(s.semana AS DATE) AS semana,
  d.segmento, d.uf, d.regiao, d.hub, d.porte, d.plano,
  coalesce(v.tpv_cartao, 0) AS tpv_cartao, coalesce(v.tpv_pix, 0) AS tpv_pix, coalesce(v.tpv_total, 0) AS tpv_total,
  coalesce(v.receita_mdr, 0) AS receita_mdr, coalesce(v.qtd_transacoes, 0) AS qtd_transacoes, coalesce(v.dias_com_venda, 0) AS dias_com_venda,
  coalesce(r.recebiveis_pedregulho, 0) AS recebiveis_pedregulho,
  coalesce(r.recebiveis_concorrentes, 0) AS recebiveis_concorrentes,
  coalesce(r.recebiveis_pedregulho, 0) / nullif(coalesce(r.recebiveis_pedregulho, 0) + coalesce(r.recebiveis_concorrentes, 0), 0) AS share_pedregulho,
  r.principal_concorrente,
  c.saldo_devedor, c.max_dias_atraso, c.retencao_semana, c.parcela_esperada_semana
FROM semanas s
JOIN dim_lojistas d USING (id_lojista)
LEFT JOIN vendas v ON v.id_lojista = s.id_lojista AND v.semana = s.semana
LEFT JOIN reg r ON r.id_lojista = s.id_lojista AND r.semana = s.semana
LEFT JOIN credito c ON c.id_lojista = s.id_lojista AND c.semana = s.semana;

CREATE OR REFRESH MATERIALIZED VIEW gold_concorrencia_semana
COMMENT 'Recebíveis de cartão por credenciadora, semana, segmento e UF (visão da registradora). Base para medir ganho/perda de share por concorrente.'
TBLPROPERTIES ('camada' = 'gold')
AS SELECT
  r.semana_referencia AS semana, r.credenciadora, r.e_pedregulho, d.segmento, d.uf, d.regiao, d.hub, d.porte,
  count(DISTINCT r.id_lojista) AS lojistas, sum(r.valor_recebiveis) AS valor_recebiveis
FROM silver_registradora r
JOIN dim_lojistas d USING (id_lojista)
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW gold_atendimentos_sinais
COMMENT 'Atendimentos recentes com sinais extraídos por IA (motivo, concorrente citado, risco de cancelamento), sem PII.'
TBLPROPERTIES ('camada' = 'gold')
AS SELECT
  a.id_atendimento, a.id_lojista, a.data_hora, a.canal, a.texto,
  a.sinal.motivo AS motivo, nullif(nullif(trim(a.sinal.concorrente), ''), 'null') AS concorrente_citado,
  a.sinal.risco_cancelamento AS risco_cancelamento, a.sinal.sentimento AS sentimento, a.sinal.resumo AS resumo,
  d.segmento, d.uf, d.hub
FROM silver_atendimentos_ia a
JOIN dim_lojistas d USING (id_lojista)
WHERE a.erro_ia IS NULL;
