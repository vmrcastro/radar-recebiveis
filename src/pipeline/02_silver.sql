-- =====================================================================
-- SILVER — dados limpos, tipados e com regras de qualidade (expectations).
-- PII protegida por máscaras dinâmicas do Unity Catalog definidas aqui
-- mesmo, no pipeline: a política acompanha a tabela, não o consumidor.
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_lojistas (
  id_lojista STRING COMMENT 'Identificador interno do lojista',
  cnpj STRING MASK ${catalog}.${schema}.mascara_documento COMMENT 'CNPJ do estabelecimento (PII — mascarado fora do grupo autorizado)',
  razao_social STRING MASK ${catalog}.${schema}.mascara_texto COMMENT 'Razão social (PII)',
  nome_fantasia STRING COMMENT 'Nome fantasia (público)',
  nome_titular STRING MASK ${catalog}.${schema}.mascara_texto COMMENT 'Nome do titular/sócio (PII)',
  cpf_titular STRING MASK ${catalog}.${schema}.mascara_documento COMMENT 'CPF do titular (PII)',
  telefone STRING MASK ${catalog}.${schema}.mascara_documento COMMENT 'Telefone de contato (PII)',
  email STRING MASK ${catalog}.${schema}.mascara_texto COMMENT 'E-mail de contato (PII)',
  segmento STRING COMMENT 'Segmento de atuação (MCC agrupado)',
  uf STRING COMMENT 'UF do estabelecimento',
  cidade STRING COMMENT 'Cidade do estabelecimento',
  regiao STRING COMMENT 'Região do Brasil',
  hub STRING COMMENT 'Hub de atendimento em campo responsável pelo lojista',
  agente_responsavel STRING COMMENT 'Agente de campo responsável pela carteira',
  porte STRING COMMENT 'Porte: Micro, Pequeno ou Médio',
  plano STRING COMMENT 'Plano comercial: Pedrinha (micro) ou Pedregulho Pro',
  data_credenciamento DATE COMMENT 'Data de credenciamento',
  usa_conta_digital BOOLEAN COMMENT 'Lojista usa a conta digital (recebe Pix na conta)',
  CONSTRAINT id_lojista_presente EXPECT (id_lojista IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT cnpj_formato_valido EXPECT (cnpj RLIKE '^[0-9]{2}\\.[0-9]{3}\\.[0-9]{3}/[0-9]{4}-[0-9]{2}$'),
  CONSTRAINT porte_conhecido EXPECT (porte IN ('Micro', 'Pequeno', 'Médio'))
)
COMMENT 'Cadastro de lojistas, uma linha por lojista (última versão recebida). PII mascarada via Unity Catalog.'
TBLPROPERTIES ('camada' = 'silver')
AS SELECT
  id_lojista, cnpj, razao_social, nome_fantasia, nome_titular, cpf_titular, telefone, email,
  segmento, uf, cidade, regiao, hub, agente_responsavel, porte, plano,
  CAST(data_credenciamento AS DATE) AS data_credenciamento,
  CAST(usa_conta_digital AS BOOLEAN) AS usa_conta_digital
FROM bronze_cadastro_lojistas
QUALIFY row_number() OVER (PARTITION BY id_lojista ORDER BY _ingerido_em DESC) = 1;

-- Chave de-para CNPJ → lojista, usada só dentro do pipeline (a registradora identifica por CNPJ)
CREATE OR REFRESH MATERIALIZED VIEW silver_registradora (
  CONSTRAINT lojista_identificado EXPECT (id_lojista IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valor_nao_negativo EXPECT (valor_recebiveis >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Agenda semanal de recebíveis de cartão por lojista e credenciadora (inclui concorrentes), já associada ao id do lojista.'
TBLPROPERTIES ('camada' = 'silver')
AS SELECT
  l.id_lojista,
  CAST(r.semana_referencia AS DATE) AS semana_referencia,
  r.credenciadora,
  r.credenciadora = 'Pedregulho Pagamentos' AS e_pedregulho,
  CAST(replace(r.valor_recebiveis, ',', '.') AS DOUBLE) AS valor_recebiveis
FROM bronze_registradora r
LEFT JOIN (
  SELECT id_lojista, cnpj FROM bronze_cadastro_lojistas
  QUALIFY row_number() OVER (PARTITION BY id_lojista ORDER BY _ingerido_em DESC) = 1
) l ON l.cnpj = r.cnpj;

CREATE OR REFRESH STREAMING TABLE silver_vendas_diarias (
  CONSTRAINT lojista_presente EXPECT (id_lojista IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valores_nao_negativos EXPECT (valor_credito >= 0 AND valor_debito >= 0 AND valor_pix >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Vendas diárias por lojista. valor_cartao = crédito + débito (é o que pode ser travado como garantia do crédito).'
TBLPROPERTIES ('camada' = 'silver')
AS SELECT
  id_lojista,
  CAST(data_venda AS DATE) AS data_venda,
  valor_credito, valor_debito, valor_pix,
  valor_credito + valor_debito AS valor_cartao,
  valor_credito + valor_debito + valor_pix AS tpv_total,
  qtd_transacoes, receita_mdr
FROM STREAM(bronze_vendas_diarias);

CREATE OR REFRESH MATERIALIZED VIEW silver_contratos_credito (
  CONSTRAINT contrato_valido EXPECT (id_contrato IS NOT NULL AND valor_principal > 0) ON VIOLATION DROP ROW,
  CONSTRAINT retencao_razoavel EXPECT (pct_retencao_recebiveis BETWEEN 0.01 AND 0.5)
)
COMMENT 'Contratos de capital de giro. Amortização = pct_retencao_recebiveis × recebíveis de cartão que passam na Pedregulho.'
TBLPROPERTIES ('camada' = 'silver')
AS SELECT
  id_contrato, id_lojista, produto,
  CAST(data_contratacao AS DATE) AS data_contratacao,
  CAST(valor_principal AS DOUBLE) AS valor_principal,
  CAST(taxa_juros_mes AS DOUBLE) AS taxa_juros_mes,
  CAST(prazo_meses AS INT) AS prazo_meses,
  CAST(valor_total_devido AS DOUBLE) AS valor_total_devido,
  CAST(pct_retencao_recebiveis AS DOUBLE) AS pct_retencao_recebiveis
FROM bronze_contratos_credito
QUALIFY row_number() OVER (PARTITION BY id_contrato ORDER BY _ingerido_em DESC) = 1;

-- GenAI dentro do Lakeflow: o texto livre do atendimento vira sinal estruturado.
-- Streaming table → cada atendimento é processado pelo LLM uma única vez (incremental).
CREATE OR REFRESH STREAMING TABLE silver_atendimentos_ia
COMMENT 'Atendimentos dos últimos 90 dias enriquecidos por LLM (ai_query): motivo, menção a concorrente, risco de cancelamento, sentimento.'
TBLPROPERTIES ('camada' = 'silver')
AS
WITH base AS (
  SELECT id_atendimento, id_lojista, CAST(data_hora AS TIMESTAMP) AS data_hora, canal, texto
  FROM STREAM(bronze_atendimentos)
  WHERE CAST(data_hora AS TIMESTAMP) >= date_sub(DATE'${data_referencia}', 90)
),
ia AS (
  SELECT *,
    ai_query(
      '${llm_extracao}',
      concat(
        'Você analisa atendimentos de lojistas de uma credenciadora de pagamentos brasileira. ',
        'Classifique o texto abaixo. motivo deve ser um de: Taxas e preço, Proposta de concorrente, Cancelamento, ',
        'Dificuldade financeira, Renegociação de crédito, Problema técnico, Dúvida operacional, Elogio, Outro. ',
        'concorrente: nome da empresa concorrente citada ou null. risco_cancelamento: Alto, Médio ou Baixo. ',
        'sentimento: Positivo, Neutro ou Negativo. resumo: até 12 palavras.\n\nTexto: ', texto),
      responseFormat => 'STRUCT<classificacao: STRUCT<motivo: STRING, concorrente: STRING, risco_cancelamento: STRING, sentimento: STRING, resumo: STRING>>',
      failOnError => false
    ) AS resposta
  FROM base
)
SELECT
  id_atendimento, id_lojista, data_hora, canal, texto,
  from_json(resposta.result, 'motivo STRING, concorrente STRING, risco_cancelamento STRING, sentimento STRING, resumo STRING') AS sinal,
  resposta.errorMessage AS erro_ia
FROM ia;
