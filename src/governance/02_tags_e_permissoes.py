# Databricks notebook source
# MAGIC %md
# MAGIC # Governança (pós-pipeline) — classificação, documentação e acesso
# MAGIC
# MAGIC * **Tags** de classificação (PII, camada, domínio) → descoberta e políticas por atributo
# MAGIC * **Comentários** de colunas → documentação viva no Catalog Explorer (e contexto para o Genie)
# MAGIC * **Grants** mínimos: o time de negócio e o App leem só a camada gold (sem PII)

# COMMAND ----------

dbutils.widgets.text("catalog", "vini_catalog")
dbutils.widgets.text("schema", "radar_recebiveis")
dbutils.widgets.text("grupo_negocio", "account users")
dbutils.widgets.text("app_name", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
GRUPO_NEGOCIO = dbutils.widgets.get("grupo_negocio")
APP_NAME = dbutils.widgets.get("app_name")


def sql(stmt):
    try:
        spark.sql(stmt)
    except Exception as e:  # tags/grants não devem derrubar a jornada
        print(f"[aviso] {stmt[:90]}... → {str(e).splitlines()[0][:200]}")

# COMMAND ----------

PII = {
    "silver_lojistas": {"cnpj": "cnpj", "cpf_titular": "cpf", "razao_social": "nome", "nome_titular": "nome",
                        "telefone": "telefone", "email": "email"},
}
for tabela, cols in PII.items():
    for col, tipo in cols.items():
        sql(f"ALTER MATERIALIZED VIEW {FQ}.{tabela} ALTER COLUMN {col} SET TAGS ('classificacao' = 'pii', 'tipo_pii' = '{tipo}')")

CAMADAS = {
    "bronze": ["bronze_cadastro_lojistas", "bronze_vendas_diarias", "bronze_registradora", "bronze_contratos_credito", "bronze_atendimentos"],
    "silver": ["silver_lojistas", "silver_vendas_diarias", "silver_registradora", "silver_contratos_credito", "silver_atendimentos_ia"],
    "gold": ["dim_lojistas", "gold_contrato_dia", "gold_carteira_credito", "gold_lojista_semana", "gold_concorrencia_semana",
             "gold_atendimentos_sinais"],
    "ml": ["scores_lojistas", "backtest_radar", "importancia_variaveis", "alertas_lojistas"],
}
TIPO = {t: ("STREAMING TABLE" if t.startswith("bronze_") or t in ("silver_vendas_diarias", "silver_atendimentos_ia") else
            "MATERIALIZED VIEW" if t.startswith(("silver_", "gold_", "dim_")) else "TABLE")
        for ts in CAMADAS.values() for t in ts}
for camada, tabelas in CAMADAS.items():
    for t in tabelas:
        # "camada" é uma tag governada (bronze/silver/gold): os produtos de ML são consumo, então ficam como gold
        tags = (f"'camada' = '{'gold' if camada == 'ml' else camada}', 'natureza' = '{'saida_de_ml' if camada == 'ml' else 'dados'}', "
                f"'area_negocio' = 'risco_e_retencao', 'contem_pii' = '{'sim' if camada in ('bronze',) or t == 'silver_lojistas' else 'nao'}'")
        sql(f"ALTER {TIPO[t]} {FQ}.{t} SET TAGS ({tags})")

sql(f"ALTER SCHEMA {FQ} SET TAGS ('produto_de_dados' = 'radar_vazamento_recebiveis', 'dono' = 'risco_de_credito')")

# COMMAND ----------

COMENTARIOS = {
    "gold_lojista_semana": {
        "tpv_cartao": "TPV de cartão (crédito + débito) processado na Pedregulho na semana, em R$",
        "tpv_pix": "Pix recebido na conta Pedregulho na semana, em R$",
        "recebiveis_pedregulho": "Recebíveis de cartão do lojista na Pedregulho segundo a registradora, em R$",
        "recebiveis_concorrentes": "Recebíveis de cartão do lojista em outras credenciadoras segundo a registradora, em R$",
        "share_pedregulho": "Participação da Pedregulho nos recebíveis de cartão do lojista (0 a 1)",
        "principal_concorrente": "Credenciadora concorrente com maior volume de recebíveis do lojista na semana",
        "saldo_devedor": "Saldo devedor de capital de giro ao fim da semana, em R$",
        "max_dias_atraso": "Maior número de dias de atraso entre os contratos do lojista na semana",
        "retencao_semana": "Valor retido dos recebíveis (trava) para amortizar o crédito na semana, em R$",
        "parcela_esperada_semana": "Amortização esperada no cronograma do contrato na semana, em R$",
    },
    "gold_carteira_credito": {
        "saldo_devedor": "Saldo devedor atual do contrato, em R$",
        "valor_em_atraso": "Diferença entre o esperado pelo cronograma e o efetivamente retido, em R$",
        "dias_atraso": "Dias de atraso equivalentes (valor em atraso / parcela diária esperada)",
        "faixa_atraso": "Faixa de atraso: Quitado, Em dia, 1-30, 31-60, 61-90, 90+ dias",
        "inadimplente_60": "Verdadeiro se o contrato tem mais de 60 dias de atraso",
        "pct_retencao_recebiveis": "Percentual dos recebíveis de cartão retido diariamente para pagar o contrato",
    },
}
for t, cols in COMENTARIOS.items():
    for c, txt in cols.items():
        sql(f"ALTER MATERIALIZED VIEW {FQ}.{t} ALTER COLUMN {c} COMMENT '{txt}'")

# COMMAND ----------

# Acesso de negócio: somente gold/ML, nunca bronze/silver
LEITURA = CAMADAS["gold"] + CAMADAS["ml"]
principais = [GRUPO_NEGOCIO]
if APP_NAME:
    from databricks.sdk import WorkspaceClient
    try:
        principais.append(WorkspaceClient().apps.get(APP_NAME).service_principal_client_id)
    except Exception as e:
        print(f"[aviso] App {APP_NAME} ainda não existe: {e}")

for p in principais:
    sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{p}`")
    sql(f"GRANT USE SCHEMA ON SCHEMA {FQ} TO `{p}`")
    for t in LEITURA:
        sql(f"GRANT SELECT ON TABLE {FQ}.{t} TO `{p}`")
print("Acesso de leitura (gold/ML) concedido a:", principais)
