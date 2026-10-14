# Databricks notebook source
# MAGIC %md
# MAGIC # Governança (pré-pipeline) — funções de máscara do Unity Catalog
# MAGIC
# MAGIC As máscaras são **funções SQL governadas** no UC. O pipeline Lakeflow as aplica nas colunas de PII
# MAGIC de `silver_lojistas`. Quem está no grupo autorizado vê o dado real; todos os demais — inclusive
# MAGIC Genie, App e analistas — veem o dado mascarado, sem precisar de cópia ou view paralela.

# COMMAND ----------

dbutils.widgets.text("catalog", "vini_catalog")
dbutils.widgets.text("schema", "radar_recebiveis")
dbutils.widgets.text("grupo_pii", "radar_pii_leitores")

FQ = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
GRUPO = dbutils.widgets.get("grupo_pii")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.mascara_documento(valor STRING)
RETURNS STRING
COMMENT 'Máscara de documentos/telefone (CNPJ, CPF): mantém só os 2 últimos dígitos fora do grupo {GRUPO}.'
RETURN CASE
  WHEN is_account_group_member('{GRUPO}') THEN valor
  WHEN valor IS NULL THEN NULL
  ELSE concat(regexp_replace(substr(valor, 1, length(valor) - 2), '[0-9]', '*'), right(valor, 2))
END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.mascara_texto(valor STRING)
RETURNS STRING
COMMENT 'Máscara de texto (nome, razão social, e-mail): mantém só a primeira letra fora do grupo {GRUPO}.'
RETURN CASE
  WHEN is_account_group_member('{GRUPO}') THEN valor
  WHEN valor IS NULL THEN NULL
  ELSE concat(left(valor, 1), '••••••')
END
""")

display(spark.sql(f"SELECT {FQ}.mascara_documento('12.345.678/0001-90') AS cnpj, {FQ}.mascara_texto('Maria Silva') AS nome"))
