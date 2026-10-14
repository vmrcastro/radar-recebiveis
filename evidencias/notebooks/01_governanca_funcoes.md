# Execução do notebook `governanca_funcoes`

- Task run id: `1105572102679200` · início 2026-10-14 13:30:08 UTC · fim 2026-10-14 13:30:40 UTC · resultado **SUCCESS**
- URL: https://fevm-vini.cloud.databricks.com/?o=7474658470734499#job/883752161988103/run/1105572102679200

> Transcrição em texto do notebook executado pelo job (código + saída de cada célula), extraída da exportação da execução (`jobs export-run`).

# Governança (pré-pipeline) — funções de máscara do Unity Catalog

As máscaras são **funções SQL governadas** no UC. O pipeline Lakeflow as aplica nas colunas de PII
de `silver_lojistas`. Quem está no grupo autorizado vê o dado real; todos os demais — inclusive
Genie, App e analistas — veem o dado mascarado, sem precisar de cópia ou view paralela.

**Célula [1]**

```python
dbutils.widgets.text("catalog", "vini_catalog")
dbutils.widgets.text("schema", "radar_recebiveis")
dbutils.widgets.text("grupo_pii", "radar_pii_leitores")

FQ = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
GRUPO = dbutils.widgets.get("grupo_pii")
```

**Célula [2]**

```python
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
```

Saída (tabela):

| cnpj | nome |
|---|---|
| **.***.***/****-90 | M•••••• |
