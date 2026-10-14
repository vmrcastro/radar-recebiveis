# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase — serving operacional da fila de alertas
# MAGIC
# MAGIC 1. Atualiza a **synced table** `alertas_lojistas` (Delta → Postgres) criada pelo bundle
# MAGIC 2. Concede ao service principal do App leitura dos alertas sincronizados
# MAGIC
# MAGIC A tabela transacional `app.acoes_campo` (write-back do agente) pertence ao próprio App, que a cria no startup.

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.2" "databricks-sdk>=0.105.0"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "vini_catalog")
dbutils.widgets.text("schema", "radar_recebiveis")
dbutils.widgets.text("lakebase_project", "radar-recebiveis")
dbutils.widgets.text("lakebase_database", "radar")
dbutils.widgets.text("app_name", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PROJECT = dbutils.widgets.get("lakebase_project")
DATABASE = dbutils.widgets.get("lakebase_database")
APP_NAME = dbutils.widgets.get("app_name")
BRANCH = f"projects/{PROJECT}/branches/production"
SYNCED = f"{CATALOG}.{SCHEMA}.alertas_lojistas_lakebase"

# COMMAND ----------

import time

import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

st = w.postgres.get_synced_table(name=f"synced_tables/{SYNCED}")
pipeline_id = st.status.pipeline_id if st.status and getattr(st.status, "pipeline_id", None) else st.pipeline_id
print("Synced table pipeline:", pipeline_id)
FINAIS = ("COMPLETED", "FAILED", "CANCELED")


def aguardar(update_id):
    while True:
        estado = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state.value
        if estado in FINAIS:
            return estado
        time.sleep(15)


def sincronizar(full_refresh=False):
    # Não dispara um update se já houver outro em andamento (ex.: retentativa automática do pipeline)
    for u in w.pipelines.list_updates(pipeline_id=pipeline_id, max_results=5).updates or []:
        if u.state.value not in FINAIS:
            print("Aguardando update em andamento:", u.update_id, aguardar(u.update_id))
    upd = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh)
    estado = aguardar(upd.update_id)
    print(f"Sync (full_refresh={full_refresh}):", estado)
    return estado


estado = sincronizar()
if estado != "COMPLETED":
    # Ex.: a tabela de origem foi recriada (novo id Delta) e o checkpoint do stream ficou inválido
    print("Sincronização incremental falhou; refazendo com full refresh.")
    w.pipelines.stop_and_wait(pipeline_id=pipeline_id)  # interrompe retentativas automáticas
    estado = sincronizar(full_refresh=True)
assert estado == "COMPLETED", f"Sincronização falhou: {estado}"

# COMMAND ----------

endpoint = next(iter(w.postgres.list_endpoints(parent=BRANCH)))
host = endpoint.status.hosts.host
usuario = w.current_user.me().user_name
token = w.postgres.generate_database_credential(endpoint=endpoint.name).token

app_sp = w.apps.get(APP_NAME).service_principal_client_id if APP_NAME else None
print("Host:", host, "| App SP:", app_sp)

with psycopg.connect(host=host, dbname=DATABASE, user=usuario, password=token, sslmode="require", autocommit=True) as conn:
    cur = conn.cursor()
    print(cur.execute(f'SELECT count(*) FROM "{SCHEMA}"."alertas_lojistas_lakebase"').fetchone()[0], "alertas no Postgres")
    if app_sp:
        cur.execute(f"""
            GRANT USAGE ON SCHEMA "{SCHEMA}" TO "{app_sp}";
            GRANT SELECT ON ALL TABLES IN SCHEMA "{SCHEMA}" TO "{app_sp}";
            ALTER DEFAULT PRIVILEGES IN SCHEMA "{SCHEMA}" GRANT SELECT ON TABLES TO "{app_sp}";
        """)
        print("Grants concedidos ao App.")
