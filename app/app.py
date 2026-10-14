"""Radar do Hub — Databricks App da Pedregulho Pagamentos (empresa fictícia).

Fontes:
  * Lakebase (Postgres)  → fila de alertas sincronizada do lakehouse + write-back das ações do agente
  * SQL Warehouse (UC)   → séries históricas e KPIs executivos das tabelas gold
  * Genie                → perguntas em linguagem natural
"""

import os
import threading
import time
from datetime import date, datetime
from decimal import Decimal

import psycopg
import uvicorn
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from pydantic import BaseModel

EMPRESA = os.getenv("EMPRESA", "Pedregulho Pagamentos")
UC_SCHEMA = os.environ["UC_SCHEMA"]
SYNC_SCHEMA = os.environ["SYNC_SCHEMA"]
LAKEBASE_BRANCH = os.environ["LAKEBASE_BRANCH"]
LAKEBASE_DATABASE = os.getenv("PGDATABASE") or os.environ["LAKEBASE_DATABASE"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
GENIE_SPACE_ID = os.environ["GENIE_SPACE_ID"]
ALERTAS = f'"{SYNC_SCHEMA}"."alertas_lojistas_lakebase"'

w = WorkspaceClient()
app = FastAPI(title="Radar do Hub")


# ----------------------------------------------------------------------------- Lakebase


class Lakebase:
    """Conexão Postgres com credencial OAuth renovada antes de expirar (tokens valem 1h)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._conn = None
        self._expira = 0.0
        self._endpoint = None

    def _conectar(self):
        if self._endpoint is None:
            self._endpoint = next(iter(w.postgres.list_endpoints(parent=LAKEBASE_BRANCH)))
        host = os.getenv("PGHOST") or self._endpoint.status.hosts.host
        token = w.postgres.generate_database_credential(endpoint=self._endpoint.name).token
        user = os.getenv("PGUSER") or w.config.client_id or w.current_user.me().user_name
        self._conn = psycopg.connect(host=host, dbname=LAKEBASE_DATABASE, user=user, password=token,
                                     sslmode="require", autocommit=True, row_factory=dict_row)
        self._expira = time.time() + 45 * 60

    def query(self, sql, params=None):
        with self._lock:
            for tentativa in range(2):
                try:
                    if self._conn is None or self._conn.closed or time.time() > self._expira:
                        self._conectar()
                    with self._conn.cursor() as cur:
                        cur.execute(sql, params)
                        return cur.fetchall() if cur.description else []
                except psycopg.OperationalError:
                    self._conn = None
                    if tentativa:
                        raise


pg = Lakebase()


def _garantir_tabela_acoes():
    # Tabela transacional do App (o service principal do App é o dono). Leitura liberada para análise no lakehouse.
    try:
        pg.query("""
            CREATE SCHEMA IF NOT EXISTS app;
            CREATE TABLE IF NOT EXISTS app.acoes_campo (
                id_acao BIGSERIAL PRIMARY KEY, id_lojista TEXT NOT NULL, agente TEXT NOT NULL, acao TEXT NOT NULL,
                resultado TEXT NOT NULL, observacao TEXT, valor_em_risco NUMERIC(14, 2),
                registrado_em TIMESTAMPTZ NOT NULL DEFAULT now());
            CREATE INDEX IF NOT EXISTS acoes_campo_lojista ON app.acoes_campo (id_lojista, registrado_em DESC);
            GRANT USAGE ON SCHEMA app TO PUBLIC;
            GRANT SELECT ON app.acoes_campo TO PUBLIC;
        """)
    except Exception as e:
        print("acoes_campo:", e)


# ----------------------------------------------------------------------------- SQL Warehouse (UC)

_cache: dict = {}


def sql_uc(query, ttl=600):
    hit = _cache.get(query)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    r = w.statement_execution.execute_statement(statement=query, warehouse_id=WAREHOUSE_ID, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise HTTPException(500, f"Erro no SQL: {r.status.error.message if r.status.error else r.status.state}")
    cols = [c.name for c in r.manifest.schema.columns]
    rows = [dict(zip(cols, row)) for row in (r.result.data_array or [])]
    _cache[query] = (time.time(), rows)
    return rows


def _json(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def limpar(rows):
    return [{k: _json(v) for k, v in r.items()} for r in rows]


# ----------------------------------------------------------------------------- API


@app.get("/api/config")
def config():
    hubs = pg.query(f"SELECT DISTINCT hub FROM {ALERTAS} ORDER BY hub")
    return {"empresa": EMPRESA, "hubs": [h["hub"] for h in hubs]}


@app.get("/api/resumo")
def resumo():
    k = pg.query(f"""
        SELECT count(*) AS alertas,
               count(*) FILTER (WHERE faixa_risco = 'Crítico') AS criticos,
               coalesce(sum(valor_em_risco), 0) AS valor_em_risco,
               coalesce(sum(valor_risco_credito), 0) AS valor_risco_credito,
               coalesce(sum(valor_risco_receita), 0) AS valor_risco_receita,
               coalesce(sum(saldo_devedor), 0) AS saldo_em_alerta
        FROM {ALERTAS}""")[0]
    por_tipo = pg.query(f"""
        SELECT tipo_sinal, count(*) AS lojistas, sum(valor_em_risco) AS valor_em_risco
        FROM {ALERTAS} GROUP BY 1 ORDER BY 3 DESC""")
    por_hub = pg.query(f"""
        SELECT hub, count(*) AS lojistas, count(*) FILTER (WHERE faixa_risco = 'Crítico') AS criticos,
               sum(valor_em_risco) AS valor_em_risco
        FROM {ALERTAS} GROUP BY 1 ORDER BY 4 DESC LIMIT 12""")
    acoes = pg.query("""
        SELECT count(DISTINCT id_lojista) AS lojistas_tratados,
               count(*) FILTER (WHERE resultado = 'Retido / acordo fechado') AS acordos,
               coalesce(sum(valor_em_risco) FILTER (WHERE resultado = 'Retido / acordo fechado'), 0) AS valor_protegido
        FROM app.acoes_campo""")[0]
    carteira = sql_uc(f"""
        SELECT round(sum(saldo_devedor), 0) AS saldo_ativo,
               round(sum(CASE WHEN dias_atraso > 60 THEN saldo_devedor ELSE 0 END), 0) AS saldo_inad_60,
               count(*) AS contratos_ativos
        FROM {UC_SCHEMA}.gold_carteira_credito WHERE faixa_atraso <> 'Quitado'""")
    backtest = sql_uc(f"SELECT modelo, metrica, valor, descricao FROM {UC_SCHEMA}.backtest_radar")
    tendencia = sql_uc(f"""
        SELECT semana,
               round(sum(CASE WHEN e_pedregulho THEN valor_recebiveis END), 0) AS pedregulho,
               round(sum(CASE WHEN NOT e_pedregulho THEN valor_recebiveis END), 0) AS concorrentes
        FROM {UC_SCHEMA}.gold_concorrencia_semana
        WHERE semana >= date_sub((SELECT max(semana_referencia) FROM {UC_SCHEMA}.scores_lojistas), 7 * 30)
          AND semana <= (SELECT max(semana_referencia) FROM {UC_SCHEMA}.scores_lojistas)
        GROUP BY 1 ORDER BY 1""")
    concorrentes = sql_uc(f"""
        WITH ref AS (SELECT max(semana_referencia) AS s FROM {UC_SCHEMA}.scores_lojistas)
        SELECT credenciadora,
               round(sum(CASE WHEN semana > date_sub(ref.s, 28) THEN valor_recebiveis END)
                   - sum(CASE WHEN semana <= date_sub(ref.s, 28) AND semana > date_sub(ref.s, 28 + 84) THEN valor_recebiveis END) / 3, 0) AS ganho_4s
        FROM {UC_SCHEMA}.gold_concorrencia_semana, ref
        WHERE NOT e_pedregulho AND semana <= ref.s
        GROUP BY 1 ORDER BY 2 DESC""")
    return {"kpis": limpar([k])[0], "por_tipo": limpar(por_tipo), "por_hub": limpar(por_hub),
            "acoes": limpar([acoes])[0], "carteira": carteira[0] if carteira else {},
            "backtest": backtest, "tendencia": tendencia, "concorrentes": concorrentes}


@app.get("/api/fila")
def fila(hub: str = "", tipo: str = "", faixa: str = "", busca: str = "", limite: int = 60):
    filtros, params = [], []
    for col, val in (("a.hub", hub), ("a.tipo_sinal", tipo), ("a.faixa_risco", faixa)):
        if val:
            filtros.append(f"{col} = %s")
            params.append(val)
    if busca:
        filtros.append("(a.nome_fantasia ILIKE %s OR a.id_lojista ILIKE %s)")
        params += [f"%{busca}%"] * 2
    where = ("WHERE " + " AND ".join(filtros)) if filtros else ""
    t0 = time.perf_counter()
    rows = pg.query(f"""
        SELECT a.id_lojista, a.prioridade, a.nome_fantasia, a.segmento, a.porte, a.cidade, a.uf, a.hub,
               a.agente_responsavel, a.faixa_risco, a.tipo_sinal, a.prob_vazamento, a.prob_inadimplencia,
               a.valor_em_risco, a.saldo_devedor, a.receita_mdr_anual, a.dias_atraso, a.variacao_tpv_cartao,
               a.principal_concorrente, a.acao_recomendada, a.resumo_ia,
               u.acao AS ultima_acao, u.resultado AS ultimo_resultado, u.registrado_em AS ultima_acao_em
        FROM {ALERTAS} a
        LEFT JOIN LATERAL (
            SELECT acao, resultado, registrado_em FROM app.acoes_campo c
            WHERE c.id_lojista = a.id_lojista ORDER BY registrado_em DESC LIMIT 1) u ON true
        {where}
        ORDER BY a.valor_em_risco DESC
        LIMIT %s""", params + [min(limite, 300)])
    return {"latencia_ms": round((time.perf_counter() - t0) * 1000, 1), "itens": limpar(rows)}


@app.get("/api/lojista/{id_lojista}")
def lojista(id_lojista: str):
    a = pg.query(f"SELECT * FROM {ALERTAS} WHERE id_lojista = %s", (id_lojista,))
    if not a:
        raise HTTPException(404, "Lojista não está na fila de alertas")
    acoes = pg.query("SELECT * FROM app.acoes_campo WHERE id_lojista = %s ORDER BY registrado_em DESC", (id_lojista,))
    safe_id = id_lojista.replace("'", "")
    historico = sql_uc(f"""
        SELECT semana, round(tpv_cartao, 0) AS tpv_cartao, round(tpv_pix, 0) AS tpv_pix,
               round(recebiveis_concorrentes, 0) AS recebiveis_concorrentes, round(saldo_devedor, 0) AS saldo_devedor,
               max_dias_atraso
        FROM {UC_SCHEMA}.gold_lojista_semana
        WHERE id_lojista = '{safe_id}'
          AND semana <= (SELECT max(semana_referencia) FROM {UC_SCHEMA}.scores_lojistas)
        ORDER BY semana DESC LIMIT 26""", ttl=120)
    atendimentos = sql_uc(f"""
        SELECT date_format(data_hora, 'dd/MM/yyyy') AS data, canal, texto, motivo, concorrente_citado, risco_cancelamento
        FROM {UC_SCHEMA}.gold_atendimentos_sinais WHERE id_lojista = '{safe_id}'
        ORDER BY data_hora DESC LIMIT 5""", ttl=120)
    return {"alerta": limpar(a)[0], "acoes": limpar(acoes), "historico": list(reversed(historico)),
            "atendimentos": atendimentos}


class Acao(BaseModel):
    id_lojista: str
    agente: str
    acao: str
    resultado: str
    observacao: str = ""


@app.post("/api/acoes")
def registrar_acao(a: Acao):
    t0 = time.perf_counter()
    row = pg.query(f"""
        INSERT INTO app.acoes_campo (id_lojista, agente, acao, resultado, observacao, valor_em_risco)
        SELECT %s, %s, %s, %s, %s, (SELECT valor_em_risco FROM {ALERTAS} WHERE id_lojista = %s)
        RETURNING id_acao, registrado_em""", (a.id_lojista, a.agente, a.acao, a.resultado, a.observacao, a.id_lojista))
    return {"latencia_ms": round((time.perf_counter() - t0) * 1000, 1), **limpar(row)[0]}


class Pergunta(BaseModel):
    pergunta: str
    conversation_id: str | None = None


@app.post("/api/genie")
def genie(p: Pergunta):
    if p.conversation_id:
        msg = w.genie.create_message_and_wait(GENIE_SPACE_ID, p.conversation_id, p.pergunta)
    else:
        msg = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, p.pergunta)
    resp = {"conversation_id": msg.conversation_id, "texto": [], "sql": None, "colunas": [], "linhas": []}
    for att in msg.attachments or []:
        if att.text and att.text.content:
            resp["texto"].append(att.text.content)
        if att.query:
            resp["sql"] = att.query.query
            if att.query.description:
                resp["texto"].insert(0, att.query.description)
            r = w.genie.get_message_attachment_query_result(GENIE_SPACE_ID, msg.conversation_id, msg.id, att.attachment_id)
            sr = r.statement_response
            if sr and sr.manifest:
                resp["colunas"] = [c.name for c in sr.manifest.schema.columns]
                resp["linhas"] = (sr.result.data_array or [])[:50] if sr.result else []
        if getattr(att, "suggested_questions", None) and att.suggested_questions.questions:
            resp["sugestoes"] = att.suggested_questions.questions
    return resp


app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


if __name__ == "__main__":
    _garantir_tabela_acoes()
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("DATABRICKS_APP_PORT", "8000")))
