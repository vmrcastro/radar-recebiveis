"""Coleta evidências de execução (em texto) da jornada do Radar e grava em evidencias/.

Uso:
    DATABRICKS_CONFIG_PROFILE=fevm-vini python scripts/coletar_evidencias.py [--run-id N] [--target dev]

Gera:
  * 01_execucao_job.md          — tarefas da execução do job (estado, duração, URL)
  * notebooks/*.ipynb + *.md    — notebooks executados pelo job, COM as saídas das células
  * 02_lakeflow_pipeline.md     — update do pipeline, linhas por tabela e métricas das expectations
  * 03_unity_catalog.md         — tabelas, máscaras de PII em ação, tags, comentários, grants e lineage
  * 04_ml_modelos.md            — modelos no UC (alias champion), métricas MLflow, backtest e importância
  * 05_genai_alertas.md         — saídas reais do LLM (ai_query) no pipeline e nos alertas
  * 06_lakebase.md              — synced table no Postgres, write-back e latência
  * 07_genie.md                 — perguntas em português → SQL gerado → resultado
  * 08_app_api.md               — respostas reais da API do Databricks App
"""

import argparse
import base64
import json
import os
import re
import subprocess
import time
import urllib.parse
from datetime import datetime, timezone

import psycopg
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(RAIZ, "evidencias")
NB_OUT = os.path.join(OUT, "notebooks")
# linhas de log do runtime que não são saída do notebook
NOTACAO_CIENTIFICA = re.compile(r"^-?\d+(\.\d+)?E-?\d+$")
RUIDO = re.compile(r'^\{"ts": .*"logger": "(pyspark|py4j)|Effective usage policy')


# ----------------------------------------------------------------------------- utilitários


def ts(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if ms else "-"


def md_tabela(colunas, linhas, max_linhas=40, max_cel=160):
    def cel(v):
        s = "" if v is None else str(v)
        if NOTACAO_CIENTIFICA.match(s):  # 8.9366866E7 → 89366866
            s = f"{float(s):.4f}".rstrip("0").rstrip(".")
        s = s.replace("|", "\\|").replace("\n", " ")
        return s if len(s) <= max_cel else s[: max_cel - 1] + "…"

    out = ["| " + " | ".join(colunas) + " |", "|" + "---|" * len(colunas)]
    out += ["| " + " | ".join(cel(v) for v in l) + " |" for l in linhas[:max_linhas]]
    if len(linhas) > max_linhas:
        out.append(f"\n_… {len(linhas) - max_linhas} linhas omitidas_")
    return "\n".join(out)


def gravar(nome, conteudo):
    caminho = os.path.join(OUT, nome)
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(conteudo)
    print("✔", os.path.relpath(caminho, RAIZ))


def cabecalho(titulo, w):
    return (f"# {titulo}\n\n> Coletado automaticamente por `scripts/coletar_evidencias.py` em "
            f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC} · workspace `{w.config.host}`\n\n")


class SQL:
    def __init__(self, w, warehouse_id):
        self.w, self.wh = w, warehouse_id

    def __call__(self, q):
        r = self.w.statement_execution.execute_statement(statement=q, warehouse_id=self.wh, wait_timeout="50s")
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(2)
            r = self.w.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"{r.status.error.message if r.status.error else r.status.state}\n{q}")
        cols = [c.name for c in r.manifest.schema.columns]
        return cols, (r.result.data_array or []) if r.result else []

    def bloco(self, titulo, q, explicacao="", max_linhas=40):
        try:
            cols, rows = self(q)
            corpo = md_tabela(cols, rows, max_linhas)
        except Exception as e:  # registra o erro em vez de esconder
            corpo = f"```\nERRO: {e}\n```"
        return f"### {titulo}\n\n{explicacao}\n\n```sql\n{q.strip()}\n```\n\n{corpo}\n\n"


# ----------------------------------------------------------------------------- notebooks executados


def notebook_da_execucao(w, task_run_id):
    """Exporta a execução de um notebook e devolve o modelo (comandos + resultados)."""
    exp = w.jobs.export_run(task_run_id)
    html = exp.views[0].content
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html)
    return json.loads(urllib.parse.unquote(base64.b64decode(m.group(1)).decode("utf-8")))


def _saidas(cmd):
    """Converte os resultados de uma célula Databricks em saídas nbformat + texto legível."""
    outs, textos = [], []
    res = cmd.get("results") or {}
    itens = res.get("data") if res.get("type") == "listResults" else ([res] if res else [])
    for it in itens or []:
        tipo = it.get("type")
        if tipo == "ansi" and it.get("data"):
            txt = re.sub(r"\x1b\[[0-9;]*m", "", it["data"])
            txt = "".join(l for l in txt.splitlines(keepends=True) if not RUIDO.search(l))
            if not txt.strip():
                continue
            outs.append({"output_type": "stream", "name": "stdout", "text": txt})
            textos.append(("texto", txt))
        elif tipo == "table":
            cols = [c["name"] for c in it.get("schema", [])]
            md = md_tabela(cols, it.get("data", []), max_linhas=50)
            outs.append({"output_type": "display_data", "metadata": {},
                         "data": {"text/markdown": md, "text/plain": md}})
            textos.append(("tabela", md))
        elif tipo == "mimeBundle":
            txt = (it.get("data") or {}).get("text/plain", "")
            if txt and "Uploading artifacts" not in txt and "Downloading artifacts" not in txt:
                outs.append({"output_type": "display_data", "metadata": {}, "data": {"text/plain": txt}})
                textos.append(("texto", txt))
    erro = cmd.get("errorSummary") or cmd.get("error")
    if erro:
        erro = re.sub(r"<[^>]+>", "", str(erro))
        outs.append({"output_type": "error", "ename": "Erro", "evalue": erro[:2000], "traceback": []})
        textos.append(("erro", erro[:2000]))
    return outs, textos


def exportar_notebook(w, task, nome_arquivo):
    nb = notebook_da_execucao(w, task.run_id)
    linguagem = nb.get("language", "python")
    cells, md = [], [f"# Execução do notebook `{task.task_key}`\n",
                     f"- Task run id: `{task.run_id}` · início {ts(task.start_time)} · fim {ts(task.end_time)} · "
                     f"resultado **{task.state.result_state.value if task.state and task.state.result_state else task.status.termination_details.code.value}**",
                     f"- URL: {task.run_page_url}\n",
                     "> Transcrição em texto do notebook executado pelo job (código + saída de cada célula), "
                     "extraída da exportação da execução (`jobs export-run`).\n"]
    n = 0
    for cmd in sorted(nb["commands"], key=lambda c: c.get("position", 0)):
        src = cmd.get("command", "")
        if src.lstrip().startswith("%md"):
            corpo = re.sub(r"^\s*%md\s?", "", src)
            cells.append({"cell_type": "markdown", "metadata": {}, "source": corpo})
            md.append(corpo + "\n")
            continue
        n += 1
        outs, textos = _saidas(cmd)
        for o in outs:
            if o["output_type"] == "display_data":
                o["metadata"] = {}
        cells.append({"cell_type": "code", "execution_count": n, "metadata": {}, "source": src, "outputs": outs})
        md.append(f"**Célula [{n}]**\n\n```{linguagem}\n{src}\n```\n")
        for tipo, t in textos:
            if tipo == "tabela":
                md.append("Saída (tabela):\n\n" + t + "\n")
            elif tipo == "erro":
                md.append("Saída (erro):\n\n```\n" + t + "\n```\n")
            else:
                md.append("Saída:\n\n```\n" + t.rstrip() + "\n```\n")
    ipynb = {"nbformat": 4, "nbformat_minor": 5, "cells": cells,
             "metadata": {"language_info": {"name": linguagem},
                          "databricks": {"task_key": task.task_key, "task_run_id": task.run_id,
                                         "run_page_url": task.run_page_url}}}
    with open(os.path.join(NB_OUT, nome_arquivo + ".ipynb"), "w", encoding="utf-8") as f:
        json.dump(ipynb, f, ensure_ascii=False, indent=1)
    with open(os.path.join(NB_OUT, nome_arquivo + ".md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    print("✔ notebooks/" + nome_arquivo + ".ipynb/.md")


# ----------------------------------------------------------------------------- seções


def secao_job(w, run, job):
    linhas = []
    for t in sorted(run.tasks, key=lambda t: t.start_time or 0):
        code = t.status.termination_details.code.value if t.status and t.status.termination_details else "-"
        tipo = "pipeline" if t.pipeline_task else "notebook" if t.notebook_task else "-"
        linhas.append([t.task_key, tipo, code, ts(t.start_time), ts(t.end_time),
                       f"{round(((t.end_time or 0) - (t.start_time or 0)) / 1000 / 60, 1)} min", t.run_id])
    estado = run.status.termination_details.code.value if run.status and run.status.termination_details else run.status.state.value
    dur = round(((run.end_time or 0) - (run.start_time or 0)) / 1000 / 60, 1)
    params = md_tabela(["parâmetro", "valor"], [[p.name, p.value if p.value is not None else p.default] for p in (run.job_parameters or [])])
    txt = cabecalho("Execução do job — jornada completa", w)
    txt += (f"- Job: **{job.settings.name}** (job id `{run.job_id}`)\n- Run id: `{run.run_id}` · {run.run_page_url}\n"
            f"- Resultado: **{estado}** · início {ts(run.start_time)} · fim {ts(run.end_time)} · duração {dur} min\n"
            f"- Todas as tarefas abaixo rodaram nesta **mesma execução**, na ordem de dependência do DAG.\n\n"
            f"## Tarefas\n\n{md_tabela(['tarefa', 'tipo', 'resultado', 'início', 'fim', 'duração', 'task run id'], linhas)}\n\n"
            f"## Parâmetros da execução\n\n{params}\n")
    gravar("01_execucao_job.md", txt)


def secao_pipeline(w, sql, run, fq):
    t = next(t for t in run.tasks if t.pipeline_task)
    pid = t.pipeline_task.pipeline_id
    p = w.pipelines.get(pid)
    upd = next((u for u in w.pipelines.list_updates(pid, max_results=10).updates
                if u.creation_time and u.creation_time >= (t.start_time or 0) - 60_000), None)
    uid = upd.update_id if upd else None
    txt = cabecalho("Lakeflow Declarative Pipeline — execução", w)
    txt += (f"- Pipeline: **{p.name}** (`{pid}`) · serverless={p.spec.serverless} · canal {p.spec.channel}\n"
            f"- Update: `{uid}` · estado **{upd.state.value if upd else '?'}** · full_refresh={upd.full_refresh if upd else '?'} "
            f"· disparado pela tarefa `{t.task_key}` do job\n\n")
    filtro = f"AND origin.update_id = '{uid}'" if uid else ""
    txt += sql.bloco("Linhas escritas por tabela (event log, flow_progress COMPLETED)", f"""
        SELECT origin.flow_name AS tabela,
               -- materialized views reportam o total no evento COMPLETED; streaming tables, por micro-batch (RUNNING)
               coalesce(max(CASE WHEN details:flow_progress.status = 'COMPLETED' THEN details:flow_progress.metrics.num_output_rows::bigint END),
                        sum(CASE WHEN details:flow_progress.status = 'RUNNING' THEN details:flow_progress.metrics.num_output_rows::bigint END)) AS linhas_escritas,
               coalesce(sum(details:flow_progress.data_quality.dropped_records::bigint), 0) AS registros_descartados
        FROM event_log('{pid}')
        WHERE event_type = 'flow_progress' AND origin.flow_name LIKE '%.%' {filtro}
        GROUP BY 1 ORDER BY 1""", "Quantas linhas cada tabela bronze/silver/gold escreveu neste update.")
    txt += sql.bloco("Qualidade de dados — expectations avaliadas", f"""
        SELECT origin.flow_name AS tabela, e.name AS expectation, e.passed_records::bigint AS aprovados,
               e.failed_records::bigint AS reprovados
        FROM (SELECT origin, explode(from_json(details:flow_progress.data_quality.expectations,
                 'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
              FROM event_log('{pid}')
              WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL {filtro})
        ORDER BY 1, 2""", "Métricas das `CONSTRAINT ... EXPECT` declaradas nas camadas silver.")
    txt += sql.bloco("Linha do tempo do update", f"""
        SELECT timestamp, event_type, level, message
        FROM event_log('{pid}')
        WHERE event_type IN ('create_update', 'update_progress') {filtro}
        ORDER BY timestamp""")
    txt += sql.bloco("Arquivos brutos ingeridos pelo Auto Loader (bronze)", f"""
        SELECT 'bronze_cadastro_lojistas' AS tabela, count(DISTINCT _arquivo_origem) AS arquivos, count(*) AS linhas FROM {fq}.bronze_cadastro_lojistas
        UNION ALL SELECT 'bronze_vendas_diarias', count(DISTINCT _arquivo_origem), count(*) FROM {fq}.bronze_vendas_diarias
        UNION ALL SELECT 'bronze_registradora', count(DISTINCT _arquivo_origem), count(*) FROM {fq}.bronze_registradora
        UNION ALL SELECT 'bronze_contratos_credito', count(DISTINCT _arquivo_origem), count(*) FROM {fq}.bronze_contratos_credito
        UNION ALL SELECT 'bronze_atendimentos', count(DISTINCT _arquivo_origem), count(*) FROM {fq}.bronze_atendimentos""")
    gravar("02_lakeflow_pipeline.md", txt)


def secao_uc(w, sql, catalog, schema, app_name):
    fq = f"{catalog}.{schema}"
    txt = cabecalho("Unity Catalog — governança", w)
    txt += sql.bloco("Tabelas do schema", f"""
        SELECT table_name, table_type, comment FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}' AND table_name NOT LIKE '\\_\\_%' AND table_name NOT LIKE 'event_log_%'
        ORDER BY table_name""", max_linhas=60)
    txt += sql.bloco("Máscaras de coluna em ação (usuário fora do grupo PII)", f"""
        SELECT id_lojista, cnpj, razao_social, nome_titular, cpf_titular, telefone, email, nome_fantasia
        FROM {fq}.silver_lojistas ORDER BY id_lojista LIMIT 5""",
                     "A coleta roda com um usuário que **não** pertence ao grupo de PII: CNPJ, CPF, nome, e-mail e "
                     "telefone voltam mascarados pelas funções UC aplicadas no `MASK` das colunas.")
    txt += sql.bloco("Funções de máscara registradas no UC", f"""
        SELECT routine_name, routine_definition FROM {catalog}.information_schema.routines
        WHERE routine_schema = '{schema}' ORDER BY 1""")
    txt += sql.bloco("Tags de coluna (PII)", f"""
        SELECT table_name, column_name, tag_name, tag_value FROM {catalog}.information_schema.column_tags
        WHERE schema_name = '{schema}' ORDER BY 1, 2""")
    txt += sql.bloco("Tags de tabela", f"""
        SELECT table_name, tag_name, tag_value FROM {catalog}.information_schema.table_tags
        WHERE schema_name = '{schema}' ORDER BY 1, 2""", max_linhas=80)
    app_sp = w.apps.get(app_name).service_principal_client_id
    consumidores = f"('account users', '{app_sp}')"
    txt += sql.bloco("Grants concedidos pela jornada (consumidores e service principal do App)", f"""
        SELECT grantee, 'SCHEMA' AS objeto, '{schema}' AS nome, privilege_type
        FROM {catalog}.information_schema.schema_privileges
        WHERE schema_name = '{schema}' AND grantee IN {consumidores}
        UNION ALL
        SELECT grantee, 'TABLE', table_name, privilege_type FROM {catalog}.information_schema.table_privileges
        WHERE table_schema = '{schema}' AND grantee IN {consumidores} AND table_name NOT LIKE '\\_\\_%'
        ORDER BY 1, 2, 3""", f"Least privilege: consumidores (`account users`) e o App (service principal `{app_sp}`) só "
                             "leem gold/ML; bronze e silver (com PII) não recebem grant.", max_linhas=60)
    txt += sql.bloco("Lineage capturado automaticamente (system.access.table_lineage)", f"""
        SELECT source_table_name AS origem, target_table_name AS destino, entity_type AS executado_por,
               max(event_time) AS ultimo_evento
        FROM system.access.table_lineage
        WHERE target_table_schema = '{schema}' AND target_table_catalog = '{catalog}'
          AND source_table_name IS NOT NULL AND event_date >= current_date() - INTERVAL 7 DAYS
        GROUP BY 1, 2, 3 ORDER BY 2, 1""", "Do arquivo bruto até a tabela de alertas consumida pelo App/Lakebase.",
                     max_linhas=60)
    gravar("03_unity_catalog.md", txt)


def secao_ml(w, sql, fq):
    txt = cabecalho("Machine Learning — modelos, métricas e backtest", w)
    for nome in ("vazamento", "inadimplencia"):
        full = f"{fq}.modelo_{nome}"
        try:
            rm = w.registered_models.get(full, include_aliases=True)
            versoes = sorted(w.model_versions.list(full), key=lambda v: int(v.version), reverse=True)
            aliases = {a.alias_name: a.version_num for a in (rm.aliases or [])}
            txt += f"## Modelo `{full}`\n\n- Aliases: `{aliases}`\n- Versões registradas: {len(versoes)}\n\n"
            v = next((x for x in versoes if int(x.version) == aliases.get("champion")), versoes[0])
            txt += f"Versão champion **v{v.version}** · criada em {ts(v.created_at)} · run MLflow `{v.run_id}` · status {v.status.value if v.status else "-"}\n\n"
            run = w.experiments.get_run(v.run_id).run
            mets = sorted((m.key, round(m.value, 4)) for m in (run.data.metrics or []))
            pars = sorted((p.key, p.value) for p in (run.data.params or []))
            txt += "Métricas registradas no MLflow (run final):\n\n" + md_tabela(["métrica", "valor"], mets) + "\n\n"
            if pars:
                txt += "Parâmetros:\n\n" + md_tabela(["parâmetro", "valor"], pars) + "\n\n"
        except Exception as e:
            txt += f"## Modelo `{full}`\n\n```\nERRO: {e}\n```\n\n"
    txt += sql.bloco("Backtest de negócio (tabela backtest_radar)", f"SELECT * FROM {fq}.backtest_radar ORDER BY modelo, metrica")
    txt += sql.bloco("Importância das variáveis (permutation importance, top 8 por modelo)", f"""
        SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY modelo ORDER BY importancia DESC) AS rk
                       FROM {fq}.importancia_variaveis) WHERE rk <= 8 ORDER BY modelo, rk""")
    txt += sql.bloco("Distribuição das faixas de risco na semana de referência", f"""
        SELECT faixa_risco, tipo_sinal, count(*) AS lojistas, round(avg(prob_vazamento), 3) AS prob_vaz_media,
               round(avg(prob_inadimplencia), 3) AS prob_inad_media, round(sum(valor_em_risco), 0) AS valor_em_risco
        FROM {fq}.scores_lojistas GROUP BY 1, 2 ORDER BY 1, 2""")
    gravar("04_ml_modelos.md", txt)


def secao_genai(w, sql, fq):
    txt = cabecalho("GenAI — saídas reais do LLM", w)
    txt += sql.bloco("1) Pipeline: texto livre do atendimento → sinal estruturado (ai_query + responseFormat)", f"""
        SELECT data_hora, canal, texto, motivo, concorrente_citado, risco_cancelamento, sentimento
        FROM {fq}.gold_atendimentos_sinais WHERE motivo IS NOT NULL ORDER BY data_hora DESC LIMIT 12""",
                     "Classificação feita pelo LLM dentro do Lakeflow (`silver_atendimentos_ia`).")
    txt += sql.bloco("Volume classificado pelo LLM", f"""
        SELECT motivo, count(*) AS atendimentos, count(concorrente_citado) AS com_concorrente_citado
        FROM {fq}.gold_atendimentos_sinais WHERE motivo IS NOT NULL GROUP BY 1 ORDER BY 2 DESC""")
    q = f"""SELECT prioridade, nome_fantasia, segmento, hub, faixa_risco, tipo_sinal, round(valor_em_risco, 0) AS valor_em_risco,
               sinais, resumo_ia, acao_recomendada, oferta_sugerida, roteiro_abordagem
        FROM {fq}.alertas_lojistas ORDER BY prioridade LIMIT 6"""
    txt += (f"### 2) Alertas: resumo, ação, oferta e roteiro gerados pelo LLM para o agente do hub\n\n"
            f"Entrada do LLM = sinais calculados pelo modelo; saída = texto estruturado (`responseFormat`). Texto completo, sem cortes.\n\n"
            f"```sql\n{q}\n```\n\n")
    cols, rows = sql(q)
    for r in rows:
        a = dict(zip(cols, r))
        txt += (f"#### #{a['prioridade']} · {a['nome_fantasia']} ({a['segmento']}, {a['hub']}) · {a['faixa_risco']} · "
                f"{a['tipo_sinal']} · valor em risco R$ {float(a['valor_em_risco']):,.0f}\n\n"
                f"- **Sinais (entrada):** {a['sinais']}\n- **Resumo (LLM):** {a['resumo_ia']}\n"
                f"- **Ação recomendada (LLM):** {a['acao_recomendada']}\n- **Oferta sugerida (LLM):** {a['oferta_sugerida']}\n"
                f"- **Roteiro de abordagem (LLM):**\n\n```\n{a['roteiro_abordagem']}\n```\n\n")
    txt += sql.bloco("Origem dos textos dos alertas (LLM × template de contingência)", f"""
        SELECT CASE WHEN roteiro_abordagem IS NOT NULL THEN 'LLM (ai_query)' ELSE 'template de contingência' END AS origem_texto,
               count(*) AS alertas
        FROM {fq}.alertas_lojistas GROUP BY 1""")
    gravar("05_genai_alertas.md", txt)


def secao_lakebase(w, project, database, schema):
    txt = cabecalho("Lakebase (Postgres) — serving operacional", w)
    branch = f"projects/{project}/branches/production"
    ep = next(iter(w.postgres.list_endpoints(parent=branch)))
    txt += f"- Projeto: `{project}` · branch `{branch}` · endpoint `{ep.name}`\n- Host: `{ep.status.hosts.host}` · database `{database}`\n\n"
    try:
        st = w.postgres.get_synced_table(name=f"synced_tables/{w._catalog_fq}.alertas_lojistas_lakebase")
        txt += "Synced table (Delta → Postgres):\n\n```json\n" + json.dumps(st.as_dict(), indent=1, default=str)[:3000] + "\n```\n\n"
    except Exception as e:
        txt += f"```\nsynced table: {e}\n```\n\n"
    token = w.postgres.generate_database_credential(endpoint=ep.name).token
    user = w.current_user.me().user_name
    with psycopg.connect(host=ep.status.hosts.host, dbname=database, user=user, password=token, sslmode="require",
                         autocommit=True) as conn:
        cur = conn.cursor()
        consultas = [
            ("Versão do Postgres", "SELECT version()"),
            ("Tabelas no Lakebase", "SELECT table_schema, table_name, table_type FROM information_schema.tables "
                                   f"WHERE table_schema IN ('{schema}', 'app') ORDER BY 1, 2"),
            ("Linhas sincronizadas do lakehouse", f'SELECT count(*) AS alertas, count(*) FILTER (WHERE faixa_risco = \'Crítico\') AS criticos, '
                                                  f'round(sum(valor_em_risco)::numeric, 0) AS valor_em_risco FROM "{schema}".alertas_lojistas_lakebase'),
            ("Fila priorizada (o que o App lê)", f'SELECT prioridade, id_lojista, nome_fantasia, hub, faixa_risco, tipo_sinal, '
                                                  f'round(valor_em_risco::numeric, 0) AS valor_em_risco FROM "{schema}".alertas_lojistas_lakebase ORDER BY prioridade LIMIT 10'),
            ("Write-back do agente (tabela transacional app.acoes_campo)", "SELECT id_acao, id_lojista, agente, acao, resultado, registrado_em FROM app.acoes_campo ORDER BY registrado_em DESC LIMIT 10"),
        ]
        for titulo, q in consultas:
            try:
                t0 = time.perf_counter()
                cur.execute(q)
                rows = cur.fetchall()
                ms = round((time.perf_counter() - t0) * 1000, 1)
                txt += f"### {titulo}\n\n```sql\n{q}\n```\n\n{md_tabela([d.name for d in cur.description], rows)}\n\n_tempo de consulta (cliente → Lakebase, incl. rede): {ms} ms_\n\n"
            except Exception as e:
                txt += f"### {titulo}\n\n```\nERRO: {e}\n```\n\n"
    gravar("06_lakebase.md", txt)


def secao_genie(w, space_id, perguntas):
    txt = cabecalho("Genie — perguntas em português sobre dados governados", w)
    sp = w.genie.get_space(space_id)
    txt += f"- Genie space: **{sp.title}** (`{space_id}`)\n\n"
    conv = None
    for i, q in enumerate(perguntas, 1):
        try:
            msg = (w.genie.create_message_and_wait(space_id, conv, q) if conv
                   else w.genie.start_conversation_and_wait(space_id, q))
            conv = msg.conversation_id
            txt += f"## Pergunta {i}{' (follow-up na mesma conversa)' if i > 1 else ''}\n\n> **{q}**\n\n- Status: `{msg.status.value}` · conversa `{conv}` · mensagem `{msg.id}`\n\n"
            for att in msg.attachments or []:
                if att.text and att.text.content:
                    txt += f"**Resposta do Genie:**\n\n{att.text.content}\n\n"
                if att.query:
                    txt += f"**Interpretação:** {att.query.description}\n\n**SQL gerado:**\n\n```sql\n{att.query.query}\n```\n\n"
                    r = w.genie.get_message_attachment_query_result(space_id, conv, msg.id, att.attachment_id)
                    sr = r.statement_response
                    if sr and sr.manifest:
                        cols = [c.name for c in sr.manifest.schema.columns]
                        txt += "**Resultado:**\n\n" + md_tabela(cols, (sr.result.data_array or []) if sr.result else [], 20) + "\n\n"
        except Exception as e:
            txt += f"## Pergunta {i}\n\n> {q}\n\n```\nERRO: {e}\n```\n\n"
    gravar("07_genie.md", txt)


def secao_app(w, app_name):
    app = w.apps.get(app_name)
    headers = w.config.authenticate()
    url = app.url.rstrip("/")
    txt = cabecalho("Databricks App — respostas reais da API", w)
    txt += (f"- App: **{app.name}** · {url}\n- Estado: compute `{app.compute_status.state.value if app.compute_status else '?'}` · "
            f"app `{app.app_status.state.value if app.app_status else '?'}`\n"
            f"- Deployment ativo: `{app.active_deployment.deployment_id if app.active_deployment else '?'}` "
            f"({app.active_deployment.status.state.value if app.active_deployment and app.active_deployment.status else '?'})\n"
            f"- Recursos: {', '.join(r.name for r in (app.resources or []))}\n\n")

    def chamar(metodo, caminho, corpo=None, resumir=None):
        nonlocal txt
        t0 = time.perf_counter()
        r = requests.request(metodo, url + caminho, headers=headers, json=corpo, timeout=180)
        ms = round((time.perf_counter() - t0) * 1000)
        try:
            dados = r.json()
        except ValueError:
            dados = r.text[:500]
        mostrado = resumir(dados) if resumir and r.ok else dados
        txt += (f"### `{metodo} {caminho}` → HTTP {r.status_code} ({ms} ms ponta a ponta)\n\n"
                + (f"Corpo enviado:\n\n```json\n{json.dumps(corpo, ensure_ascii=False, indent=1)}\n```\n\n" if corpo else "")
                + f"```json\n{json.dumps(mostrado, ensure_ascii=False, indent=1, default=str)[:6000]}\n```\n\n")
        return dados

    r = requests.get(url + "/", headers=headers, timeout=60)
    txt += f"### `GET /` → HTTP {r.status_code} · {len(r.text)} bytes · título: `{(re.search(r'<title>(.*?)</title>', r.text) or [None, '?'])[1]}`\n\n"
    chamar("GET", "/api/config")
    chamar("GET", "/api/resumo", resumir=lambda d: {**d, "tendencia": d.get("tendencia", [])[-6:]})
    fila = chamar("GET", "/api/fila?faixa=Cr%C3%ADtico&limite=5")
    if isinstance(fila, dict) and fila.get("itens"):
        idl = fila["itens"][0]["id_lojista"]
        chamar("GET", f"/api/lojista/{idl}", resumir=lambda d: {**d, "historico": d.get("historico", [])[-6:]})
        chamar("POST", "/api/acoes", {"id_lojista": idl, "agente": "coleta-de-evidencias", "acao": "Ligação",
                                       "resultado": "Sem contato", "observacao": "Registro automático de evidência de write-back"})
    chamar("POST", "/api/genie", {"pergunta": "Quantos lojistas estão em alerta por faixa de risco?"})
    gravar("08_app_api.md", txt)


# ----------------------------------------------------------------------------- main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="dev")
    ap.add_argument("--run-id", type=int)
    args = ap.parse_args()
    os.makedirs(NB_OUT, exist_ok=True)

    resumo = json.loads(subprocess.check_output(["databricks", "bundle", "summary", "-t", args.target, "-o", "json"], cwd=RAIZ))
    var = {k: v.get("value", v.get("default")) for k, v in resumo["variables"].items()}
    res = resumo["resources"]
    catalog, schema = var["catalog"], var["schema"]
    fq = f"{catalog}.{schema}"
    job_id = int(res["jobs"]["radar_jornada"]["id"])
    app_name = res["apps"]["radar_app"]["name"]
    genie_id = res["genie_spaces"]["radar_genie"]["id"]

    w = WorkspaceClient()
    w._catalog_fq = fq
    sql = SQL(w, var["warehouse_id"])

    job = w.jobs.get(job_id)
    if args.run_id:
        run = w.jobs.get_run(args.run_id)
    else:  # última execução bem-sucedida com TODAS as tarefas executadas
        run = None
        for r in w.jobs.list_runs(job_id=job_id, completed_only=True, limit=25):
            cand = w.jobs.get_run(r.run_id)
            codes = {t.task_key: t.status.termination_details.code.value for t in cand.tasks}
            if cand.status.termination_details.code.value == "SUCCESS" and all(c == "SUCCESS" for c in codes.values()):
                run = cand
                break
        assert run, "Nenhuma execução completa e bem-sucedida encontrada"
    print("Execução:", run.run_id, run.run_page_url)

    secao_job(w, run, job)
    for i, t in enumerate(sorted(run.tasks, key=lambda t: t.start_time or 0), 1):
        if t.notebook_task and t.status.termination_details and t.status.termination_details.code.value == "SUCCESS":
            exportar_notebook(w, t, f"{i:02d}_{t.task_key}")
    secao_pipeline(w, sql, run, fq)
    secao_uc(w, sql, catalog, schema, app_name)
    secao_ml(w, sql, fq)
    secao_genai(w, sql, fq)
    secao_genie(w, genie_id, ["Para quais concorrentes estamos perdendo mais recebíveis nas últimas 4 semanas?",
                              "E em quais segmentos isso está concentrado?",
                              "Qual o percentual do saldo com mais de 60 dias de atraso, por segmento?"])
    secao_app(w, app_name)
    secao_lakebase(w, var["lakebase_project"], var["lakebase_database"], schema)  # depois do App: mostra o write-back


if __name__ == "__main__":
    main()
