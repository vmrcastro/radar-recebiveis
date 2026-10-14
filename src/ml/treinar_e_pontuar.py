# Databricks notebook source
# MAGIC %md
# MAGIC # Radar de Vazamento — treino, backtest e pontuação
# MAGIC
# MAGIC Dois modelos sobre a mesma visão semanal do lojista (`gold_lojista_semana`):
# MAGIC
# MAGIC | Modelo | Pergunta | Rótulo (olhando para o futuro) |
# MAGIC |---|---|---|
# MAGIC | `modelo_vazamento` | Este lojista vai tirar o cartão da Pedregulho? | TPV de cartão médio das próximas 8 semanas < 50% da média das últimas 12 |
# MAGIC | `modelo_inadimplencia` | Este contrato vai passar de 60 dias de atraso? | Atraso > 60 dias em algum momento das próximas 13 semanas |
# MAGIC
# MAGIC Validação **out-of-time** (treina no passado, mede no futuro) e backtest em métricas de negócio:
# MAGIC quanto do saldo que virou inadimplência teria sido sinalizado, e com quantos dias de antecedência.

# COMMAND ----------

dbutils.widgets.text("catalog", "vini_catalog")
dbutils.widgets.text("schema", "radar_recebiveis")
dbutils.widgets.text("data_referencia", "2026-10-08")
dbutils.widgets.text("experimento", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DATA_REF = dbutils.widgets.get("data_referencia")
EXPERIMENTO = dbutils.widgets.get("experimento")
FQ = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

mlflow.set_registry_uri("databricks-uc")
if EXPERIMENTO:
    mlflow.set_experiment(EXPERIMENTO)

ref = pd.Timestamp(DATA_REF)
# Última semana COMPLETA (segunda a domingo) até a data de referência
ultima_semana = (ref - pd.Timedelta(days=ref.dayofweek)) - pd.Timedelta(days=7 if ref.dayofweek < 6 else 0)

sem = spark.table(f"{FQ}.gold_lojista_semana").toPandas()
sem["semana"] = pd.to_datetime(sem["semana"])
sem = sem[sem["semana"] <= ultima_semana].sort_values(["id_lojista", "semana"]).reset_index(drop=True)
dim = spark.table(f"{FQ}.dim_lojistas").select("id_lojista", "data_credenciamento").toPandas()
print(len(sem), "linhas lojista×semana; snapshot de pontuação:", ultima_semana.date())

# COMMAND ----------

# MAGIC %md ## Features (somente passado) e rótulos (somente futuro)

# COMMAND ----------

def G():
    return sem.groupby("id_lojista", sort=False)


def roll(col, w, fn="mean"):
    return G()[col].transform(lambda s: getattr(s.rolling(w, min_periods=1), fn)())


def fut(col, w, fn="mean"):
    # janela das próximas w semanas (exclui a atual); NaN se não houver futuro completo
    return G()[col].transform(lambda s: getattr(s[::-1].rolling(w, min_periods=w), fn)()[::-1].shift(-1))


sem["saldo_devedor"] = sem["saldo_devedor"].fillna(0)
sem["max_dias_atraso"] = sem["max_dias_atraso"].fillna(0)
sem["tpv_cartao_4s"] = roll("tpv_cartao", 4)
sem["tpv_cartao_12s"] = roll("tpv_cartao", 12)
sem["tpv_total_4s"] = roll("tpv_total", 4)
sem["tpv_total_12s"] = roll("tpv_total", 12)
sem["conc_4s"] = roll("recebiveis_concorrentes", 4)
sem["conc_12s"] = roll("recebiveis_concorrentes", 12)
sem["pix_share"] = sem["tpv_pix"] / sem["tpv_total"].replace(0, np.nan)
sem["pix_share_4s"] = G()["pix_share"].transform(lambda s: s.rolling(4, min_periods=1).mean())
sem["pix_share_12s"] = G()["pix_share"].transform(lambda s: s.rolling(12, min_periods=1).mean())
sem["share_ped_4s"] = G()["share_pedregulho"].transform(lambda s: s.rolling(4, min_periods=1).mean())
sem["share_ped_12s"] = G()["share_pedregulho"].transform(lambda s: s.rolling(12, min_periods=1).mean())
sem["dias_venda_4s"] = roll("dias_com_venda", 4, "sum")
sem["receita_mdr_12s"] = roll("receita_mdr", 12)
sem["retencao_4s"] = roll("retencao_semana", 4, "sum")
sem["parcela_4s"] = roll("parcela_esperada_semana", 4, "sum")
sem["semanas_historico"] = G().cumcount() + 1

eps = 1.0
feat = pd.DataFrame({
    "id_lojista": sem["id_lojista"],
    "semana": sem["semana"],
    "segmento": sem["segmento"],
    "porte": sem["porte"],
    "regiao": sem["regiao"],
    "razao_tpv_cartao_1s_12s": sem["tpv_cartao"] / (sem["tpv_cartao_12s"] + eps),
    "razao_tpv_cartao_4s_12s": sem["tpv_cartao_4s"] / (sem["tpv_cartao_12s"] + eps),
    "razao_tpv_total_4s_12s": sem["tpv_total_4s"] / (sem["tpv_total_12s"] + eps),
    "razao_concorrentes_4s_12s": (sem["conc_4s"] + eps) / (sem["conc_12s"] + eps),
    "share_pedregulho_4s": sem["share_ped_4s"],
    "delta_share_pedregulho": sem["share_ped_4s"] - sem["share_ped_12s"],
    "pix_share_4s": sem["pix_share_4s"],
    "delta_pix_share": sem["pix_share_4s"] - sem["pix_share_12s"],
    "dias_com_venda_4s": sem["dias_venda_4s"],
    "log_tpv_cartao_12s": np.log1p(sem["tpv_cartao_12s"]),
    "tem_credito": (sem["saldo_devedor"] > 0).astype(int),
    "alavancagem": sem["saldo_devedor"] / (sem["tpv_cartao_12s"] * 4 + eps),
    "dias_atraso_atual": sem["max_dias_atraso"],
    "cobertura_retencao_4s": (sem["retencao_4s"] / sem["parcela_4s"].replace(0, np.nan)).fillna(1.0).clip(0, 3),
    "semanas_historico": sem["semanas_historico"],
})

# Rótulos
fut_tpv = fut("tpv_cartao", 8)
sem["max_atraso_fut"] = fut("max_dias_atraso", 13, "max")
feat["y_vazamento"] = np.where(fut_tpv.isna(), np.nan, (fut_tpv < 0.5 * sem["tpv_cartao_12s"]).astype(float))
# Alerta antecipado: só prevemos eventos NOVOS (quem já vazou ou já está em atraso relevante não conta)
elegivel_vaz = (sem["tpv_cartao_12s"] > 300) & (sem["semanas_historico"] >= 8) & (feat["razao_tpv_cartao_4s_12s"] >= 0.7)
elegivel_inad = (sem["saldo_devedor"] > 0) & (sem["max_dias_atraso"] <= 15) & (sem["semanas_historico"] >= 4)
feat["y_inadimplencia"] = np.where(sem["max_atraso_fut"].isna(), np.nan, (sem["max_atraso_fut"] > 60).astype(float))
feat["elegivel_vaz"] = elegivel_vaz
feat["elegivel_inad"] = elegivel_inad
feat["saldo_devedor"] = sem["saldo_devedor"]
feat["receita_mdr_12s"] = sem["receita_mdr_12s"]

CAT = ["segmento", "porte", "regiao"]
NUM = [c for c in feat.columns if c.startswith(("razao_", "share_", "delta_", "pix_", "dias_", "log_", "tem_", "alavancagem", "cobertura_", "semanas_"))]
FEATURES = CAT + NUM
print("features:", FEATURES)

# COMMAND ----------

# MAGIC %md ## Treino out-of-time + backtest

# COMMAND ----------


def novo_modelo():
    pre = ColumnTransformer(
        [("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT)],
        remainder="passthrough", verbose_feature_names_out=False,
    )
    clf = HistGradientBoostingClassifier(
        max_iter=250, learning_rate=0.06, max_leaf_nodes=31, l2_regularization=1.0,
        categorical_features=list(range(len(CAT))), random_state=7,
    )
    return Pipeline([("pre", pre), ("clf", clf)])


def precision_at(y, p, frac=0.05):
    k = max(1, int(len(p) * frac))
    idx = np.argsort(-p)[:k]
    return float(np.mean(np.asarray(y)[idx]))


semanas_rotuladas = sorted(feat.loc[feat["y_vazamento"].notna(), "semana"].unique())
corte = semanas_rotuladas[-12]  # últimas 12 semanas rotuladas = teste (futuro)
resultados = {}

for nome, y_col, eleg in [("vazamento", "y_vazamento", "elegivel_vaz"), ("inadimplencia", "y_inadimplencia", "elegivel_inad")]:
    base = feat[feat[eleg] & feat[y_col].notna()]
    treino, teste = base[base["semana"] < corte], base[base["semana"] >= corte]
    with mlflow.start_run(run_name=f"oot_{nome}") as run:
        m = novo_modelo().fit(treino[FEATURES], treino[y_col])
        p = m.predict_proba(teste[FEATURES])[:, 1]
        met = {
            "auc_oot": roc_auc_score(teste[y_col], p),
            "pr_auc_oot": average_precision_score(teste[y_col], p),
            "precisao_top5pct": precision_at(teste[y_col], p, 0.05),
            "taxa_base": float(teste[y_col].mean()),
            "n_treino": len(treino), "n_teste": len(teste),
        }
        mlflow.log_params({"corte_oot": str(pd.Timestamp(corte).date()), "features": ",".join(FEATURES)})
        mlflow.log_metrics(met)
        amostra = teste.sample(min(8000, len(teste)), random_state=1)
        imp = permutation_importance(m, amostra[FEATURES], amostra[y_col], scoring="roc_auc", n_repeats=2, random_state=1)
        imp_df = pd.DataFrame({"modelo": nome, "variavel": FEATURES, "importancia": imp.importances_mean}).sort_values("importancia", ascending=False)
        mlflow.log_table(imp_df, "importancia_variaveis.json")
        resultados[nome] = {"metricas": met, "teste": teste.assign(prob=p), "importancia": imp_df}
        print(nome, {k: round(v, 3) if isinstance(v, float) else v for k, v in met.items()})

# COMMAND ----------

# Backtest de negócio — "se o radar estivesse ligado nas últimas 12 semanas"
# Evento novo no período de teste; conta como detectado se houve alerta entre 1 e 13 semanas ANTES do evento.
bt = []
saldo_sem = sem.set_index(["id_lojista", "semana"])["saldo_devedor"]
receita_sem = sem.set_index(["id_lojista", "semana"])["receita_mdr_12s"]
for nome, alvo in [("inadimplencia", "inadimplência (>60 dias)"), ("vazamento", "vazamento de TPV (>50%)")]:
    t = resultados[nome]["teste"]
    thr = float(np.quantile(t["prob"], 0.85))
    if nome == "inadimplencia":
        evento = sem[sem["max_dias_atraso"] > 60].groupby("id_lojista")["semana"].min()
    else:
        queda = sem["tpv_cartao_4s"] < 0.5 * G()["tpv_cartao_12s"].shift(4)
        evento = sem[queda].groupby("id_lojista")["semana"].min()
    evento = evento[(evento >= corte + pd.Timedelta(weeks=2)) & evento.index.isin(t["id_lojista"])]
    alertas = t[t["prob"] >= thr][["id_lojista", "semana"]].merge(evento.rename("evento").reset_index(), on="id_lojista")
    alertas = alertas[(alertas["semana"] <= alertas["evento"] - pd.Timedelta(weeks=1)) &
                      (alertas["semana"] >= alertas["evento"] - pd.Timedelta(weeks=13))]
    primeiro = alertas.groupby("id_lojista").agg(primeiro_sinal=("semana", "min"), evento=("evento", "first"))
    antecedencia = (primeiro["evento"] - primeiro["primeiro_sinal"]).dt.days
    # valor exposto 4 semanas antes do evento (saldo devedor ou receita anual de MDR)
    ref_sem = evento - pd.Timedelta(weeks=4)
    serie = saldo_sem if nome == "inadimplencia" else receita_sem * 52
    valor = pd.Series([serie.get((i, s_), 0.0) for i, s_ in ref_sem.items()], index=ref_sem.index).fillna(0)
    pct_valor = float(valor.reindex(primeiro.index).sum() / max(1.0, valor.sum()))
    desc_valor = ("% do saldo devedor dos contratos que viraram inadimplência e foram sinalizados antes" if nome == "inadimplencia"
                  else "% da receita anual de MDR dos lojistas que vazaram e foram sinalizados antes")
    bt += [
        {"modelo": nome, "metrica": "eventos_no_periodo", "valor": float(len(evento)), "descricao": f"Novos casos de {alvo} no período de teste"},
        {"modelo": nome, "metrica": "recall_antecipado", "valor": len(primeiro) / max(1, len(evento)), "descricao": f"% dos casos de {alvo} sinalizados 1 a 13 semanas antes (top 15% de risco)"},
        {"modelo": nome, "metrica": "antecedencia_mediana_dias", "valor": float(antecedencia.median()) if len(antecedencia) else 0.0, "descricao": "Mediana de dias entre o primeiro alerta e o evento"},
        {"modelo": nome, "metrica": "pct_valor_sinalizado", "valor": pct_valor, "descricao": desc_valor},
        {"modelo": nome, "metrica": "valor_exposto_eventos", "valor": float(valor.sum()), "descricao": "R$ expostos nos casos do período (saldo devedor ou receita anual de MDR)"},
        {"modelo": nome, "metrica": "auc_oot", "valor": resultados[nome]["metricas"]["auc_oot"], "descricao": "AUC fora do tempo (treino no passado, teste no futuro)"},
        {"modelo": nome, "metrica": "precisao_top5pct", "valor": resultados[nome]["metricas"]["precisao_top5pct"], "descricao": "Precisão nos 5% de maior risco"},
        {"modelo": nome, "metrica": "taxa_base", "valor": resultados[nome]["metricas"]["taxa_base"], "descricao": "Taxa de eventos na população elegível (referência para a precisão)"},
    ]
bt_df = pd.DataFrame(bt)
display(bt_df)
spark.createDataFrame(bt_df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.backtest_radar")
spark.createDataFrame(pd.concat([r["importancia"] for r in resultados.values()])).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.importancia_variaveis")

# COMMAND ----------

# MAGIC %md ## Modelos finais (todo o histórico) → Unity Catalog → pontuação da semana

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()
atual = feat[feat["semana"] == ultima_semana].copy()
for nome, y_col, eleg in [("vazamento", "y_vazamento", "elegivel_vaz"), ("inadimplencia", "y_inadimplencia", "elegivel_inad")]:
    base = feat[feat[eleg] & feat[y_col].notna()]
    with mlflow.start_run(run_name=f"final_{nome}"):
        m = novo_modelo().fit(base[FEATURES], base[y_col])
        mlflow.log_metrics({f"oot_{k}": v for k, v in resultados[nome]["metricas"].items()})
        info = mlflow.sklearn.log_model(
            m, name="modelo", registered_model_name=f"{FQ}.modelo_{nome}",
            signature=infer_signature(base[FEATURES].head(50), m.predict_proba(base[FEATURES].head(50))[:, 1]),
            input_example=base[FEATURES].head(3),
        )
        client.set_registered_model_alias(f"{FQ}.modelo_{nome}", "champion", info.registered_model_version)
    atual[f"prob_{nome}"] = m.predict_proba(atual[FEATURES])[:, 1]

atual.loc[atual["tem_credito"] == 0, "prob_inadimplencia"] = np.nan
atual = atual[sem.loc[sem["semana"] == ultima_semana, "tpv_cartao_12s"].values > 100]

# COMMAND ----------

# MAGIC %md ## Sinais explicáveis para o agente de campo

# COMMAND ----------

s_atual = sem[sem["semana"] == ultima_semana].set_index("id_lojista")
atual = atual.set_index("id_lojista")
s_atual = s_atual.reindex(atual.index)

atd = spark.sql(f"""
  SELECT id_lojista,
    max_by(concorrente_citado, data_hora) FILTER (WHERE concorrente_citado IS NOT NULL AND lower(concorrente_citado) <> 'null') AS concorrente_citado_atendimento,
    max_by(resumo, data_hora) AS ultimo_atendimento_resumo,
    max_by(motivo, data_hora) AS ultimo_atendimento_motivo,
    count(*) FILTER (WHERE risco_cancelamento = 'Alto') AS atendimentos_risco_alto
  FROM {FQ}.gold_atendimentos_sinais
  WHERE data_hora >= date_sub(DATE'{DATA_REF}', 60)
  GROUP BY id_lojista
""").toPandas().set_index("id_lojista")
atual = atual.join(atd, how="left")


def brl(v):
    return "R$ " + f"{v:,.0f}".replace(",", ".")


def sinais(r, s):
    out = []
    queda = 1 - r["razao_tpv_cartao_4s_12s"]
    if queda > 0.2:
        out.append(f"TPV de cartão caiu {queda:.0%} nas últimas 4 semanas vs média de 12 semanas")
    if r["razao_concorrentes_4s_12s"] > 1.2 and s["recebiveis_concorrentes"] > 200:
        conc = s["principal_concorrente"] or "concorrente"
        out.append(f"Registradora: recebíveis em {conc} subiram {r['razao_concorrentes_4s_12s'] - 1:.0%} (agora {brl(s['recebiveis_concorrentes'])}/semana)")
    if r["delta_share_pedregulho"] < -0.08:
        out.append(f"Share da Pedregulho nos recebíveis do lojista caiu {abs(r['delta_share_pedregulho']) * 100:.0f} p.p.")
    if r["delta_pix_share"] > 0.06:
        out.append(f"Pix ganhou {r['delta_pix_share'] * 100:.0f} p.p. de participação (fora da trava de recebíveis)")
    if r["razao_tpv_total_4s_12s"] < 0.75 and r["razao_concorrentes_4s_12s"] <= 1.05:
        out.append(f"Faturamento total do negócio caiu {1 - r['razao_tpv_total_4s_12s']:.0%} (inclusive nos concorrentes)")
    if r["tem_credito"] and r["cobertura_retencao_4s"] < 0.8:
        out.append(f"Trava de recebíveis cobre só {r['cobertura_retencao_4s']:.0%} da parcela esperada")
    if r["dias_atraso_atual"] > 0:
        out.append(f"Contrato com {int(r['dias_atraso_atual'])} dias de atraso")
    if isinstance(r.get("concorrente_citado_atendimento"), str):
        out.append(f"Em atendimento recente, citou proposta da {r['concorrente_citado_atendimento']}")
    return out[:5]


def tipo(r):
    if r["razao_concorrentes_4s_12s"] > 1.15 and (r["razao_tpv_cartao_4s_12s"] < 0.8 or r["delta_share_pedregulho"] < -0.05):
        return "Desvio para concorrente"
    if r["razao_tpv_total_4s_12s"] < 0.8 and r["razao_concorrentes_4s_12s"] <= 1.05:
        return "Queda de faturamento"
    if r["delta_pix_share"] > 0.06 and r["razao_tpv_cartao_4s_12s"] < 0.9:
        return "Migração para Pix"
    if r["tem_credito"] and r["cobertura_retencao_4s"] < 0.8:
        return "Retenção insuficiente"
    return "Sinais combinados"


atual["sinais"] = [sinais(r, s_atual.loc[i]) for i, r in atual.iterrows()]
atual["tipo_sinal"] = atual.apply(tipo, axis=1)
atual["principal_concorrente"] = s_atual["principal_concorrente"]
atual["recebiveis_concorrentes_semana"] = s_atual["recebiveis_concorrentes"]
atual["tpv_cartao_semana"] = s_atual["tpv_cartao"]
atual["tpv_cartao_media_12s"] = s_atual["tpv_cartao_12s"]
atual["receita_mdr_anual"] = atual["receita_mdr_12s"] * 52
atual["valor_risco_credito"] = (atual["prob_inadimplencia"].fillna(0) * atual["saldo_devedor"]).round(2)
atual["valor_risco_receita"] = (atual["prob_vazamento"] * atual["receita_mdr_anual"]).round(2)
atual["valor_em_risco"] = atual["valor_risco_credito"] + atual["valor_risco_receita"]
pmax = atual[["prob_vazamento", "prob_inadimplencia"]].max(axis=1)
atual["faixa_risco"] = np.select(
    [atual["dias_atraso_atual"] > 60, pmax >= 0.5, pmax >= 0.25, pmax >= 0.12],
    ["Em cobrança", "Crítico", "Alto", "Médio"], "Baixo")

cols = ["semana", "prob_vazamento", "prob_inadimplencia", "faixa_risco", "tipo_sinal", "sinais",
        "valor_em_risco", "valor_risco_credito", "valor_risco_receita", "saldo_devedor", "receita_mdr_anual",
        "dias_atraso_atual", "cobertura_retencao_4s", "razao_tpv_cartao_4s_12s", "delta_share_pedregulho", "delta_pix_share",
        "tpv_cartao_semana", "tpv_cartao_media_12s", "principal_concorrente", "recebiveis_concorrentes_semana",
        "concorrente_citado_atendimento", "ultimo_atendimento_resumo", "ultimo_atendimento_motivo", "atendimentos_risco_alto"]
out = atual[cols].reset_index().rename(columns={"semana": "semana_referencia"})
out["semana_referencia"] = out["semana_referencia"].dt.date
out["atendimentos_risco_alto"] = out["atendimentos_risco_alto"].fillna(0).astype(int)
(spark.createDataFrame(out).write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{FQ}.scores_lojistas"))
spark.sql(f"COMMENT ON TABLE {FQ}.scores_lojistas IS 'Pontuação semanal do Radar: probabilidade de vazamento de TPV (8 semanas) e de inadimplência >60 dias (13 semanas), valor em risco em R$ e sinais explicáveis. Uma linha por lojista ativo.'")
print(out["faixa_risco"].value_counts().to_dict())
print("Valor em risco total R$", round(out["valor_em_risco"].sum() / 1e6, 2), "mi")
