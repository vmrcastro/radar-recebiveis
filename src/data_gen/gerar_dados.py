# Databricks notebook source
# MAGIC %md
# MAGIC # Gerador de dados sintéticos — Pedregulho Pagamentos
# MAGIC
# MAGIC Simula os arquivos brutos que chegam ao lakehouse de uma credenciadora focada em micro, pequenos e médios lojistas:
# MAGIC
# MAGIC | Fonte (landing) | Formato | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | `cadastro_lojistas/` | CSV | Cadastro do lojista (com PII: CNPJ, CPF do titular, telefone, e-mail) |
# MAGIC | `vendas_diarias/` | Parquet | Vendas diárias por lojista: crédito, débito e Pix recebido na conta |
# MAGIC | `registradora/` | CSV | Agenda de recebíveis de cartão por credenciadora (visão da registradora, chave = CNPJ) |
# MAGIC | `contratos_credito/` | CSV | Contratos de capital de giro pagos com trava de % dos recebíveis de cartão |
# MAGIC | `atendimentos/` | JSON | Textos livres de atendimento (chat, WhatsApp, telefone) |
# MAGIC
# MAGIC Os **cenários comportamentais** (desvio para concorrente, estresse financeiro, fechamento) só determinam o *fluxo de vendas*.
# MAGIC Atraso e inadimplência **emergem** desse fluxo no pipeline (trava de recebíveis × saldo devedor) — não há rótulo fixo no gerador.

# COMMAND ----------

dbutils.widgets.text("catalog", "vini_catalog")
dbutils.widgets.text("schema", "radar_recebiveis")
dbutils.widgets.text("n_lojistas", "15000")
dbutils.widgets.text("data_referencia", "2026-10-08")
dbutils.widgets.text("seed", "42")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
N = int(dbutils.widgets.get("n_lojistas"))
DATA_REF = dbutils.widgets.get("data_referencia")
SEED = int(dbutils.widgets.get("seed"))

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# COMMAND ----------

import json
import os
import shutil

import numpy as np
import pandas as pd

rng = np.random.default_rng(SEED)

ref = pd.Timestamp(DATA_REF)
inicio = (ref - pd.DateOffset(months=12)).replace(day=1)
dias = pd.date_range(inicio, ref, freq="D")
D = len(dias)
dow = dias.dayofweek.values  # 0 = segunda
mes = dias.month.values

# Recomeça do zero a cada execução (idempotente)
if os.path.exists(LANDING):
    for sub in os.listdir(LANDING):
        shutil.rmtree(f"{LANDING}/{sub}", ignore_errors=True)
for sub in ["cadastro_lojistas", "vendas_diarias", "registradora", "contratos_credito", "atendimentos"]:
    os.makedirs(f"{LANDING}/{sub}", exist_ok=True)

print(f"{N} lojistas, {D} dias ({dias[0].date()} a {dias[-1].date()}) → {LANDING}")

# COMMAND ----------

# MAGIC %md ## 1. Cadastro de lojistas

# COMMAND ----------

# segmento: (peso, ticket médio R$, fator de TPV, padrão semanal seg..dom, fator dezembro)
SEGMENTOS = {
    "Restaurantes e Bares":       (0.17, 58,  1.0, [0.8, 0.85, 0.9, 1.0, 1.25, 1.45, 1.05], 1.15),
    "Mercados e Mercearias":      (0.15, 72,  1.4, [0.95, 0.9, 0.95, 1.0, 1.1, 1.3, 0.8], 1.20),
    "Vestuário e Calçados":       (0.11, 165, 0.9, [0.9, 0.9, 0.95, 1.0, 1.15, 1.4, 0.4], 1.55),
    "Farmácias":                  (0.08, 64,  1.2, [1.05, 1.0, 1.0, 1.0, 1.05, 1.0, 0.75], 1.05),
    "Salões e Estética":          (0.09, 95,  0.6, [0.6, 0.95, 1.0, 1.05, 1.2, 1.45, 0.2], 1.25),
    "Autopeças e Oficinas":       (0.08, 240, 1.1, [1.1, 1.05, 1.05, 1.05, 1.05, 0.8, 0.1], 0.95),
    "Materiais de Construção":    (0.07, 310, 1.5, [1.1, 1.05, 1.05, 1.05, 1.05, 0.95, 0.15], 0.90),
    "Padarias e Cafés":           (0.10, 28,  0.7, [1.0, 0.95, 0.95, 0.95, 1.0, 1.2, 1.1], 1.10),
    "Pet Shops":                  (0.06, 110, 0.7, [0.95, 0.95, 1.0, 1.0, 1.1, 1.3, 0.45], 1.10),
    "Postos de Combustível":      (0.04, 150, 2.5, [1.0, 1.0, 1.0, 1.0, 1.1, 1.05, 0.85], 1.05),
    "Eletrônicos e Celulares":    (0.05, 380, 1.0, [0.95, 0.95, 1.0, 1.0, 1.1, 1.35, 0.35], 1.60),
}
seg_nomes = list(SEGMENTOS)
seg_pesos = np.array([v[0] for v in SEGMENTOS.values()])
seg_idx = rng.choice(len(seg_nomes), N, p=seg_pesos / seg_pesos.sum())

# UF / cidade / hub de atendimento em campo
GEO = [
    # (uf, cidade, região, peso, hubs)
    ("SP", "São Paulo", "Sudeste", 0.20, ["Hub SP Centro", "Hub SP Zona Leste", "Hub SP Zona Sul", "Hub SP Zona Norte"]),
    ("SP", "Campinas", "Sudeste", 0.05, ["Hub Campinas"]),
    ("SP", "Ribeirão Preto", "Sudeste", 0.03, ["Hub Ribeirão Preto"]),
    ("RJ", "Rio de Janeiro", "Sudeste", 0.10, ["Hub RJ Centro", "Hub RJ Barra", "Hub RJ Zona Norte"]),
    ("MG", "Belo Horizonte", "Sudeste", 0.08, ["Hub BH Savassi", "Hub BH Contagem"]),
    ("PR", "Curitiba", "Sul", 0.05, ["Hub Curitiba"]),
    ("RS", "Porto Alegre", "Sul", 0.05, ["Hub Porto Alegre"]),
    ("SC", "Florianópolis", "Sul", 0.03, ["Hub Florianópolis"]),
    ("BA", "Salvador", "Nordeste", 0.06, ["Hub Salvador"]),
    ("PE", "Recife", "Nordeste", 0.05, ["Hub Recife"]),
    ("CE", "Fortaleza", "Nordeste", 0.05, ["Hub Fortaleza"]),
    ("GO", "Goiânia", "Centro-Oeste", 0.05, ["Hub Goiânia"]),
    ("DF", "Brasília", "Centro-Oeste", 0.05, ["Hub Brasília"]),
    ("AM", "Manaus", "Norte", 0.03, ["Hub Manaus"]),
    ("PA", "Belém", "Norte", 0.02, ["Hub Belém"]),
]
geo_pesos = np.array([g[3] for g in GEO])
geo_idx = rng.choice(len(GEO), N, p=geo_pesos / geo_pesos.sum())
hub = np.array([rng.choice(GEO[g][4]) for g in geo_idx])

PRENOMES = ["Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isabela", "João", "Karina",
            "Lucas", "Mariana", "Nicolas", "Olívia", "Paulo", "Rafaela", "Sérgio", "Tatiane", "Vinícius", "Wesley",
            "Juliana", "Marcos", "Patrícia", "Ricardo", "Aline", "Thiago", "Fernanda", "Rodrigo", "Camila", "Jéssica"]
SOBRENOMES = ["Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira", "Lima", "Gomes",
              "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida", "Lopes", "Soares", "Fernandes", "Vieira", "Barbosa",
              "Rocha", "Dias", "Nascimento", "Andrade", "Moreira", "Nunes", "Marques", "Machado", "Mendes", "Freitas"]
NOME_FANTASIA = {
    "Restaurantes e Bares": ["Sabor de Casa", "Boteco do {s}", "Cantina {s}", "Bistrô Aurora", "Espetinho do {p}", "Bar da Esquina"],
    "Mercados e Mercearias": ["Mercadinho {s}", "Empório Bom Preço", "Mercearia São Jorge", "Mini Mercado {p}", "Hortifruti Verde Vida"],
    "Vestuário e Calçados": ["Boutique {p}", "Moda & Estilo", "Calçados {s}", "Ateliê Bela Moda", "Look Certo"],
    "Farmácias": ["Drogaria {s}", "Farma Vida", "Farmácia Popular do Bairro", "Drogaria Saúde Já"],
    "Salões e Estética": ["Studio {p} Beauty", "Salão Charme", "Espaço Estética {p}", "Barbearia Navalha de Ouro"],
    "Autopeças e Oficinas": ["Auto Center {s}", "Oficina do {p}", "Autopeças Rota", "Mecânica Pit Stop"],
    "Materiais de Construção": ["Casa & Construção {s}", "Depósito Bom Tijolo", "Materiais {s}", "Constrular"],
    "Padarias e Cafés": ["Padaria Pão Dourado", "Panificadora {s}", "Café do {p}", "Doce Trigo"],
    "Pet Shops": ["Pet Feliz", "Mundo Animal {s}", "Pet Shop Patinhas", "Bicho Mimado"],
    "Postos de Combustível": ["Auto Posto {s}", "Posto Estrela", "Posto Rota Sul"],
    "Eletrônicos e Celulares": ["Cell Center {p}", "Eletro {s}", "Tech Point", "Conecta Celulares"],
}


def cnpj_dv(base12):
    def dv(nums, pesos):
        s = sum(n * p for n, p in zip(nums, pesos)) % 11
        return 0 if s < 2 else 11 - s
    d1 = dv(base12, [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    d2 = dv(base12 + [d1], [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return base12 + [d1, d2]


def cpf_dv(base9):
    def dv(nums):
        s = sum(n * p for n, p in zip(nums, range(len(nums) + 1, 1, -1))) % 11
        return 0 if s < 2 else 11 - s
    d1 = dv(base9)
    return base9 + [d1, dv(base9 + [d1])]


def fmt_cnpj(d):
    s = "".join(map(str, d))
    return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:]}"


def fmt_cpf(d):
    s = "".join(map(str, d))
    return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"


porte = rng.choice(["Micro", "Pequeno", "Médio"], N, p=[0.55, 0.33, 0.12])
plano = np.where(porte == "Micro", "Pedrinha", "Pedregulho Pro")
tpv_base_porte = {"Micro": 450, "Pequeno": 3200, "Médio": 18000}

lojistas, raizes_usadas = [], set()
for i in range(N):
    p, s = rng.choice(PRENOMES), rng.choice(SOBRENOMES)
    seg = seg_nomes[seg_idx[i]]
    fantasia = rng.choice(NOME_FANTASIA[seg]).format(p=p, s=s)
    # CNPJ único: em caso de colisão da raiz sorteada, avança para a próxima livre (sem consumir o gerador aleatório)
    raiz = int("".join(map(str, rng.integers(0, 10, 8))))
    while raiz in raizes_usadas:
        raiz = (raiz + 1) % 100_000_000
    raizes_usadas.add(raiz)
    cnpj = fmt_cnpj(cnpj_dv([int(c) for c in f"{raiz:08d}"] + [0, 0, 0, 1]))
    lojistas.append({
        "id_lojista": f"LJ{i + 1:06d}",
        "cnpj": cnpj,
        "razao_social": f"{fantasia} {'MEI' if porte[i] == 'Micro' else 'LTDA'}".upper(),
        "nome_fantasia": fantasia,
        "nome_titular": f"{p} {s} {rng.choice(SOBRENOMES)}",
        "cpf_titular": fmt_cpf(cpf_dv(list(rng.integers(0, 10, 9)))),
        "telefone": f"({rng.integers(11, 99)}) 9{rng.integers(1000, 9999)}-{rng.integers(1000, 9999)}",
        "email": f"{p.lower()}.{s.lower()}{rng.integers(1, 999)}@exemplo.com.br",
        "segmento": seg,
        "uf": GEO[geo_idx[i]][0],
        "cidade": GEO[geo_idx[i]][1],
        "regiao": GEO[geo_idx[i]][2],
        "hub": hub[i],
        "porte": porte[i],
        "plano": plano[i],
    })
cad = pd.DataFrame(lojistas)
assert cad["cnpj"].is_unique and cad["id_lojista"].is_unique

# Data de credenciamento: 85% já eram clientes antes da janela, 15% entraram durante o ano
antigos = rng.random(N) < 0.85
dia_entrada = np.where(antigos, 0, rng.integers(0, D - 60, N))
data_cred = np.where(
    antigos,
    (pd.Timestamp(inicio) - pd.to_timedelta(rng.integers(30, 2200, N), unit="D")).values,
    dias[dia_entrada].values,
)
cad["data_credenciamento"] = pd.to_datetime(data_cred).date
cad["usa_conta_digital"] = rng.random(N) < np.where(porte == "Micro", 0.55, 0.75)

# Agente de campo responsável (carteira por hub)
agentes = {h: [f"{rng.choice(PRENOMES)} {rng.choice(SOBRENOMES)}" for _ in range(3)] for h in np.unique(hub)}
cad["agente_responsavel"] = [rng.choice(agentes[h]) for h in hub]

# COMMAND ----------

# MAGIC %md ## 2. Cenários comportamentais e vendas diárias

# COMMAND ----------

# Cenário latente (NUNCA sai do gerador — o modelo precisa descobrir pelos sinais)
cen = rng.choice(["normal", "desvio", "estresse", "fechamento"], N, p=[0.855, 0.10, 0.035, 0.01])
ini_cen = rng.integers(75, D - 5, N)          # dia em que o cenário começa
rampa = rng.integers(20, 80, N)               # dias até atingir o patamar
piso = np.where(cen == "desvio", rng.uniform(0.05, 0.45, N),
        np.where(cen == "estresse", rng.uniform(0.30, 0.65, N), 0.0))

t = np.arange(D)[None, :]
prog = np.clip((t - ini_cen[:, None]) / rampa[:, None], 0, 1)
fator_cen = 1 - (1 - piso[:, None]) * prog
fator_cen = np.where((cen == "normal")[:, None], 1.0, fator_cen)
# Parte dos casos se recupera sozinha (lojista volta, negócio reage) → nem todo sinal vira perda
recupera = np.isin(cen, ["desvio", "estresse"]) & (rng.random(N) < 0.30)
ini_rec = ini_cen + rampa + rng.integers(14, 60, N)
prog_rec = np.clip((t - ini_rec[:, None]) / 30, 0, 1)
fator_cen = np.where(recupera[:, None], fator_cen + (0.9 - fator_cen) * prog_rec, fator_cen)
# Fechamento: queda abrupta para zero após ~2 semanas
fator_cen = np.where((cen == "fechamento")[:, None], np.where(t < ini_cen[:, None] + 14, 1 - 0.5 * prog, 0.0), fator_cen)

# Desvio afeta o que passa NA PEDREGULHO; estresse/fechamento afetam o negócio inteiro (inclusive concorrentes)
captura_pedregulho = np.where((cen == "desvio")[:, None], fator_cen, 1.0)
saude_negocio = np.where(np.isin(cen, ["estresse", "fechamento"])[:, None], fator_cen, 1.0)

seg_tkt = np.array([SEGMENTOS[seg_nomes[s]][1] for s in seg_idx])
seg_fator = np.array([SEGMENTOS[seg_nomes[s]][2] for s in seg_idx])
seg_dow = np.array([SEGMENTOS[seg_nomes[s]][3] for s in seg_idx])          # N x 7
seg_dez = np.array([SEGMENTOS[seg_nomes[s]][4] for s in seg_idx])

base = np.array([tpv_base_porte[p] for p in porte]) * seg_fator * rng.lognormal(0, 0.55, N)
sazonal = np.ones((N, D))
sazonal = np.where((mes == 12)[None, :], seg_dez[:, None], sazonal)
sazonal = np.where((mes == 1)[None, :], 0.86, sazonal)
sazonal = np.where((mes == 2)[None, :], 0.92, sazonal)
sazonal = np.where((mes == 11)[None, :], 1.06, sazonal)
black_friday = np.asarray(dias == pd.Timestamp("2025-11-28"))
sazonal = np.where(black_friday[None, :], sazonal * 1.7, sazonal)

fecha_domingo = rng.random(N) < 0.35
semanal = seg_dow[:, dow]
semanal = np.where(fecha_domingo[:, None] & (dow == 6)[None, :], 0.0, semanal)
ruido = rng.lognormal(-0.045, 0.30, (N, D))
crescimento = (1 + rng.normal(0.06, 0.10, N))[:, None] ** (t / 365)

ativo = (t >= dia_entrada[:, None])
# Ruído de negócio saudável: quedas temporárias (obra na rua, férias, reforma) que se recuperam
quedas = np.ones((N, D))
for _ in range(2):
    tem = rng.random(N) < 0.18
    ini_q = rng.integers(0, D, N)
    dur_q = rng.integers(10, 42, N)
    prof_q = rng.uniform(0.3, 0.7, N)
    em_queda = tem[:, None] & (t >= ini_q[:, None]) & (t < (ini_q + dur_q)[:, None])
    quedas = np.where(em_queda, quedas * (1 - prof_q[:, None]), quedas)
saude_negocio = saude_negocio * quedas
tpv_total_negocio = base[:, None] * sazonal * semanal * ruido * crescimento * saude_negocio * ativo  # tudo que o lojista vende

# Multi-adquirência: parte do cartão já passava em concorrentes antes de qualquer desvio
share_concorrente_base = np.clip(rng.beta(1.1, 5.5, N), 0, 0.6)
# Pix cresce ao longo do ano (pressão estrutural sobre receita de cartão)
pix_share = np.clip(rng.uniform(0.12, 0.32, N)[:, None] + 0.07 * t / D, 0, 0.6)

cartao_total = tpv_total_negocio * (1 - pix_share)
# Multi-adquirência que deriva devagar (lojista testa concorrente sem sair) + ruído semanal
deriva = np.where((cen == "normal") & (rng.random(N) < 0.12), rng.uniform(0.08, 0.25, N), 0.0)
share_conc_t = np.clip(share_concorrente_base[:, None] + deriva[:, None] * np.clip((t - rng.integers(0, D, N)[:, None]) / 90, 0, 1), 0, 0.8)
cartao_pedregulho = cartao_total * (1 - share_conc_t) * captura_pedregulho
cartao_concorrentes = cartao_total - cartao_pedregulho
pix_pedregulho = tpv_total_negocio * pix_share * np.where((cen == "desvio")[:, None], captura_pedregulho ** 0.7, 1.0) * \
    np.where(cad["usa_conta_digital"].values[:, None], 1.0, 0.35)

share_credito = np.clip(rng.normal(0.62, 0.08, N), 0.3, 0.85)[:, None]
credito = cartao_pedregulho * share_credito
debito = cartao_pedregulho - credito

# COMMAND ----------

mdr_cred = np.where(porte == "Micro", 0.0329, np.where(porte == "Pequeno", 0.0279, 0.0219))
mdr_deb = np.where(porte == "Micro", 0.0149, np.where(porte == "Pequeno", 0.0119, 0.0089))

for m in pd.period_range(dias[0], dias[-1], freq="M"):
    idx = np.where(dias.to_period("M") == m)[0]
    c, d_, p_ = credito[:, idx], debito[:, idx], pix_pedregulho[:, idx]
    tot = c + d_ + p_
    ii, jj = np.nonzero(tot > 1)
    df = pd.DataFrame({
        "id_lojista": cad["id_lojista"].values[ii],
        "data_venda": dias[idx][jj].date,
        "valor_credito": np.round(c[ii, jj], 2),
        "valor_debito": np.round(d_[ii, jj], 2),
        "valor_pix": np.round(p_[ii, jj], 2),
        "qtd_transacoes": np.maximum(1, np.round(tot[ii, jj] / (seg_tkt[ii] * rng.lognormal(0, 0.2, len(ii))))).astype(int),
        "receita_mdr": np.round(c[ii, jj] * mdr_cred[ii] + d_[ii, jj] * mdr_deb[ii] + p_[ii, jj] * 0.0049, 2),
    })
    df.to_parquet(f"{LANDING}/vendas_diarias/vendas_{m}.parquet", index=False)
    print(m, len(df))

# COMMAND ----------

# MAGIC %md ## 3. Registradora de recebíveis (visão por CNPJ e credenciadora)

# COMMAND ----------

CONCORRENTES = ["NuvemPay", "PagaLogo", "Maquininha Azul", "Banco Laranja PJ", "MercaPix"]
# Cada lojista tem 1 concorrente "principal" (destino de desvio) com viés regional
conc_principal = rng.choice(len(CONCORRENTES), N, p=[0.30, 0.25, 0.20, 0.15, 0.10])
conc_principal = np.where((cad["regiao"].values == "Nordeste") & (rng.random(N) < 0.5), 1, conc_principal)
conc_principal = np.where((cad["uf"].values == "SP") & (rng.random(N) < 0.3), 0, conc_principal)
conc_secundario = rng.choice(len(CONCORRENTES), N)

semana = dias - pd.to_timedelta(dow, unit="D")
semanas = semana.unique()
linhas = []
for s in semanas:
    idx = np.where(semana == s)[0]
    ped = cartao_pedregulho[:, idx].sum(1)
    conc = cartao_concorrentes[:, idx].sum(1) * rng.lognormal(0, 0.25, N)
    conc_1 = conc * np.where(share_concorrente_base > 0.15, 0.7, 1.0)
    conc_2 = conc - conc_1
    for nome, val, mask in [
        ("Pedregulho Pagamentos", ped, ped > 1),
        (None, conc_1, conc_1 > 1),
        ("__sec__", conc_2, conc_2 > 1),
    ]:
        ii = np.nonzero(mask)[0]
        if nome is None:
            cred = np.array(CONCORRENTES)[conc_principal[ii]]
        elif nome == "__sec__":
            cred = np.array(CONCORRENTES)[conc_secundario[ii]]
        else:
            cred = np.full(len(ii), nome)
        linhas.append(pd.DataFrame({
            "cnpj": cad["cnpj"].values[ii],
            "semana_referencia": pd.Timestamp(s).date(),
            "credenciadora": cred,
            "valor_recebiveis": np.round(val[ii], 2),
        }))
reg = pd.concat(linhas)
reg = reg.groupby(["cnpj", "semana_referencia", "credenciadora"], as_index=False)["valor_recebiveis"].sum()
reg["data_arquivo"] = pd.to_datetime(reg["semana_referencia"]) + pd.Timedelta(days=7)
for m, g in reg.groupby(pd.to_datetime(reg["semana_referencia"]).dt.to_period("M")):
    g.to_csv(f"{LANDING}/registradora/agenda_{m}.csv", index=False, sep=";", decimal=",")
print("registradora:", len(reg))

# COMMAND ----------

# MAGIC %md ## 4. Contratos de capital de giro (pagos via trava de % dos recebíveis de cartão)

# COMMAND ----------

# Seleção adversa: quem vai entrar em desvio/estresse tende a ter tomado crédito antes
p_credito = np.select([cen == "desvio", cen == "estresse", cen == "fechamento"], [0.40, 0.45, 0.45], 0.30)
tem_credito = (rng.random(N) < p_credito) & (porte != "Médio") | (rng.random(N) < 0.12)
dia_contrato = np.where(
    cen == "normal",
    rng.integers(0, D - 30, N),
    np.clip(ini_cen - rng.integers(10, 150, N), 0, D - 30),
)
dia_contrato = np.maximum(dia_contrato, dia_entrada + 30)
tem_credito &= dia_contrato < D - 20

contratos = []
cid = 0
for i in np.nonzero(tem_credito)[0]:
    c0 = dia_contrato[i]
    janela = slice(max(0, c0 - 60), c0)
    media_cartao = float((cartao_pedregulho[i, janela]).mean()) if c0 > 0 else float(base[i] * 0.7)
    if media_cartao < 30:
        continue
    prazo_meses = int(rng.choice([6, 9, 12, 18], p=[0.25, 0.30, 0.35, 0.10]))
    taxa = float(rng.uniform(0.029, 0.059))
    principal = round(float(media_cartao * rng.uniform(18, 55)), -2)
    if principal < 500:
        continue
    total = principal * (1 + taxa) ** prazo_meses
    pct = float(np.clip(total / (media_cartao * prazo_meses * 30) * 1.12, 0.04, 0.35))
    cid += 1
    contratos.append({
        "id_contrato": f"CG{cid:07d}",
        "id_lojista": cad["id_lojista"].values[i],
        "produto": "Capital de Giro Pedregulho",
        "data_contratacao": dias[c0].date(),
        "valor_principal": principal,
        "taxa_juros_mes": round(taxa, 4),
        "prazo_meses": prazo_meses,
        "valor_total_devido": round(total, 2),
        "pct_retencao_recebiveis": round(pct, 4),
    })
ctr = pd.DataFrame(contratos)
ctr.to_csv(f"{LANDING}/contratos_credito/contratos.csv", index=False)
print("contratos:", len(ctr), "carteira contratada R$", round(ctr["valor_principal"].sum() / 1e6, 1), "mi")

# COMMAND ----------

# MAGIC %md ## 5. Atendimentos (texto livre — sinal não estruturado para a GenAI)

# COMMAND ----------

TEMAS_NEUTROS = [
    "Bom dia, a maquininha está pedindo atualização e não conclui. Podem ajudar?",
    "Quero entender a diferença entre o valor bruto e o líquido no extrato de ontem.",
    "Como faço para cadastrar uma chave Pix nova na conta?",
    "Preciso da segunda via do comprovante de uma venda de sábado.",
    "A bobina acabou, vocês enviam mais ou preciso comprar?",
    "Gostaria de saber quando cai o dinheiro das vendas no débito.",
    "Estou muito satisfeito com o atendimento do agente {agente}, resolveu rápido.",
    "Queria aumentar o limite do capital de giro, minhas vendas cresceram.",
    "A maquininha está sem sinal no fundo da loja, tem chip de outra operadora?",
    "Como emito boleto pela conta digital?",
]
TEMAS_DESVIO = [
    "Recebi proposta da {conc} com taxa de {taxa}% no crédito. Vocês cobrem?",
    "O pessoal da {conc} passou aqui oferecendo maquininha de graça e antecipação mais barata. Qual a condição de vocês?",
    "Estou pensando em cancelar, a {conc} me ofereceu D+0 sem custo.",
    "Minha taxa está alta comparada com a da {conc}. Se não baixar vou trocar.",
    "Quero saber como faço para cancelar a maquininha, vou ficar só com a {conc}.",
    "Comecei a usar a {conc} em paralelo, ela cobra menos no débito. Vocês têm alguma condição melhor?",
    "Meu contador falou para eu concentrar tudo na {conc} por causa da taxa. Tem como negociar?",
]
TEMAS_ESTRESSE = [
    "O movimento caiu muito nos últimos meses, está difícil pagar as parcelas do empréstimo.",
    "Preciso renegociar o capital de giro, as vendas não estão cobrindo.",
    "Vou ter que fechar a loja por umas semanas para reforma, como fica a retenção?",
    "Estou com dificuldade de caixa, consigo pausar a retenção dos recebíveis?",
    "A obra na rua derrubou meu movimento pela metade. Tem alguma carência?",
    "Pensando em encerrar as atividades, como faço para quitar o que devo?",
]
CANAIS = ["Chat no app", "WhatsApp", "Telefone", "Visita do agente"]

tickets = []
tid = 0
nomes_conc = np.array(CONCORRENTES)
for i in range(N):
    # base: ~0,10 atendimento/mês
    n_base = rng.poisson(0.10 * 12)
    dias_base = rng.integers(max(0, dia_entrada[i]), D, n_base)
    textos = [(d, rng.choice(TEMAS_NEUTROS)) for d in dias_base]
    if cen[i] == "desvio":
        k = rng.poisson(1.6)
        for d in rng.integers(max(0, ini_cen[i] - 30), min(D, ini_cen[i] + 45), k):
            textos.append((d, rng.choice(TEMAS_DESVIO)))
    elif cen[i] in ("estresse", "fechamento"):
        k = rng.poisson(1.3)
        for d in rng.integers(max(0, ini_cen[i] - 10), min(D, ini_cen[i] + 60), k):
            textos.append((d, rng.choice(TEMAS_ESTRESSE)))
    elif rng.random() < 0.04:
        # ruído: lojista normal que só pergunta da concorrência
        textos.append((rng.integers(0, D), rng.choice(TEMAS_DESVIO)))
    for d, txt in textos:
        tid += 1
        tickets.append({
            "id_atendimento": f"AT{tid:08d}",
            "id_lojista": cad["id_lojista"].values[i],
            "data_hora": (dias[d] + pd.Timedelta(minutes=int(rng.integers(8 * 60, 20 * 60)))).isoformat(),
            "canal": str(rng.choice(CANAIS, p=[0.4, 0.35, 0.15, 0.10])),
            "texto": txt.format(
                conc=nomes_conc[conc_principal[i]],
                taxa=f"{rng.uniform(0.79, 2.49):.2f}".replace(".", ","),
                agente=cad["agente_responsavel"].values[i].split()[0],
            ),
        })
tk = pd.DataFrame(tickets).sort_values("data_hora")
for m, g in tk.groupby(pd.to_datetime(tk["data_hora"]).dt.to_period("M")):
    with open(f"{LANDING}/atendimentos/atendimentos_{m}.json", "w") as f:
        for r in g.to_dict("records"):
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("atendimentos:", len(tk))

# COMMAND ----------

cad.to_csv(f"{LANDING}/cadastro_lojistas/lojistas.csv", index=False)
print("lojistas:", len(cad))
print(pd.Series(cen).value_counts().to_dict())
