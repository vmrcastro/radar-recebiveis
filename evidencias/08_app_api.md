# Databricks App — respostas reais da API

> Coletado automaticamente por `scripts/coletar_evidencias.py` em 2026-10-14 14:07 UTC · workspace `https://fevm-vini.cloud.databricks.com`

- App: **radar-recebiveis-dev** · https://radar-recebiveis-dev-7474658470734499.aws.databricksapps.com
- Estado: compute `ACTIVE` · app `RUNNING`
- Deployment ativo: `01f1c740a59619cd9eba2f0ab672deb5` (SUCCEEDED)
- Recursos: warehouse, genie, lakebase

### `GET /` → HTTP 200 · 26420 bytes · título: `Radar do Hub`

### `GET /api/config` → HTTP 200 (959 ms ponta a ponta)

```json
{
 "empresa": "Pedregulho Pagamentos",
 "hubs": [
  "Hub BH Contagem",
  "Hub BH Savassi",
  "Hub Belém",
  "Hub Brasília",
  "Hub Campinas",
  "Hub Curitiba",
  "Hub Florianópolis",
  "Hub Fortaleza",
  "Hub Goiânia",
  "Hub Manaus",
  "Hub Porto Alegre",
  "Hub RJ Barra",
  "Hub RJ Centro",
  "Hub RJ Zona Norte",
  "Hub Recife",
  "Hub Ribeirão Preto",
  "Hub SP Centro",
  "Hub SP Zona Leste",
  "Hub SP Zona Norte",
  "Hub SP Zona Sul",
  "Hub Salvador"
 ]
}
```

### `GET /api/resumo` → HTTP 200 (6256 ms ponta a ponta)

```json
{
 "kpis": {
  "alertas": 440,
  "criticos": 245,
  "valor_em_risco": 7147496.900000001,
  "valor_risco_credito": 3614620.070000002,
  "valor_risco_receita": 3532876.830000001,
  "saldo_em_alerta": 10203063.31
 },
 "por_tipo": [
  {
   "tipo_sinal": "Desvio para concorrente",
   "lojistas": 325,
   "valor_em_risco": 6283418.450000002
  },
  {
   "tipo_sinal": "Sinais combinados",
   "lojistas": 69,
   "valor_em_risco": 623078.81
  },
  {
   "tipo_sinal": "Queda de faturamento",
   "lojistas": 33,
   "valor_em_risco": 135009.72999999998
  },
  {
   "tipo_sinal": "Retenção insuficiente",
   "lojistas": 11,
   "valor_em_risco": 104562.49
  },
  {
   "tipo_sinal": "Migração para Pix",
   "lojistas": 2,
   "valor_em_risco": 1427.42
  }
 ],
 "por_hub": [
  {
   "hub": "Hub Campinas",
   "lojistas": 26,
   "criticos": 17,
   "valor_em_risco": 843620.9800000002
  },
  {
   "hub": "Hub Porto Alegre",
   "lojistas": 23,
   "criticos": 15,
   "valor_em_risco": 590375.1199999999
  },
  {
   "hub": "Hub SP Zona Sul",
   "lojistas": 27,
   "criticos": 20,
   "valor_em_risco": 524101.56999999995
  },
  {
   "hub": "Hub Curitiba",
   "lojistas": 31,
   "criticos": 17,
   "valor_em_risco": 490564.09
  },
  {
   "hub": "Hub Brasília",
   "lojistas": 28,
   "criticos": 13,
   "valor_em_risco": 427958.0
  },
  {
   "hub": "Hub RJ Zona Norte",
   "lojistas": 19,
   "criticos": 11,
   "valor_em_risco": 406963.47
  },
  {
   "hub": "Hub SP Centro",
   "lojistas": 25,
   "criticos": 13,
   "valor_em_risco": 402858.19
  },
  {
   "hub": "Hub Florianópolis",
   "lojistas": 14,
   "criticos": 10,
   "valor_em_risco": 371072.2900000001
  },
  {
   "hub": "Hub Salvador",
   "lojistas": 29,
   "criticos": 16,
   "valor_em_risco": 362851.7599999999
  },
  {
   "hub": "Hub Recife",
   "lojistas": 22,
   "criticos": 12,
   "valor_em_risco": 358354.74
  },
  {
   "hub": "Hub Ribeirão Preto",
   "lojistas": 14,
   "criticos": 7,
   "valor_em_risco": 325309.97
  },
  {
   "hub": "Hub RJ Centro",
   "lojistas": 17,
   "criticos": 9,
   "valor_em_risco": 318800.47
  }
 ],
 "acoes": {
  "lojistas_tratados": 1,
  "acordos": 0,
  "valor_protegido": 0.0
 },
 "carteira": {
  "saldo_ativo": "1.72069352E8",
  "saldo_inad_60": "1.6295974E7",
  "contratos_ativos": "3951"
 },
 "backtest": [
  {
   "modelo": "inadimplencia",
   "metrica": "eventos_no_periodo",
   "valor": "90.0",
   "descricao": "Novos casos de inadimplência (>60 dias) no período de teste"
  },
  {
   "modelo": "inadimplencia",
   "metrica": "recall_antecipado",
   "valor": "0.9",
   "descricao": "% dos casos de inadimplência (>60 dias) sinalizados 1 a 13 semanas antes (top 15% de risco)"
  },
  {
   "modelo": "inadimplencia",
   "metrica": "antecedencia_mediana_dias",
   "valor": "91.0",
   "descricao": "Mediana de dias entre o primeiro alerta e o evento"
  },
  {
   "modelo": "inadimplencia",
   "metrica": "pct_valor_sinalizado",
   "valor": "0.9157192850119527",
   "descricao": "% do saldo devedor dos contratos que viraram inadimplência e foram sinalizados antes"
  },
  {
   "modelo": "inadimplencia",
   "metrica": "valor_exposto_eventos",
   "valor": "5318450.6933",
   "descricao": "R$ expostos nos casos do período (saldo devedor ou receita anual de MDR)"
  },
  {
   "modelo": "inadimplencia",
   "metrica": "auc_oot",
   "valor": "0.9713404256129275",
   "descricao": "AUC fora do tempo (treino no passado, teste no futuro)"
  },
  {
   "modelo": "inadimplencia",
   "metrica": "precisao_top5pct",
   "valor": "0.18744394618834082",
   "descricao": "Precisão nos 5% de maior risco"
  },
  {
   "modelo": "inadimplencia",
   "metrica": "taxa_base",
   "valor": "0.010668818361126053",
   "descricao": "Taxa de eventos na população elegível (referência para a precisão)"
  },
  {
   "modelo": "vazamento",
   "metrica": "eventos_no_periodo",
   "valor": "1044.0",
   "descricao": "Novos casos de vazamento de TPV (>50%) no período de teste"
  },
  {
   "modelo": "vazamento",
   "metrica": "recall_antecipado",
   "valor": "0.8840996168582376",
   "descricao": "% dos casos de vazamento de TPV (>50%) sinalizados 1 a 13 semanas antes (top 15% de risco)"
  },
  {
   "modelo": "vazamento",
   "metrica": "antecedencia_mediana_dias",
   "valor": "35.0",
   "descricao": "Mediana de dias entre o primeiro alerta e o evento"
  },
  {
   "modelo": "vazamento",
   "metrica": "pct_valor_sinalizado",
   "valor": "0.8685163551868629",
   "descricao": "% da receita anual de MDR dos lojistas que vazaram e foram sinalizados antes"
  },
  {
   "modelo": "vazamento",
   "metrica": "valor_exposto_eventos",
   "valor": "1.9119121445910532E7",
   "descricao": "R$ expostos nos casos do período (saldo devedor ou receita anual de MDR)"
  },
  {
   "modelo": "vazamento",
   "metrica": "auc_oot",
   "valor": "0.9235489229466982",
   "descricao": "AUC fora do tempo (treino no passado, teste no futuro)"
  },
  {
   "modelo": "vazamento",
   "metrica": "precisao_top5pct",
   "valor": "0.18913017105422783",
   "descricao": "Precisão nos 5% de maior risco"
  },
  {
   "modelo": "vazamento",
   "metrica": "taxa_base",
   "valor": "0.012166793025018954",
   "descricao": "Taxa de eventos na população elegível (referência para a precisão)"
  }
 ],
 "tendencia": [
  {
   "semana": "2026-08-24",
   "pedregulho": "2.44634097E8",
   "concorrentes": "7.1357219E7"
  },
  {
   "semana": "2026-08-31",
   "pedregulho": "2.44291689E8",
   "concorrentes": "7.2042243E7"
  },
  {
   "semana": "2026-09-07",
   "pedregulho": "2.42406513E8",
   "concorrentes": "7.1227217E7"
  },
  {
   "semana": "2026-09-14",
   "pedregulho": "2.4234803E8",
   "concorrentes": "7.2258759E7"
  },
  {
   "semana": "2026-09-21",
   "pedregulho": "2.40418629E8",
   "concorrentes": "7.2741677E7"
  },
  {
   "semana": "2026-09-28",
   "pedregulho": "2.4059087E8",
   "concorrentes": "7.3783244E7"
  }
 ],
 "concorrentes": [
  {
   "credenciadora": "NuvemPay",
   "ganho_4s": "3278507.0"
  },
  {
   "credenciadora": "PagaLogo",
   "ganho_4s": "2899107.0"
  },

```

### `GET /api/fila?faixa=Cr%C3%ADtico&limite=5` → HTTP 200 (634 ms ponta a ponta)

```json
{
 "latencia_ms": 9.3,
 "itens": [
  {
   "id_lojista": "LJ008915",
   "prioridade": 1,
   "nome_fantasia": "Drogaria Saúde Já",
   "segmento": "Farmácias",
   "porte": "Pequeno",
   "cidade": "Campinas",
   "uf": "SP",
   "hub": "Hub Campinas",
   "agente_responsavel": "Eduarda Ribeiro",
   "faixa_risco": "Crítico",
   "tipo_sinal": "Desvio para concorrente",
   "prob_vazamento": 0.9611,
   "prob_inadimplencia": 0.8544,
   "valor_em_risco": 336295.24,
   "saldo_devedor": 324123.26,
   "receita_mdr_anual": 61770.67,
   "dias_atraso": 2,
   "variacao_tpv_cartao": -0.1514,
   "principal_concorrente": "NuvemPay",
   "acao_recomendada": "Visita presencial em 48h",
   "resumo_ia": "A Drogaria Saúde Já está desviando recebíveis para a NuvemPay em ritmo acelerado — share da Pedregulho caiu 14 p.p. e o contrato já acumula atraso —, colocando em risco R$ 336.295 entre saldo devedor e receita de MDR, com 96% de probabilidade de perda total do TPV em 8 semanas.",
   "ultima_acao": "Ligação",
   "ultimo_resultado": "Sem contato",
   "ultima_acao_em": "2026-10-14T13:28:29.944640+00:00"
  },
  {
   "id_lojista": "LJ003304",
   "prioridade": 3,
   "nome_fantasia": "Eletro Lima",
   "segmento": "Eletrônicos e Celulares",
   "porte": "Médio",
   "cidade": "Belo Horizonte",
   "uf": "MG",
   "hub": "Hub BH Contagem",
   "agente_responsavel": "Rodrigo Martins",
   "faixa_risco": "Crítico",
   "tipo_sinal": "Desvio para concorrente",
   "prob_vazamento": 0.874,
   "prob_inadimplencia": null,
   "valor_em_risco": 206750.93,
   "saldo_devedor": 0.0,
   "receita_mdr_anual": 236549.0,
   "dias_atraso": 0,
   "variacao_tpv_cartao": -0.1236,
   "principal_concorrente": "PagaLogo",
   "acao_recomendada": "Visita presencial em 48h",
   "resumo_ia": "A Eletro Lima apresenta forte sinal de migração para a PagaLogo, com 87% de probabilidade de desvio do TPV em 8 semanas e R$ 59.112/semana já registrados no concorrente. A receita anual de MDR em risco é de R$ 206.751, exigindo ação imediata de retenção.",
   "ultima_acao": null,
   "ultimo_resultado": null,
   "ultima_acao_em": null
  },
  {
   "id_lojista": "LJ004470",
   "prioridade": 4,
   "nome_fantasia": "Empório Bom Preço",
   "segmento": "Mercados e Mercearias",
   "porte": "Médio",
   "cidade": "Ribeirão Preto",
   "uf": "SP",
   "hub": "Hub Ribeirão Preto",
   "agente_responsavel": "Patrícia Freitas",
   "faixa_risco": "Crítico",
   "tipo_sinal": "Desvio para concorrente",
   "prob_vazamento": 0.8838,
   "prob_inadimplencia": 0.0006,
   "valor_em_risco": 178134.15,
   "saldo_devedor": 315888.44,
   "receita_mdr_anual": 201331.43,
   "dias_atraso": 0,
   "variacao_tpv_cartao": -0.3395,
   "principal_concorrente": "NuvemPay",
   "acao_recomendada": "Visita presencial em 48h",
   "resumo_ia": "O Empório Bom Preço está desviando volume expressivo de recebíveis para a NuvemPay — o share da Pedregulho caiu 33 p.p. e a trava já cobre apenas 64% da parcela esperada, colocando R$ 178.134 em risco com saldo devedor de R$ 315.888 em aberto.",
   "ultima_acao": null,
   "ultimo_resultado": null,
   "ultima_acao_em": null
  },
  {
   "id_lojista": "LJ002383",
   "prioridade": 5,
   "nome_fantasia": "Espetinho do Sérgio",
   "segmento": "Restaurantes e Bares",
   "porte": "Pequeno",
   "cidade": "Brasília",
   "uf": "DF",
   "hub": "Hub Brasília",
   "agente_responsavel": "Bruno Rocha",
   "faixa_risco": "Crítico",
   "tipo_sinal": "Desvio para concorrente",
   "prob_vazamento": 0.7538,
   "prob_inadimplencia": 0.5064,
   "valor_em_risco": 165668.41,
   "saldo_devedor": 272855.31,
   "receita_mdr_anual": 36471.5,
   "dias_atraso": 2,
   "variacao_tpv_cartao": -0.2801,
   "principal_concorrente": "PagaLogo",
   "acao_recomendada": "Visita presencial em 48h",
   "resumo_ia": "O Espetinho do Sérgio está migrando ativamente para a PagaLogo: o TPV na Pedregulho caiu 28% em 4 semanas, o share de recebíveis despencou 25 p.p. e a trava já cobre apenas 73% da parcela — com contrato em atraso. Com R$ 272.855 de saldo devedor e R$ 165.668 em valor total em risco, a janela de retenção é imediata.",
   "ultima_acao": null,
   "ultimo_resultado": null,
   "ultima_acao_em": null
  },
  {
   "id_lojista": "LJ012359",
   "prioridade": 6,
   "nome_fantasia": "Posto Rota Sul",
   "segmento": "Postos de Combustível",
   "porte": "Médio",
   "cidade": "São Paulo",
   "uf": "SP",
   "hub": "Hub SP Centro",
   "agente_responsavel": "Mariana Carvalho",
   "faixa_risco": "Crítico",
   "tipo_sinal": "Desvio para concorrente",
   "prob_vazamento": 0.8058,
   "prob_inadimplencia": null,
   "valor_em_risco": 159396.5,
   "saldo_devedor": 0.0,
   "receita_mdr_anual": 197814.15,
   "dias_atraso": 0,
   "variacao_tpv_cartao": -0.1751,
   "principal_concorrente": "NuvemPay",
   "acao_recomendada": "Visita presencial em 48h",
   "resumo_ia": "O Posto Rota Sul apresenta sinais claros de migração para a NuvemPay: os recebíveis registrados no concorrente subiram 142% e o share da Pedregulho caiu 16 p.p., colocando em risco R$ 159.396 de receita anual de MDR sem qualquer saldo devedor que justifique inércia.",
   "ultima_acao": null,
   "ultimo_resultado": null,
   "ultima_acao_em": null
  }
 ]
}
```

### `GET /api/lojista/LJ008915` → HTTP 200 (3271 ms ponta a ponta)

```json
{
 "alerta": {
  "id_lojista": "LJ008915",
  "prioridade": 1,
  "semana_referencia": "2026-09-28",
  "nome_fantasia": "Drogaria Saúde Já",
  "segmento": "Farmácias",
  "porte": "Pequeno",
  "plano": "Pedregulho Pro",
  "uf": "SP",
  "cidade": "Campinas",
  "hub": "Hub Campinas",
  "agente_responsavel": "Eduarda Ribeiro",
  "faixa_risco": "Crítico",
  "tipo_sinal": "Desvio para concorrente",
  "prob_vazamento": 0.9611,
  "prob_inadimplencia": 0.8544,
  "valor_em_risco": 336295.24,
  "valor_risco_credito": 276924.39,
  "valor_risco_receita": 59370.85,
  "saldo_devedor": 324123.26,
  "receita_mdr_anual": 61770.67,
  "dias_atraso": 2,
  "variacao_tpv_cartao": -0.1514,
  "tpv_cartao_media_semanal": 48151.16,
  "principal_concorrente": "NuvemPay",
  "concorrente_citado_atendimento": null,
  "ultimo_atendimento_resumo": null,
  "sinais": "Registradora: recebíveis em NuvemPay subiram 61% (agora R$ 46.112/semana) | Share da Pedregulho nos recebíveis do lojista caiu 14 p.p. | Contrato com 2 dias de atraso",
  "resumo_ia": "A Drogaria Saúde Já está desviando recebíveis para a NuvemPay em ritmo acelerado — share da Pedregulho caiu 14 p.p. e o contrato já acumula atraso —, colocando em risco R$ 336.295 entre saldo devedor e receita de MDR, com 96% de probabilidade de perda total do TPV em 8 semanas.",
  "acao_recomendada": "Visita presencial em 48h",
  "oferta_sugerida": "Redução de MDR por 6 meses + carência de 30 dias no contrato de capital de giro.",
  "roteiro_abordagem": "1. \"Notamos que seu movimento de cartão conosco reduziu nas últimas semanas e queremos entender como podemos melhorar sua experiência antes que isso gere qualquer problema no seu contrato de crédito.\"\n2. \"Para valorizar sua parceria, estamos preparados para reduzir sua taxa de MDR pelos próximos 6 meses e oferecer 30 dias de carência no seu contrato, aliviando o caixa da farmácia agora.\"\n3. \"Manter suas vendas de cartão conosco garante que o crédito continue fluindo sem interrupção — posso formalizar essa proposta ainda hoje para você analisar com calma.\"",
  "gerado_em": "2026-10-14T14:01:16.541958+00:00"
 },
 "acoes": [
  {
   "id_acao": 3,
   "id_lojista": "LJ008915",
   "agente": "coleta-de-evidencias",
   "acao": "Ligação",
   "resultado": "Sem contato",
   "observacao": "Registro automático de evidência de write-back",
   "valor_em_risco": 336295.24,
   "registrado_em": "2026-10-14T13:28:29.944640+00:00"
  },
  {
   "id_acao": 2,
   "id_lojista": "LJ008915",
   "agente": "coleta-de-evidencias",
   "acao": "Ligação",
   "resultado": "Sem contato",
   "observacao": "Registro automático de evidência de write-back",
   "valor_em_risco": 344041.29,
   "registrado_em": "2026-10-14T12:46:35.367282+00:00"
  }
 ],
 "historico": [
  {
   "semana": "2026-08-24",
   "tpv_cartao": "46896.0",
   "tpv_pix": "33511.0",
   "recebiveis_concorrentes": "10956.0",
   "saldo_devedor": "375179.0",
   "max_dias_atraso": "0"
  },
  {
   "semana": "2026-08-31",
   "tpv_cartao": "56054.0",
   "tpv_pix": "40299.0",
   "recebiveis_concorrentes": "13844.0",
   "saldo_devedor": "362141.0",
   "max_dias_atraso": "0"
  },
  {
   "semana": "2026-09-07",
   "tpv_cartao": "57723.0",
   "tpv_pix": "41834.0",
   "recebiveis_concorrentes": "14648.0",
   "saldo_devedor": "348715.0",
   "max_dias_atraso": "0"
  },
  {
   "semana": "2026-09-14",
   "tpv_cartao": "44583.0",
   "tpv_pix": "34041.0",
   "recebiveis_concorrentes": "13683.0",
   "saldo_devedor": "338345.0",
   "max_dias_atraso": "0"
  },
  {
   "semana": "2026-09-21",
   "tpv_cartao": "35127.0",
   "tpv_pix": "28797.0",
   "recebiveis_concorrentes": "25101.0",
   "saldo_devedor": "330174.0",
   "max_dias_atraso": "0"
  },
  {
   "semana": "2026-09-28",
   "tpv_cartao": "26015.0",
   "tpv_pix": "23373.0",
   "recebiveis_concorrentes": "46112.0",
   "saldo_devedor": "324123.0",
   "max_dias_atraso": "2"
  }
 ],
 "atendimentos": []
}
```

### `POST /api/acoes` → HTTP 200 (681 ms ponta a ponta)

Corpo enviado:

```json
{
 "id_lojista": "LJ008915",
 "agente": "coleta-de-evidencias",
 "acao": "Ligação",
 "resultado": "Sem contato",
 "observacao": "Registro automático de evidência de write-back"
}
```

```json
{
 "latencia_ms": 5.6,
 "id_acao": 4,
 "registrado_em": "2026-10-14T14:08:11.765648+00:00"
}
```

### `POST /api/genie` → HTTP 200 (26179 ms ponta a ponta)

Corpo enviado:

```json
{
 "pergunta": "Quantos lojistas estão em alerta por faixa de risco?"
}
```

```json
{
 "conversation_id": "01f1c7d8b137110a9653564434e8bfe0",
 "texto": [
  "Você quer saber quantos lojistas estão em alerta, divididos por faixa de risco (Crítico, Alto, Médio).",
  "**440 lojistas** aparecem em alerta no resultado, distribuídos por faixa de risco.\n- **Crítico:** **245**\n- **Alto:** **101**\n- **Médio:** **94**\nA faixa **Crítico** concentra a maior parte dos alertas, com mais lojistas do que **Alto** e **Médio** somados.",
  "Você prefere ver o valor total em risco (R$) por faixa de risco, além da quantidade de lojistas?"
 ],
 "sql": "SELECT `faixa_risco`, COUNT(*) AS `lojistas_em_alerta`\nFROM `vini_catalog`.`radar_recebiveis`.`alertas_lojistas`\nWHERE `faixa_risco` IS NOT NULL\nGROUP BY `faixa_risco`\nORDER BY `lojistas_em_alerta` DESC, `faixa_risco`",
 "colunas": [
  "faixa_risco",
  "lojistas_em_alerta"
 ],
 "linhas": [
  [
   "Crítico",
   "245"
  ],
  [
   "Alto",
   "101"
  ],
  [
   "Médio",
   "94"
  ]
 ],
 "sugestoes": [
  "Qual o valor total em risco por faixa de risco?",
  "Quais os segmentos com mais lojistas em alerta crítico?",
  "Qual a distribuição de lojistas em alerta por UF e faixa de risco?"
 ]
}
```

