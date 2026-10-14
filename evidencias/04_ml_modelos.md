# Machine Learning — modelos, métricas e backtest

> Coletado automaticamente por `scripts/coletar_evidencias.py` em 2026-10-14 14:06 UTC · workspace `https://fevm-vini.cloud.databricks.com`

## Modelo `vini_catalog.radar_recebiveis.modelo_vazamento`

- Aliases: `{'champion': 8}`
- Versões registradas: 8

Versão champion **v8** · criada em 2026-10-14 13:59:50 UTC · run MLflow `027b55431d9e48fcb3f0d5d1a352b71a` · status READY

Métricas registradas no MLflow (run final):

| métrica | valor |
|---|---|
| oot_auc_oot | 0.9235 |
| oot_n_teste | 164875.0 |
| oot_n_treino | 339442.0 |
| oot_pr_auc_oot | 0.5913 |
| oot_precisao_top5pct | 0.1891 |
| oot_taxa_base | 0.0122 |

## Modelo `vini_catalog.radar_recebiveis.modelo_inadimplencia`

- Aliases: `{'champion': 8}`
- Versões registradas: 8

Versão champion **v8** · criada em 2026-10-14 14:00:02 UTC · run MLflow `95ed72c775d6405f91fe7a70fe672e06` · status READY

Métricas registradas no MLflow (run final):

| métrica | valor |
|---|---|
| oot_auc_oot | 0.9713 |
| oot_n_teste | 22308.0 |
| oot_n_treino | 51684.0 |
| oot_pr_auc_oot | 0.5356 |
| oot_precisao_top5pct | 0.1874 |
| oot_taxa_base | 0.0107 |

### Backtest de negócio (tabela backtest_radar)



```sql
SELECT * FROM vini_catalog.radar_recebiveis.backtest_radar ORDER BY modelo, metrica
```

| modelo | metrica | valor | descricao |
|---|---|---|---|
| inadimplencia | antecedencia_mediana_dias | 91.0 | Mediana de dias entre o primeiro alerta e o evento |
| inadimplencia | auc_oot | 0.9713404256129275 | AUC fora do tempo (treino no passado, teste no futuro) |
| inadimplencia | eventos_no_periodo | 90.0 | Novos casos de inadimplência (>60 dias) no período de teste |
| inadimplencia | pct_valor_sinalizado | 0.9157192850119527 | % do saldo devedor dos contratos que viraram inadimplência e foram sinalizados antes |
| inadimplencia | precisao_top5pct | 0.18744394618834082 | Precisão nos 5% de maior risco |
| inadimplencia | recall_antecipado | 0.9 | % dos casos de inadimplência (>60 dias) sinalizados 1 a 13 semanas antes (top 15% de risco) |
| inadimplencia | taxa_base | 0.010668818361126053 | Taxa de eventos na população elegível (referência para a precisão) |
| inadimplencia | valor_exposto_eventos | 5318450.6933 | R$ expostos nos casos do período (saldo devedor ou receita anual de MDR) |
| vazamento | antecedencia_mediana_dias | 35.0 | Mediana de dias entre o primeiro alerta e o evento |
| vazamento | auc_oot | 0.9235489229466982 | AUC fora do tempo (treino no passado, teste no futuro) |
| vazamento | eventos_no_periodo | 1044.0 | Novos casos de vazamento de TPV (>50%) no período de teste |
| vazamento | pct_valor_sinalizado | 0.8685163551868629 | % da receita anual de MDR dos lojistas que vazaram e foram sinalizados antes |
| vazamento | precisao_top5pct | 0.18913017105422783 | Precisão nos 5% de maior risco |
| vazamento | recall_antecipado | 0.8840996168582376 | % dos casos de vazamento de TPV (>50%) sinalizados 1 a 13 semanas antes (top 15% de risco) |
| vazamento | taxa_base | 0.012166793025018954 | Taxa de eventos na população elegível (referência para a precisão) |
| vazamento | valor_exposto_eventos | 19119121.4459 | R$ expostos nos casos do período (saldo devedor ou receita anual de MDR) |

### Importância das variáveis (permutation importance, top 8 por modelo)



```sql
SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY modelo ORDER BY importancia DESC) AS rk
                       FROM vini_catalog.radar_recebiveis.importancia_variaveis) WHERE rk <= 8 ORDER BY modelo, rk
```

| modelo | variavel | importancia | rk |
|---|---|---|---|
| inadimplencia | razao_tpv_cartao_1s_12s | 0.02215772870662458 | 1 |
| inadimplencia | alavancagem | 0.019954574132492076 | 2 |
| inadimplencia | delta_pix_share | 0.017292113564668754 | 3 |
| inadimplencia | dias_atraso_atual | 0.01582502628811777 | 4 |
| inadimplencia | cobertura_retencao_4s | 0.01191251314405889 | 5 |
| inadimplencia | delta_share_pedregulho | 0.0072866456361723175 | 6 |
| inadimplencia | pix_share_4s | 0.0027962145110410064 | 7 |
| inadimplencia | log_tpv_cartao_12s | 0.0017388012618296256 | 8 |
| vazamento | delta_pix_share | 0.04821757770993146 | 1 |
| vazamento | razao_tpv_cartao_1s_12s | 0.047033821758892924 | 2 |
| vazamento | share_pedregulho_4s | 0.027586562855196783 | 3 |
| vazamento | pix_share_4s | 0.01161146773395444 | 4 |
| vazamento | delta_share_pedregulho | 0.010920849925552312 | 5 |
| vazamento | segmento | 0.008745768493241324 | 6 |
| vazamento | log_tpv_cartao_12s | 0.007441953881765717 | 7 |
| vazamento | tem_credito | 0.002970610313151101 | 8 |

### Distribuição das faixas de risco na semana de referência



```sql
SELECT faixa_risco, tipo_sinal, count(*) AS lojistas, round(avg(prob_vazamento), 3) AS prob_vaz_media,
               round(avg(prob_inadimplencia), 3) AS prob_inad_media, round(sum(valor_em_risco), 0) AS valor_em_risco
        FROM vini_catalog.radar_recebiveis.scores_lojistas GROUP BY 1, 2 ORDER BY 1, 2
```

| faixa_risco | tipo_sinal | lojistas | prob_vaz_media | prob_inad_media | valor_em_risco |
|---|---|---|---|---|---|
| Alto | Desvio para concorrente | 67 | 0.326 | 0.239 | 645479.0 |
| Alto | Migração para Pix | 1 | 0.274 |  | 514.0 |
| Alto | Queda de faturamento | 10 | 0.164 | 0.311 | 62891.0 |
| Alto | Retenção insuficiente | 4 | 0.206 | 0.13 | 16553.0 |
| Alto | Sinais combinados | 19 | 0.333 | 0.025 | 124426.0 |
| Baixo | Desvio para concorrente | 232 | 0.022 | 0.008 | 91585.0 |
| Baixo | Queda de faturamento | 274 | 0.028 | 0.009 | 124306.0 |
| Baixo | Retenção insuficiente | 218 | 0.005 | 0.009 | 43455.0 |
| Baixo | Sinais combinados | 13468 | 0.003 | 0.0 | 935739.0 |
| Crítico | Desvio para concorrente | 219 | 0.675 | 0.49 | 5479541.0 |
| Crítico | Queda de faturamento | 3 | 0.229 | 0.395 | 26067.0 |
| Crítico | Retenção insuficiente | 4 | 0.118 | 0.73 | 80056.0 |
| Crítico | Sinais combinados | 19 | 0.682 | 0.059 | 454103.0 |
| Em cobrança | Desvio para concorrente | 2 | 0.001 | 0.007 | 121.0 |
| Em cobrança | Queda de faturamento | 9 | 0.007 | 0.056 | 15305.0 |
| Em cobrança | Retenção insuficiente | 199 | 0.001 | 0.02 | 103892.0 |
| Em cobrança | Sinais combinados | 14 | 0.001 | 0.01 | 7078.0 |
| Médio | Desvio para concorrente | 39 | 0.173 | 0.048 | 158399.0 |
| Médio | Migração para Pix | 1 | 0.118 | 0.136 | 913.0 |
| Médio | Queda de faturamento | 20 | 0.13 | 0.104 | 46052.0 |
| Médio | Retenção insuficiente | 3 | 0.05 | 0.1 | 7954.0 |
| Médio | Sinais combinados | 31 | 0.16 | 0.002 | 44550.0 |

