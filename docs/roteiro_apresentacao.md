# Roteiro de apresentação — Radar de Vazamento de Recebíveis

> Empresa fictícia: **Pedregulho Pagamentos** 🪨 — credenciadora brasileira focada em micro, pequenos e médios lojistas,
> com maquininha, conta digital e capital de giro pago por trava de recebíveis. Todos os dados são sintéticos.
> Os números abaixo vêm da execução de 14/10/2026 (job run `10454867535961`, ver `evidencias/`). Se regerar os dados, confira de novo.

**Duração sugerida:** 20 min de demo + 10 min de perguntas
**Audiência:** patrocinador executivo (CFO / VP de Crédito) + dono do domínio (Head de Risco de Crédito e/ou Head de Canais/Hubs) + liderança técnica (CDO / Head de Dados)

---

## 0. A tese em uma frase (decore)

> "Quando o lojista leva o cartão para a maquininha do concorrente, a Pedregulho perde **duas vezes**: perde a receita
> de MDR **e** perde a garantia do capital de giro. Hoje, Crédito e Comercial enxergam esse sinal separados — e tarde.
> O Radar transforma esse sinal em uma ação do agente do hub **semanas antes** de virar perda."

---

## 1. Abertura — setup da demo (2 min) · *TELL*

**Contexto do cliente** (fale para o CFO):
- A Pedregulho está crescendo a carteira de capital de giro rapidamente: é a principal alavanca de lucro e também a principal pergunta do mercado. *"Dá para crescer o crédito sem explodir o custo de risco?"*
- O crédito é pago com **% dos recebíveis de cartão**. A garantia é o fluxo de vendas, que pode ir para o concorrente ou migrar para Pix fora da trava.
- Concorrência agressiva na base de MSMB (maquininha grátis, D+0, taxa menor) e o Pix pressionando o take rate.

**O problema, em números da carteira demo** (app → Visão executiva):
- 15 mil lojistas, 5,3 mil contratos de capital de giro (R$ 296 mi contratados) · carteira ativa: **R$ 172 mi** · inadimplência >60 dias: **9,5%** do saldo (R$ 16,3 mi)
- Os lojistas que *vão* gerar a próxima perda ainda estão "em dia" hoje.

**O que vou mostrar** (combinado com a audiência):
1. Como o gestor enxerga o **valor em risco** e de onde ele vem (concorrente, queda de faturamento, Pix);
2. Como o **agente do hub** recebe uma fila priorizada com o porquê e o que fazer, e registra a ação;
3. Como qualquer gestor **pergunta em português** e recebe a resposta governada;
4. Os bastidores, em 3 minutos: uma única plataforma da ingestão ao app.

> 💡 Frase de transição: "Vou começar pelo que o executivo vê na segunda-feira de manhã."

---

## 2. Demo — *SHOW* (13 min)

### 2.1 Visão executiva (3 min) — patrocinador
App → aba **Visão executiva**
- **Valor em risco** (KPI verde): probabilidade × saldo devedor + probabilidade × receita anual de MDR. *"É a métrica que prioriza o trabalho do campo."*
- **Faixa de backtest**: *"Se o Radar estivesse ligado nas últimas 12 semanas, teria sinalizado **90%** dos contratos que viraram inadimplência (**92% do saldo**), cerca de **3 meses antes** (mediana de 91 dias), e **88%** dos lojistas que vazaram o TPV (87% da receita de MDR), cerca de **5 semanas antes**."* → **é aqui que o CFO presta atenção.**
- Gráfico **Recebíveis na Pedregulho × em concorrentes** (dado da registradora): *"Isso é algo que só uma credenciadora tem: vemos os recebíveis do lojista em todo o mercado."*
- **Quem está ganhando**: barra por concorrente. *"Isso orienta a resposta comercial: preço, D+0, produto."*
- **Por hub**: clique em um hub → abre a fila daquele hub (transição para o dono do domínio).

### 2.2 Fila do Hub (5 min) — dono do domínio (Risco/Canais)
App → aba **Fila do Hub** (filtrada pelo hub)
- Mostre o badge **"consulta no Lakebase em ~5 ms"**: *"É uma aplicação operacional, não um dashboard; o agente usa no celular, no balcão do lojista."*
- Abra um lojista **Crítico / Desvio para concorrente**:
  - **Resumo por IA** + **sinais observados** (ex.: *"TPV de cartão caiu 58%"*, *"recebíveis na NuvemPay subiram 140%"*, *"citou proposta da NuvemPay no WhatsApp"*)
  - Gráfico de 26 semanas: a linha verde caindo enquanto a laranja (concorrente) sobe, e o saldo devedor parado. *"Isso é o vazamento acontecendo."*
  - **Próxima melhor ação** + **roteiro**: *"O agente não precisa interpretar modelo; ele chega com a oferta certa."*
- **Registre a ação** (Visita presencial → Retido / acordo fechado) → toast *"gravado no Lakebase em X ms"* → volte à Visão executiva: **"Protegido pelo campo"** aumentou.
  *"Fechamos o ciclo: a ação do campo vira dado, mede o ROI e realimenta o modelo."*

### 2.3 Pergunte ao Radar (3 min) — qualquer gestor
App → aba **Pergunte ao Radar** (Genie)
1. *"Para quais concorrentes estamos perdendo mais recebíveis nas últimas 4 semanas?"*
2. Follow-up: *"E em quais segmentos isso está concentrado?"*
3. *"Qual o percentual do saldo com mais de 60 dias de atraso, por segmento?"*
- Abra **"Ver SQL gerado"**: *"Nada de caixa-preta: a resposta é SQL sobre tabelas governadas, com as definições oficiais da empresa (valor em risco, inadimplência, share)."*

### 2.4 Bastidores (2-3 min) — liderança técnica
Workspace Databricks, nesta ordem:
1. **Lakeflow Pipeline** (grafo): bronze → silver → gold. Auto Loader nos arquivos da registradora, autorizador, CRM e atendimento; **expectations** de qualidade; `ai_query` dentro do pipeline transforma o texto do WhatsApp em sinal estruturado (motivo, concorrente citado, risco de cancelamento).
2. **Unity Catalog**: abra `silver_lojistas` → CNPJ/CPF **mascarados** (função UC aplicada na coluna) · **tags PII** · **Lineage** do arquivo bruto até `alertas_lojistas` e o App.
3. **MLflow / Models**: 2 modelos registrados no UC com alias `champion`; métricas **out-of-time**; importância de variáveis.
4. **Lakebase**: projeto Postgres autoscaling (escala a zero), synced table do Delta + tabela transacional `app.acoes_campo`.
5. **Bundle**: *"Tudo isso é um Databricks Asset Bundle: `./scripts/deploy.sh` sobe a jornada inteira em outro workspace."*

---

## 3. Fechamento — *TELL* (3 min) · valor para o negócio

### Para o patrocinador executivo (CFO / VP Crédito)
| KPI do comprador | Como o Radar move | Ordem de grandeza (template para calibrar com dados reais) |
|---|---|---|
| **Custo de risco / inadimplência >60** | Ação de 1 a 13 semanas antes do atraso (renegociação, reforço de trava) | Carteira R$ X bi × redução de 30-60 bps no custo de risco = **R$ X × 0,3-0,6% por ano** |
| **Crescimento da carteira com qualidade** | Limite e renovação guiados pelo share do lojista na registradora | Mais renovação para quem concentra, menos exposição para quem dispersa |
| **Churn / base ativa / TPV** | Retenção dirigida aos **440** lojistas que importam, não a 15 mil | Recuperar 15-25% do TPV sinalizado × take rate |
| **Take rate** | Oferta de retenção calibrada (não desconto generalizado) | Proteger margem: desconto só onde o risco é real |

> Na demo: **R$ 7,1 mi** de valor em risco (R$ 3,6 mi de crédito + R$ 3,5 mi de receita anual) concentrados em **440 de 15 mil lojistas** (2,9% da base), sendo 245 críticos; R$ 10,2 mi de saldo devedor em alerta. O backtest sinalizou 92% do saldo que virou inadimplência.

### Para o dono do domínio (Head de Risco / Canais)
- Uma **fila única** para Crédito e Comercial, priorizada em R$, com o **porquê** e o **o que fazer**.
- **Produtividade do hub**: o agente sai com 10 visitas certas, não com uma lista de 300 lojistas.
- **Medição de ROI**: cada ação registrada no Lakebase → "valor protegido pelo campo".

### Para a liderança técnica (CDO / Dados)
- Uma plataforma: ingestão, qualidade, governança, ML, GenAI, OLTP, NL-query e app, **sem copiar dado entre ferramentas**.
- Governança na origem: máscaras e tags no UC valem para Genie, App e notebooks igualmente.
- Portável e versionado (Asset Bundle, CI/CD).

### Próximos passos (proposta de 90 dias)
1. **Semanas 1-3:** conectar 3 fontes reais (autorizador, registradora, carteira de crédito) no Lakeflow; definições oficiais no UC.
2. **Semanas 4-8:** recalibrar os modelos com histórico real; piloto com 2 hubs (grupo de controle × grupo tratado).
3. **Semanas 9-12:** medir o delta de inadimplência e TPV retido no piloto → business case para rollout.

---

## 4. Leitura da sala — respostas por altitude

| Pergunta provável | Quem pergunta | Resposta curta (executiva) | Detalhe (técnico) |
|---|---|---|---|
| "Quanto isso vale?" | CFO | "No piloto medimos isso com grupo de controle; a ordem de grandeza é de dezenas de bps de custo de risco sobre a carteira." | Backtest out-of-time, recall e antecedência em `backtest_radar`; valor = prob × exposição |
| "Já temos modelo de crédito." | Risco | "Este não substitui a concessão; ele monitora o contrato **depois** da concessão, onde o sinal é o fluxo de recebíveis." | Features: share na registradora, cobertura da trava, Δ Pix, atraso; rótulo = >60 dias em 13 semanas |
| "Os modelos não estão bons demais?" | Dados | "É dado sintético, então os números ilustram o método, não prometem resultado. O que importa é o desenho." | Validação out-of-time; elegibilidade só para eventos **novos**; precisão no top 5% de **~19%** vs taxa base de **~1,1-1,2%** (lift de ~16x); AUC out-of-time de 0,92 (vazamento) e 0,97 (inadimplência) |
| "E LGPD / dados pessoais?" | Jurídico/Dados | "PII nunca sai da camada governada; o Genie e o App não veem CNPJ/CPF." | Column masks como funções UC, tags PII, grants só na gold, lineage |
| "Por que Lakebase e não o Postgres que já temos?" | Arquitetura | "Ele é sincronizado do lakehouse sem pipeline de cópia, escala a zero e é governado no mesmo lugar." | Synced table (CDF, triggered), OAuth do UC, branching para dev/teste |
| "Vocês substituem nossa plataforma de GenAI (gateway em AWS)?" | CDO | "Não. Os dois convivem: o Databricks é a camada de dados e de IA *sobre os dados*." | `ai_query` com qualquer endpoint (Foundation Models, externos); o gateway pode chamar o Genie/Serving |
| "Quanto custa rodar?" | Dados/FinOps | "Tudo serverless, pago pelo uso; a jornada inteira roda em minutos por semana." | Pipeline serverless, job semanal, Lakebase com autoscaling e scale-to-zero |
| "E se o lojista só está numa fase ruim?" | Comercial | "Por isso diferenciamos **desvio** (concorrente sobe) de **queda de faturamento** (tudo cai). A ação é diferente." | `tipo_sinal`; os dados simulam quedas temporárias e recuperação (falsos positivos reais) |

### Como lidar com objeção (estrutura)
1. **Valide**: "Faz todo sentido se preocupar com isso."
2. **Reenquadre** para o KPI: "O que estamos tentando mover é o custo de risco…"
3. **Evidência**: mostre algo na tela (SQL do Genie, lineage, backtest).
4. **Próximo passo**: "Isso é exatamente o que o piloto de 2 hubs mede."
