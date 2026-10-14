# Genie — perguntas em português sobre dados governados

> Coletado automaticamente por `scripts/coletar_evidencias.py` em 2026-10-14 14:06 UTC · workspace `https://fevm-vini.cloud.databricks.com`

- Genie space: **Radar de Recebíveis — Pedregulho Pagamentos** (`01f1c402f1b1160ca739617302d6b292`)

## Pergunta 1

> **Para quais concorrentes estamos perdendo mais recebíveis nas últimas 4 semanas?**

- Status: `COMPLETED` · conversa `01f1c7d8785419ab8fbb5bb197bf22d8` · mensagem `01f1c7d878631a5abb20f4ef936b1039`

**Interpretação:** Você quer saber para quais concorrentes estamos perdendo mais recebíveis nas últimas 4 semanas, comparando o volume atual com a média das 12 semanas anteriores.

**SQL gerado:**

```sql
WITH base AS (
  SELECT
    `credenciadora`,
    SUM(CASE WHEN `semana` BETWEEN DATE'2026-09-07' AND DATE'2026-09-28' THEN `valor_recebiveis` END) AS `ult_4s`,
    try_divide(SUM(CASE WHEN `semana` BETWEEN DATE'2026-06-15' AND DATE'2026-08-31' THEN `valor_recebiveis` END),12) * 4 AS `base_4s`
  FROM `vini_catalog`.`radar_recebiveis`.`gold_concorrencia_semana`
  WHERE `e_pedregulho` = false
    AND `credenciadora` IS NOT NULL
  GROUP BY `credenciadora`
)
SELECT
  `credenciadora`,
  ROUND(`ult_4s`, 0) AS `recebiveis_ult_4s`,
  ROUND(`base_4s`, 0) AS `recebiveis_base_4s`,
  ROUND(`ult_4s` - `base_4s`, 0) AS `ganho_concorrente`,
  ROUND((try_divide(`ult_4s`,`base_4s`) - 1) * 100, 1) AS `crescimento_pct`
FROM base
WHERE `ult_4s` IS NOT NULL
  AND `base_4s` IS NOT NULL
ORDER BY `ganho_concorrente` DESC, `credenciadora` ASC
```

**Resultado:**

| credenciadora | recebiveis_ult_4s | recebiveis_base_4s | ganho_concorrente | crescimento_pct |
|---|---|---|---|---|
| NuvemPay | 89366149 | 86087642 | 3278507.0 | 3.8 |
| PagaLogo | 82001826 | 79102719 | 2899107.0 | 3.7 |
| Maquininha Azul | 49255156 | 46784039 | 2471117.0 | 5.3 |
| Banco Laranja PJ | 39338880 | 37719314 | 1619566.0 | 4.3 |
| MercaPix | 30048885 | 29508629 | 540256.0 | 1.8 |

**Resposta do Genie:**

**5 concorrentes.** Nas últimas 4 semanas (2026-09-07 a 2026-09-28), os maiores ganhos de recebíveis dos concorrentes — e, portanto, onde mais estamos perdendo — foram:
- **NuvemPay:** **R$ 3,3 mi** acima da base, com **+3,8%**
- **PagaLogo:** **R$ 2,9 mi** acima da base, com **+3,7%**
- **Maquininha Azul:** **R$ 2,5 mi** acima da base, com **+5,3%**
- **Banco Laranja PJ:** **R$ 1,6 mi** acima da base, com **+4,3%**
- **MercaPix:** **R$ 540 mil** acima da base, com **+1,8%**
Entre os 5 concorrentes, **NuvemPay** lidera em ganho absoluto de recebíveis, enquanto **Maquininha Azul** teve o maior ritmo de crescimento percentual.

## Pergunta 2 (follow-up na mesma conversa)

> **E em quais segmentos isso está concentrado?**

- Status: `COMPLETED` · conversa `01f1c7d8785419ab8fbb5bb197bf22d8` · mensagem `01f1c7d8887d10ccb4ef6b468a8b7fd1`

**Interpretação:** Você quer ver para quais concorrentes estamos perdendo mais recebíveis nas últimas 4 semanas, detalhado por segmento de atuação.

**SQL gerado:**

```sql
WITH base AS (
  SELECT
    `credenciadora`,
    `segmento`,
    SUM(CASE WHEN `semana` BETWEEN DATE'2026-09-07' AND DATE'2026-09-28' THEN `valor_recebiveis` END) AS `ult_4s`,
    try_divide(SUM(CASE WHEN `semana` BETWEEN DATE'2026-06-15' AND DATE'2026-08-31' THEN `valor_recebiveis` END), 12) * 4 AS `base_4s`
  FROM `vini_catalog`.`radar_recebiveis`.`gold_concorrencia_semana`
  WHERE `e_pedregulho` = false
    AND `credenciadora` IS NOT NULL
    AND `segmento` IS NOT NULL
  GROUP BY `credenciadora`, `segmento`
)
SELECT
  `credenciadora`,
  `segmento`,
  ROUND(`ult_4s`, 0) AS `recebiveis_ult_4s`,
  ROUND(`base_4s`, 0) AS `recebiveis_base_4s`,
  ROUND(`ult_4s` - `base_4s`, 0) AS `ganho_concorrente`,
  ROUND((try_divide(`ult_4s`, `base_4s`) - 1) * 100, 1) AS `crescimento_pct`
FROM base
WHERE `ult_4s` IS NOT NULL
  AND `base_4s` IS NOT NULL
ORDER BY `ganho_concorrente` DESC, `credenciadora` ASC, `segmento` ASC
LIMIT 20
```

**Resultado:**

| credenciadora | segmento | recebiveis_ult_4s | recebiveis_base_4s | ganho_concorrente | crescimento_pct |
|---|---|---|---|---|---|
| NuvemPay | Mercados e Mercearias | 17179360 | 16311459 | 867901.0 | 5.3 |
| NuvemPay | Restaurantes e Bares | 13862538 | 12998388 | 864151.0 | 6.6 |
| PagaLogo | Postos de Combustível | 6798256.0 | 6077420.0 | 720836.0 | 11.9 |
| Banco Laranja PJ | Autopeças e Oficinas | 4453031.0 | 3799136.0 | 653895.0 | 17.2 |
| NuvemPay | Farmácias | 8245713.0 | 7639038.0 | 606675.0 | 7.9 |
| Maquininha Azul | Restaurantes e Bares | 8139707.0 | 7597394.0 | 542313.0 | 7.1 |
| Maquininha Azul | Mercados e Mercearias | 9058726.0 | 8533056.0 | 525670.0 | 6.2 |
| PagaLogo | Pet Shops | 3383035.0 | 2895877.0 | 487159.0 | 16.8 |
| Banco Laranja PJ | Postos de Combustível | 2656167.0 | 2172901.0 | 483266.0 | 22.2 |
| Maquininha Azul | Postos de Combustível | 5037848.0 | 4575159.0 | 462689.0 | 10.1 |
| PagaLogo | Autopeças e Oficinas | 5991262.0 | 5596737.0 | 394525.0 | 7.0 |
| NuvemPay | Postos de Combustível | 7348033.0 | 6980942.0 | 367091.0 | 5.3 |
| MercaPix | Mercados e Mercearias | 4808480.0 | 4456154.0 | 352326.0 | 7.9 |
| PagaLogo | Vestuário e Calçados | 7535160.0 | 7190246.0 | 344914.0 | 4.8 |
| Maquininha Azul | Farmácias | 4917741.0 | 4597007.0 | 320733.0 | 7.0 |
| PagaLogo | Materiais de Construção | 8528052.0 | 8210234.0 | 317818.0 | 3.9 |
| NuvemPay | Vestuário e Calçados | 8091055.0 | 7803477.0 | 287578.0 | 3.7 |
| Maquininha Azul | Vestuário e Calçados | 5182611.0 | 4910998.0 | 271613.0 | 5.5 |
| MercaPix | Restaurantes e Bares | 4970634.0 | 4728014.0 | 242620.0 | 5.1 |
| MercaPix | Materiais de Construção | 2294603.0 | 2071088.0 | 223515.0 | 10.8 |

**Resposta do Genie:**

**Maior concentração:** nas últimas 4 semanas (2026-09-07 a 2026-09-28), os ganhos dos concorrentes aparecem principalmente em **Mercados e Mercearias**, **Restaurantes e Bares** e **Postos de Combustível**.
- **Mercados e Mercearias:** NuvemPay **R$ 867,9 mil**, Maquininha Azul **R$ 525,7 mil** e MercaPix **R$ 352,3 mil**
- **Restaurantes e Bares:** NuvemPay **R$ 864,2 mil**, Maquininha Azul **R$ 542,3 mil** e MercaPix **R$ 242,6 mil**
- **Postos de Combustível:** PagaLogo **R$ 720,8 mil**, Banco Laranja PJ **R$ 483,3 mil**, Maquininha Azul **R$ 462,7 mil** e NuvemPay **R$ 367,1 mil**

Nos 20 resultados exibidos, **Postos de Combustível** é o segmento mais recorrente entre os maiores ganhos de concorrentes, aparecendo em **4** combinações de credenciadora.

## Pergunta 3 (follow-up na mesma conversa)

> **Qual o percentual do saldo com mais de 60 dias de atraso, por segmento?**

- Status: `COMPLETED` · conversa `01f1c7d8785419ab8fbb5bb197bf22d8` · mensagem `01f1c7d898a51666a11b0dd9756b1605`

**Interpretação:** Você quer ver o percentual do saldo de capital de giro que está com mais de 60 dias de atraso, separado por segmento de atuação dos lojistas.

**SQL gerado:**

```sql
SELECT
  `segmento`,
  ROUND(SUM(`saldo_devedor`), 0) AS `saldo_ativo`,
  ROUND(SUM(CASE WHEN `dias_atraso` > 60 THEN `saldo_devedor` ELSE 0 END), 0) AS `saldo_inadimplente_60`,
  ROUND(try_divide(100 * SUM(CASE WHEN `dias_atraso` > 60 THEN `saldo_devedor` ELSE 0 END),SUM(`saldo_devedor`)), 1) AS `inadimplencia_pct`
FROM `vini_catalog`.`radar_recebiveis`.`gold_carteira_credito`
WHERE `faixa_atraso` <> 'Quitado'
  AND `segmento` IS NOT NULL
GROUP BY `segmento`
ORDER BY `inadimplencia_pct` DESC
```

**Resultado:**

| segmento | saldo_ativo | saldo_inadimplente_60 | inadimplencia_pct |
|---|---|---|---|
| Salões e Estética | 7966809.0 | 1435223.0 | 18.0 |
| Padarias e Cafés | 10131045 | 1675873.0 | 16.5 |
| Pet Shops | 5850929.0 | 681174.0 | 11.6 |
| Restaurantes e Bares | 30582378 | 3403922.0 | 11.1 |
| Autopeças e Oficinas | 12554427 | 1362188.0 | 10.9 |
| Vestuário e Calçados | 14018573 | 1198923.0 | 8.6 |
| Mercados e Mercearias | 38448010 | 3174059.0 | 8.3 |
| Farmácias | 19281842 | 1502034.0 | 7.8 |
| Materiais de Construção | 13410759 | 845452.0 | 6.3 |
| Eletrônicos e Celulares | 8484546.0 | 529605.0 | 6.2 |
| Postos de Combustível | 11340034 | 487521.0 | 4.3 |

**Resposta do Genie:**

**18,0%** é o maior percentual de saldo com mais de 60 dias de atraso, em **Salões e Estética**; o menor é **4,3%**, em **Postos de Combustível**.

Exemplos por segmento:
- **Salões e Estética:** **18,0%** (R$ **1,4 mi** de R$ **8,0 mi**)
- **Padarias e Cafés:** **16,5%** (R$ **1,7 mi** de R$ **10,1 mi**)
- **Pet Shops:** **11,6%** (R$ **681,2 mil** de R$ **5,9 mi**)
- **Restaurantes e Bares:** **11,1%** (R$ **3,4 mi** de R$ **30,6 mi**)
- **Autopeças e Oficinas:** **10,9%** (R$ **1,4 mi** de R$ **12,6 mi**)

Entre os 11 segmentos retornados, a inadimplência acima de 60 dias varia de **4,3%** a **18,0%**, com maior pressão nos segmentos de serviços e alimentação.

**Resposta do Genie:**

Você gostaria de ver o percentual do saldo com mais de 60 dias de atraso considerando também os contratos quitados ou segmentado por outra dimensão além do segmento?

