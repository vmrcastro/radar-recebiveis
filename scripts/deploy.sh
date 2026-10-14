#!/usr/bin/env bash
# Deploy end-to-end do Radar de Recebíveis em qualquer workspace.
#
#   ./scripts/deploy.sh [target] [--var="catalog=main" ...]
#
# Fase 1: infraestrutura de dados (UC, Lakeflow, Jobs, Lakebase) + execução da jornada até os alertas
# Fase 2: camada de consumo que depende das tabelas existirem (synced table, Genie, App)
# Fase 3: publica no Lakebase, aplica governança/grants e sobe o código do App
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="${1:-dev}"; shift || true
EXTRA=("$@")
B=(databricks bundle -t "$TARGET")

echo "▶ Validando bundle ($TARGET)"
"${B[@]}" validate "${EXTRA[@]}" >/dev/null

echo "▶ Fase 1 — deploy da infraestrutura de dados"
"${B[@]}" deploy --auto-approve "${EXTRA[@]}" \
  --select schemas.radar_schema,volumes.landing,experiments.radar_experimento,pipelines.radar_pipeline,jobs.radar_jornada,postgres_projects.radar_lakebase

echo "▶ Fase 1 — dados sintéticos → Lakeflow → ML → GenAI"
"${B[@]}" run radar_jornada "${EXTRA[@]}" \
  --only governanca_funcoes,gerar_dados_sinteticos,lakeflow_pipeline,ml_treinar_e_pontuar,genai_alertas

echo "▶ Fase 2 — deploy completo (synced table Lakebase, Genie, App)"
"${B[@]}" deploy --auto-approve "${EXTRA[@]}"

echo "▶ Fase 3 — governança + publicação no Lakebase (grants para o App)"
"${B[@]}" run radar_jornada "${EXTRA[@]}" --only governanca_tags_e_acessos,lakebase_publicar

echo "▶ Fase 3 — subindo o App"
"${B[@]}" run radar_app "${EXTRA[@]}"

echo "✅ Pronto. Resumo dos recursos:"
"${B[@]}" summary "${EXTRA[@]}"
