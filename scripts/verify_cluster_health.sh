#!/usr/bin/env bash
# ==============================================================================
# Diagnóstico e verificação de saúde do cluster SmartFactory (make health)
#
# Somente leitura: mostra containers, quórum KRaft, tópico (líderes/ISR), consumer
# group (distribuição e lag) e um resumo do arquivo compartilhado de alertas.
# Não faz asserções (para isso, veja test_cluster_inicial.sh).
# ==============================================================================
SCRIPT_ARGS="$*"
source "$(dirname "$0")/lib.sh"

init_log "cluster_health.log" "DIAGNÓSTICO DE SAÚDE DO CLUSTER SMARTFACTORY"

step "1. Containers"
docker compose ps --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}' | block

step "2. Quórum KRaft (controllers)"
quorum_status | block

step "3. Tópico $KAFKA_TOPIC (líderes, réplicas e ISR)"
describe_topic | block

step "4. Consumer group $KAFKA_GROUP_ID (distribuição e lag)"
describe_group | block
describe_members | block

step "5. Alertas persistidos ($ALERT_LOG_PATH)"
if docker compose exec -T consumer test -f "$ALERT_LOG_PATH" </dev/null 2>/dev/null; then
    total=$(docker compose exec -T consumer sh -c "wc -l < $ALERT_LOG_PATH" </dev/null 2>/dev/null | tr -d ' \r')
    critical=$(docker compose exec -T consumer grep -c '"severity": "CRITICAL"' "$ALERT_LOG_PATH" </dev/null 2>/dev/null | tr -d '\r' || true)
    warning=$(docker compose exec -T consumer grep -c '"severity": "WARNING"' "$ALERT_LOG_PATH" </dev/null 2>/dev/null | tr -d '\r' || true)
    log "    Total de anomalias registradas: ${total:-0}"
    log "      - CRITICAL: ${critical:-0}"
    log "      - WARNING : ${warning:-0}"
    log "    Últimos 3 alertas:"
    docker compose exec -T consumer tail -n 3 "$ALERT_LOG_PATH" </dev/null | cut -c1-240 | block
else
    log "    Arquivo de alertas ainda não gerado."
fi

log "\nDiagnóstico concluído. Log salvo em: $LOG_FILE"
