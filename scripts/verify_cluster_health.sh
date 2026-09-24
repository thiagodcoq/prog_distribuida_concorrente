#!/usr/bin/env bash
# ==============================================================================
# Script de Diagnóstico e Verificação de Saúde do Cluster SmartFactory
# ==============================================================================

set -e

# Cores
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

LOG_DIR="reports/logs"
LOG_FILE="${LOG_DIR}/cluster_health.log"
mkdir -p "${LOG_DIR}"

log() {
    local msg="$1"
    echo -e "$msg" | tee -a "$LOG_FILE"
}

echo -e "${CYAN}================================================================================"
echo -e "       RELATÓRIO DE SAÚDE E STATUS DO CLUSTER KAFKA SMARTFACTORY"
echo -e "================================================================================${NC}"
echo "--- DIAGNÓSTICO DO CLUSTER: $(date -u +"%Y-%m-%dT%H:%M:%SZ") ---" > "$LOG_FILE"

# 1. Status dos Containers Docker
log "${BLUE}[1. STATUS DOS CONTAINERS EM EXECUÇÃO]${NC}"
docker compose ps | tee -a "$LOG_FILE"

# 2. Descrição do Tópico de Sensores
log "\n${BLUE}[2. DETALHES DO TÓPICO 'dados-sensores' (Líderes, Réplicas e ISRs)]${NC}"
docker compose exec -T kafka-1 kafka-topics.sh \
    --bootstrap-server kafka-1:9092 \
    --describe --topic dados-sensores 2>/dev/null || \
docker compose exec -T kafka-2 kafka-topics.sh \
    --bootstrap-server kafka-2:9092 \
    --describe --topic dados-sensores | tee -a "$LOG_FILE"

# 3. Estado do Consumer Group
log "\n${BLUE}[3. DISTRIBUIÇÃO E LAG DO CONSUMER GROUP 'smartfactory-processors']${NC}"
docker compose exec -T kafka-1 kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:9092 \
    --describe --group smartfactory-processors 2>/dev/null || \
docker compose exec -T kafka-2 kafka-consumer-groups.sh \
    --bootstrap-server kafka-2:9092 \
    --describe --group smartfactory-processors | tee -a "$LOG_FILE"

# 4. Resumo de Alertas Registrados no Volume Compartilhado
log "\n${BLUE}[4. RESUMO DO ARQUIVO PERSISTENTE DE ALERTAS (/var/log/smartfactory/alerts.log)]${NC}"
if docker compose exec -T consumer test -f /var/log/smartfactory/alerts.log 2>/dev/null; then
    TOTAL_ALERTS=$(docker compose exec -T consumer wc -l < /var/log/smartfactory/alerts.log | tr -d '\r')
    CRITICAL_COUNT=$(docker compose exec -T consumer grep -c '"severity": "CRITICAL"' /var/log/smartfactory/alerts.log 2>/dev/null || echo 0)
    WARN_COUNT=$(docker compose exec -T consumer grep -c '"severity": "WARNING"' /var/log/smartfactory/alerts.log 2>/dev/null || echo 0)
    
    log "Total de anomalias registradas : ${TOTAL_ALERTS}"
    log "  - Alertas CRÍTICOS          : ${CRITICAL_COUNT}"
    log "  - Alertas de AVISO (WARNING): ${WARN_COUNT}"
    log "\nÚltimos 3 alertas registrados:"
    docker compose exec -T consumer tail -n 3 /var/log/smartfactory/alerts.log | tee -a "$LOG_FILE"
else
    log "${YELLOW}Arquivo de alertas ainda não gerado ou nenhum alerta registrado até o momento.${NC}"
fi

log "\n${GREEN}================================================================================"
log " Diagnóstico finalizado com sucesso! Log salvo em: ${LOG_FILE}"
log "================================================================================${NC}"
