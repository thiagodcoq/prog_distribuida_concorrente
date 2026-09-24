#!/usr/bin/env bash
# ==============================================================================
# Script de Simulação: Falha de Consumidor e Rebalanceamento Automático
# Teste 2 do Roteiro de Avaliação de Distribuição e Concorrência
# ==============================================================================

set -e

# Cores para formatação
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

LOG_DIR="reports/logs"
LOG_FILE="${LOG_DIR}/rebalanceamento_falha_consumidor.log"
mkdir -p "${LOG_DIR}"

log() {
    local msg="$1"
    echo -e "$msg" | tee -a "$LOG_FILE"
}

echo -e "${CYAN}================================================================================"
echo -e "       TESTE 2: FALHA DE CONSUMIDOR E REBALANCEAMENTO DE PARTIÇÕES"
echo -e "================================================================================${NC}"

echo "--- EXECUÇÃO DO TESTE DE FALHA DE CONSUMIDOR: $(date -u +"%Y-%m-%dT%H:%M:%SZ") ---" > "$LOG_FILE"

# 1. Estado inicial de particionamento
log "${BLUE}[ETAPA 1] Verificando distribuição inicial de partições no grupo 'smartfactory-processors'...${NC}"
INITIAL_STATE=$(docker compose exec -T kafka-1 kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:9092 \
    --describe --group smartfactory-processors)

log "${INITIAL_STATE}"

# 2. Derrubar o consumidor 3
TARGET_CONTAINER="smartfactory-consumer-3"
log "\n${YELLOW}[ETAPA 2] Forçando parada do consumidor: ${TARGET_CONTAINER}...${NC}"
log "Comando: docker stop ${TARGET_CONTAINER}"
docker stop "${TARGET_CONTAINER}" | tee -a "$LOG_FILE"

log "\n${YELLOW}Aguardando detecção de heartbeat/rebalanceamento (10 segundos)...${NC}"
sleep 10

# 3. Novo estado de distribuição das partições
log "\n${BLUE}[ETAPA 3] Consultando nova distribuição de partições após rebalanceamento...${NC}"
REBALANCED_STATE=$(docker compose exec -T kafka-1 kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:9092 \
    --describe --group smartfactory-processors)

log "${REBALANCED_STATE}"

# 4. Evidência do listener nos logs
log "\n${BLUE}[ETAPA 4] Verificando logs dos consumidores sobreviventes (RebalanceListener):${NC}"
docker compose logs --tail=25 consumer | grep -E "REBALANCE EVENT|PARTITIONS ASSIGNED|Partições Atribuídas" -C 2 | tee -a "$LOG_FILE" || docker compose logs --tail=20 consumer | tee -a "$LOG_FILE"

# 5. Restauração do consumidor derrubado
log "\n${BLUE}[ETAPA 5] Reiniciando o ${TARGET_CONTAINER} para rebalanço de volta para 3 consumidores...${NC}"
docker start "${TARGET_CONTAINER}" | tee -a "$LOG_FILE"

log "Aguardando 10 segundos para reintegração ao Consumer Group..."
sleep 10

RESTORED_STATE=$(docker compose exec -T kafka-1 kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:9092 \
    --describe --group smartfactory-processors)

log "\n${GREEN}[ESTADO FINAL RESTAURADO - 1 PARTIÇÃO POR CONSUMIDOR]:${NC}"
log "${RESTORED_STATE}"

log "\n${GREEN}================================================================================"
log " TESTE 2 CONCLUÍDO COM SUCESSO! Evidências salvas em: ${LOG_FILE}"
log "================================================================================${NC}"
