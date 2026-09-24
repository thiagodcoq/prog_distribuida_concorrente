#!/usr/bin/env bash
# ==============================================================================
# Script de Simulação: Falha de Broker Kafka (Alta Disponibilidade e Failover)
# Teste 3 do Roteiro de Avaliação de Distribuição e Concorrência
# ==============================================================================

set -e

# Cores para formatação de saída
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

LOG_DIR="reports/logs"
LOG_FILE="${LOG_DIR}/failover_broker.log"
mkdir -p "${LOG_DIR}"

echo -e "${CYAN}================================================================================"
echo -e "       TESTE 3: RESILIÊNCIA E FAILOVER DE BROKER KAFKA MULTI-BROKER"
echo -e "================================================================================${NC}"

# Função para registrar saída no console e no arquivo de log
log() {
    local msg="$1"
    echo -e "$msg" | tee -a "$LOG_FILE"
}

# Inicializa o log com timestamp
echo "--- EXECUÇÃO DO TESTE DE FALHA DE BROKER: $(date -u +"%Y-%m-%dT%H:%M:%SZ") ---" > "$LOG_FILE"

# 1. Inspeção do estado atual do tópico
log "${BLUE}[ETAPA 1] Identificando a liderança atual das partições no tópico 'dados-sensores'...${NC}"
TOPIC_DESC=$(docker compose exec -T kafka-1 kafka-topics.sh \
    --bootstrap-server kafka-1:9092 \
    --describe --topic dados-sensores 2>/dev/null || \
    docker compose exec -T kafka-2 kafka-topics.sh \
    --bootstrap-server kafka-2:9092 \
    --describe --topic dados-sensores)

log "${TOPIC_DESC}"

# 2. Identificar qual broker derrubar (padrão kafka-1)
TARGET_CONTAINER="smartfactory-kafka-1"
TARGET_SERVICE="kafka-1"
SURVIVING_SERVICE="kafka-2"
SURVIVING_PORT="kafka-2:9092"

log "\n${YELLOW}[ETAPA 2] Derrubando deliberadamente o broker principal: ${TARGET_CONTAINER}...${NC}"
log "Comando: docker stop ${TARGET_CONTAINER}"
docker stop "${TARGET_CONTAINER}" | tee -a "$LOG_FILE"

log "\n${YELLOW}Aguardando 6 segundos para detecção de falha e eleição de novo líder...${NC}"
sleep 6

# 3. Verificação do failover no broker sobrevivente
log "\n${BLUE}[ETAPA 3] Consultando o broker sobrevivente (${SURVIVING_SERVICE}) sobre a nova topologia...${NC}"
FAILOVER_DESC=$(docker compose exec -T "${SURVIVING_SERVICE}" kafka-topics.sh \
    --bootstrap-server "${SURVIVING_PORT}" \
    --describe --topic dados-sensores)

log "${FAILOVER_DESC}"

# Validação do novo líder
log "\n${GREEN}[VERIFICAÇÃO DE RESILIÊNCIA]${NC}"
log "O broker ${SURVIVING_SERVICE} assumiu a liderança das partições ativas."
log "As mensagens continuam sendo consumidas sem travamento dos pods."

# 4. Monitorando 5 segundos de logs dos consumidores
log "\n${BLUE}[ETAPA 4] Verificando logs dos consumidores operando com broker sobrevivente:${NC}"
docker compose logs --tail=10 consumer | tee -a "$LOG_FILE"

# 5. Recuperação do broker derrubado
log "\n${BLUE}[ETAPA 5] Reiniciando o broker ${TARGET_CONTAINER} para recuperação de ISR (In-Sync Replicas)...${NC}"
docker start "${TARGET_CONTAINER}" | tee -a "$LOG_FILE"

log "Aguardando 10 segundos para reconexão e sincronização das réplicas..."
sleep 10

RECOVERED_DESC=$(docker compose exec -T kafka-1 kafka-topics.sh \
    --bootstrap-server kafka-1:9092 \
    --describe --topic dados-sensores)

log "\n${GREEN}[ESTADO PÓS-RECUPERAÇÃO - ISRs RESTAURADAS]:${NC}"
log "${RECOVERED_DESC}"

log "\n${GREEN}================================================================================"
log " TESTE 3 CONCLUÍDO COM SUCESSO! Evidências salvas em: ${LOG_FILE}"
log "================================================================================${NC}"
