#!/usr/bin/env bash
# T7: Limite de tolerância a falhas: 2 de 3 brokers fora
#
# O cluster tolera a queda de UM broker (quórum KRaft 2 de 3; min.insync.replicas=2).
# Este teste mostra o LIMITE: com dois brokers fora não há quórum nem ISR suficiente,
# então escritas com acks=all falham (o sistema prefere indisponibilidade a perder
# consistência). Depois religa os brokers e verifica a recuperação completa.
# Serve de evidência para a seção "o que não funcionou / limitações" do relatório.
# Uso: ./scripts/test_dois_brokers_fora.sh
# Pré-requisito: make up
OUTAGE_S="${OUTAGE_S:-50}"
SCRIPT_ARGS="indisponibilidade=${OUTAGE_S}s"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

init_log "T7_dois_brokers_fora.log" "T7: LIMITE DE TOLERÂNCIA: 2 DE 3 BROKERS FORA"

restore() {
    local n
    for n in kafka-1 kafka-2 kafka-3; do broker_running "$n" || start_container "smartfactory-$n"; done
}
trap restore EXIT

step "Pré-condições"
all_isr_equal() {
    [ "$(partition_table | wc -l | tr -d ' ')" -eq "$TOPIC_PARTITIONS" ] &&
        [ "$(partition_table | awk -v n="$1" '$3 != n' | wc -l | tr -d ' ')" -eq 0 ]
}
check "ISR completo antes da falha" all_isr_equal "$TOPIC_REPLICATION_FACTOR"
SURVIVOR="kafka-1"
T0_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)

step "Derrubando kafka-2 e kafka-3 (SIGKILL); sobrevivente: $SURVIVOR"
hard_kill smartfactory-kafka-2
hard_kill smartfactory-kafka-3
log "    $(date -u +%H:%M:%S) 2 de 3 brokers fora do ar"
sleep 15

step "Tentativa de escrita com acks=all e min.insync.replicas=2 (esperado: FALHAR)"
run_perf_test "$SURVIVOR" "$KAFKA_TOPIC" 20 5 \
    request.timeout.ms=8000 delivery.timeout.ms=15000 max.block.ms=15000 >"$LOG_DIR/T7_perf_sem_quorum.out" 2>&1
tail -3 "$LOG_DIR/T7_perf_sem_quorum.out" | cut -c1-200 | block
# O perf-test devolve exit 0 mesmo quando todos os envios expiram; o que vale é a contagem de registros enviados.
SENT=$(grep "records sent" "$LOG_DIR/T7_perf_sem_quorum.out" | tail -1 | awk '{print $1}')
check "Escrita com acks=all é REJEITADA sem quórum/ISR suficiente (registros confirmados: ${SENT:-?} de 20)" [ "${SENT:-1}" -eq 0 ]

step "Mantendo a indisponibilidade por ${OUTAGE_S}s"
sleep "$OUTAGE_S"
check "Produtores dos sensores continuam em execução (não crasham)" \
    [ "$(docker ps --filter 'name=smartfactory-producer-' --filter status=running -q | wc -l | tr -d ' ')" -eq 4 ]
check "Consumidores continuam em execução" [ "$(consumers_running)" -eq 3 ]
dropped=$(docker compose logs --since "$T0_ISO" producer-linha-producao producer-refrigeracao \
    producer-empacotamento producer-fundicao 2>/dev/null | grep -c "Erro assíncrono")
log "    LIMITAÇÃO OBSERVADA: lotes de telemetria descartados pelos sensores durante a indisponibilidade total: $dropped"
log "    (os produtores Python só guardam em buffer por request.timeout.ms = ${PRODUCER_REQUEST_TIMEOUT_MS} ms)"

step "Religando kafka-2 e kafka-3"
start_container smartfactory-kafka-2
start_container smartfactory-kafka-3
brokers_healthy() {
    local n
    for n in 1 2 3; do
        [ "$(docker inspect -f '{{.State.Health.Status}}' "smartfactory-kafka-$n" 2>/dev/null)" = "healthy" ] || return 1
    done
}
check "3 brokers saudáveis novamente" wait_until 180 brokers_healthy
isr_full() { all_isr_equal "$TOPIC_REPLICATION_FACTOR"; }
if wait_until 180 isr_full; then
    pass "ISR completo restabelecido em ${ELAPSED}s"
else
    fail "ISR não voltou ao normal"
fi
describe_topic | block

step "Sistema retoma sozinho"
BEFORE=$(total_end_offsets)
run_perf_test "$(live_broker)" "$KAFKA_TOPIC" 20 5 >"$LOG_DIR/T7_perf_recuperado.out" 2>&1
SENT=$(grep "records sent" "$LOG_DIR/T7_perf_recuperado.out" | tail -1 | awk '{print $1}')
check "Escrita com acks=all volta a funcionar (registros confirmados: ${SENT:-?} de 20)" [ "${SENT:-0}" -eq 20 ]
sensors_flow() { [ "$(total_end_offsets)" -gt "$((BEFORE + 25))" ]; }
check "Sensores voltam a gravar no tópico" wait_until 90 sensors_flow
consumers_ok() { [ "$(group_member_count)" -eq 3 ] && [ "$(total_lag)" -le 30 ]; }
check "3 consumidores no grupo e lag <= 30" wait_until 120 consumers_ok
describe_group | block

finish
