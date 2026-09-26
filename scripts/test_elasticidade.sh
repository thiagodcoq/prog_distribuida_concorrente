#!/usr/bin/env bash
# ==============================================================================
# T5: Elasticidade horizontal dos consumidores (Requisito de escala + Objetivo 3)
#
# Com um custo simulado por mensagem (PROCESSING_DELAY_MS), injeta o MESMO lote de
# mensagens (distribuído por round-robin entre as partições) com 1, 2, 3, 4 e 5
# consumidores e mede o tempo para esvaziar o lag. Demonstra que:
#   - mais consumidores reduzem o tempo de processamento até o número de partições;
#   - acima de P consumidores, os excedentes ficam OCIOSOS (sem ganho).
# Uso: ./scripts/test_elasticidade.sh
# Variáveis: ELASTIC_DELAY_MS (padrão 20), ELASTIC_RECORDS (padrão 3000)
# Pré-requisito: make up
# ==============================================================================
DELAY="${ELASTIC_DELAY_MS:-20}"
BATCH="${ELASTIC_RECORDS:-3000}"
SCRIPT_ARGS="PROCESSING_DELAY_MS=$DELAY lote=$BATCH"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

init_log "T5_elasticidade.log" "T5: ELASTICIDADE DE CONSUMIDORES (1 a 5 réplicas, ${DELAY} ms/msg)"

# Sempre com o mesmo valor de PROCESSING_DELAY_MS, para que só o número de réplicas mude
scale_consumers() { PROCESSING_DELAY_MS="$DELAY" docker compose up -d --scale consumer="$1" consumer >/dev/null 2>&1; }
restore() {
    log "\n>>> Restaurando 3 consumidores sem custo simulado"
    docker compose up -d --scale consumer=3 consumer >/dev/null 2>&1
}
trap restore EXIT

lag_low() { [ "$(total_lag)" -le 5 ]; }

for n in 1 2 3 4 5; do
    step "Degrau: $n consumidor(es)"
    scale_consumers "$n"
    members_ok() { [ "$(group_member_count)" -eq "$n" ]; }
    wait_until 120 members_ok
    wait_until 120 lag_low
    describe_members | block
    check "Grupo com $n membro(s)" members_ok
    idle=$(idle_member_count)
    expected_idle=$((n > TOPIC_PARTITIONS ? n - TOPIC_PARTITIONS : 0))
    check "Consumidores ociosos = $expected_idle (obtido: $idle)" [ "$idle" -eq "$expected_idle" ]

    # O cronômetro só começa depois que o lote foi produzido (exclui a partida da JVM do perf-test)
    run_perf_test "$(live_broker)" "$KAFKA_TOPIC" "$BATCH" -1 \
        partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner >/dev/null 2>&1
    START=$(now)
    wait_until 600 lag_low
    t=$(($(now) - START))
    [ "$t" -lt 1 ] && t=1
    eval "DRAIN_$n=$t"
    log "    lote de $BATCH mensagens esvaziado em ${t}s (~$((BATCH / t)) msg/s)"
done

step "Resumo (lote de $BATCH mensagens, ${DELAY} ms de processamento por mensagem)"
log "    consumidores | tempo p/ esvaziar | vazão (msg/s) | comentário"
log "    -------------+-------------------+---------------+------------------------------------------"
log "    1            | ${DRAIN_1}s | ~$((BATCH / DRAIN_1)) | 1 consumidor lê as $TOPIC_PARTITIONS partições"
log "    2            | ${DRAIN_2}s | ~$((BATCH / DRAIN_2)) | um consumidor fica com 2 partições (gargalo)"
log "    3            | ${DRAIN_3}s | ~$((BATCH / DRAIN_3)) | 1 partição por consumidor (paralelismo máximo)"
log "    4            | ${DRAIN_4}s | ~$((BATCH / DRAIN_4)) | 4º consumidor ocioso (hot standby)"
log "    5            | ${DRAIN_5}s | ~$((BATCH / DRAIN_5)) | 4º e 5º ociosos"

check "3 consumidores esvaziam a fila bem mais rápido que 1 (${DRAIN_3}s vs ${DRAIN_1}s, >= 1,8x)" [ $((DRAIN_1 * 10)) -ge $((DRAIN_3 * 18)) ]
two_in_between() { [ "$DRAIN_2" -gt "$DRAIN_3" ] && [ "$DRAIN_2" -lt "$DRAIN_1" ]; }
check "2 consumidores ficam entre 1 e 3 (${DRAIN_3}s < ${DRAIN_2}s < ${DRAIN_1}s)" two_in_between
# Tolerância de 25% para o ruído de medição (resolução de ~2 s por consulta ao grupo)
check "Acima do número de partições não há ganho relevante: 4 consumidores (${DRAIN_4}s) vs 3 (${DRAIN_3}s)" [ $((DRAIN_4 * 100)) -ge $((DRAIN_3 * 75)) ]
check "Acima do número de partições não há ganho relevante: 5 consumidores (${DRAIN_5}s) vs 3 (${DRAIN_3}s)" [ $((DRAIN_5 * 100)) -ge $((DRAIN_3 * 75)) ]

finish
