#!/usr/bin/env bash
# ==============================================================================
# T3: Falha de broker Kafka e failover (Objetivo 4 do enunciado)
#
# Uso: ./scripts/simulate_broker_failure.sh [graceful|hard] [controller|follower]
#   graceful : docker stop (desligamento controlado do broker)
#   hard     : docker kill (queda abrupta, sem desligamento controlado)
#   controller: derruba o líder do quórum KRaft (padrão); follower: derruba outro broker.
#
# Verifica com asserções: novo líder do quórum, líderes de partição realocados, ISR
# reduzido a 2, sensores e consumidores seguindo em frente e, com carga contínua
# (kafka-producer-perf-test, acks=all + idempotência) num tópico exclusivo, que
# NENHUMA mensagem foi perdida nem duplicada durante a queda. Depois religa o broker
# e confere a ressincronização do ISR.
# Pré-requisito: make up
# ==============================================================================
MODE="${1:-graceful}"
TARGET_KIND="${2:-controller}"
LOSS_TOPIC="${LOSS_TOPIC:-teste-perda}"
LOSS_RECORDS="${LOSS_RECORDS:-1500}"
LOSS_RATE="${LOSS_RATE:-50}"
SCRIPT_ARGS="modo=$MODE alvo=$TARGET_KIND"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

init_log "T3_falha_broker_${MODE}_${TARGET_KIND}.log" "T3: FALHA DE BROKER KAFKA (modo: $MODE, alvo: $TARGET_KIND)"
PERF_OUT="$LOG_DIR/T3_perf_${MODE}_${TARGET_KIND}.out"

VICTIM=""
restore() { [ -n "$VICTIM" ] && ! broker_running "$VICTIM" && start_container "smartfactory-$VICTIM"; }
trap restore EXIT

all_isr_equal() { # all_isr_equal <n>
    local n="$1"
    [ "$(partition_table | wc -l | tr -d ' ')" -eq "$TOPIC_PARTITIONS" ] &&
        [ "$(partition_table | awk -v n="$n" '$3 != n' | wc -l | tr -d ' ')" -eq 0 ]
}

step "Pré-condições"
check "3 brokers em execução" [ "$(for n in 1 2 3; do broker_running kafka-$n && echo x; done | wc -l | tr -d ' ')" -eq 3 ]
check "ISR completo em todas as partições" all_isr_equal "$TOPIC_REPLICATION_FACTOR"
describe_topic | block
quorum_status | block
LEADER0=$(quorum_field LeaderId)
EPOCH0=$(quorum_field LeaderEpoch)
log "    Líder do quórum KRaft antes da falha: broker $LEADER0 (epoch $EPOCH0)"

if [ "$TARGET_KIND" = "controller" ]; then
    VICTIM_ID="$LEADER0"
else
    VICTIM_ID=$(for n in 1 2 3; do [ "$n" != "$LEADER0" ] && echo "$n"; done | head -1)
fi
VICTIM="kafka-$VICTIM_ID"
RUNNER=$(live_broker "$VICTIM")
log "    Broker a derrubar: $VICTIM | perf-test executa em: $RUNNER"

step "Preparando tópico de verificação de perda ($LOSS_TOPIC, P=$TOPIC_PARTITIONS, R=$TOPIC_REPLICATION_FACTOR)"
kafka_tool kafka-topics.sh --create --if-not-exists --topic "$LOSS_TOPIC" \
    --partitions "$TOPIC_PARTITIONS" --replication-factor "$TOPIC_REPLICATION_FACTOR" | block
LOSS_BEFORE=$(total_end_offsets "$LOSS_TOPIC")
MAIN_BEFORE=$(total_end_offsets)
log "    end offsets de $LOSS_TOPIC antes: $LOSS_BEFORE"

step "Iniciando carga contínua: $LOSS_RECORDS msgs a $LOSS_RATE msg/s (acks=all, idempotente)"
run_perf_test "$RUNNER" "$LOSS_TOPIC" "$LOSS_RECORDS" "$LOSS_RATE" \
    enable.idempotence=true delivery.timeout.ms=120000 request.timeout.ms=30000 >"$PERF_OUT" 2>&1 &
PERF_PID=$!
sleep 8
MAIN_AT_KILL=$(total_end_offsets)

step "Derrubando $VICTIM (modo $MODE)"
T0=$(now)
KILL_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
stop_or_kill "smartfactory-$VICTIM" "$MODE"
log "    $(date -u +%H:%M:%S) $VICTIM fora do ar"

step "Failover das lideranças de partição"
leaders_moved() {
    [ "$(partition_table | wc -l | tr -d ' ')" -eq "$TOPIC_PARTITIONS" ] &&
        [ "$(partition_table | awk -v v="$VICTIM_ID" '$2 == v || $2 == -1' | wc -l | tr -d ' ')" -eq 0 ]
}
if wait_until 90 leaders_moved; then
    pass "Nenhuma partição lidera no broker caído; novos líderes eleitos em ${ELAPSED}s"
else
    fail "Lideranças não foram realocadas em 90 s"
fi
isr_shrunk() { all_isr_equal $((TOPIC_REPLICATION_FACTOR - 1)); }
if wait_until 60 isr_shrunk; then
    pass "ISR reduzido a $((TOPIC_REPLICATION_FACTOR - 1)) réplicas em todas as partições (>= min.insync.replicas=$TOPIC_MIN_INSYNC_REPLICAS)"
else
    fail "ISR não convergiu para $((TOPIC_REPLICATION_FACTOR - 1)) réplicas"
fi
describe_topic | block

step "Quórum KRaft com o broker fora"
new_leader_ok() {
    local l
    l=$(quorum_field LeaderId)
    [ -n "$l" ] && [ "$l" != "-1" ] && [ "$l" != "$VICTIM_ID" ]
}
if wait_until 60 new_leader_ok; then
    LEADER1=$(quorum_field LeaderId)
    EPOCH1=$(quorum_field LeaderEpoch)
    pass "Quórum mantém líder após a queda: broker $LEADER1 (epoch $EPOCH1) com 2 de 3 votantes"
    if [ "$TARGET_KIND" = "controller" ]; then
        election_happened() { [ "$LEADER1" != "$LEADER0" ] && [ "$EPOCH1" -gt "$EPOCH0" ]; }
        check "Nova eleição: líder mudou ($LEADER0 -> $LEADER1) e epoch aumentou ($EPOCH0 -> $EPOCH1)" election_happened
    fi
else
    fail "Quórum sem líder válido após a queda"
fi
quorum_status | block

step "Sensores e consumidores continuam funcionando durante a falha"
sensors_flow() { [ "$(total_end_offsets)" -gt "$((MAIN_AT_KILL + 4))" ]; }
if wait_until 60 sensors_flow; then
    pass "Produtores seguem gravando em $KAFKA_TOPIC durante a falha (offsets totais: $MAIN_AT_KILL -> $(total_end_offsets))"
else
    fail "Produtores pararam de gravar em $KAFKA_TOPIC"
fi
check "4 produtores continuam em execução (nenhum caiu)" \
    [ "$(docker ps --filter 'name=smartfactory-producer-' --filter status=running -q | wc -l | tr -d ' ')" -eq 4 ]
consumers_ok() { [ "$(group_member_count)" -eq 3 ] && [ "$(total_lag)" -le 30 ]; }
check "3 consumidores no grupo e lag <= 30" wait_until 90 consumers_ok
describe_group | block

step "Aguardando o fim da carga contínua (perf-test)"
wait "$PERF_PID"
PERF_RC=$?
tail -3 "$PERF_OUT" | block
check "perf-test terminou com sucesso (exit $PERF_RC): todas as mensagens confirmadas com acks=all" [ "$PERF_RC" -eq 0 ]
LOSS_AFTER=$(total_end_offsets "$LOSS_TOPIC")
DELTA=$((LOSS_AFTER - LOSS_BEFORE))
log "    end offsets de $LOSS_TOPIC depois: $LOSS_AFTER (delta = $DELTA; enviadas = $LOSS_RECORDS)"
check "Zero perda e zero duplicação: delta de offsets ($DELTA) == mensagens enviadas ($LOSS_RECORDS)" [ "$DELTA" -eq "$LOSS_RECORDS" ]

step "Integridade da telemetria dos sensores durante a falha"
dropped=$(docker compose logs --since "$KILL_ISO" producer-linha-producao producer-refrigeracao \
    producer-empacotamento producer-fundicao 2>/dev/null | grep -c "Erro assíncrono")
log "    Lotes descartados pelos produtores dos sensores desde $KILL_ISO: $dropped"
check "Nenhuma leitura de sensor descartada (0 erros assíncronos de envio)" [ "$dropped" -eq 0 ]

step "Recuperando $VICTIM"
start_container "smartfactory-$VICTIM"
isr_full() { all_isr_equal "$TOPIC_REPLICATION_FACTOR"; }
if wait_until 120 isr_full; then
    pass "ISR completo novamente (${TOPIC_REPLICATION_FACTOR} réplicas) em ${ELAPSED}s após religar $VICTIM"
else
    fail "ISR não voltou ao normal em 120 s"
fi
describe_topic | block
victim_healthy() { [ "$(docker inspect -f '{{.State.Health.Status}}' "smartfactory-$VICTIM")" = "healthy" ]; }
check "$VICTIM saudável novamente (healthcheck)" wait_until 60 victim_healthy
quorum_status | block

finish
