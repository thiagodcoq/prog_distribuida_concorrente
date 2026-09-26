#!/usr/bin/env bash
# ==============================================================================
# T4: Comportamento do sistema sob carga (Objetivo 5 do enunciado)
#
# Injeta um surto de mensagens JSON válidas no tópico principal com o
# kafka-producer-perf-test (acks=all, replicação 3) e acompanha o lag do consumer
# group até ser absorvido. Registra vazão, latência e a curva de lag.
# Uso: ./scripts/test_carga.sh [registros]   (padrão 100000)
# Pré-requisito: make up
# ==============================================================================
RECORDS="${1:-100000}"
SCRIPT_ARGS="registros=$RECORDS"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

init_log "T4_carga.log" "T4: COMPORTAMENTO SOB CARGA ($RECORDS mensagens)"
PERF_OUT="$LOG_DIR/T4_perf_test.out"

step "Estado antes da carga"
lag_low() { [ "$(total_lag)" -le 5 ]; }
wait_until 120 lag_low
log "    membros: $(group_member_count) | lag inicial: $(total_lag)"
check "Grupo com $TOPIC_PARTITIONS consumidores antes da carga" [ "$(group_member_count)" -eq "$TOPIC_PARTITIONS" ]
CONSUMED_BEFORE=$(total_committed)

step "Injetando $RECORDS mensagens (throughput máximo, acks=all)"
START=$(now)
run_perf_test "$(live_broker)" "$KAFKA_TOPIC" "$RECORDS" -1 >"$PERF_OUT" 2>&1 &
PERF_PID=$!

step "Curva de lag do grupo (amostra a cada ~2 s)"
MAX_LAG=0
DRAINED=0
log "    t(s)  lag_total"
while true; do
    lag=$(total_lag)
    t=$(($(now) - START))
    log "    $(printf '%4d' "$t")  $lag"
    [ "$lag" -gt "$MAX_LAG" ] && MAX_LAG=$lag
    if ! kill -0 "$PERF_PID" 2>/dev/null && [ "$lag" -le 5 ]; then DRAINED=1; break; fi
    [ "$t" -ge 600 ] && break
    sleep 2
done
wait "$PERF_PID"
PERF_RC=$?
DRAIN_S=$(($(now) - START))

step "Resultado do kafka-producer-perf-test"
tail -2 "$PERF_OUT" | block
throughput=$(grep "records sent" "$PERF_OUT" | tail -1 | sed -n 's/.*, \([0-9.]*\) records\/sec.*/\1/p' | cut -d. -f1)
log "    vazão de produção: ${throughput:-?} msg/s | lag máximo: $MAX_LAG | tempo até lag ≈ 0: ${DRAIN_S}s"

check "perf-test concluiu sem erro (exit $PERF_RC)" [ "$PERF_RC" -eq 0 ]
check "Vazão de produção com acks=all e R=3 acima de 1000 msg/s (${throughput:-0} msg/s)" [ "${throughput:-0}" -gt 1000 ]
check "Lag absorvido até <= 5 mensagens em ${DRAIN_S}s" [ "$DRAINED" -eq 1 ]
CONSUMED=$(($(total_committed) - CONSUMED_BEFORE))
log "    mensagens consumidas (offsets confirmados) durante o teste: $CONSUMED (injetadas: $RECORDS + leituras dos sensores)"
check "Todas as $RECORDS mensagens injetadas foram consumidas (consumidas: $CONSUMED)" [ "$CONSUMED" -ge $((RECORDS - 5)) ]
log "    (o lag máximo amostrado depende de o consumidor acompanhar ou não a produção; é informativo, não critério)"
check "Grupo permaneceu com $TOPIC_PARTITIONS consumidores durante a carga" [ "$(group_member_count)" -eq "$TOPIC_PARTITIONS" ]

step "Estado final"
describe_group | block

finish
