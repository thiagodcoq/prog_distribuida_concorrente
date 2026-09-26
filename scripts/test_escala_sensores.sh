#!/usr/bin/env bash
# ==============================================================================
# T6: Escala de sensores/produtores (Requisito "escale conforme o número de sensores aumenta")
#
# Mede a taxa de mensagens que chegam ao tópico com os 4 sensores originais e depois
# com sensores extras (serviço sensor-extra, escalado com `make scale-sensors`),
# verificando que a vazão cresce, que os novos sensores se espalham pelas partições
# (chave = sensor_id) e que os consumidores acompanham sem acumular lag.
# Uso: ./scripts/test_escala_sensores.sh
# Pré-requisito: make up
# ==============================================================================
WINDOW="${WINDOW_S:-20}"
SCRIPT_ARGS="janela=${WINDOW}s"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

init_log "T6_escala_sensores.log" "T6: ESCALA DE SENSORES (produtores)"

restore() { docker compose --profile scale up -d --no-deps --scale sensor-extra=0 sensor-extra >/dev/null 2>&1; }
trap restore EXIT

# measure <rótulo>: mede mensagens totais e por partição numa janela; define M_TOTAL, M_P0.., M_RATE.
measure() {
    local p before_total after_total
    for p in $(seq 0 $((TOPIC_PARTITIONS - 1))); do eval "B_$p=$(end_offset "$p")"; done
    before_total=$(total_end_offsets)
    sleep "$WINDOW"
    after_total=$(total_end_offsets)
    M_TOTAL=$((after_total - before_total))
    M_ALL_PARTITIONS=1
    local line=""
    for p in $(seq 0 $((TOPIC_PARTITIONS - 1))); do
        local b a
        b=$(eval echo "\$B_$p")
        a=$(end_offset "$p")
        line="$line P$p=+$((a - b))"
        [ $((a - b)) -gt 0 ] || M_ALL_PARTITIONS=0
    done
    log "    $1: +$M_TOTAL mensagens em ${WINDOW}s (~$((M_TOTAL / WINDOW)) msg/s) |$line"
}

step "Baseline: 4 sensores originais"
docker compose --profile scale up -d --no-deps --scale sensor-extra=0 sensor-extra >/dev/null 2>&1
measure "4 sensores"
BASE=$M_TOTAL
check "Baseline gera mensagens ($BASE)" [ "$BASE" -gt 0 ]

for extra in 4 8; do
    step "Escalando para +$extra sensores ($((4 + extra)) no total)"
    docker compose --profile scale up -d --no-deps --no-recreate --scale sensor-extra="$extra" sensor-extra >/dev/null 2>&1
    sleep 12
    log "    sensor-extra em execução: $(docker ps --filter 'name=sensor-extra' --filter status=running -q | wc -l | tr -d ' ')"
    measure "$((4 + extra)) sensores"
    eval "RATE_$extra=$M_TOTAL"
    # Cada sensor extra emite ~0,5 msg/s; o baseline (4 sensores) ~2 msg/s. Exige-se ao menos +15% por sensor extra.
    check "Vazão com +$extra sensores (${M_TOTAL}) cresce mais de $((extra * 15))% sobre o baseline (${BASE})" \
        [ $((M_TOTAL * 100)) -gt $((BASE * (100 + extra * 15))) ]
    check "Todas as $TOPIC_PARTITIONS partições recebem dados com $((4 + extra)) sensores" [ "$M_ALL_PARTITIONS" -eq 1 ]
    lag_ok() { [ "$(total_lag)" -le 30 ]; }
    check "Consumidores acompanham (lag <= 30)" wait_until 60 lag_ok
done

step "Consumer group após a escala"
describe_group | block

finish
