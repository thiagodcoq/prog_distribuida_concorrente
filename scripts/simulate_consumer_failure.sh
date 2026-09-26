#!/usr/bin/env bash
# ==============================================================================
# T2: Falha de consumidor e rebalanceamento (Objetivo 4 do enunciado)
#
# Uso: ./scripts/simulate_consumer_failure.sh [graceful|hard]
#   graceful (padrão): docker stop -> SIGTERM; o consumidor sai do grupo (LeaveGroup)
#                      e o rebalanceamento é imediato.
#   hard             : docker kill -> SIGKILL; simula queda abrupta. O coordenador só
#                      percebe pela ausência de heartbeats após session.timeout.ms.
#
# Derruba o consumidor dono de uma partição COM tráfego (TARGET_PARTITION, padrão 1),
# mede o tempo até outro consumidor assumir e verifica que o processamento continua.
# Pré-requisito: make up (3 consumidores)
# ==============================================================================
MODE="${1:-graceful}"
TARGET_PARTITION="${TARGET_PARTITION:-1}"
SCRIPT_ARGS="modo=$MODE partição-alvo=$TARGET_PARTITION"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

init_log "T2_falha_consumidor_${MODE}.log" "T2: FALHA DE CONSUMIDOR E REBALANCEAMENTO (modo: $MODE)"

TARGET=""
restore() { [ -n "$TARGET" ] && ! docker ps -q --filter "name=$TARGET" --filter status=running | grep -q . && start_container "$TARGET"; }
trap restore EXIT

step "Pré-condições"
check "3 consumidores no grupo antes do teste" [ "$(group_member_count)" -eq 3 ]
check "Partição $TARGET_PARTITION recebe dados (end offset > 0)" [ "$(end_offset "$TARGET_PARTITION")" -gt 0 ]
describe_group | block

OWNER_CLIENT=$(partition_owner "$TARGET_PARTITION")
TARGET=$(container_of_client "$OWNER_CLIENT")
check "Dono da partição $TARGET_PARTITION identificado ($OWNER_CLIENT -> $TARGET)" [ -n "$TARGET" ]
committed_before=$(describe_group | awk -v p="$TARGET_PARTITION" '$3==p {print $4}')
T0_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)

step "Derrubando $TARGET (modo $MODE)"
T0=$(now)
stop_or_kill "$TARGET" "$MODE"
log "    $(date -u +%H:%M:%S) container $TARGET derrubado"

step "Aguardando outro consumidor assumir a partição $TARGET_PARTITION (até 90 s)"
taken_over() {
    local owner
    owner=$(partition_owner "$TARGET_PARTITION")
    [ -n "$owner" ] && [ "$owner" != "-" ] && [ "$owner" != "$OWNER_CLIENT" ]
}
if wait_until 90 taken_over; then
    NEW_OWNER=$(partition_owner "$TARGET_PARTITION")
    pass "Partição $TARGET_PARTITION reatribuída a $NEW_OWNER em ${ELAPSED}s"
else
    NEW_OWNER=""
    fail "Nenhum consumidor assumiu a partição $TARGET_PARTITION em 90 s"
fi
DETECTION_S=$(($(now) - T0))
log "    Tempo até a reatribuição: ${DETECTION_S}s (session.timeout.ms = ${SESSION_TIMEOUT_MS} ms)"
if [ "$MODE" = "hard" ]; then
    check "Queda abrupta detectada por ausência de heartbeats (>= ~session timeout)" \
        [ "$DETECTION_S" -ge $((SESSION_TIMEOUT_MS / 1000 - 3)) ]
    check "Detecção em tempo razoável (< 3 x session timeout)" [ "$DETECTION_S" -le $((SESSION_TIMEOUT_MS / 1000 * 3)) ]
fi

step "Distribuição após o rebalanceamento"
describe_group | block
check "2 consumidores restantes no grupo" [ "$(group_member_count)" -eq 2 ]
check "Consumidor derrubado não é mais dono de nenhuma partição" \
    [ "$(describe_group | grep -c "$OWNER_CLIENT")" -eq 0 ]
check "Todas as $TOPIC_PARTITIONS partições continuam atribuídas" \
    [ "$(describe_group | awk -v g="$KAFKA_GROUP_ID" '$1==g && $NF != "-" && $3 ~ /^[0-9]+$/' | wc -l | tr -d ' ')" -eq "$TOPIC_PARTITIONS" ]

step "Processamento continua na partição reatribuída (offset consumido avança)"
progressed() {
    local now_off
    now_off=$(describe_group | awk -v p="$TARGET_PARTITION" '$3==p {print $4}')
    [ "${now_off:-0}" -gt "${committed_before:-0}" ]
}
if wait_until 60 progressed; then
    pass "CURRENT-OFFSET da partição $TARGET_PARTITION avançou de $committed_before para $(describe_group | awk -v p="$TARGET_PARTITION" '$3==p {print $4}')"
else
    fail "Offset da partição $TARGET_PARTITION não avançou"
fi
lag_small() { [ "$(total_lag)" -le 30 ]; }
check "Lag total volta a ficar pequeno (<= 30 mensagens)" wait_until 60 lag_small
log "    (lag residual em modo hard pode incluir mensagens reprocessadas: entrega at-least-once)"

step "Eventos do ConsumerRebalanceListener nos sobreviventes desde $T0_ISO"
docker compose logs --since "$T0_ISO" consumer 2>/dev/null | grep -A6 -E "PARTITIONS (REVOKED|ASSIGNED)" | block
check "Listener registrou revogação/atribuição" \
    [ "$(docker compose logs --since "$T0_ISO" consumer 2>/dev/null | grep -c 'REBALANCE EVENT')" -ge 1 ]

step "Restaurando $TARGET e voltando a 3 consumidores"
start_container "$TARGET"
back_to_three() { [ "$(group_member_count)" -eq 3 ] && [ "$(max_partitions_per_member)" -eq 1 ]; }
if wait_until 90 back_to_three; then
    pass "Grupo voltou a 3 consumidores com 1 partição cada em ${ELAPSED}s"
else
    fail "Grupo não voltou ao estado 1:1 em 90 s"
fi
describe_group | block

finish
