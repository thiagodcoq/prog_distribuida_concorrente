#!/usr/bin/env bash
# T1: Balanceamento automático de carga entre consumidores (Objetivo 3)
#
# Com 3 consumidores no mesmo grupo e 3 partições, cada consumidor deve receber
# exatamente 1 partição, e cada partição deve estar recebendo dados (sensores
# distribuídos por chave). Mostra também a atribuição registrada pelo
# ConsumerRebalanceListener e quais sensores caem em cada partição.
# Pré-requisito: make up (3 consumidores)
SCRIPT_ARGS="$*"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

init_log "T1_balanceamento.log" "T1: BALANCEAMENTO DE PARTIÇÕES ENTRE CONSUMIDORES"

step "Distribuição atual (kafka-consumer-groups.sh --describe)"
describe_group | block
step "Membros do grupo"
describe_members | block

check "$TOPIC_PARTITIONS consumidores no grupo" [ "$(group_member_count)" -eq "$TOPIC_PARTITIONS" ]
check "Nenhum consumidor ocioso" [ "$(idle_member_count)" -eq 0 ]
check "Cada consumidor com exatamente 1 partição" [ "$(max_partitions_per_member)" -eq 1 ]
owners=$(describe_group | awk -v g="$KAFKA_GROUP_ID" '$1==g && $3 ~ /^[0-9]+$/ {print $NF}' | sort -u | wc -l | tr -d ' ')
check "Cada partição com um dono distinto" [ "$owners" -eq "$TOPIC_PARTITIONS" ]

step "Sensores por partição (chave = sensor_id, particionador murmur2 do Kafka)"
distinct_total=0
for p in $(seq 0 $((TOPIC_PARTITIONS - 1))); do
    # Lê no máximo 40 mensagens já existentes (sem isso o consumidor ficaria esperando novas)
    n_msgs=$(end_offset "$p")
    [ "${n_msgs:-0}" -gt 40 ] && n_msgs=40
    ids=$(docker compose exec -T "$(live_broker)" kafka-console-consumer.sh \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic "$KAFKA_TOPIC" \
        --partition "$p" --offset earliest --max-messages "${n_msgs:-1}" --timeout-ms 8000 </dev/null 2>/dev/null |
        grep -o '"sensor_id": "[^"]*"' | sort | uniq -c)
    log "  Partição $p:"
    echo "$ids" | block
    n=$(echo "$ids" | grep -c 'sensor_id')
    check "Partição $p recebe pelo menos 1 sensor" [ "$n" -ge 1 ]
    distinct_total=$((distinct_total + n))
done
check "Os 4 sensores estão distribuídos pelas partições" [ "$distinct_total" -ge 4 ]

step "Eventos do ConsumerRebalanceListener (PARTITIONS ASSIGNED)"
docker compose logs consumer 2>/dev/null | grep -A6 "PARTITIONS ASSIGNED" | tail -32 | block
check "Listener registrou atribuições" [ "$(docker compose logs consumer 2>/dev/null | grep -c 'PARTITIONS ASSIGNED')" -ge "$TOPIC_PARTITIONS" ]

finish
