#!/usr/bin/env bash
# T0: Estado inicial do cluster (Objetivo 1 do enunciado)
#
# Verifica, com asserções, que o cluster subiu como projetado: 3 brokers KRaft
# saudáveis, quórum de 3 votantes, tópico com P/R/ISR configurados e réplicas em
# sincronia, produtores e consumidores em execução e dados chegando nas 3 partições.
# Pré-requisito: make up
SCRIPT_ARGS="$*"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

init_log "T0_cluster_inicial.log" "T0: ESTADO INICIAL DO CLUSTER"

step "Containers em execução"
docker compose ps --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}' | block
brokers_ok=0
for n in 1 2 3; do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "smartfactory-kafka-$n" 2>/dev/null)" = "healthy" ] && brokers_ok=$((brokers_ok + 1))
done
producers=$(docker ps --filter "name=smartfactory-producer-" --filter status=running -q | wc -l | tr -d ' ')
check "3 brokers Kafka saudáveis (healthcheck)" [ "$brokers_ok" -eq 3 ]
check "4 produtores (sensores) em execução" [ "$producers" -eq 4 ]
check "3 consumidores em execução" [ "$(consumers_running)" -eq 3 ]

step "Quórum KRaft (kafka-metadata-quorum.sh describe --status)"
quorum_status | block
voters=$(quorum_field CurrentVoters | tr ',' '\n' | grep -c '[0-9]')
check "Quórum com 3 votantes (maioria = 2)" [ "$voters" -eq 3 ]

step "Tópico $KAFKA_TOPIC"
describe_topic | block
header=$(describe_topic | head -1)
partitions=$(echo "$header" | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p')
replication=$(echo "$header" | sed -n 's/.*ReplicationFactor: *\([0-9]*\).*/\1/p')
min_isr=$(echo "$header" | sed -n 's/.*min.insync.replicas=\([0-9]*\).*/\1/p')
check "PartitionCount = $TOPIC_PARTITIONS" [ "$partitions" = "$TOPIC_PARTITIONS" ]
check "ReplicationFactor = $TOPIC_REPLICATION_FACTOR" [ "$replication" = "$TOPIC_REPLICATION_FACTOR" ]
check "min.insync.replicas = $TOPIC_MIN_INSYNC_REPLICAS" [ "$min_isr" = "$TOPIC_MIN_INSYNC_REPLICAS" ]
isr_incomplete=$(partition_table | awk -v r="$TOPIC_REPLICATION_FACTOR" '$3 != r' | wc -l | tr -d ' ')
check "Todas as partições com ISR completo ($TOPIC_REPLICATION_FACTOR réplicas em sincronia)" [ "$isr_incomplete" -eq 0 ]
leaders=$(partition_table | awk '$2 >= 1 {print $2}' | sort -u | wc -l | tr -d ' ')
check "Liderança das partições distribuída entre mais de um broker" [ "$leaders" -ge 2 ]

step "Dados chegando em TODAS as partições (aguarda até 90 s)"
all_partitions_have_data() {
    local p
    for p in $(seq 0 $((TOPIC_PARTITIONS - 1))); do
        [ "$(end_offset "$p")" -gt 0 ] 2>/dev/null || return 1
    done
}
wait_until 90 all_partitions_have_data
for p in $(seq 0 $((TOPIC_PARTITIONS - 1))); do
    off=$(end_offset "$p")
    check "Partição $p recebeu mensagens (log-end-offset = $off)" [ "${off:-0}" -gt 0 ]
done

step "Consumer group"
describe_group | block
check "Grupo com $TOPIC_PARTITIONS membros" [ "$(group_member_count)" -eq "$TOPIC_PARTITIONS" ]

finish
