#!/usr/bin/env bash
# ==============================================================================
# lib.sh: funções comuns dos scripts de teste da SmartFactory.
#
# Uso (no início de cada script):   source "$(dirname "$0")/lib.sh"
#
# - Carrega config/sensor_thresholds.env (tópico, grupo, partições...).
# - Fornece logging em arquivo, asserções (PASS/FAIL) e helpers para consultar o
#   cluster Kafka. Os scripts terminam com `finish`, que devolve exit code != 0
#   quando alguma asserção falhou.
# - Compatível com o bash 3.2 do macOS (sem arrays associativos nem mapfile).
# ==============================================================================

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR" || exit 1

set -a
# shellcheck disable=SC1091
source config/sensor_thresholds.env
set +a

LOG_DIR="reports/logs"
mkdir -p "$LOG_DIR"

PASS_COUNT=0
FAIL_COUNT=0
LOG_FILE="/dev/null"

# ------------------------------------------------------------------------------
# Logging e asserções
# ------------------------------------------------------------------------------

# init_log <arquivo> <título>: cria o log do teste com cabeçalho de rastreabilidade.
init_log() {
    LOG_FILE="$LOG_DIR/$1"
    {
        echo "================================================================================"
        echo " $2"
        echo "================================================================================"
        echo "Data/hora (UTC) : $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "Commit          : $(git rev-parse --short HEAD 2>/dev/null || echo n/a)$(git diff --quiet 2>/dev/null || echo ' (com alterações não commitadas)')"
        echo "Tópico / grupo  : $KAFKA_TOPIC (P=$TOPIC_PARTITIONS, R=$TOPIC_REPLICATION_FACTOR) / $KAFKA_GROUP_ID"
        echo "Session timeout : ${SESSION_TIMEOUT_MS} ms | Heartbeat: ${HEARTBEAT_INTERVAL_MS} ms"
        echo "Argumentos      : ${SCRIPT_ARGS:-nenhum}"
        echo "================================================================================"
    } | tee "$LOG_FILE"
}

# log <mensagem...>: escreve no console e no arquivo de log.
log() { echo -e "$*" | tee -a "$LOG_FILE"; }

# step <mensagem>: cabeçalho de etapa.
step() { log "\n>>> [$(date -u +%H:%M:%S)] $*"; }

# block: copia stdin (saída de comando) para console e log, indentado.
block() { sed 's/^/    /' | tee -a "$LOG_FILE"; }

pass() { PASS_COUNT=$((PASS_COUNT + 1)); log "    [PASS] $*"; }
fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); log "    [FAIL] $*"; }

# check <descrição> <comando...>: PASS se o comando retornar 0. Ex.: check "x" [ "$a" -eq 3 ]
check() {
    local desc="$1"
    shift
    if "$@"; then pass "$desc"; else fail "$desc"; fi
}

# finish: imprime o resumo e encerra com o status do teste.
finish() {
    local total=$((PASS_COUNT + FAIL_COUNT))
    log "\n================================================================================"
    if [ "$FAIL_COUNT" -eq 0 ]; then
        log " RESULTADO: PASS ($PASS_COUNT/$total verificações)  |  evidência: $LOG_FILE"
    else
        log " RESULTADO: FAIL ($FAIL_COUNT de $total verificações falharam)  |  evidência: $LOG_FILE"
    fi
    log "================================================================================"
    [ "$FAIL_COUNT" -eq 0 ]
    exit $?
}

# now: epoch em segundos (o date do macOS não tem %N; resolução de 1 s basta aqui).
now() { date +%s; }

# wait_until <timeout_s> <comando...>: repete o comando a cada 2 s até retornar 0.
# Ao terminar, ELAPSED contém os segundos gastos.
wait_until() {
    local timeout="$1"
    shift
    local start
    start=$(now)
    while ! "$@" >/dev/null 2>&1; do
        ELAPSED=$(($(now) - start))
        [ "$ELAPSED" -ge "$timeout" ] && return 1
        sleep 2
    done
    ELAPSED=$(($(now) - start))
    return 0
}

# ------------------------------------------------------------------------------
# Acesso ao cluster Kafka (executa as ferramentas dentro de um broker vivo)
# ------------------------------------------------------------------------------

broker_running() { [ "$(docker inspect -f '{{.State.Running}}' "smartfactory-$1" 2>/dev/null)" = "true" ]; }

# live_broker [excluir]: nome do primeiro broker (kafka-1..3) em execução, exceto o excluído.
live_broker() {
    local n
    for n in kafka-1 kafka-2 kafka-3; do
        [ "$n" = "${1:-}" ] && continue
        broker_running "$n" && { echo "$n"; return 0; }
    done
    return 1
}

# kafka_tool <ferramenta> [args...]: roda a ferramenta num broker vivo com --bootstrap-server.
kafka_tool() {
    local tool="$1" b
    shift
    b=$(live_broker) || return 1
    docker compose exec -T "$b" "$tool" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" "$@" </dev/null
}

describe_topic() { kafka_tool kafka-topics.sh --describe --topic "${1:-$KAFKA_TOPIC}" 2>/dev/null; }
describe_group() { kafka_tool kafka-consumer-groups.sh --describe --group "$KAFKA_GROUP_ID" 2>/dev/null; }
describe_members() { kafka_tool kafka-consumer-groups.sh --describe --group "$KAFKA_GROUP_ID" --members 2>/dev/null; }
quorum_status() { kafka_tool kafka-metadata-quorum.sh describe --status 2>/dev/null; }

# quorum_field <campo>: valor de um campo do `describe --status` (LeaderId, LeaderEpoch...).
quorum_field() { quorum_status | awk -F': *' -v k="$1" '$1==k {print $2; exit}'; }

# partition_table: linhas "partição líder qtd_isr" do tópico principal.
partition_table() {
    describe_topic | sed -n 's/.*Partition: \([0-9]*\).*Leader: \([-0-9]*\).*Isr: \(.*\)$/\1 \2 \3/p' |
        awk '{n=split($3, a, ","); print $1, $2, n}'
}

# end_offset <partição> [tópico]
end_offset() {
    kafka_tool kafka-get-offsets.sh --topic "${2:-$KAFKA_TOPIC}" 2>/dev/null |
        awk -F: -v p="$1" '$2==p {print $3}'
}

# total_end_offsets [tópico]: soma dos log-end-offsets de todas as partições.
total_end_offsets() {
    kafka_tool kafka-get-offsets.sh --topic "${1:-$KAFKA_TOPIC}" 2>/dev/null |
        awk -F: '{s+=$3} END {print s+0}'
}

# total_lag: lag somado do consumer group.
total_lag() {
    describe_group | awk -v g="$KAFKA_GROUP_ID" '$1==g && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'
}

# total_committed: soma dos offsets consumidos (CURRENT-OFFSET) do grupo.
total_committed() {
    describe_group | awk -v g="$KAFKA_GROUP_ID" '$1==g && $4 ~ /^[0-9]+$/ {s+=$4} END {print s+0}'
}

# group_member_count: membros no grupo (inclui os ociosos, sem partição).
group_member_count() {
    describe_members | awk -v g="$KAFKA_GROUP_ID" '$1==g && $NF ~ /^[0-9]+$/ {c++} END {print c+0}'
}

# idle_member_count: membros do grupo sem nenhuma partição atribuída.
idle_member_count() {
    describe_members | awk -v g="$KAFKA_GROUP_ID" '$1==g && $NF == "0" {c++} END {print c+0}'
}

# partitions_of_max_member: maior nº de partições atribuídas a um único membro.
max_partitions_per_member() {
    describe_members | awk -v g="$KAFKA_GROUP_ID" '$1==g && $NF ~ /^[0-9]+$/ && $NF>m {m=$NF} END {print m+0}'
}

# partition_owner <partição>: CLIENT-ID (consumer-<hostname>) dono da partição.
partition_owner() {
    describe_group | awk -v g="$KAFKA_GROUP_ID" -v p="$1" '$1==g && $3==p {print $NF; exit}'
}

# container_of_client <client-id>: nome do container cujo hostname (ID curto) é o do cliente.
container_of_client() {
    local host="${1#consumer-}"
    docker ps -a --format '{{.ID}} {{.Names}}' | awk -v h="$host" '$1==h {print $2; exit}'
}

# CONSUMER_CONTAINERS_RUNNING: contagem de consumidores em execução.
consumers_running() { docker ps --filter "name=smartfactory-consumer-" --filter status=running -q | wc -l | tr -d ' '; }

# hard_kill <container> / restore_restart_policy <container>: falha "dura" (SIGKILL).
# Sem `--restart=no` o Docker religaria o container automaticamente.
hard_kill() {
    docker update --restart=no "$1" >/dev/null
    docker kill "$1" >/dev/null
}
restore_restart_policy() { docker update --restart=unless-stopped "$1" >/dev/null; }

# stop_or_kill <container> <graceful|hard>
stop_or_kill() {
    case "$2" in
        graceful) docker stop "$1" >/dev/null ;;
        hard) hard_kill "$1" ;;
        *) echo "modo inválido: $2 (use graceful ou hard)" >&2; return 1 ;;
    esac
}

# start_container <container>: religa e restaura a política de restart.
start_container() {
    docker start "$1" >/dev/null
    restore_restart_policy "$1"
}

# run_perf_test <broker> <tópico> <registros> <throughput> [props extras...]
# Executa o kafka-producer-perf-test com payload JSON válido (o consumidor consegue processá-lo).
run_perf_test() {
    local broker="$1" topic="$2" records="$3" rate="$4"
    shift 4
    docker compose exec -T "$broker" sh -c 'printf "%s\n" "{\"sensor_id\":\"carga-teste\",\"setor\":\"linha_producao\",\"temperatura\":60.0,\"vibracao\":2.0,\"consumo_energia_kw\":15.0,\"timestamp\":\"2026-01-01T00:00:00+00:00\"}" > /tmp/payload.json' </dev/null
    docker compose exec -T "$broker" kafka-producer-perf-test.sh \
        --topic "$topic" --num-records "$records" --throughput "$rate" \
        --payload-file /tmp/payload.json \
        --producer-props bootstrap.servers="$KAFKA_BOOTSTRAP_SERVERS" acks=all "$@" </dev/null
}

# ensure_stack_up: aborta se o cluster não estiver no ar.
ensure_stack_up() {
    live_broker >/dev/null || { echo "Cluster fora do ar. Rode: make up" >&2; exit 2; }
    [ "$(consumers_running)" -ge 1 ] || { echo "Nenhum consumidor em execução. Rode: make up" >&2; exit 2; }
}
