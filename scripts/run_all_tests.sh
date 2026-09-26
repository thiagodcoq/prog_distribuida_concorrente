#!/usr/bin/env bash
# ==============================================================================
# run_all_tests.sh: executa toda a suíte de testes em sequência (make test-all).
#
# Assume o cluster no ar (make up). Cada teste grava sua evidência em reports/logs/.
# Ao final imprime uma tabela PASS/FAIL e grava reports/logs/RESULTADO_TESTES.txt.
# Sai com código != 0 se algum teste falhar.
# ==============================================================================
source "$(dirname "$0")/lib.sh"
ensure_stack_up

RESULT_FILE="$LOG_DIR/RESULTADO_TESTES.txt"
ROWS=""
FAILED=0

run_test() { # run_test <id> <descrição> <comando...>
    local id="$1" desc="$2"
    shift 2
    echo -e "\n########## $id: $desc ##########"
    local start rc
    start=$(now)
    "$@"
    rc=$?
    local status="PASS"
    [ "$rc" -ne 0 ] && { status="FAIL"; FAILED=$((FAILED + 1)); }
    ROWS="$ROWS$(printf '%-5s %-52s %-5s %4ss' "$id" "$desc" "$status" "$(($(now) - start))")\n"
}

# Estabiliza o grupo antes de começar
until [ "$(group_member_count)" -eq "$TOPIC_PARTITIONS" ]; do sleep 2; done

run_test T0 "Estado inicial do cluster" ./scripts/test_cluster_inicial.sh
run_test T1 "Balanceamento de partições" ./scripts/test_balanceamento.sh
run_test T2a "Falha de consumidor (graceful)" ./scripts/simulate_consumer_failure.sh graceful
run_test T2b "Falha de consumidor (hard, SIGKILL)" ./scripts/simulate_consumer_failure.sh hard
run_test T3a "Falha de broker seguidor (graceful)" ./scripts/simulate_broker_failure.sh graceful follower
if [ -x ./scripts/capture_failover_pcap.sh ]; then
    run_test T3b "Falha de controller (hard) + T9 pcap" ./scripts/capture_failover_pcap.sh
else
    run_test T3b "Falha de broker controller (hard)" ./scripts/simulate_broker_failure.sh hard controller
fi
run_test T4 "Comportamento sob carga (100k msgs)" ./scripts/test_carga.sh
run_test T5 "Elasticidade de consumidores (1 a 5)" ./scripts/test_elasticidade.sh
run_test T6 "Escala de sensores" ./scripts/test_escala_sensores.sh
run_test T7 "Limite: 2 de 3 brokers fora" ./scripts/test_dois_brokers_fora.sh
run_test T8 "Persistência dos alertas" ./scripts/test_persistencia_alertas.sh
run_test T10 "Testes unitários (regras de severidade)" make unit-test

{
    echo "RESULTADO DA SUÍTE DE TESTES  ($(date -u +%Y-%m-%dT%H:%M:%SZ), commit $(git rev-parse --short HEAD 2>/dev/null))"
    echo "--------------------------------------------------------------------------"
    printf '%-5s %-52s %-5s %5s\n' "ID" "Teste" "Res." "Tempo"
    echo -e "$ROWS"
    if [ "$FAILED" -eq 0 ]; then echo "TODOS OS TESTES PASSARAM"; else echo "$FAILED TESTE(S) FALHARAM"; fi
} | tee "$RESULT_FILE"

[ "$FAILED" -eq 0 ]
