#!/usr/bin/env bash
# T8: Persistência e compartilhamento dos alertas (Componente 4: banco de dados/logger)
#
# Os consumidores gravam anomalias em /var/log/smartfactory/alerts.log, num volume
# Docker compartilhado. Verifica que o arquivo é visto por todas as réplicas, que
# sobrevive à reinicialização dos consumidores e que continua recebendo alertas.
# Uso: ./scripts/test_persistencia_alertas.sh
# Pré-requisito: make up
SCRIPT_ARGS="$*"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

init_log "T8_persistencia_alertas.log" "T8: PERSISTÊNCIA DOS ALERTAS"

# Nomes reais das réplicas (o Compose não reaproveita números após escalar: pode ser consumer-8, -9, -10...)
CONSUMERS=$(docker ps --filter "name=smartfactory-consumer-" --filter status=running --format '{{.Names}}' | sort -V)
CA=$(echo "$CONSUMERS" | sed -n 1p)
CB=$(echo "$CONSUMERS" | sed -n 2p)
CC=$(echo "$CONSUMERS" | sed -n 3p)

# alert_count <container>: nº de linhas do arquivo de alertas visto por aquele container.
alert_count() { docker exec "$1" sh -c "wc -l < $ALERT_LOG_PATH 2>/dev/null || echo 0" | tr -d ' \r'; }

step "Aguardando existir ao menos um alerta"
has_alerts() { [ "$(alert_count $CA)" -gt 0 ]; }
check "Arquivo de alertas existe e tem registros" wait_until 120 has_alerts

step "Arquivo compartilhado entre réplicas (mesmo volume)"
C1=$(alert_count $CA)
C2=$(alert_count $CB)
C3=$(alert_count $CC)
log "    linhas vistas por $CA: $C1 | $CB: $C2 | $CC: $C3"
close_enough() { [ $((C2 - C1)) -le 5 ] && [ $((C1 - C2)) -le 5 ] && [ $((C3 - C1)) -le 5 ] && [ $((C1 - C3)) -le 5 ]; }
check "As 3 réplicas enxergam o mesmo arquivo (diferença <= 5 linhas por timing)" close_enough

step "Últimos alertas (JSON) e produtores por severidade"
docker exec $CA tail -n 3 "$ALERT_LOG_PATH" | cut -c1-260 | block
log "    CRITICAL: $(docker exec $CA grep -c '"severity": "CRITICAL"' "$ALERT_LOG_PATH")"
log "    WARNING : $(docker exec $CA grep -c '"severity": "WARNING"' "$ALERT_LOG_PATH")"
log "    réplicas que gravaram (consumer_id distintos): $(docker exec $CA grep -o '"consumer_id": "[^"]*"' "$ALERT_LOG_PATH" | sort -u | wc -l | tr -d ' ')"

step "Reiniciando todos os consumidores (docker compose restart consumer)"
BEFORE=$(alert_count $CA)
docker compose restart consumer >/dev/null 2>&1
members_back() { [ "$(group_member_count)" -eq 3 ]; }
check "Grupo volta a ter 3 consumidores" wait_until 120 members_back
AFTER_RESTART=$(alert_count $CA)
log "    linhas antes: $BEFORE | logo após o restart: $AFTER_RESTART"
check "Arquivo NÃO foi truncado pelo restart ($AFTER_RESTART >= $BEFORE)" [ "$AFTER_RESTART" -ge "$BEFORE" ]
grows() { [ "$(alert_count $CA)" -gt "$AFTER_RESTART" ]; }
check "Novos alertas continuam sendo anexados após o restart" wait_until 180 grows
log "    linhas agora: $(alert_count $CA)"

finish
