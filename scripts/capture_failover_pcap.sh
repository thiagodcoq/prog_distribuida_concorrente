#!/usr/bin/env bash
# ==============================================================================
# T9: Captura de tráfego de rede durante a falha de um broker (Wireshark / tcpdump)
#
# Um container auxiliar (nicolaka/netshoot) compartilha a pilha de rede do produtor
# smartfactory-producer-1 (--net=container:...) e grava, com tcpdump, todo o tráfego
# TCP do cliente com os brokers (porta 9092). Durante a captura executa o teste T3
# (SIGKILL no líder do quórum KRaft). O .pcap resultante pode ser aberto no Wireshark.
#
# Saídas:
#   reports/pcaps/failover_broker.pcap          captura binária (abrir no Wireshark)
#   reports/logs/T9_wireshark_resumo.txt        contagens de SYN/FIN/RST por broker
#   reports/logs/T9_captura_pcap.log            asserções desta etapa
#
# Uso: ./scripts/capture_failover_pcap.sh
# Pré-requisito: make up (e acesso ao Docker Hub na primeira execução, para a imagem netshoot)
# ==============================================================================
SCRIPT_ARGS="$*"
source "$(dirname "$0")/lib.sh"
ensure_stack_up

IMAGE="nicolaka/netshoot"
CLIENT="smartfactory-producer-1"
CAP="smartfactory-pcap"
PCAP_DIR="reports/pcaps"
PCAP_FILE="failover_broker.pcap"
SUMMARY="$LOG_DIR/T9_wireshark_resumo.txt"
mkdir -p "$PCAP_DIR"

init_log "T9_captura_pcap.log" "T9: CAPTURA DE PACOTES (tcpdump/Wireshark) DURANTE FALHA DE BROKER"

cleanup() { docker rm -f "$CAP" >/dev/null 2>&1; }
trap cleanup EXIT

# ip_of <container>: endereço IP na rede do compose.
ip_of() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"; }

# pcap_count <filtro BPF>: nº de pacotes do arquivo que satisfazem o filtro.
pcap_count() {
    docker run --rm -v "$ROOT_DIR/$PCAP_DIR:/pcaps:ro" "$IMAGE" \
        tcpdump -nn -r "/pcaps/$PCAP_FILE" "$1" 2>/dev/null | wc -l | tr -d ' '
}

step "Preparando a imagem $IMAGE"
docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" | block
check "Imagem $IMAGE disponível" docker image inspect "$IMAGE"

step "Endereços IP dos brokers (rede do compose)"
for n in 1 2 3; do
    eval "IP_$n=$(ip_of smartfactory-kafka-$n)"
    log "    kafka-$n = $(eval echo "\$IP_$n")"
done
log "    cliente capturado: $CLIENT = $(ip_of "$CLIENT")"

step "Iniciando tcpdump na pilha de rede de $CLIENT"
rm -f "$PCAP_DIR/$PCAP_FILE"
docker run -d --rm --name "$CAP" --net="container:$CLIENT" --cap-add=NET_ADMIN --cap-add=NET_RAW \
    -v "$ROOT_DIR/$PCAP_DIR:/pcaps" "$IMAGE" \
    tcpdump -i any -nn -U -w "/pcaps/$PCAP_FILE" "tcp port 9092" >/dev/null
sleep 5
check "Container de captura em execução" [ "$(docker ps -q --filter "name=$CAP" | wc -l | tr -d ' ')" -eq 1 ]
LEADER=$(quorum_field LeaderId)
VICTIM_IP=$(eval echo "\$IP_$LEADER")
log "    Alvo da falha: líder do quórum = kafka-$LEADER ($VICTIM_IP)"

step "Executando T3 (SIGKILL no líder do quórum) durante a captura"
./scripts/simulate_broker_failure.sh hard controller >/dev/null 2>&1
T3_RC=$?
log "    T3 terminou com código $T3_RC (evidência: $LOG_DIR/T3_falha_broker_hard_controller.log)"
check "T3 (falha dura do controller) passou durante a captura" [ "$T3_RC" -eq 0 ]

step "Finalizando a captura"
sleep 3
docker stop "$CAP" >/dev/null 2>&1
SIZE=$(wc -c <"$PCAP_DIR/$PCAP_FILE" | tr -d ' ')
check "Arquivo $PCAP_DIR/$PCAP_FILE gerado ($SIZE bytes)" [ "${SIZE:-0}" -gt 1000 ]

step "Análise do .pcap (contagem de pacotes por broker)"
# Os contadores são calculados fora do pipeline de saída (dentro dele as variáveis se perderiam em um subshell)
for n in 1 2 3; do
    ip=$(eval echo "\$IP_$n")
    eval "SYN_$n=$(pcap_count "tcp[tcpflags] & (tcp-syn|tcp-ack) = tcp-syn and dst host $ip")"
    eval "FIN_$n=$(pcap_count "tcp[tcpflags] & tcp-fin != 0 and src host $ip")"
    eval "RST_$n=$(pcap_count "tcp[tcpflags] & tcp-rst != 0 and src host $ip")"
    eval "DATA_$n=$(pcap_count "host $ip and (tcp[tcpflags] & tcp-push != 0)")"
done
{
    echo "RESUMO DA CAPTURA: $PCAP_DIR/$PCAP_FILE  ($SIZE bytes)"
    echo "Capturado na pilha de rede de $CLIENT (cliente/sensor) | falha: SIGKILL em kafka-$LEADER ($VICTIM_IP)"
    echo "Gerado em $(date -u +%Y-%m-%dT%H:%M:%SZ) por scripts/capture_failover_pcap.sh"
    echo
    printf '%-10s %-15s %10s %10s %12s %14s\n' "broker" "ip" "SYN->" "FIN<-" "RST<-" "pacotes c/ dados"
    for n in 1 2 3; do
        mark=""
        [ "$n" = "$LEADER" ] && mark="  <-- derrubado"
        printf '%-10s %-15s %10s %10s %12s %14s%s\n' "kafka-$n" "$(eval echo "\$IP_$n")" \
            "$(eval echo "\$SYN_$n")" "$(eval echo "\$FIN_$n")" "$(eval echo "\$RST_$n")" "$(eval echo "\$DATA_$n")" "$mark"
    done
    echo
    echo "Legenda: SYN-> tentativas de conexão do cliente ao broker (sem resposta = broker morto); FIN<-/RST<- encerramentos enviados pelo broker."
    echo "O sensor capturado só fala com os líderes das partições que usa; brokers sem pacotes de dados não lideram essas partições."
    echo "Filtros úteis no Wireshark:  ip.addr==$VICTIM_IP && (tcp.flags.fin==1 || tcp.flags.reset==1)"
    echo "                             tcp.flags.syn==1 && tcp.flags.ack==0        (reconexões)"
    echo "                             kafka                                       (decodifica o protocolo)"
} | tee "$SUMMARY" | block

VF=$(eval echo "\$FIN_$LEADER")
VR=$(eval echo "\$RST_$LEADER")
VS=$(eval echo "\$SYN_$LEADER")
# O sensor capturado só mantém conexão de dados com os líderes das partições que usa; se o broker derrubado
# não era um deles, não há FIN/RST a observar, mas o cliente ainda tenta reconectar (SYN sem resposta).
check "Cliente reagiu à queda de kafka-$LEADER: encerramento (FIN=$VF, RST=$VR) ou tentativas de reconexão (SYN=$VS)" [ $((VF + VR + VS)) -ge 1 ]
survivors_data=0
for n in 1 2 3; do
    [ "$n" = "$LEADER" ] && continue
    survivors_data=$((survivors_data + $(eval echo "\$DATA_$n")))
done
check "Cliente manteve tráfego de dados com os brokers sobreviventes ($survivors_data pacotes PSH)" [ "$survivors_data" -ge 10 ]

finish
