#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de Processamento de Dados e Detecção de Anomalias para SmartFactory.

Este módulo consome dados de telemetria industrial enviados pelos sensores IoT ao
cluster Apache Kafka. Ele pertence a um Consumer Group unificado ('smartfactory-processors'),
permitindo balanceamento automático de partições, elasticidade horizontal e tolerância a falhas.
Implementa um ConsumerRebalanceListener para evidenciar rebalanceamentos em tempo real
e realiza a validação de parâmetros físicos (temperatura, vibração e potência),
registrando ocorrências normais em stdout e anomalias críticas/avisos em arquivo compartilhado.

Disciplina: Distribuição e Concorrência (PUC)
Data: 2026
"""

import json
import logging
import os
import signal
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from kafka import ConsumerRebalanceListener, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("DataProcessor")


class SmartFactoryRebalanceListener(ConsumerRebalanceListener):
    """
    Listener customizado para eventos de rebalanceamento do grupo de consumidores Kafka.

    Captura os momentos exatos em que partições são revogadas ou atribuídas a esta
    instância de consumidor, registrando detalhes completos (nomes de tópicos, IDs de partições,
    identificador do consumidor e carimbo de data/hora) com formatação visual destacada.

    Attributes:
        consumer_id (str): Identificador exclusivo deste consumidor na topologia.
    """

    def __init__(self, consumer_id: str) -> None:
        """
        Inicializa o listener de rebalanceamento com o identificador do consumidor.

        Args:
            consumer_id (str): Nome ou identificador da instância do consumidor.
        """
        super().__init__()
        self.consumer_id: str = consumer_id

    def on_partitions_revoked(self, revoked: Set[TopicPartition]) -> None:
        """
        Callback disparado antes que o Kafka revogue partições deste consumidor.

        Indica que uma renegociação do grupo foi iniciada (ex: adição ou remoção de nós).

        Args:
            revoked (Set[TopicPartition]): Conjunto de partições anteriormente atribuídas que foram revogadas.
        """
        part_list = [f"{tp.topic}-P{tp.partition}" for tp in revoked] if revoked else ["Nenhuma"]
        timestamp = datetime.now(timezone.utc).isoformat()

        logger.warning(
            "\n"
            "================================================================================\n"
            " [REBALANCE EVENT - PARTITIONS REVOKED]\n"
            " Consumidor ID      : %s\n"
            " Partições Revogadas: %s\n"
            " Quantidade         : %d\n"
            " Timestamp          : %s\n"
            " Motivo             : Reorganização de carga do Consumer Group iniciada.\n"
            "================================================================================",
            self.consumer_id,
            ", ".join(part_list),
            len(revoked),
            timestamp,
        )

    def on_partitions_assigned(self, assigned: Set[TopicPartition]) -> None:
        """
        Callback disparado após o coordenador do Kafka concluir a atribuição de partições.

        Confirma quais partições este consumidor específico ficou encarregado de processar.

        Args:
            assigned (Set[TopicPartition]): Conjunto de partições atribuídas a este consumidor.
        """
        part_list = [f"{tp.topic}-P{tp.partition}" for tp in assigned] if assigned else ["Nenhuma (Ocioso/Standby)"]
        timestamp = datetime.now(timezone.utc).isoformat()

        logger.info(
            "\n"
            "================================================================================\n"
            " [REBALANCE EVENT - PARTITIONS ASSIGNED]\n"
            " Consumidor ID       : %s\n"
            " Partições Atribuídas: %s\n"
            " Total Atribuído     : %d\n"
            " Timestamp           : %s\n"
            " Status Operacional  : %s\n"
            "================================================================================",
            self.consumer_id,
            ", ".join(part_list),
            len(assigned),
            timestamp,
            "ATIVO (Processando dados)" if assigned else "OCIOSO (Standby - Mais réplicas que partições)",
        )


class SmartFactoryConsumer:
    """
    Classe consumidora responsável por processar telemetria e detectar anomalias operacionais.

    Conecta-se ao tópico de dados dos sensores utilizando o grupo compartilhado
    'smartfactory-processors'. Analisa cada payload recebido contra os limites configurados
    via variáveis de ambiente e grava registros de anomalia em arquivo compartilhado.

    Attributes:
        bootstrap_servers (str): Endereços dos brokers Kafka.
        topic (str): Nome do tópico consumido.
        group_id (str): Identificador do Consumer Group.
        consumer_id (str): Identificador desta réplica consumidora.
        warn_temp (float): Limite de alerta amarelo para temperatura (°C).
        max_temp (float): Limite crítico para temperatura (°C).
        warn_vibration (float): Limite de alerta para vibração mecânica (mm/s).
        max_vibration (float): Limite crítico para vibração mecânica (mm/s).
        warn_power_kw (float): Limite de alerta para consumo energético (kW).
        max_power_kw (float): Limite crítico para consumo energético (kW).
        alert_log_path (str): Caminho absoluto do arquivo compartilhado de alertas.
        auto_offset_reset (str): Onde começar a ler quando o grupo não tem offset salvo.
        auto_commit_interval_ms (int): Intervalo (ms) do commit automático dos offsets consumidos.
        session_timeout_ms (int): Tempo sem heartbeat (ms) após o qual o consumidor é dado como
            falho e o grupo é rebalanceado.
        heartbeat_interval_ms (int): Intervalo (ms) entre heartbeats enviados ao coordenador.
        max_poll_interval_ms (int): Tempo máximo (ms) entre duas chamadas de poll.
        metadata_max_age_ms (int): Idade máxima (ms) dos metadados antes de renová-los; evita buscar
            em um broker que já caiu depois de um failover.
        poll_timeout_ms (int): Tempo de espera (ms) de cada poll, curto para reagir a sinais de parada.
        poll_max_records (int): Máximo de registros devolvidos por poll.
        processing_delay_seg (float): Custo simulado de processamento por mensagem, em segundos
            (0 desliga; usado no teste de elasticidade).
        connect_max_retries (int): Número máximo de tentativas de conexão ao cluster.
        connect_retry_delay (float): Espera inicial (s) entre tentativas; dobra a cada falha.
        connect_retry_max_delay (float): Limite (s) da espera entre tentativas de conexão.
        running (bool): Flag de controle do ciclo de execução.
        consumer (KafkaConsumer): Instância do consumidor Kafka.
    """

    def __init__(self) -> None:
        """
        Inicializa o consumidor carregando as variáveis de ambiente e criando os limites.

        O identificador do consumidor é CONSUMER_ID ou, na falta dele, `consumer-<hostname>`
        (o hostname de um container é o seu ID curto). Os limites de alarme, o caminho do arquivo
        de alertas e os parâmetros do cliente Kafka (detecção de falha, commit, polling e
        reconexão com recuo exponencial) vêm todos de variáveis de ambiente.
        """
        self.bootstrap_servers: str = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        )
        self.topic: str = os.getenv("KAFKA_TOPIC", "dados-sensores")
        self.group_id: str = os.getenv("KAFKA_GROUP_ID", "smartfactory-processors")

        default_id = socket.gethostname()
        self.consumer_id: str = os.getenv("CONSUMER_ID", f"consumer-{default_id}")

        self.warn_temp: float = float(os.getenv("WARN_TEMP", "75.0"))
        self.max_temp: float = float(os.getenv("MAX_TEMP", "85.0"))
        self.warn_vibration: float = float(os.getenv("WARN_VIBRATION", "4.0"))
        self.max_vibration: float = float(os.getenv("MAX_VIBRATION", "5.0"))
        self.warn_power_kw: float = float(os.getenv("WARN_POWER_KW", "25.0"))
        self.max_power_kw: float = float(os.getenv("MAX_POWER_KW", "30.0"))

        self.alert_log_path: str = os.getenv(
            "ALERT_LOG_PATH", "/var/log/smartfactory/alerts.log"
        )

        self.auto_offset_reset: str = os.getenv("AUTO_OFFSET_RESET", "earliest")
        self.auto_commit_interval_ms: int = int(
            os.getenv("AUTO_COMMIT_INTERVAL_MS", "2000")
        )
        self.session_timeout_ms: int = int(os.getenv("SESSION_TIMEOUT_MS", "10000"))
        self.heartbeat_interval_ms: int = int(os.getenv("HEARTBEAT_INTERVAL_MS", "3000"))
        self.max_poll_interval_ms: int = int(os.getenv("MAX_POLL_INTERVAL_MS", "300000"))
        self.metadata_max_age_ms: int = int(
            os.getenv("CONSUMER_METADATA_MAX_AGE_MS", "10000")
        )
        self.poll_timeout_ms: int = int(os.getenv("POLL_TIMEOUT_MS", "1000"))
        self.poll_max_records: int = int(os.getenv("POLL_MAX_RECORDS", "50"))
        self.processing_delay_seg: float = (
            float(os.getenv("PROCESSING_DELAY_MS", "0")) / 1000.0
        )

        self.connect_max_retries: int = int(os.getenv("CONNECT_MAX_RETRIES", "30"))
        self.connect_retry_delay: float = float(
            os.getenv("CONNECT_RETRY_DELAY_SEG", "3.0")
        )
        self.connect_retry_max_delay: float = float(
            os.getenv("CONNECT_RETRY_MAX_DELAY_SEG", "30.0")
        )

        self.running: bool = True
        self.consumer: Optional[KafkaConsumer] = None

        logger.info(
            "Configurando Consumidor | ID: %s | Grupo: %s | Tópico: %s | Brokers: %s",
            self.consumer_id,
            self.group_id,
            self.topic,
            self.bootstrap_servers,
        )
        logger.info(
            "Limites de Alarme | Temp: [Aviso > %.1f°C, Crítico > %.1f°C] | "
            "Vibração: [Aviso > %.1fmm/s, Crítico > %.1fmm/s] | "
            "Potência: [Aviso > %.1fkW, Crítico > %.1fkW]",
            self.warn_temp,
            self.max_temp,
            self.warn_vibration,
            self.max_vibration,
            self.warn_power_kw,
            self.max_power_kw,
        )

    def _setup_signal_handlers(self) -> None:
        """
        Configura os tratadores de interrupção (SIGINT, SIGTERM) para desconexão graciosa.
        """
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """
        Tratador de sinal que interrompe o loop e força o consumidor a sair do grupo.

        Args:
            signum (int): Código numérico do sinal POSIX.
            frame (Any): Quadro de execução no momento do disparo.
        """
        logger.warning(
            "Sinal de término capturado (%d). Realizando shutdown gracioso do consumidor %s...",
            signum,
            self.consumer_id,
        )
        self.running = False

    def connect(self) -> None:
        """
        Conecta ao cluster Apache Kafka com assinatura no tópico e registro do listener.

        Em caso de falha, a espera entre tentativas começa em `connect_retry_delay`, dobra
        a cada erro e é limitada a `connect_retry_max_delay` (recuo exponencial), até
        `connect_max_retries` tentativas.

        Raises:
            SystemExit: Se o cluster estiver inacessível após todas as tentativas.
        """
        max_retries = self.connect_max_retries
        retries = 0
        rebalance_listener = SmartFactoryRebalanceListener(self.consumer_id)

        while self.running and retries < max_retries:
            try:
                logger.info(
                    "Conectando ao Kafka em %s (Tentativa %d/%d)...",
                    self.bootstrap_servers,
                    retries + 1,
                    max_retries,
                )
                def _safe_deserialize(m: bytes) -> Optional[Dict[str, Any]]:
                    """
                    Converte o corpo de uma mensagem em dicionário sem nunca lançar exceção.

                    Args:
                        m (bytes): Valor bruto da mensagem Kafka.

                    Returns:
                        Optional[Dict[str, Any]]: O JSON decodificado, ou None se a mensagem
                        estiver vazia ou não for um JSON válido (tráfego de benchmark, por exemplo).
                    """
                    if not m:
                        return None
                    try:
                        return json.loads(m.decode("utf-8"))
                    except Exception:
                        return None

                self.consumer = KafkaConsumer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    group_id=self.group_id,
                    client_id=self.consumer_id,
                    auto_offset_reset=self.auto_offset_reset,
                    enable_auto_commit=True,
                    auto_commit_interval_ms=self.auto_commit_interval_ms,
                    value_deserializer=_safe_deserialize,
                    session_timeout_ms=self.session_timeout_ms,
                    heartbeat_interval_ms=self.heartbeat_interval_ms,
                    max_poll_interval_ms=self.max_poll_interval_ms,
                    metadata_max_age_ms=self.metadata_max_age_ms,
                )

                self.consumer.subscribe([self.topic], listener=rebalance_listener)
                logger.info("Assinatura realizada no tópico '%s'. Aguardando mensagens...", self.topic)
                return
            except (NoBrokersAvailable, KafkaError) as err:
                retries += 1
                retry_delay = min(
                    self.connect_retry_delay * (2 ** (retries - 1)),
                    self.connect_retry_max_delay,
                )
                logger.warning(
                    "Falha ao conectar aos brokers (%s). Tentando novamente em %.1f segundos...",
                    str(err),
                    retry_delay,
                )
                time.sleep(retry_delay)

        logger.error("Erro fatal: Impossível conectar ao cluster Kafka após %d tentativas.", max_retries)
        sys.exit(1)

    def evaluate_telemetry(self, data: Dict[str, Any]) -> Tuple[str, List[str]]:
        """
        Avalia as métricas do sensor contra as regras de detecção de anomalia.

        Cada grandeza (temperatura, vibração e consumo) é comparada com dois limites inclusivos:
        igual ou acima do limite de aviso gera `WARNING`; igual ou acima do crítico gera
        `CRITICAL`. A severidade final é a mais alta entre as três grandezas e cada violação
        acrescenta uma descrição à lista de motivos.

        Args:
            data (Dict[str, Any]): Dicionário com as métricas do sensor.

        Returns:
            Tuple[str, List[str]]:
                - Status de severidade ('NORMAL', 'WARNING' ou 'CRITICAL').
                - Lista de descrições das infrações detectadas (caso existam).
        """
        reasons: List[str] = []
        severity = "NORMAL"

        temp = float(data.get("temperatura", 0.0))
        vib = float(data.get("vibracao", 0.0))
        kw = float(data.get("consumo_energia_kw", 0.0))

        if temp >= self.max_temp:
            severity = "CRITICAL"
            reasons.append(f"Temperatura CRÍTICA ({temp:.1f}°C >= {self.max_temp:.1f}°C)")
        elif temp >= self.warn_temp:
            if severity != "CRITICAL":
                severity = "WARNING"
            reasons.append(f"Temperatura ELEVADA ({temp:.1f}°C >= {self.warn_temp:.1f}°C)")

        if vib >= self.max_vibration:
            severity = "CRITICAL"
            reasons.append(f"Vibração CRÍTICA ({vib:.2f}mm/s >= {self.max_vibration:.2f}mm/s)")
        elif vib >= self.warn_vibration:
            if severity != "CRITICAL":
                severity = "WARNING"
            reasons.append(f"Vibração ELEVADA ({vib:.2f}mm/s >= {self.warn_vibration:.2f}mm/s)")

        if kw >= self.max_power_kw:
            severity = "CRITICAL"
            reasons.append(f"Consumo CRÍTICO ({kw:.1f}kW >= {self.max_power_kw:.1f}kW)")
        elif kw >= self.warn_power_kw:
            if severity != "CRITICAL":
                severity = "WARNING"
            reasons.append(f"Consumo ELEVADO ({kw:.1f}kW >= {self.warn_power_kw:.1f}kW)")

        return severity, reasons

    def record_alert(
        self,
        severity: str,
        reasons: List[str],
        data: Dict[str, Any],
        partition: int,
        offset: int,
    ) -> None:
        """
        Registra um evento anômalo no arquivo persistente de alertas compartilhados.

        Args:
            severity (str): Categoria do alerta ('WARNING' ou 'CRITICAL').
            reasons (List[str]): Lista de motivos da anomalia.
            data (Dict[str, Any]): Conteúdo completo da telemetria recebida.
            partition (int): Partição Kafka de onde o evento se originou.
            offset (int): Posição do offset da mensagem processada.
        """
        alert_record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "consumer_id": self.consumer_id,
            "partition": partition,
            "offset": offset,
            "severity": severity,
            "sensor_id": data.get("sensor_id", "desconhecido"),
            "setor": data.get("setor", "desconhecido"),
            "reasons": reasons,
            "telemetry": data,
        }

        try:
            os.makedirs(os.path.dirname(self.alert_log_path), exist_ok=True)
            with open(self.alert_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(alert_record, ensure_ascii=False) + "\n")
                f.flush()
        except Exception as exc:
            logger.error("Falha ao persistir alerta em %s: %s", self.alert_log_path, str(exc))

    def run(self) -> None:
        """
        Executa o loop contínuo de polling e consumo de mensagens do Apache Kafka.

        Processa registros em lotes curtos, avalia cada telemetria, atualiza logs estruturados
        e registra alertas caso anomalias operacionais sejam identificadas. O poll usa um
        timeout curto para que o laço reaja rapidamente aos sinais de parada. Mensagens que não
        são um JSON de telemetria (por exemplo, tráfego de benchmark) são descartadas; leituras
        normais vão para o log e as anômalas também para o arquivo compartilhado de alertas.
        """
        self._setup_signal_handlers()
        self.connect()

        logger.info("Loop de consumo iniciado para o grupo '%s'!", self.group_id)

        try:
            while self.running:
                records = self.consumer.poll(
                    timeout_ms=self.poll_timeout_ms, max_records=self.poll_max_records
                )

                if not records:
                    continue

                for topic_partition, messages in records.items():
                    for msg in messages:
                        try:
                            payload: Optional[Dict[str, Any]] = msg.value
                            if not isinstance(payload, dict):
                                continue

                            if self.processing_delay_seg > 0:
                                time.sleep(self.processing_delay_seg)

                            severity, reasons = self.evaluate_telemetry(payload)

                            sensor_id = payload.get("sensor_id", "N/A")
                            setor = payload.get("setor", "N/A")
                            temp = payload.get("temperatura", 0.0)
                            vib = payload.get("vibracao", 0.0)
                            kw = payload.get("consumo_energia_kw", 0.0)

                            if severity in ("WARNING", "CRITICAL"):
                                logger.warning(
                                    "[ALERTA %s] [Partição %d | Offset %d] Sensor: %s (%s) | Motivos: %s | T: %.1f°C | V: %.2fmm/s | Pot: %.1fkW",
                                    severity,
                                    msg.partition,
                                    msg.offset,
                                    sensor_id,
                                    setor,
                                    "; ".join(reasons),
                                    temp,
                                    vib,
                                    kw,
                                )
                                self.record_alert(
                                    severity=severity,
                                    reasons=reasons,
                                    data=payload,
                                    partition=msg.partition,
                                    offset=msg.offset,
                                )
                            else:
                                logger.info(
                                    "[NORMAL] [Partição %d | Offset %d] Sensor: %s (%s) | T: %.1f°C | V: %.2fmm/s | Pot: %.1fkW",
                                    msg.partition,
                                    msg.offset,
                                    sensor_id,
                                    setor,
                                    temp,
                                    vib,
                                    kw,
                                )

                        except Exception as msg_err:
                            logger.error(
                                "Erro ao processar mensagem individual (Offset %d): %s",
                                msg.offset,
                                str(msg_err),
                            )

        except Exception as loop_err:
            logger.error("Erro fatal no loop de consumo: %s", str(loop_err), exc_info=True)
        finally:
            self.close()

    def close(self) -> None:
        """
        Fecha a conexão do consumidor Kafka e avisa o Group Coordinator para rebalanceamento imediato.
        """
        if self.consumer:
            logger.info("Desconectando consumidor %s e liberando partições...", self.consumer_id)
            try:
                self.consumer.close(autocommit=True)
                logger.info("Consumidor desconectado com sucesso.")
            except Exception as err:
                logger.warning("Aviso durante o encerramento do consumidor: %s", str(err))


def main() -> None:
    """
    Ponto de entrada do script consumidor.
    """
    consumer = SmartFactoryConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
