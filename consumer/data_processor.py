#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Processador de telemetria da SmartFactory.

Lê as leituras dos sensores no tópico do Kafka e classifica cada uma como NORMAL, WARNING ou
CRITICAL. Todas as réplicas usam o mesmo consumer group, então o Kafka reparte as partições
entre elas e refaz a divisão quando uma réplica entra ou sai. As leituras vão para o log
(stdout) e as anomalias também para um arquivo de alertas compartilhado entre as réplicas.

Toda a configuração vem de variáveis de ambiente (ver config/sensor_thresholds.env).
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
    Registra no log quando o grupo tira ou dá partições a este consumidor.

    As mensagens do rebalanceamento (REVOKED e ASSIGNED) são a evidência usada nos testes
    de falha de consumidor e de elasticidade.

    Attributes:
        consumer_id (str): Identificador deste consumidor, incluído em cada mensagem.
    """

    def __init__(self, consumer_id: str) -> None:
        """
        Args:
            consumer_id (str): Identificador do consumidor dono do listener.
        """
        super().__init__()
        self.consumer_id: str = consumer_id

    def on_partitions_revoked(self, revoked: Set[TopicPartition]) -> None:
        """
        Chamado quando o Kafka vai retirar partições deste consumidor (início de um rebalanceamento).

        Args:
            revoked (Set[TopicPartition]): Partições retiradas.
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
        Chamado quando o rebalanceamento termina, com as partições que este consumidor passou a ter.

        Uma lista vazia significa que há mais consumidores que partições e este ficou ocioso.

        Args:
            assigned (Set[TopicPartition]): Partições atribuídas.
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
    Uma réplica do processador: consome leituras, detecta anomalias e grava os alertas.

    Attributes:
        bootstrap_servers (str): Brokers do cluster, separados por vírgula.
        topic (str): Tópico consumido.
        group_id (str): Consumer group compartilhado por todas as réplicas.
        consumer_id (str): Identificador desta réplica.
        warn_temp, max_temp (float): Limites de aviso e crítico de temperatura (°C).
        warn_vibration, max_vibration (float): Limites de aviso e crítico de vibração (mm/s).
        warn_power_kw, max_power_kw (float): Limites de aviso e crítico de consumo (kW).
        alert_log_path (str): Arquivo de alertas, num volume compartilhado entre as réplicas.
        auto_offset_reset (str): Onde começar quando o grupo ainda não tem offset salvo.
        auto_commit_interval_ms (int): Intervalo (ms) do commit automático dos offsets.
        session_timeout_ms (int): Tempo (ms) sem heartbeat até o grupo dar esta réplica como
            morta e rebalancear.
        heartbeat_interval_ms (int): Intervalo (ms) entre heartbeats ao coordenador.
        max_poll_interval_ms (int): Tempo máximo (ms) permitido entre dois polls.
        metadata_max_age_ms (int): Idade máxima (ms) dos metadados. Um valor baixo evita buscar
            em um broker que já caiu depois de um failover.
        poll_timeout_ms (int): Quanto (ms) cada poll espera por mensagens; curto para o laço
            reagir a sinais de parada.
        poll_max_records (int): Máximo de registros por poll.
        processing_delay_seg (float): Custo simulado por mensagem, em segundos (0 = desligado);
            usado no teste de elasticidade.
        connect_max_retries (int): Tentativas de conexão ao cluster.
        connect_retry_delay (float): Espera inicial (s) entre tentativas; dobra a cada falha.
        connect_retry_max_delay (float): Teto (s) da espera entre tentativas.
        running (bool): False depois de um SIGINT/SIGTERM, para o laço principal terminar.
        consumer (KafkaConsumer): Cliente do Kafka (criado em connect()).
    """

    def __init__(self) -> None:
        """
        Lê a configuração das variáveis de ambiente.

        Sem CONSUMER_ID, o identificador é `consumer-<hostname>` (o hostname de um container
        é o seu ID curto).
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
        """Faz SIGINT e SIGTERM encerrarem o laço principal de forma limpa."""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """
        Trata o sinal de parada marcando `running` como False; o laço termina e close() avisa o
        grupo que esta réplica está saindo.

        Args:
            signum (int): Número do sinal recebido.
            frame (Any): Frame de execução no momento do sinal (não usado).
        """
        logger.warning(
            "Sinal de término capturado (%d). Realizando shutdown gracioso do consumidor %s...",
            signum,
            self.consumer_id,
        )
        self.running = False

    def connect(self) -> None:
        """
        Conecta ao Kafka e assina o tópico com o listener de rebalanceamento.

        Se o cluster não responde, tenta de novo: a espera começa em `connect_retry_delay`,
        dobra a cada falha e fica limitada a `connect_retry_max_delay`.

        Raises:
            SystemExit: Se as `connect_max_retries` tentativas se esgotarem.
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
                    Decodifica o JSON da mensagem; devolve None (sem lançar exceção) se estiver
                    vazia ou inválida, como o tráfego de teste do kafka-producer-perf-test.

                    Args:
                        m (bytes): Valor bruto da mensagem.
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
        Classifica uma leitura comparando cada grandeza com seus limites.

        Os limites são inclusivos: valor igual ou acima do limite de aviso dá WARNING, e igual ou
        acima do crítico dá CRITICAL. A severidade final é a mais alta entre temperatura,
        vibração e consumo, e cada violação acrescenta um motivo à lista.

        Args:
            data (Dict[str, Any]): A leitura (chaves `temperatura`, `vibracao` e `consumo_energia_kw`).

        Returns:
            Tuple[str, List[str]]: A severidade ('NORMAL', 'WARNING' ou 'CRITICAL') e a lista de
            motivos (vazia se NORMAL).
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
        Acrescenta o alerta, como uma linha JSON, ao arquivo de alertas.

        Uma falha ao gravar só é registrada no log; não interrompe o consumo.

        Args:
            severity (str): 'WARNING' ou 'CRITICAL'.
            reasons (List[str]): Motivos do alerta.
            data (Dict[str, Any]): A leitura original.
            partition (int): Partição de onde a mensagem veio.
            offset (int): Offset da mensagem na partição.
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
        Conecta e consome mensagens até receber um sinal de parada.

        Cada leitura é classificada e registrada no log; as anômalas também vão para o arquivo de
        alertas. Mensagens que não são um JSON de telemetria são ignoradas. O poll tem timeout
        curto para o laço perceber o sinal de parada logo.
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
        """Sai do grupo e fecha a conexão, o que faz o Kafka rebalancear na hora."""
        if self.consumer:
            logger.info("Desconectando consumidor %s e liberando partições...", self.consumer_id)
            try:
                self.consumer.close(autocommit=True)
                logger.info("Consumidor desconectado com sucesso.")
            except Exception as err:
                logger.warning("Aviso durante o encerramento do consumidor: %s", str(err))


def main() -> None:
    """Cria a réplica a partir do ambiente e a executa."""
    consumer = SmartFactoryConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
