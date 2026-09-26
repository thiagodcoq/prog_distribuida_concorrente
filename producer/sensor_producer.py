#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sensor IoT simulado da SmartFactory.

Cada instância representa um sensor (definido por SENSOR_ID e SENSOR_SETOR) que, a cada
poucos segundos, publica no tópico do Kafka uma leitura em JSON com temperatura, vibração
e consumo de energia. Uma fração das leituras sai propositalmente fora dos limites, para
exercitar a detecção de anomalias do consumidor.

Toda a configuração vem de variáveis de ambiente (ver config/sensor_thresholds.env e o
docker-compose.yml).
"""

import json
import logging
import os
import random
import signal
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SensorProducer")


class SensorTelemetryProducer:
    """
    Gera leituras de um sensor e as envia ao Kafka.

    Attributes:
        bootstrap_servers (str): Brokers do cluster, separados por vírgula.
        topic (str): Tópico de destino.
        sensor_id (str): Identificador do sensor. Também é a chave da mensagem, então todas as
            leituras de um sensor vão para a mesma partição.
        sensor_setor (str): Setor da fábrica onde o sensor está (ex.: 'refrigeracao').
        intervalo_envio (float): Segundos entre duas leituras.
        chance_anomalia (float): Probabilidade (0 a 100) de uma leitura sair anômala.
        acks (str): Confirmação exigida do Kafka; 'all' espera todas as réplicas em sincronia.
        producer_retries (int): Reenvios automáticos em caso de erro transitório.
        request_timeout_ms (int): Tempo (ms) que um lote pode esperar confirmação antes de ser
            descartado. Precisa ser maior que o tempo de failover do controller do Kafka.
        metadata_max_age_ms (int): Idade máxima (ms) dos metadados do cluster. Um valor baixo
            evita insistir em um líder de partição que já caiu.
        connect_max_retries (int): Tentativas de conexão inicial ao cluster.
        connect_retry_delay (float): Espera inicial (s) entre tentativas; dobra a cada falha.
        connect_retry_max_delay (float): Teto (s) da espera entre tentativas.
        temp_base, vib_base, kw_base (float): Valores nominais do setor (°C, mm/s e kW).
        ruido_temp, ruido_vib, ruido_kw (float): Desvio padrão do ruído sobre cada valor nominal.
        max_temp, max_vibration, max_power_kw (float): Limites críticos; as anomalias são geradas
            acima deles.
        anomalia_fator_min, anomalia_fator_max (float): Faixa, como múltiplo do limite crítico,
            dos valores de uma leitura anômala.
        running (bool): False depois de um SIGINT/SIGTERM, para o laço principal terminar.
        producer (KafkaProducer): Cliente do Kafka (criado em connect()).
    """

    def __init__(self) -> None:
        """
        Lê a configuração das variáveis de ambiente.

        Sem SENSOR_ID (caso das réplicas criadas com `--scale`), o identificador é montado a
        partir do setor e do hostname do container.
        """
        self.bootstrap_servers: str = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        )
        self.topic: str = os.getenv("KAFKA_TOPIC", "dados-sensores")
        self.sensor_setor: str = os.getenv("SENSOR_SETOR", "linha_producao")
        self.sensor_id: str = os.getenv(
            "SENSOR_ID", f"sensor-{self.sensor_setor}-{socket.gethostname()}"
        )
        self.intervalo_envio: float = float(os.getenv("INTERVALO_ENVIO_SEG", "2.0"))
        self.chance_anomalia: float = float(
            os.getenv("CHANCE_ANOMALIA_PERCENTUAL", "15.0")
        )

        self.acks: str = os.getenv("PRODUCER_ACKS", "all")
        self.producer_retries: int = int(os.getenv("PRODUCER_RETRIES", "5"))
        self.request_timeout_ms: int = int(
            os.getenv("PRODUCER_REQUEST_TIMEOUT_MS", "45000")
        )
        self.metadata_max_age_ms: int = int(
            os.getenv("PRODUCER_METADATA_MAX_AGE_MS", "10000")
        )
        self.connect_max_retries: int = int(os.getenv("CONNECT_MAX_RETRIES", "30"))
        self.connect_retry_delay: float = float(
            os.getenv("CONNECT_RETRY_DELAY_SEG", "3.0")
        )
        self.connect_retry_max_delay: float = float(
            os.getenv("CONNECT_RETRY_MAX_DELAY_SEG", "30.0")
        )

        self.temp_base: float = float(os.getenv("TEMP_BASE", "50.0"))
        self.vib_base: float = float(os.getenv("VIB_BASE", "2.0"))
        self.kw_base: float = float(os.getenv("KW_BASE", "15.0"))
        self.ruido_temp: float = float(os.getenv("RUIDO_TEMPERATURA", "2.0"))
        self.ruido_vib: float = float(os.getenv("RUIDO_VIBRACAO", "0.4"))
        self.ruido_kw: float = float(os.getenv("RUIDO_POTENCIA_KW", "1.5"))

        self.max_temp: float = float(os.getenv("MAX_TEMP", "85.0"))
        self.max_vibration: float = float(os.getenv("MAX_VIBRATION", "5.0"))
        self.max_power_kw: float = float(os.getenv("MAX_POWER_KW", "30.0"))
        self.anomalia_fator_min: float = float(os.getenv("ANOMALIA_FATOR_MIN", "1.02"))
        self.anomalia_fator_max: float = float(os.getenv("ANOMALIA_FATOR_MAX", "1.5"))

        self.running: bool = True
        self.producer: KafkaProducer = None

        logger.info(
            "Inicializando Produtor | Sensor: %s | Setor: %s | Tópico: %s | Brokers: %s",
            self.sensor_id,
            self.sensor_setor,
            self.topic,
            self.bootstrap_servers,
        )

    def _setup_signal_handlers(self) -> None:
        """Faz SIGINT e SIGTERM encerrarem o laço principal de forma limpa."""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """
        Trata o sinal de parada marcando `running` como False.

        Args:
            signum (int): Número do sinal recebido.
            frame (Any): Frame de execução no momento do sinal (não usado).
        """
        logger.warning(
            "Sinal de interrupção recebido (%d). Encerrando produtor %s...",
            signum,
            self.sensor_id,
        )
        self.running = False

    def connect(self) -> None:
        """
        Conecta ao cluster Kafka, tentando de novo enquanto ele não responde.

        Serve para quando os brokers ainda estão subindo ou elegendo o líder do quórum. A espera
        entre tentativas começa em `connect_retry_delay`, dobra a cada falha e fica limitada a
        `connect_retry_max_delay`.

        Raises:
            SystemExit: Se as `connect_max_retries` tentativas se esgotarem.
        """
        max_retries = self.connect_max_retries
        retries = 0
        while self.running and retries < max_retries:
            try:
                logger.info(
                    "Tentando conectar ao Kafka em %s (Tentativa %d/%d)...",
                    self.bootstrap_servers,
                    retries + 1,
                    max_retries,
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks=self.acks,
                    retries=self.producer_retries,
                    max_in_flight_requests_per_connection=1,
                    request_timeout_ms=self.request_timeout_ms,
                    metadata_max_age_ms=self.metadata_max_age_ms,
                )
                logger.info("Conexão com cluster Kafka estabelecida com sucesso!")
                return
            except (NoBrokersAvailable, KafkaError) as err:
                retries += 1
                retry_delay = min(
                    self.connect_retry_delay * (2 ** (retries - 1)),
                    self.connect_retry_max_delay,
                )
                logger.warning(
                    "Brokers indisponíveis (%s). Nova tentativa em %.1f segundos...",
                    str(err),
                    retry_delay,
                )
                time.sleep(retry_delay)

        logger.error(
            "Falha crítica: Não foi possível conectar ao Kafka após %d tentativas.",
            max_retries,
        )
        sys.exit(1)

    def generate_telemetry_payload(self) -> Dict[str, Any]:
        """
        Gera uma leitura do sensor.

        Parte do valor nominal do setor mais um ruído gaussiano. Com probabilidade
        `chance_anomalia`, sorteia uma grandeza (temperatura, vibração, potência ou as três) e a
        troca por um valor acima do limite crítico, entre `anomalia_fator_min` e
        `anomalia_fator_max` vezes esse limite.

        Returns:
            Dict[str, Any]: A leitura, com as chaves `sensor_id`, `setor`, `temperatura` (°C),
            `vibracao` (mm/s), `consumo_energia_kw` (kW) e `timestamp` (ISO 8601, UTC).
        """
        is_anomaly = random.uniform(0, 100) < self.chance_anomalia

        temp = self.temp_base + random.gauss(0, self.ruido_temp)
        vibracao = max(0.1, self.vib_base + random.gauss(0, self.ruido_vib))
        consumo_kw = max(1.0, self.kw_base + random.gauss(0, self.ruido_kw))

        if is_anomaly:
            anomaly_type = random.choice(["temp", "vib", "power", "multi"])
            if anomaly_type in ("temp", "multi"):
                temp = self.max_temp * random.uniform(
                    self.anomalia_fator_min, self.anomalia_fator_max
                )
            if anomaly_type in ("vib", "multi"):
                vibracao = self.max_vibration * random.uniform(
                    self.anomalia_fator_min, self.anomalia_fator_max
                )
            if anomaly_type in ("power", "multi"):
                consumo_kw = self.max_power_kw * random.uniform(
                    self.anomalia_fator_min, self.anomalia_fator_max
                )

        payload: Dict[str, Any] = {
            "sensor_id": self.sensor_id,
            "setor": self.sensor_setor,
            "temperatura": round(temp, 2),
            "vibracao": round(vibracao, 2),
            "consumo_energia_kw": round(consumo_kw, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return payload

    def on_send_success(self, record_metadata: Any) -> None:
        """
        Callback de mensagem confirmada pelo Kafka (só registra em nível DEBUG).

        Args:
            record_metadata (RecordMetadata): Tópico, partição e offset da mensagem gravada.
        """
        logger.debug(
            "Mensagem entregue com sucesso! Tópico: %s | Partição: %d | Offset: %d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def on_send_error(self, exc: Exception) -> None:
        """
        Callback de falha no envio de uma mensagem (só registra o erro; não reenvia).

        Args:
            exc (Exception): Motivo da falha.
        """
        logger.error("Erro assíncrono ao enviar mensagem para o Kafka: %s", str(exc))

    def run(self) -> None:
        """
        Conecta e publica uma leitura a cada `intervalo_envio` segundos até receber um sinal de parada.

        O envio é assíncrono: o resultado de cada mensagem chega aos callbacks
        `on_send_success` e `on_send_error`.
        """
        self._setup_signal_handlers()
        self.connect()

        logger.info(
            "Iniciando ciclo de envio de telemetria (Intervalo: %.1fs | Anomalia: %.1f%%)...",
            self.intervalo_envio,
            self.chance_anomalia,
        )

        msg_count = 0
        try:
            while self.running:
                payload = self.generate_telemetry_payload()
                future = self.producer.send(
                    self.topic,
                    key=self.sensor_id,
                    value=payload,
                )
                future.add_callback(self.on_send_success)
                future.add_errback(self.on_send_error)

                msg_count += 1
                logger.info(
                    "[Msg #%d] Enviada por %s (%s) -> Temp: %.1f°C | Vib: %.2fmm/s | Pot: %.1fkW",
                    msg_count,
                    self.sensor_id,
                    self.sensor_setor,
                    payload["temperatura"],
                    payload["vibracao"],
                    payload["consumo_energia_kw"],
                )

                time.sleep(self.intervalo_envio)

        except Exception as exc:
            logger.error("Erro inesperado no loop do produtor: %s", str(exc), exc_info=True)
        finally:
            self.close()

    def close(self) -> None:
        """Envia o que ainda está no buffer e fecha a conexão com o Kafka."""
        if self.producer:
            logger.info("Esvaziando buffers e desconectando do cluster Kafka...")
            try:
                self.producer.flush(timeout=5)
                self.producer.close(timeout=5)
                logger.info("Produtor Kafka encerrado com sucesso.")
            except Exception as err:
                logger.warning("Aviso durante o encerramento do produtor: %s", str(err))


def main() -> None:
    """Cria o sensor a partir do ambiente e o executa."""
    producer = SensorTelemetryProducer()
    producer.run()


if __name__ == "__main__":
    main()
