#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de Produção de Telemetria IoT para SmartFactory.

Este módulo implementa a simulação de sensores industriais inteligentes instalados
em diferentes setores da fábrica (ex: linha de produção, refrigeração, empacotamento, fundição).
Os sensores geram métricas de temperatura (°C), vibração mecânica (mm/s) e consumo de energia (kW),
injetando esporadicamente leituras anômalas para validação do sistema de detecção de anomalias.
As mensagens são serializadas em JSON e despachadas para o cluster Apache Kafka multi-broker.

Disciplina: Distribuição e Concorrência (PUC)
Data: 2026
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
    Classe responsável pela geração de telemetria e envio de mensagens para o Apache Kafka.

    Simula o comportamento de um sensor físico em ambiente industrial, realizando a
    leitura periódica de grandezas físicas e encaminhando dados para o tópico configurado.
    Possui tolerância a falhas na inicialização, realizando tentativas de reconexão
    automáticas com recuo exponencial (exponential backoff).

    Attributes:
        bootstrap_servers (str): Lista de endereços dos brokers Kafka (ex: 'kafka-1:9092,kafka-2:9092,kafka-3:9092').
        topic (str): Nome do tópico Kafka de destino (ex: 'dados-sensores').
        sensor_id (str): Identificador exclusivo do sensor físico (ex: 'sensor-usinagem-01').
        sensor_setor (str): Setor fabril onde o sensor está alocado (ex: 'linha_producao').
        intervalo_envio (float): Intervalo em segundos entre cada medição transmitida.
        chance_anomalia (float): Probabilidade percentual (0 a 100) de gerar leitura fora dos limites operacionais.
        acks (str): Nível de confirmação exigido do broker ('all' espera todas as réplicas em sincronia).
        producer_retries (int): Reenvios automáticos do cliente Kafka em caso de erro transitório.
        request_timeout_ms (int): Tempo máximo (ms) para um lote ser confirmado antes de ser descartado.
            Deve superar o tempo de failover do líder do quórum KRaft.
        metadata_max_age_ms (int): Idade máxima (ms) dos metadados do cluster antes de renová-los,
            evitando manter líderes de partição desatualizados após a queda de um broker.
        connect_max_retries (int): Número máximo de tentativas de conexão inicial ao cluster.
        connect_retry_delay (float): Espera inicial (s) entre tentativas; dobra a cada falha.
        connect_retry_max_delay (float): Limite (s) da espera entre tentativas de conexão.
        temp_base (float): Temperatura nominal do setor, em °C.
        vib_base (float): Vibração nominal do setor, em mm/s.
        kw_base (float): Consumo nominal do setor, em kW.
        ruido_temp (float): Desvio padrão do ruído gaussiano da temperatura.
        ruido_vib (float): Desvio padrão do ruído gaussiano da vibração.
        ruido_kw (float): Desvio padrão do ruído gaussiano do consumo.
        max_temp (float): Limite crítico de temperatura (°C); as anomalias são geradas acima dele.
        max_vibration (float): Limite crítico de vibração (mm/s).
        max_power_kw (float): Limite crítico de consumo (kW).
        anomalia_fator_min (float): Menor múltiplo do limite crítico usado numa leitura anômala.
        anomalia_fator_max (float): Maior múltiplo do limite crítico usado numa leitura anômala.
        running (bool): Flag de controle do ciclo de vida da execução.
        producer (KafkaProducer): Instância do cliente produtor do Apache Kafka.
    """

    def __init__(self) -> None:
        """
        Inicializa o produtor de telemetria carregando as variáveis de ambiente necessárias.

        Todos os parâmetros vêm do ambiente (ver config/sensor_thresholds.env e o docker-compose.yml).
        Quando SENSOR_ID não é informado (caso das réplicas criadas com `--scale`), o identificador
        é derivado do setor e do hostname do container. As anomalias são geradas acima dos limites
        críticos configurados (MAX_*), de modo que acompanhem qualquer ajuste feito nesses limites.
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
        """
        Registra tratadores de sinais POSIX (SIGINT e SIGTERM) para encerramento gracioso.
        """
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        """
        Tratador de sinal para interrupção segura do processo de envio.

        Args:
            signum (int): Código numérico do sinal recebido.
            frame (Any): Quadro de execução no momento da interrupção.
        """
        logger.warning(
            "Sinal de interrupção recebido (%d). Encerrando produtor %s...",
            signum,
            self.sensor_id,
        )
        self.running = False

    def connect(self) -> None:
        """
        Estabelece a conexão com o cluster Apache Kafka com política de repetição.

        Caso os brokers ainda estejam em fase de inicialização ou eleição de quórum KRaft,
        o método aguarda e tenta novamente com recuo exponencial: a espera começa em
        `connect_retry_delay`, dobra a cada falha e é limitada a `connect_retry_max_delay`,
        até `connect_max_retries` tentativas (todos configurados por variáveis de ambiente).

        Raises:
            SystemExit: Caso todas as tentativas de conexão se esgotem sem sucesso.
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
                    acks=self.acks,  # "all": confirmação de todas as réplicas em sincronia (ISR)
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
        Gera uma leitura de telemetria industrial com base no perfil do setor.

        O perfil nominal (TEMP_BASE, VIB_BASE, KW_BASE) e o ruído vêm de variáveis de
        ambiente. Possui lógica estocástica para injetar anomalias de temperatura, vibração
        ou sobretensão elétrica de acordo com o percentual estipulado em `chance_anomalia`;
        os valores anômalos ficam entre o limite crítico (MAX_*) multiplicado por
        ANOMALIA_FATOR_MIN e por ANOMALIA_FATOR_MAX, acompanhando os limites configurados.

        Returns:
            Dict[str, Any]: Dicionário contendo os dados da telemetria:
                - sensor_id (str): ID do sensor.
                - setor (str): Nome do setor fabril.
                - temperatura (float): Temperatura medida em °C.
                - vibracao (float): Vibração medida em mm/s.
                - consumo_energia_kw (float): Potência consumida em kW.
                - timestamp (str): Carimbo de data/hora no padrão ISO 8601 UTC.
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
        Callback executado após a confirmação de recebimento da mensagem pelo broker Kafka.

        Args:
            record_metadata (RecordMetadata): Metadados retornados pelo broker (tópico, partição, offset).
        """
        logger.debug(
            "Mensagem entregue com sucesso! Tópico: %s | Partição: %d | Offset: %d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def on_send_error(self, exc: Exception) -> None:
        """
        Callback executado em caso de erro na transmissão assíncrona da mensagem.

        Args:
            exc (Exception): Exceção reportada durante a tentativa de envio.
        """
        logger.error("Erro assíncrono ao enviar mensagem para o Kafka: %s", str(exc))

    def run(self) -> None:
        """
        Inicia o loop contínuo de publicação de telemetria dos sensores.

        Executa periodicamente a geração de métricas e o envio ao Kafka até que um
        sinal de terminação seja interceptado. Cada mensagem usa o `sensor_id` como chave,
        o que garante que todas as leituras de um mesmo sensor sigam para a mesma partição
        (ordem preservada por sensor). O envio é assíncrono: o resultado chega pelos
        callbacks `on_send_success` e `on_send_error`.
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
        """
        Libera os recursos e fecha a conexão do produtor Kafka com esvaziamento de buffers.
        """
        if self.producer:
            logger.info("Esvaziando buffers e desconectando do cluster Kafka...")
            try:
                self.producer.flush(timeout=5)
                self.producer.close(timeout=5)
                logger.info("Produtor Kafka encerrado com sucesso.")
            except Exception as err:
                logger.warning("Aviso durante o encerramento do produtor: %s", str(err))


def main() -> None:
    """
    Ponto de entrada principal para execução do script produtor.
    """
    producer = SensorTelemetryProducer()
    producer.run()


if __name__ == "__main__":
    main()
