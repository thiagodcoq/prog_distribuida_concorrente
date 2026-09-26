# ==============================================================================
# Makefile para o Trabalho 1 de Distribuição e Concorrência
# Sistema SmartFactory IoT com Apache Kafka em Modo KRaft
#
# Parâmetros (tópico, partições, grupo...) vêm de config/sensor_thresholds.env.
# ==============================================================================

include config/sensor_thresholds.env

# Quantidade de réplicas para scale-consumers / scale-sensors (ex.: make scale-sensors N=8)
N ?= 3

.PHONY: help build up down create-topic logs alerts scale-up scale-down scale-consumers scale-sensors \
	test-cluster test-balance test-consumer-fail test-broker-fail test-load test-elasticity test-sensors \
	test-two-brokers test-alerts capture-pcap test-all evidence unit-test health clean

help:
	@echo "================================================================================"
	@echo "       Comandos Disponíveis para Automação da SmartFactory Kafka"
	@echo "================================================================================"
	@echo "  make build               - Compila as imagens Docker do produtor e consumidor"
	@echo "  make up                  - Compila, sobe o cluster (aguarda healthcheck), cria o tópico e inicia tudo"
	@echo "  make down                - Para os serviços sem remover os volumes de dados"
	@echo "  make create-topic        - Cria o tópico $(KAFKA_TOPIC) ($(TOPIC_PARTITIONS) partições, replicação $(TOPIC_REPLICATION_FACTOR))"
	@echo "  make logs                - Acompanha os logs em tempo real dos consumidores"
	@echo "  make alerts              - Acompanha o arquivo compartilhado de alertas"
	@echo "  make scale-up            - Escala os consumidores para 3 réplicas"
	@echo "  make scale-down          - Reduz os consumidores para 1 réplica"
	@echo "  make scale-consumers N=5 - Escala os consumidores para N réplicas"
	@echo "  make scale-sensors N=8   - Sobe N sensores adicionais (produtores extras)"
	@echo "  --- Testes (cada um grava evidência em reports/logs/) ---"
	@echo "  make test-cluster        - T0: estado inicial do cluster"
	@echo "  make test-balance        - T1: balanceamento de partições entre consumidores"
	@echo "  make test-consumer-fail  - T2: falha de consumidor (MODE=graceful|hard)"
	@echo "  make test-broker-fail    - T3: falha de broker (MODE=graceful|hard KIND=controller|follower)"
	@echo "  make test-load           - T4: comportamento sob carga (100k mensagens)"
	@echo "  make test-elasticity     - T5: elasticidade, 1 a 5 consumidores"
	@echo "  make test-sensors        - T6: escala de sensores"
	@echo "  make test-two-brokers    - T7: limite de tolerância (2 de 3 brokers fora)"
	@echo "  make test-alerts         - T8: persistência dos alertas"
	@echo "  make capture-pcap        - T9: falha de broker com captura de pacotes (Wireshark)"
	@echo "  make unit-test           - T10: testes unitários das regras de severidade"
	@echo "  make test-all            - Executa T0 a T10 em sequência (cluster já no ar)"
	@echo "  make evidence            - clean + up + test-all: regenera TODAS as evidências"
	@echo "  make health              - Diagnóstico e inspeção de saúde do cluster"
	@echo "  make clean               - Para os serviços e remove volumes temporários"
	@echo "================================================================================"

build:
	docker compose --profile build build

up: build
	docker compose up -d --wait kafka-1 kafka-2 kafka-3
	$(MAKE) create-topic
	docker compose up -d

down:
	docker compose down

create-topic:
	docker compose exec -T kafka-1 kafka-topics.sh \
		--bootstrap-server kafka-1:9092 \
		--create --if-not-exists \
		--topic $(KAFKA_TOPIC) \
		--partitions $(TOPIC_PARTITIONS) \
		--replication-factor $(TOPIC_REPLICATION_FACTOR)
	docker compose exec -T kafka-1 kafka-topics.sh \
		--bootstrap-server kafka-1:9092 \
		--describe --topic $(KAFKA_TOPIC)

logs:
	docker compose logs -f consumer

alerts:
	docker compose exec consumer tail -f $(ALERT_LOG_PATH)

scale-up:
	docker compose up -d --scale consumer=3

scale-down:
	docker compose up -d --scale consumer=1

scale-consumers:
	docker compose up -d --no-recreate --scale consumer=$(N) consumer

scale-sensors:
	docker compose --profile scale up -d --no-deps --no-recreate --scale sensor-extra=$(N) sensor-extra

test-cluster:
	./scripts/test_cluster_inicial.sh

test-balance:
	./scripts/test_balanceamento.sh

test-consumer-fail:
	./scripts/simulate_consumer_failure.sh $(MODE)

test-broker-fail:
	./scripts/simulate_broker_failure.sh $(MODE) $(KIND)

test-load:
	./scripts/test_carga.sh

test-elasticity:
	./scripts/test_elasticidade.sh

test-sensors:
	./scripts/test_escala_sensores.sh

test-two-brokers:
	./scripts/test_dois_brokers_fora.sh

test-alerts:
	./scripts/test_persistencia_alertas.sh

capture-pcap:
	./scripts/capture_failover_pcap.sh

test-all:
	./scripts/run_all_tests.sh

evidence:
	$(MAKE) clean
	$(MAKE) up
	./scripts/run_all_tests.sh

unit-test:
	docker compose run --rm --no-deps -v "$(CURDIR)/consumer:/app" -v "$(CURDIR)/tests:/tests" consumer \
		python -m unittest discover -v -s /tests

health:
	./scripts/verify_cluster_health.sh

clean:
	docker compose --profile scale --profile build down -v
