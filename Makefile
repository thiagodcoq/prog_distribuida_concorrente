# ==============================================================================
# Makefile para o Trabalho 1 de Distribuição e Concorrência
# Sistema SmartFactory IoT com Apache Kafka em Modo KRaft
# ==============================================================================

.PHONY: help build up down create-topic logs scale-up scale-down test-consumer-fail test-broker-fail health clean

help:
	@echo "================================================================================"
	@echo "       Comandos Disponíveis para Automação da SmartFactory Kafka"
	@echo "================================================================================"
	@echo "  make build               - Compila as imagens Docker do produtor e consumidor"
	@echo "  make up                  - Inicia o cluster Kafka e todos os containers"
	@echo "  make down                - Para os serviços sem remover os volumes de dados"
	@echo "  make create-topic        - Cria o tópico dados-sensores (3 partições, replicação 2)"
	@echo "  make logs                - Acompanha os logs em tempo real dos consumidores"
	@echo "  make scale-up            - Escala os consumidores para 3 réplicas"
	@echo "  make scale-down          - Reduz os consumidores para 1 réplica"
	@echo "  make test-consumer-fail  - Executa script de teste de rebalanço de consumidor"
	@echo "  make test-broker-fail    - Executa script de teste de resiliência de broker"
	@echo "  make health              - Executa diagnóstico e inspeção de saúde do cluster"
	@echo "  make clean               - Para os serviços e remove volumes temporários"
	@echo "================================================================================"

build:
	docker compose build

up:
	docker compose up -d kafka-1 kafka-2
	@echo "Aguardando estabilização dos brokers..."
	@sleep 10
	$(MAKE) create-topic
	docker compose up -d

down:
	docker compose down

create-topic:
	docker compose exec -T kafka-1 kafka-topics.sh \
		--bootstrap-server kafka-1:9092 \
		--create --if-not-exists \
		--topic dados-sensores \
		--partitions 3 \
		--replication-factor 2
	docker compose exec -T kafka-1 kafka-topics.sh \
		--bootstrap-server kafka-1:9092 \
		--describe --topic dados-sensores

logs:
	docker compose logs -f consumer-1 consumer-2 consumer-3 2>/dev/null || docker compose logs -f consumer

scale-up:
	docker compose up -d --scale consumer=3

scale-down:
	docker compose up -d --scale consumer=1

test-consumer-fail:
	chmod +x scripts/simulate_consumer_failure.sh
	./scripts/simulate_consumer_failure.sh

test-broker-fail:
	chmod +x scripts/simulate_broker_failure.sh
	./scripts/simulate_broker_failure.sh

health:
	chmod +x scripts/verify_cluster_health.sh
	./scripts/verify_cluster_health.sh

clean:
	docker compose down -v
