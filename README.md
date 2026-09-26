# SmartFactory IoT: Cluster Apache Kafka Multi-Broker

**Disciplina:** Distribuição e Concorrência (PUC)  
**Tema:** Balanceamento de Carga, Elasticidade e Failover com Kafka em Containers / Docker Compose  
**Modo Kafka:** KRaft (Kafka Raft Metadata Mode, sem dependência de ZooKeeper)

> Para executar, demonstrar e mapear cada item do enunciado ao que foi implementado, veja
> [`docs/ROTEIRO_DE_EXECUCAO.md`](docs/ROTEIRO_DE_EXECUCAO.md). O relatório técnico está em
> [`reports/relatorio_trabalho1.md`](reports/relatorio_trabalho1.md).

---

## 1. Visão Geral da Arquitetura

O projeto simula uma fábrica inteligente (*SmartFactory*) com sensores de telemetria em quatro setores operacionais. As medições são publicadas em um cluster Apache Kafka com particionamento e replicação. Consumidores concorrentes, agrupados no mesmo *Consumer Group*, processam as leituras em tempo real e registram anomalias térmicas, mecânicas e elétricas.

```
   [Sensor 1]        [Sensor 2]        [Sensor 3]        [Sensor 4]      [Sensores extras (escala)]
  Linha Produção     Refrigeração      Empacotamento       Fundição
        \                 |                 |                 /
         v                v                 v                v
   =====================================================================
                         APACHE KAFKA CLUSTER (KRaft)
        Broker 1 (:9092)      Broker 2 (:9094)      Broker 3 (:9096)
        (broker + controller)  (broker + controller)  (broker + controller)
   ---------------------------------------------------------------------
                       Tópico: dados-sensores
              [Partição 0]      [Partição 1]      [Partição 2]
        (cada partição: 1 líder + 2 réplicas nos outros brokers; líderes
         distribuídos e realocados automaticamente em caso de falha)
   =====================================================================
                    /                |                 \
                   v                 v                  v
            [Consumidor 1]    [Consumidor 2]     [Consumidor 3]
            \---------------------------------------------------/
                 Consumer Group: "smartfactory-processors"
                                     |
                                     v
                 [Alertas JSON em /var/log/smartfactory/alerts.log]
                             (volume Docker compartilhado)
```

### Topologia do tópico `dados-sensores`
- **Partições (P = 3):** permite até 3 consumidores em paralelo (1:1); consumidores excedentes ficam ociosos.
- **Fator de replicação (R = 3):** cada partição tem cópia nos 3 brokers.
- **`min.insync.replicas` = 2:** com `acks=all`, tolera a queda de 1 broker sem perder disponibilidade de escrita.
- **Quórum KRaft:** 3 votantes (`kafka-1..3`); a maioria é 2 de 3, então a queda de 1 nó não derruba o plano de controle. Com apenas 2 votantes a queda de qualquer nó destruiria o quórum, por isso a escolha por 3.
- **Chave de partição:** `sensor_id`. Todas as leituras de um sensor vão para a mesma partição (ordem preservada por sensor). Os IDs dos sensores foram escolhidos de modo que as 3 partições recebam dados (verificado por `make test-balance`).

---

## 2. Estrutura de Diretórios

```text
prog_distribuida_concorrente/
├── Makefile                          # Automação do ciclo de vida e dos testes
├── README.md                         # Este manual de operação
├── docker-compose.yml                # Cluster, sensores e consumidores (âncoras YAML)
├── config/
│   └── sensor_thresholds.env         # ÚNICA fonte de configuração (limites, timeouts, tópico...)
├── producer/                         # Sensor IoT (Python): sensor_producer.py + Dockerfile
├── consumer/                         # Processador de telemetria (Python): data_processor.py + Dockerfile
├── scripts/
│   ├── lib.sh                        # Funções comuns (asserções, consultas ao Kafka)
│   ├── test_cluster_inicial.sh       # T0
│   ├── test_balanceamento.sh         # T1
│   ├── simulate_consumer_failure.sh  # T2 (graceful | hard)
│   ├── simulate_broker_failure.sh    # T3 (graceful | hard, controller | follower)
│   ├── test_carga.sh                 # T4
│   ├── test_elasticidade.sh          # T5
│   ├── test_escala_sensores.sh       # T6
│   ├── test_dois_brokers_fora.sh     # T7
│   ├── test_persistencia_alertas.sh  # T8
│   ├── capture_failover_pcap.sh      # T9 (tcpdump / Wireshark)
│   ├── run_all_tests.sh              # executa T0 a T10
│   └── verify_cluster_health.sh      # diagnóstico (make health)
├── tests/
│   └── test_evaluate_telemetry.py    # T10: testes unitários das regras de severidade
├── docs/
│   └── ROTEIRO_DE_EXECUCAO.md        # roteiro de demonstração, testes e rastreabilidade do enunciado
└── reports/
    ├── relatorio_trabalho1.md        # relatório técnico
    ├── logs/                         # evidências geradas pelos scripts (não editar à mão)
    └── pcaps/                        # captura de pacotes (abrir no Wireshark)
```

---

## 3. Pré-requisitos

- **Docker** 20.10+ (Docker Desktop ou Engine) e **Docker Compose v2** (com suporte a `up --wait`)
- **GNU Make** e **bash** (os scripts funcionam com o bash 3.2 do macOS)
- Acesso ao Docker Hub na primeira execução (imagens `apache/kafka:3.7.0`, `python:3.11-slim` e, para o T9, `nicolaka/netshoot`)
- Portas livres no host: 9092, 9094 e 9096

---

## 4. Guia Rápido

```bash
make build     # constrói as imagens (produtor e consumidor)
make up        # sobe 3 brokers (aguarda healthcheck), cria o tópico e inicia sensores e consumidores
make logs      # acompanha o processamento dos consumidores (Ctrl+C para sair)
make alerts    # acompanha o arquivo de alertas compartilhado
make health    # diagnóstico: containers, quórum, tópico, grupo, alertas
make down      # para tudo mantendo os volumes;  make clean  # para e APAGA os volumes
```

### Demonstrações e testes
Cada alvo grava sua evidência em `reports/logs/` e **falha (exit != 0) se alguma verificação falhar**.

| Alvo | Teste | O que demonstra |
|---|---|---|
| `make test-cluster` | T0 | Cluster saudável, P/R/ISR corretos, dados em todas as partições |
| `make test-balance` | T1 | 1 partição por consumidor; sensores distribuídos pelas partições |
| `make test-consumer-fail [MODE=graceful\|hard]` | T2 | Queda de consumidor: outro assume a partição (rebalanceamento) |
| `make test-broker-fail [MODE=graceful\|hard] [KIND=controller\|follower]` | T3 | Queda de broker: failover, zero perda de mensagens, quórum KRaft |
| `make test-load` | T4 | Surto de 100 mil mensagens e absorção do lag |
| `make test-elasticity` | T5 | 1 a 5 consumidores: ganho de vazão e consumidores ociosos |
| `make test-sensors` | T6 | Escala de produtores (sensores extras) |
| `make test-two-brokers` | T7 | Limite: com 2 de 3 brokers fora, escritas `acks=all` falham; recuperação |
| `make test-alerts` | T8 | Alertas persistem e são compartilhados entre réplicas |
| `make capture-pcap` | T9 | Falha de broker com captura de pacotes para o Wireshark |
| `make unit-test` | T10 | Regras de severidade (NORMAL/WARNING/CRITICAL) |
| `make test-all` | T0 a T10 | Suíte completa em sequência (`reports/logs/RESULTADO_TESTES.txt`) |
| `make evidence` | tudo | `clean` + `up` + `test-all`: regenera todas as evidências do zero |

`MODE=graceful` usa `docker stop` (saída controlada, o consumidor avisa o grupo). `MODE=hard` usa `docker kill` (queda abrupta: a falha só é percebida por ausência de heartbeats / sessão expirada).

### Escala manual
```bash
make scale-consumers N=5   # 5 consumidores (os 2 excedentes ficam ociosos: só há 3 partições)
make scale-down            # 1 consumidor assume as 3 partições
make scale-up              # volta a 3 consumidores
make scale-sensors N=8     # +8 sensores (produtores extras, ID derivado do hostname)
make scale-sensors N=0     # remove os sensores extras
```

### Lendo os alertas
Os consumidores gravam anomalias como linhas JSON em `/var/log/smartfactory/alerts.log` (volume `alerts-data`):
```bash
make alerts                                                        # tail -f
docker compose exec consumer tail -n 5 /var/log/smartfactory/alerts.log
docker compose exec consumer grep -c '"severity": "CRITICAL"' /var/log/smartfactory/alerts.log
```

---

## 5. Configuração (`config/sensor_thresholds.env`)

Todos os parâmetros vêm deste arquivo (carregado pelo compose, pelo Makefile e pelos scripts) ou de variáveis de ambiente do `docker-compose.yml`. Não há limites nem timeouts fixos no código Python (os valores padrão do código apenas espelham o arquivo).

| Grupo | Variáveis |
| :--- | :--- |
| Conexão | `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID` |
| Tópico | `TOPIC_PARTITIONS`, `TOPIC_REPLICATION_FACTOR`, `TOPIC_MIN_INSYNC_REPLICAS` |
| Limites de alarme | `WARN_TEMP`/`MAX_TEMP` (°C), `WARN_VIBRATION`/`MAX_VIBRATION` (mm/s), `WARN_POWER_KW`/`MAX_POWER_KW` (kW) |
| Simulação dos sensores | `INTERVALO_ENVIO_SEG`, `CHANCE_ANOMALIA_PERCENTUAL`, `ANOMALIA_FATOR_MIN/MAX`, `RUIDO_*` |
| Perfil do setor (por serviço no compose) | `SENSOR_ID`, `SENSOR_SETOR`, `TEMP_BASE`, `VIB_BASE`, `KW_BASE` |
| Produtor Kafka | `PRODUCER_ACKS`, `PRODUCER_RETRIES`, `PRODUCER_REQUEST_TIMEOUT_MS`, `PRODUCER_METADATA_MAX_AGE_MS` |
| Consumidor Kafka | `AUTO_OFFSET_RESET`, `AUTO_COMMIT_INTERVAL_MS`, `SESSION_TIMEOUT_MS`, `HEARTBEAT_INTERVAL_MS`, `MAX_POLL_INTERVAL_MS`, `CONSUMER_METADATA_MAX_AGE_MS`, `POLL_TIMEOUT_MS`, `POLL_MAX_RECORDS` |
| Reconexão (backoff exponencial) | `CONNECT_MAX_RETRIES`, `CONNECT_RETRY_DELAY_SEG`, `CONNECT_RETRY_MAX_DELAY_SEG` |
| Alertas | `ALERT_LOG_PATH` |
| Elasticidade (compose) | `PROCESSING_DELAY_MS` (custo simulado por mensagem; 0 por padrão) |

---

## 6. Solução de problemas

- **`make up` falha ao criar o tópico:** os brokers ainda não estavam saudáveis. Rode `docker compose ps` (deve mostrar `healthy`) e repita `make up`.
- **Porta 9092/9094/9096 em uso:** pare o serviço que a ocupa ou altere o mapeamento de portas em `docker-compose.yml`.
- **Testes falham logo no início com "Cluster fora do ar":** rode `make up` antes.
- **Estado "sujo" de execuções anteriores:** `make clean && make up` recria tudo do zero.
- **Logs de erro `DNS lookup failed for kafka-N` durante testes de falha:** esperado. O Docker remove o nome do container parado da rede; é o cliente Python reagindo à queda do broker.
- **Clientes fora do Docker (no host):** os brokers anunciam `kafka-N:9092`, resolvível apenas dentro da rede do compose; use `docker compose exec` para as ferramentas do Kafka.
