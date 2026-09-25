# SmartFactory IoT: Cluster Apache Kafka Multi-Broker

**Disciplina:** Distribuição e Concorrência (PUC)  
**Tema:** Balanceamento de Carga, Elasticidade e Failover com Kafka em Containers / Docker Compose  
**Modo Kafka:** KRaft (Kafka Raft Metadata Mode - sem dependência de ZooKeeper)

---

## 1. Visão Geral da Arquitetura

O projeto simula um ambiente de fábrica inteligente (*SmartFactory*) com sensores de telemetria distribuídos por quatro setores operacionais. As medições físicas são transmitidas continuamente para um cluster Apache Kafka com alta disponibilidade, particionamento e replicação de dados. Consumidores concorrentes agrupados no mesmo *Consumer Group* processam as leituras em tempo real e detectam anomalias térmicas, mecânicas e elétricas.

```
       [ Sensor Pod 1 ]       [ Sensor Pod 2 ]       [ Sensor Pod 3 ]       [ Sensor Pod 4 ]
       (Linha Produção)        (Refrigeração)         (Empacotamento)          (Fundição)
              \                      |                      |                     /
               \                     |                      |                    /
                v                    v                      v                   v
         =============================================================================
                                     APACHE KAFKA CLUSTER
             Broker 1 (9092)           Broker 2 (9094)           Broker 3 (9096)
         -----------------------------------------------------------------------------
                                      Tópico: dados-sensores
                   [ Partição 0 ]            [ Partição 1 ]           [ Partição 2 ]
                 (L: B1, Réps: B2,B3)      (L: B2, Réps: B3,B1)     (L: B3, Réps: B1,B2)
         =============================================================================
                                /                   |                  \
                               /                    |                   \
                              v                     v                    v
                       [ Consumidor 1 ]      [ Consumidor 2 ]     [ Consumidor 3 ]
                       \---------------------------------------------------------/
                                 Consumer Group: "smartfactory-processors"
                                                    |
                                                    v
                                    [ Alertas /var/log/smartfactory ]
```

### Topologia do Tópico `dados-sensores`
- **Partições ($P$):** 3 (permite balanceamento ótimo com até 3 consumidores ativos em paralelo).
- **Fator de Replicação ($R$):** 3 (cada partição é replicada nos 3 brokers, garantindo tolerância a falhas).
- **Mínimo de Réplicas em Sincronia (`min.insync.replicas`):** 2 (tolera a queda de 1 broker mantendo disponibilidade de escrita para `acks=all`).
- **Quórum KRaft (Controller):** 3 votantes (`kafka-1`, `kafka-2`, `kafka-3`). Com $N=3$, a maioria requerida é 2 de 3, permitindo tolerância à falha de 1 nó sem perda de quórum de metadados.

### Motivação da Arquitetura de 3 Brokers (KRaft)
Em modo KRaft (Kafka Raft Metadata Mode), o quórum de metadados opera por algoritmo de consenso Raft, no qual decisões requerem maioria estrita de votos ($\lfloor N/2 \rfloor + 1$). Em uma topologia de apenas 2 votantes, a maioria mínima é 2; logo, a queda de 1 único nó já derrubaria o quórum do controller, impedindo novas eleições de liderança e alterações de metadados. Para eliminar esse antipadrão e alcançar alta disponibilidade real tanto no plano de controle quanto no plano de dados, o cluster foi configurado com **3 brokers/controllers** (número ímpar, conforme recomendação oficial do Apache Kafka), assegurando que o cluster continue totalmente operacional com a queda de qualquer nó.

---

## 2. Estrutura de Diretórios

```text
prog_distribuida_concorrente/
├── Makefile                          # Automação completa do ciclo de vida
├── README.md                         # Este manual de operação
├── docker-compose.yml                # Topologia multi-broker e pods em containers
├── config/
│   └── sensor_thresholds.env         # Limites operacionais e variáveis de ambiente
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── sensor_producer.py            # Produtor IoT com DocStrings completas
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── data_processor.py             # Consumidor com RebalanceListener e detecção de falha
├── scripts/
│   ├── simulate_broker_failure.sh    # Script do Teste 3 (queda de broker)
│   ├── simulate_consumer_failure.sh  # Script do Teste 2 (queda de consumidor)
│   └── verify_cluster_health.sh      # Inspeção de saúde e diagnóstico
└── reports/
    ├── relatorio_trabalho1.md        # Relatório técnico completo
    └── logs/                         # Evidências brutas coletadas nos testes
```

---

## 3. Pré-requisitos de Instalação

- **Docker:** Versão 20.10+ (Docker Desktop / Docker Engine)
- **Docker Compose:** Versão v2+
- **GNU Make:** Instalado nativamente no Linux / macOS

---

## 4. Guia Rápido de Execução com Makefile

O projeto foi inteiramente automatizado através do `Makefile`, eliminando comandos manuais avulsos.

### Passo 1: Construir as Imagens Docker
```bash
make build
```

### Passo 2: Subir o Cluster e os Serviços
Este comando inicializa os 3 brokers Kafka em modo KRaft, aguarda a estabilização do quórum de metadados, cria o tópico `dados-sensores` com 3 partições e fator de replicação 3, e inicia os produtores e consumidores:
```bash
make up
```

### Passo 3: Monitorar o Processamento e Alertas em Tempo Real
```bash
make logs
```

### Passo 4: Executar Teste de Queda de Consumidor e Rebalanceamento
Demonstra a redistribuição automática de partições quando um consumidor é forçado a parar:
```bash
make test-consumer-fail
```

### Passo 5: Executar Teste de Tolerância a Falhas de Broker Kafka
Demonstra o failover imediato para os brokers sobreviventes sem interrupção de fluxo de dados:
```bash
make test-broker-fail
```

### Passo 6: Verificar Diagnóstico e Saúde do Cluster
Inspeciona o estado dos containers, tópicos, ISRs e o arquivo compartilhado de alertas:
```bash
make health
```

### Passo 7: Testes de Elasticidade (Escalar Consumidores)
- **Reduzir para 1 consumidor** (1 consumidor assume todas as 3 partições):
  ```bash
  make scale-down
  ```
- **Escalar de volta para 3 consumidores** (cada consumidor assume 1 partição):
  ```bash
  make scale-up
  ```
- **Escalar para 4 consumidores** (demonstra que o 4º consumidor fica ocioso/standby):
  ```bash
  docker compose up -d --scale consumer=4
  ```

### Passo 8: Limpeza Completa
Para os serviços e descarta os volumes de dados temporários:
```bash
make clean
```

---

## 5. Parâmetros e Variáveis de Ambiente (`config/sensor_thresholds.env`)

Todos os parâmetros são estritamente externalizados conforme as diretrizes de avaliação:

| Variável | Valor Padrão | Descrição |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-1:9092,kafka-2:9092,kafka-3:9092` | Endereços dos brokers para conexão dos clientes |
| `KAFKA_TOPIC` | `dados-sensores` | Nome do tópico de telemetria |
| `KAFKA_GROUP_ID` | `smartfactory-processors` | Consumer group compartilhado |
| `WARN_TEMP` / `MAX_TEMP` | `75.0` / `85.0` | Limites de temperatura em °C (Aviso / Crítico) |
| `WARN_VIBRATION` / `MAX_VIBRATION` | `4.0` / `5.0` | Limites de vibração mecânica em mm/s |
| `WARN_POWER_KW` / `MAX_POWER_KW` | `25.0` / `30.0` | Limites de consumo elétrico em kW |
| `INTERVALO_ENVIO_SEG` | `2.0` | Frequência de leitura dos sensores em segundos |
| `CHANCE_ANOMALIA_PERCENTUAL` | `15` | Percentual de injeção de falhas estocásticas |
| `ALERT_LOG_PATH` | `/var/log/smartfactory/alerts.log` | Arquivo persistente compartilhado de alertas |
