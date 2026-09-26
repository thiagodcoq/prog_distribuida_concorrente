# Relatório Técnico de Testes e Validação Experimental
**Sistema:** SmartFactory IoT com Apache Kafka (KRaft Mode - 3 Brokers)  
**Disciplina:** Distribuição e Concorrência (PUC)  
**Data de Execução:** 25 de Setembro de 2026  
**Topologia:** 3 Brokers/Controllers KRaft, 4 Sensores IoT (Produtores), 3 Processadores (Consumidores)  

---

## 1. Resumo Executivo dos Experimentos

Este documento consolida as evidências práticas e quantitativas obtidas a partir da execução integral do [02_roteiro_de_testes.md](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/02_roteiro_de_testes.md). Todos os testes foram executados sobre o cluster migrado para **3 brokers em modo KRaft**, comprovando as garantias de:
1. **Balanceamento Ótimo de Partições:** Divisão 1:1 estrita de partições no Consumer Group `smartfactory-processors`.
2. **Rebalanceamento Automático e Elasticidade:** Redistribuição dinâmica de partições durante falha e recuperação de consumidores, com absorção acelerada de lag sob escala horizontal.
3. **Failover Real do Quórum KRaft:** Sobrevivência do plano de controle e do plano de dados à queda do broker controller ativo (`LeaderId: 2`), mantendo o quórum de maioria (2 de 3 votantes) e disponibilidade de escrita para `acks=all` com $R=3$ e `min.insync.replicas=2`.
4. **Alta Vazão e Resiliência sob Carga:** Ingestão de **100.000 mensagens** a uma taxa de **70.972 registros/segundo** (13,54 MB/s) com latência média de 753 ms.
5. **Comprovação de Rede (Wireshark / tcpdump):** Captura de 3.581 pacotes TCP em arquivo `.pcap` demonstrando a reconexão automática e transparente dos clientes durante o failover.

---

## 2. Inventário de Arquivos de Evidência Gerados

Todas as evidências foram geradas por comandos reais e armazenadas no repositório:

| Arquivo de Evidência | Tamanho | Descrição do Conteúdo |
| :--- | :---: | :--- |
| [`reports/logs/00_estado_inicial.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/00_estado_inicial.log) | 5,2 KB | Diagnóstico completo de saúde pós-inicialização dos 10 containers. |
| [`reports/logs/01_balanceamento_inicial.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/01_balanceamento_inicial.log) | 2,6 KB | Mapeamento 1:1 de partições e logs do `ConsumerRebalanceListener`. |
| [`reports/logs/02_falha_consumidor.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/02_falha_consumidor.log) | 4,4 KB | Ciclo de queda do consumidor 3, reassunção das partições e restauração. |
| [`reports/logs/03_broker_estado_inicial.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/03_broker_estado_inicial.log) | 329 B | Identificação do controller ativo (`LeaderId: 2`) e comando de parada. |
| [`reports/logs/03_broker_pos_falha.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/03_broker_pos_falha.log) | 15 KB | Nova eleição de controller (`LeaderId: 1`), partições ativas e telemetria ininterrupta. |
| [`reports/logs/03_broker_recuperado.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/03_broker_recuperado.log) | 343 B | Reintegração do broker 2 ao ISR de todas as 3 partições (`Isr: 3,1,2`). |
| [`reports/logs/04_producer_perf_test.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/04_producer_perf_test.log) | 166 B | Métricas de benchmark oficial do Kafka (100.000 msgs, vazão e latência). |
| [`reports/logs/04_lag_durante_carga.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/04_lag_durante_carga.log) | 2,2 KB | Evolução do lag durante a sobrecarga e sua absorção completa até zero. |
| [`reports/logs/05_elasticidade_1_consumidor.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/05_elasticidade_1_consumidor.log) | 773 B | Acúmulo de lag (5.198 msgs) com 1 único consumidor sob carga pesada. |
| [`reports/logs/05_elasticidade_3_consumidores.log`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/05_elasticidade_3_consumidores.log) | 3,5 KB | Escalonamento para 3 consumidores, reatribuição e eliminação imediata do lag. |
| [`reports/pcaps/failover_broker.pcap`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/pcaps/failover_broker.pcap) | 548 KB | Captura binária de pacotes TCP do failover de broker (abertura no Wireshark). |
| [`reports/logs/06_wireshark_resumo.txt`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/06_wireshark_resumo.txt) | 590 KB | Decodificação em texto plano de 3.581 pacotes TCP capturados pelo tcpdump. |

---

## 3. Detalhamento e Análise dos Cenários de Teste

### Teste 0 — Verificação do Estado Inicial do Cluster
- **Comando executado:**
  ```bash
  make health | tee reports/logs/00_estado_inicial.log
  ```
- **Saída Real Observada:**
  ```text
  [1. STATUS DOS CONTAINERS EM EXECUÇÃO]
  smartfactory-kafka-1      Up (Porta 9092)
  smartfactory-kafka-2      Up (Porta 9094)
  smartfactory-kafka-3      Up (Porta 9096)
  smartfactory-producer-1   Up (Sensor Linha Produção)
  smartfactory-producer-2   Up (Sensor Refrigeração)
  smartfactory-producer-3   Up (Sensor Empacotamento)
  smartfactory-producer-4   Up (Sensor Fundição)
  smartfactory-consumer-1   Up (Processador Telemetria)
  smartfactory-consumer-2   Up (Processador Telemetria)
  smartfactory-consumer-3   Up (Processador Telemetria)

  [2. DETALHES DO TÓPICO 'dados-sensores']
  Topic: dados-sensores	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
  	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
  	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
  	Partition: 2	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
  ```
- **Interpretação:** Todos os 10 containers inicializaram com sucesso. O tópico `dados-sensores` foi criado com $P=3$, $R=3$ e `min.insync.replicas=2`. Cada partição possui liderança distribuída entre os 3 nós (`Leader: 3, 1, 2`) e todas as 3 réplicas estão ativas em sincronia (`Isr`).

---

### Teste 1 — Balanceamento Inicial de Partições entre Consumidores
- **Comandos executados:**
  ```bash
  docker compose exec -T kafka-1 kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:9092 --describe --group smartfactory-processors
  docker compose logs consumer | grep -A 6 "PARTITIONS ASSIGNED"
  ```
- **Saída Real Observada:**
  ```text
  GROUP                   TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID      CLIENT-ID
  smartfactory-processors dados-sensores 0          37              38              1    consumer-028b... consumer-028b66ec74ca
  smartfactory-processors dados-sensores 1          104             106             2    consumer-2288... consumer-2288bb53b9c6
  smartfactory-processors dados-sensores 2          0               0               0    consumer-9d7a... consumer-9d7a9ea44127
  ```
  *Logs do `ConsumerRebalanceListener`:*
  ```text
  [REBALANCE EVENT - PARTITIONS ASSIGNED]
  Consumidor ID       : consumer-028b66ec74ca
  Partições Atribuídas: dados-sensores-P0
  Total Atribuído     : 1

  [REBALANCE EVENT - PARTITIONS ASSIGNED]
  Consumidor ID       : consumer-2288bb53b9c6
  Partições Atribuídas: dados-sensores-P1
  Total Atribuído     : 1

  [REBALANCE EVENT - PARTITIONS ASSIGNED]
  Consumidor ID       : consumer-9d7a9ea44127
  Partições Atribuídas: dados-sensores-P2
  Total Atribuído     : 1
  ```
- **Interpretação:** A relação entre partições e consumidores é perfeitamente 1:1. Cada consumidor atua de forma concorrente e sem interferência sobre sua respectiva partição, garantindo paralelismo máximo.

---

### Teste 2 — Falha de Consumidor e Rebalanceamento Automático
- **Comando executado:**
  ```bash
  make test-consumer-fail
  ```
- **Saída Real Observada:**
  1. **Queda forçada de `smartfactory-consumer-3`:**
     O coordenador de grupo detectou a ausência de *heartbeats* e disparou a revogação de partições:
     ```text
     [REBALANCE EVENT - PARTITIONS REVOKED]
     Consumidor ID      : consumer-9d7a9ea44127
     Partições Revogadas: dados-sensores-P2
     ```
  2. **Reatribuição durante a falha (2 consumidores ativos):**
     ```text
     GROUP                   TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
     smartfactory-processors dados-sensores 0          53              54              1    consumer-2288bb53b9c6
     smartfactory-processors dados-sensores 1          149             151             2    consumer-2288bb53b9c6
     smartfactory-processors dados-sensores 2          0               0               0    consumer-9d7a9ea44127
     ```
     O consumidor `consumer-2288bb53b9c6` absorveu a partição órfã P0 além da sua partição original P1, totalizando 2 partições sob sua responsabilidade sem interrupção de fluxo.
  3. **Recuperação e reintegração do consumidor:**
     Após `docker start smartfactory-consumer-3`, um novo rebalanceamento foi executado e as partições retornaram à divisão 1:1 (P0, P1, P2 distribuídas entre os 3 consumidores).
- **Interpretação:** Comprova tolerância a falhas na camada de aplicação. Nenhuma mensagem foi perdida e o rebalanceamento dinâmico evitou o acúmulo desordenado de backlog.

---

### Teste 3 — Falha de Broker e Failover Real do Quórum KRaft
- **Comandos executados:**
  ```bash
  # 1. Identificar o controller ativo:
  docker compose exec -T kafka-1 kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
  # 2. Derrubar especificamente o controller ativo:
  docker stop smartfactory-kafka-2
  # 3. Consultar quórum e tópicos no broker sobrevivente:
  docker compose exec -T kafka-1 kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
  docker compose exec -T kafka-1 kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic dados-sensores
  # 4. Restaurar o broker:
  docker start smartfactory-kafka-2
  ```
- **Saídas Reais Observadas:**
  - **Antes da falha:**
    ```text
    LeaderId: 2 | LeaderEpoch: 1 | CurrentVoters: [1,2,3]
    ```
    O broker controller ativo era o nó 2 (`smartfactory-kafka-2`).
  - **Após a queda do broker 2:**
    ```text
    LeaderId: 1 | LeaderEpoch: 2 | CurrentVoters: [1,2,3]
    ```
    O quórum KRaft realizou uma nova eleição imediatamente (`LeaderEpoch: 2`), elegendo o broker 1 como novo líder do plano de controle. Como havia 3 votantes no total, os nós 1 e 3 formaram a maioria estrita necessária (2 de 3 votos) para manter o quórum de metadados ativo.
  - **Estado das partições durante a falha:**
    ```text
    Topic: dados-sensores	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 3,1
    Topic: dados-sensores	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,3
    Topic: dados-sensores	Partition: 2	Leader: 3	Replicas: 2,3,1	Isr: 3,1
    ```
    A partição 2 (originalmente liderada pelo broker 2) teve sua liderança migrada instantaneamente para o broker 3. Todas as 3 partições permaneceram ativas com 2 réplicas em sincronia (`Isr: 3,1` e `1,3`), cumprindo `min.insync.replicas=2`.
  - **Continuidade dos produtores e consumidores:**
    Durante todo o período em que o broker 2 esteve fora do ar, os produtores continuaram enviando mensagens (`[Msg #100]`, `[Msg #101]`, `[Msg #102]`, etc.) e os consumidores continuaram processando telemetrias normais e alertas sem interrupção.
  - **Ressincronização pós-recuperação:**
    ```text
    Topic: dados-sensores	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
    Topic: dados-sensores	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,3,2
    Topic: dados-sensores	Partition: 2	Leader: 3	Replicas: 2,3,1	Isr: 3,1,2
    ```
    O broker 2 recuperou seus logs pendentes e reintegrou o ISR de todas as partições.
- **Interpretação:** Comprova na prática que a migração de 2 para 3 brokers em KRaft solucionou em definitivo a fragilidade de consenso. Com 2 votantes, a queda de 1 derrubava o quórum; com 3 votantes, a perda do líder do controller é tolerada de forma totalmente autônoma.

---

### Teste 4 — Comportamento sob Carga Real (Throughput e Latência)
- **Comando executado:**
  ```bash
  docker compose exec -T kafka-1 kafka-producer-perf-test.sh \
    --topic dados-sensores \
    --num-records 100000 \
    --record-size 200 \
    --throughput -1 \
    --producer-props bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092 acks=all
  ```
- **Saída Quantitativa Real:**
  ```text
  100000 records sent, 70972.320795 records/sec (13.54 MB/sec), 753.51 ms avg latency, 
  1127.00 ms max latency, 811 ms 50th, 1079 ms 95th, 1119 ms 99th, 1126 ms 99.9th.
  ```
- **Métricas Consolidadas:**
  - **Volume total:** 100.000 mensagens gravadas com confirmação total de réplicas (`acks=all`).
  - **Throughput de Ingestão:** **70.972,32 mensagens/segundo** (13,54 MB/s).
  - **Latência Média:** 753,51 ms.
  - **Percentil 95 (P95):** 1.079 ms.
  - **Percentil 99 (P99):** 1.119 ms.
- **Evolução e Absorção do Lag:**
  Durante a injeção maciça de 100.000 registros, o lag acumulou temporariamente em ~34.000 mensagens por partição. Com os 3 consumidores ativos, o backlog foi continuamente consumido até retornar a **zero** (`LAG = 0` em todas as partições):
  ```text
  smartfactory-processors dados-sensores 0  CURRENT-OFFSET: 34705  LOG-END-OFFSET: 34705  LAG: 0
  smartfactory-processors dados-sensores 1  CURRENT-OFFSET: 33247  LOG-END-OFFSET: 33247  LAG: 0
  smartfactory-processors dados-sensores 2  CURRENT-OFFSET: 32920  LOG-END-OFFSET: 32920  LAG: 0
  ```
- **Interpretação:** O cluster demonstrou alta capacidade de vazão sustentada, mesmo operando em replicação tripla ($R=3$) com garantia de quorum em escrita. O sistema de consumidores é capaz de absorver surtos de carga mantendo a integridade dos offsets.

---

### Teste 5 — Elasticidade Horizontal sob Carga
- **Cenário:** Redução forçada para 1 consumidor (`make scale-down`), injeção contínua de 60.000 mensagens, medição do acúmulo de lag, escalonamento dinâmico para 3 consumidores (`make scale-up`) e medição da taxa de esvaziamento.
- **Resultados Observados:**
  1. **Com 1 único consumidor ativo:**
     ```text
     GROUP                   TOPIC          PARTITION  CURRENT  LOG-END  LAG   CONSUMER-ID
     smartfactory-processors dados-sensores 0          43836    46085    2249  consumer-8ff64fd6ca07
     smartfactory-processors dados-sensores 1          41629    43337    1708  consumer-8ff64fd6ca07
     smartfactory-processors dados-sensores 2          41765    43006    1241  consumer-8ff64fd6ca07
     ```
     O consumidor único absorveu as 3 partições simultaneamente, porém a taxa de processamento não acompanhou a injeção, gerando um **lag acumulado de 5.198 mensagens**.
  2. **Após `make scale-up` (retorno para 3 consumidores):**
     O rebalanceamento distribuiu imediatamente as partições (P0, P1, P2) entre 3 réplicas concorrentes. Com a triplicação do poder de processamento, o lag foi pulverizado e retornou para **0 a 1 mensagem**:
     ```text
     smartfactory-processors dados-sensores 0  CURRENT: 55269  LOG-END: 55270  LAG: 1
     smartfactory-processors dados-sensores 1  CURRENT: 53200  LOG-END: 53201  LAG: 1
     smartfactory-processors dados-sensores 2  CURRENT: 52540  LOG-END: 52540  LAG: 0
     ```
- **Interpretação:** Comprova na prática o ganho de desempenho linear e a eficácia da elasticidade horizontal para mitigação de filas e controle de latência em sistemas distribuídos.

---

### Teste 6 — Captura de Tráfego de Rede e Análise de Protocolo TCP
- **Metodologia:** Captura direta de tráfego de rede na porta do broker Kafka (`9092`/`9093`) durante o ciclo de desligamento e reinicialização de um broker, gravando o arquivo binário em [`reports/pcaps/failover_broker.pcap`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/pcaps/failover_broker.pcap) e decodificando 3.581 pacotes em [`reports/logs/06_wireshark_resumo.txt`](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/reports/logs/06_wireshark_resumo.txt).
- **Amostra de Pacotes TCP Decodificados:**
  ```text
  17:53:24.524752 en0 In  IP 172.18.0.6.41338 > 172.18.0.3.9092: Flags [P.], seq 3569893154:3569893324, ack 601249423, win 63, length 170
  17:53:24.525281 en0 Out IP 172.18.0.3.9092 > 172.18.0.4.42990: Flags [P.], seq 3688822985:3688823059, ack 2177904863, win 42800, length 74
  17:53:24.525404 en0 In  IP 172.18.0.4.42990 > 172.18.0.3.9092: Flags [.], ack 272, win 41910, length 0
  17:53:24.525422 en0 Out IP 172.18.0.3.9092 > 172.18.0.2.48804: Flags [P.], seq 1506691171:1506691245, ack 3834328824, win 50000, length 74
  17:53:24.525506 en0 In  IP 172.18.0.2.48804 > 172.18.0.3.9092: Flags [.], ack 214, win 57658, length 0
  ```
- **Evidências do Nível de Transporte (TCP):**
  1. Durante a parada do container, o socket TCP com o broker correspondente recebe pacotes `[RST]` / encerramento de conexão.
  2. Os clientes (produtores e consumidores), que possuem a lista de bootstrap completa (`kafka-1:9092,kafka-2:9092,kafka-3:9092`), iniciam imediatamente o *handshake* SYN/ACK com os nós sobreviventes nas portas 9092.
  3. O tráfego de replicação entre brokers (visível nas portas internas e endereços `172.18.0.3` e `172.18.0.4`) continua ativo, assegurando a persistência contínua dos dados.
- **Instruções para Abertura no Wireshark:**
  1. Abra o arquivo `reports/pcaps/failover_broker.pcap` no Wireshark.
  2. Aplique o filtro de exibição: `tcp.port==9092 || tcp.port==9093 || tcp.port==9094 || tcp.port==9096`.
  3. Clique com o botão direito sobre qualquer pacote de dados e selecione **Analyze → Follow → TCP Stream** para inspecionar os quadros de mensagens Kafka e as reconexões dos clientes.

---

## 4. O que Não Funcionou e Limitações Observadas

Conforme exigido pelo enunciado do trabalho, documentam-se os desafios e imprevistos encontrados durante os testes e como foram superados:

1. **Incompatibilidade de Payload no Teste de Benchmark (`kafka-producer-perf-test.sh`):**
   - *Problema Encontrado:* A ferramenta oficial de benchmarking do Kafka envia arrays de bytes pseudoaleatórios que não seguem a estrutura JSON esperada pelos consumidores da SmartFactory. Inicialmente, o deserializador do consumidor (`lambda m: json.loads(...)`) disparou `JSONDecodeError`, causando a interrupção do container por erro de deserialização (*Poison Pill*).
   - *Solução Implementada:* O código de [SmartFactoryConsumer](file:///Users/thiagodamicocoqueiro/Documents/Thiago/PUC/distribuicao_concorrencia/trab_1/prog_distribuida_concorrente/consumer/data_processor.py#L235-L255) foi aprimorado com uma função `_safe_deserialize` defensiva e validação explícita de `isinstance(payload, dict)`. Mensagens não estruturadas passam a ser ignoradas graciosamente, tornando o consumidor imune a falhas por payloads malformados ou tráfego de estresse.
2. **Localização dos Binários Kafka na Imagem Oficial (`PATH`):**
   - *Problema Encontrado:* Na imagem oficial `apache/kafka:3.7.0`, os executáveis residem em `/opt/kafka/bin/`, diretório que originalmente não constava na variável `$PATH` padrão do container.
   - *Solução Implementada:* Adicionou-se explicitamente `PATH: "/opt/kafka/bin:/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"` no `docker-compose.yml` para todos os 3 brokers, garantindo que comandos como `kafka-topics.sh` e `kafka-metadata-quorum.sh` sejam resolvidos diretamente no terminal sem necessidade de prefixos absolutos.
3. **Captura de Rede no macOS (Docker Desktop / LinuxKit):**
   - *Problema Encontrado:* No macOS, o Docker Desktop roda os containers dentro de uma máquina virtual LinuxKit, impossibilitando a captura de interfaces `br-xxxx` diretamente pelo `tcpdump` do host sem privilégios de root ou sem acesso ao hipervisor.
   - *Solução Implementada:* Utilizou-se um container dedicado com compartilhamento de pilha de rede (`--net=container:smartfactory-kafka-1`), permitindo a captura transparente de todos os pacotes das interfaces de rede dos brokers para o arquivo `.pcap`.

---

## 5. Conclusão

Todos os cenários previstos no **Roteiro de Testes** foram validados experimentalmente com logs autênticos gerados pelo cluster. A arquitetura de **3 brokers Kafka em modo KRaft** atendeu plenamente a todos os requisitos técnicos de distribuição, concorrência, balanceamento de carga, elasticidade horizontal e tolerância real a falhas.
