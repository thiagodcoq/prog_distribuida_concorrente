# Relatório Técnico: SmartFactory IoT com Apache Kafka

**Disciplina:** Distribuição e Concorrência (PUC-Rio, 2026/1)  
**Tema:** Balanceamento de Carga, Elasticidade e Failover com Kafka em Clusters Docker  
**Autores:** _[PREENCHER: nomes dos integrantes do grupo]_

> **Como este relatório foi produzido.** Todos os números abaixo vêm de logs gerados por scripts (`make evidence`, rodada única a partir de um ambiente limpo) e estão em [`logs/`](logs/). Nenhum log foi escrito à mão. Para regenerar: `make evidence` (~15 min). O roteiro que liga cada item do enunciado ao que foi implementado está em [`../docs/ROTEIRO_DE_EXECUCAO.md`](../docs/ROTEIRO_DE_EXECUCAO.md).

---

## 1. Objetivo

Construir um sistema de monitoramento de sensores de uma fábrica inteligente que **tolere falhas**, **escale** conforme cresce o número de sensores e **balanceie a carga** entre processadores de dados, usando Apache Kafka em containers. Os objetivos do enunciado e onde cada um é atendido:

| Objetivo do enunciado | Como foi atendido | Evidência |
|---|---|---|
| 1. Cluster Kafka com múltiplos brokers, tópicos com partições e replicação | 3 brokers KRaft; tópico `dados-sensores` com P=3, R=3, `min.insync.replicas=2` | `logs/T0_cluster_inicial.log` |
| 2. Sensores como produtores em containers distintos | 4 containers (um por setor) + sensores extras escaláveis | `logs/T0_*`, `logs/T6_escala_sensores.log` |
| 3. Múltiplos consumidores com balanceamento automático | Consumer group único com 3 réplicas | `logs/T1_balanceamento.log`, `logs/T5_elasticidade.log` |
| 4. Simular falhas: parar broker e parar consumidor | Scripts com modos `graceful` e `hard` | `logs/T2_*`, `logs/T3_*`, `logs/T7_*` |
| 5. Demonstrar via logs o comportamento sob carga e falhas | Logs de cada teste, curva de lag, captura de pacotes | `logs/T4_*`, `logs/T9_*`, `pcaps/` |

---

## 2. Arquitetura

### 2.1 Visão geral

```
   [Sensor 1]        [Sensor 2]        [Sensor 3]        [Sensor 4]      [Sensores extras]
  Linha Produção     Refrigeração      Empacotamento       Fundição       (escala, --scale)
        \                 |                 |                 /
         v                v                 v                v
   =====================================================================
                         APACHE KAFKA CLUSTER (KRaft)
        Broker 1 (:9092)      Broker 2 (:9094)      Broker 3 (:9096)
        (broker+controller)   (broker+controller)   (broker+controller)
   ---------------------------------------------------------------------
                       Tópico: dados-sensores
              [Partição 0]      [Partição 1]      [Partição 2]
        (1 líder + 2 réplicas por partição; líderes realocados em falhas)
   =====================================================================
                    /                |                 \
                   v                 v                  v
            [Consumidor A]    [Consumidor B]     [Consumidor C]
            \---------------------------------------------------/
                 Consumer Group: "smartfactory-processors"
                                     |
                                     v
             [Alertas JSON em /var/log/smartfactory/alerts.log]
```

Tudo roda em Docker Compose (`docker-compose.yml`) em um único host. Os componentes:

- **Sensores** (`producer/sensor_producer.py`): geram JSON `{sensor_id, setor, temperatura, vibracao, consumo_energia_kw, timestamp}` a cada `INTERVALO_ENVIO_SEG` e o publicam com `acks=all`. Uma fração das leituras (`CHANCE_ANOMALIA_PERCENTUAL`) é anômala, sempre acima dos limites críticos configurados.
- **Cluster Kafka**: três nós com papéis combinados broker+controller, imagem `apache/kafka:3.7.0`, com healthcheck.
- **Consumidores** (`consumer/data_processor.py`): classificam cada leitura em `NORMAL`, `WARNING` ou `CRITICAL` conforme os limites (`WARN_*`/`MAX_*`) e gravam as anomalias como linhas JSON no arquivo de alertas (o "logger" do enunciado), num volume Docker compartilhado entre as réplicas. Um `ConsumerRebalanceListener` registra cada revogação e atribuição de partições.

### 2.2 Decisões de projeto e justificativas

- **P=3 partições:** até 3 consumidores trabalham em paralelo (1 partição cada); o excedente fica ocioso.
- **R=3 e `min.insync.replicas=2`:** com `acks=all`, o sistema tolera a queda de 1 broker sem perder escritas.
- **3 controllers KRaft:** o quórum Raft exige maioria (⌊N/2⌋+1). Com 2 votantes a queda de qualquer nó destrói o quórum; com 3 tolera-se 1 falha. É o que sustenta o failover medido no T3.
- **Chave de partição = `sensor_id`:** preserva a ordem por sensor. Consequência descoberta nos testes: com poucas chaves, a distribuição pode ser desigual (ver §5.1).
- **Configuração externalizada:** `config/sensor_thresholds.env` é a fonte única (limites, tópico/partições/replicação, timeouts, retries, backoff, perfil de simulação), carregada pelo compose, pelo Makefile e pelos scripts. O perfil de cada setor (`TEMP_BASE`, `VIB_BASE`, `KW_BASE`) fica nas variáveis de ambiente do `docker-compose.yml`.
- **Ajustes de cliente que os testes exigiram:** `metadata_max_age_ms=10000` em produtores e consumidores e `request_timeout_ms=45000` nos produtores (ver §5.2).

---

## 3. Instalação e operação

Pré-requisitos: Docker 20.10+ com Compose v2, GNU Make e bash. Detalhes, variáveis de configuração e solução de problemas estão no [`README.md`](../README.md).

```bash
make build     # imagens do produtor e do consumidor
make up        # 3 brokers (aguarda healthcheck), cria o tópico, sobe sensores e consumidores
make logs      # processamento dos consumidores em tempo real
make health    # diagnóstico do cluster
make evidence  # clean + up + toda a suíte de testes (regenera reports/logs)
```

Escala manual: `make scale-consumers N=5`, `make scale-sensors N=8`. Cada teste tem um alvo próprio (`make help` lista todos).

---

## 4. Metodologia dos testes

- Cada teste é um script em `scripts/` com **asserções**: termina com código ≠ 0 se alguma falhar e grava seu log em `reports/logs/` com data, commit e argumentos no cabeçalho.
- Os testes de falha têm dois modos: **`graceful`** (`docker stop`, o processo recebe SIGTERM e sai do grupo avisando) e **`hard`** (`docker kill`, SIGKILL: queda abrupta). No modo `hard`, o script desativa a política de restart do container antes de matá-lo (senão o Docker o religaria sozinho) e a restaura depois.
- Comandos Kafka (`kafka-topics.sh`, `kafka-consumer-groups.sh`, `kafka-metadata-quorum.sh`, `kafka-get-offsets.sh`, `kafka-producer-perf-test.sh`) são executados dentro de um broker vivo.
- Ambiente: macOS, Docker Desktop, um único host (todos os "nós" compartilham a máquina).
- Rodada de referência: `make evidence` de 26/09/2026 (horário UTC dos logs). Resultado consolidado em [`logs/RESULTADO_TESTES.txt`](logs/RESULTADO_TESTES.txt): **12 de 12 testes PASS**.

---

## 5. Resultados

### 5.1 T0 e T1: cluster e balanceamento (`logs/T0_cluster_inicial.log`, `logs/T1_balanceamento.log`)

- 3 brokers `healthy`; quórum com `CurrentVoters: [1,2,3]`; tópico com `PartitionCount: 3`, `ReplicationFactor: 3`, `min.insync.replicas=2`; ISR completo; liderança distribuída entre brokers.
- Consumer group: 3 membros, **1 partição por consumidor**, nenhum ocioso; o listener registrou as atribuições (`PARTITIONS ASSIGNED`).
- **Sensores por partição** (chave `sensor_id`, particionador murmur2): P0 ← `sensor-usinagem-01`; P1 ← `sensor-esteira-01` e `sensor-forno-01`; P2 ← `sensor-chiller-02`.

> **Problema encontrado e corrigido.** Com o ID original `sensor-chiller-01`, os sensores de refrigeração, empacotamento e fundição caíam todos na P1 e **a P2 nunca recebia dados** (offset 0), de modo que um dos três consumidores ficava sem trabalho e o "balanceamento" existia só na atribuição. Calculamos o hash murmur2 e trocamos o ID para `sensor-chiller-02` (P2). O T0 agora **exige** que as 3 partições recebam mensagens.

### 5.2 T2 e T3: falhas de consumidor e de broker

**T2: queda de consumidor** (`logs/T2_falha_consumidor_graceful.log`, `logs/T2_falha_consumidor_hard.log`). O teste derruba o consumidor dono de uma partição **com tráfego** (P1):

| Modo | Detecção | Tempo até outro consumidor assumir a P1 | Processamento |
|---|---|---|---|
| `graceful` (SIGTERM) | O consumidor avisa que sai (LeaveGroup) | **4 s** | offset da P1 avançou de 40 para 52; grupo voltou a 3 (1:1) em 4 s |
| `hard` (SIGKILL) | Ausência de heartbeats por `session.timeout.ms` (10 s) | **13 s** | offset da P1 avançou de 63 para 83; grupo voltou a 3 (1:1) em 8 s |

A diferença entre os dois tempos é a evidência de que o `hard` realmente exercita a detecção por heartbeat. Em ambos, os banners `PARTITIONS REVOKED/ASSIGNED` dos sobreviventes estão nos logs.

**T3: queda de broker** (`logs/T3_falha_broker_graceful_follower.log`, `logs/T3_falha_broker_hard_controller.log`). Durante a queda, um `kafka-producer-perf-test` (1500 mensagens a 50/s, `acks=all`, produtor idempotente) escreve num tópico exclusivo; o script compara o delta dos offsets com o número de mensagens enviadas.

| | Follower, `graceful` (kafka-1) | Líder do quórum, `hard` (kafka-2) |
|---|---|---|
| Líderes de partição realocados em | 3 s | 21 s |
| ISR | 3 → 2 réplicas (≥ `min.insync.replicas`) | 3 → 2 réplicas |
| Quórum KRaft | líder mantido (2), epoch 1 | **nova eleição**: líder 2 → 1, epoch 1 → 3 |
| Sensores gravando durante a falha | sim (239 → 268 mensagens) | sim (354 → 418) |
| Lotes de sensores descartados | 0 | 0 |
| **Delta de offsets vs. enviadas** | **1500 = 1500** | **1500 = 1500** |
| ISR restaurado após religar | 8 s | 7 s |

Ou seja, **nenhuma mensagem foi perdida nem duplicada** na escrita idempotente com `acks=all`. Com o líder do quórum morto, a latência máxima do produtor foi de 12,7 s (ele espera o failover e conclui).

> **Problema encontrado e corrigido.** Na primeira execução do T3 em modo `hard` contra o líder do quórum, os produtores Python dos sensores **descartavam lotes** (`KafkaTimeoutError: Batch ... containing 8 record(s)`) e os consumidores **ficavam sem consumir**; tudo só se recuperava quando o broker voltava. A causa: durante a eleição do novo controlador, o cliente obtinha metadados ainda apontando para o líder morto e não os renovava (`metadata_max_age_ms` padrão de 5 min), e o `request_timeout_ms` de 15 s expirava os lotes antes do failover terminar (13 a 26 s nas medições). Corrigimos com `metadata_max_age_ms=10000` (produtor e consumidor) e `request_timeout_ms=45000` (produtor), ambos externalizados. Depois disso, 0 lotes foram descartados. O `docker stop` do T3 em modo `graceful` **não** reproduzia o problema, o que mostra o valor do modo `hard`.

### 5.3 T4: comportamento sob carga (`logs/T4_carga.log`)

100 mil mensagens JSON via `kafka-producer-perf-test` (`acks=all`, R=3), throughput máximo:

- Vazão de produção: **39.729 msg/s** (5,76 MB/s), latência média 1,2 s (máx. 2,1 s).
- O lag do grupo chegou a **20.416** mensagens na amostragem e foi absorvido até ≤ 5 em **9 s**; as 3 réplicas permaneceram no grupo.
- Todas as mensagens foram consumidas (offsets confirmados: 100.018 = 100.000 injetadas + leituras dos sensores).

Observação: o pico de lag depende de o consumidor acompanhar ou não a produção e do instante da amostragem (em outra rodada o pico amostrado foi 4), por isso ele é informativo. O critério do teste é a absorção completa.

### 5.4 T5 e T6: elasticidade e escala de sensores

**T5: consumidores** (`logs/T5_elasticidade.log`). Com um custo simulado de 20 ms por mensagem (`PROCESSING_DELAY_MS`), o **mesmo lote de 3000 mensagens** (distribuído em round-robin) é esvaziado com 1 a 5 consumidores:

| Consumidores | Tempo para esvaziar | Vazão | Observação |
|:---:|:---:|:---:|---|
| 1 | 72 s | ~41 msg/s | 1 consumidor lê as 3 partições |
| 2 | 49 s | ~61 msg/s | um consumidor fica com 2 partições (gargalo) |
| 3 | 29 s | ~103 msg/s | 1 partição por consumidor: paralelismo máximo |
| 4 | 29 s | ~103 msg/s | 1 consumidor **ocioso** (`#PARTITIONS=0`) |
| 5 | 25 s | ~120 msg/s | 2 consumidores **ociosos** |

De 1 para 3 consumidores o tempo cai ~2,5×. Acima de 3 não há ganho relevante (29 s e 25 s ficam dentro do ruído de medição de ~2 s por consulta ao grupo): o número de partições limita o paralelismo, e os excedentes atuam como reserva (*hot standby*).

**T6: sensores** (`logs/T6_escala_sensores.log`). Mensagens que chegam ao tópico em janelas de 20 s: 4 sensores → **41**; +4 extras → **79**; +8 extras → **124** (≈ 2, 4 e 6 msg/s), com **as 3 partições recebendo dados** em todos os degraus e os consumidores sem lag acumulado (≤ 30). Os sensores extras são réplicas da mesma imagem, com `SENSOR_ID` derivado do hostname.

### 5.5 T7: limite de tolerância (`logs/T7_dois_brokers_fora.log`)

Com 2 dos 3 brokers derrubados (SIGKILL) não há quórum KRaft nem ISR suficiente:

- Uma escrita `acks=all` foi **rejeitada** (0 de 20 registros confirmados; `TimeoutException: Expiring 20 record(s)`), o que é o comportamento desejado: o sistema prefere ficar indisponível a aceitar escritas sem a garantia de replicação.
- Os produtores e consumidores **não caíram** durante a indisponibilidade.
- Após religar os brokers: ISR completo em 2 s, escrita `acks=all` funcionando (20 de 20), sensores voltaram a gravar, 3 consumidores no grupo e lag ≤ 30.

Neste teste os sensores Python descartaram **64 lotes** durante a indisponibilidade, que passou de 60 s (limite: o buffer só retém dados por `request.timeout.ms`). Esse número variou entre execuções (23, 0, 65 e 64 nas quatro que fizemos), então é apenas indicativo.

### 5.6 T8: persistência dos alertas (`logs/T8_persistencia_alertas.log`)

O arquivo de alertas é visto por igual pelas 3 réplicas (230 linhas cada), tem 211 alertas `CRITICAL` e 19 `WARNING` e foi gravado por 11 `consumer_id` distintos ao longo da suíte. Após `docker compose restart consumer` ele **não foi truncado** (230 → 232 linhas) e continuou recebendo alertas (233).

### 5.7 T9: captura de pacotes (Wireshark) (`logs/T9_captura_pcap.log`, `logs/T9_wireshark_resumo.txt`, `pcaps/failover_broker.pcap`)

Um container `nicolaka/netshoot` compartilha a rede do produtor `smartfactory-producer-1` e grava com `tcpdump` o tráfego TCP da porta 9092 (31 KB) enquanto o T3 derruba o líder do quórum (kafka-2, 172.19.0.3):

| Broker | SYN enviados pelo cliente | FIN recebidos | RST recebidos | Pacotes com dados |
|---|:---:|:---:|:---:|:---:|
| kafka-1 | 1 | 0 | 0 | 44 |
| **kafka-2 (derrubado)** | **35** | **1** | **1** | 34 |
| kafka-3 | 0 | 0 | 0 | 0 |

O broker derrubado encerra a conexão (FIN e RST) e o cliente faz **35 tentativas de reconexão** a ele (sem resposta) enquanto mantém o tráfego de dados com o sobrevivente. O kafka-3 não tem pacotes de dados porque o sensor capturado só conversa com os líderes das partições que usa. Filtros úteis no Wireshark: `kafka`, `ip.addr==172.19.0.3 && (tcp.flags.fin==1 || tcp.flags.reset==1)` e `tcp.flags.syn==1 && tcp.flags.ack==0`.

### 5.8 T10: regras de severidade (`make unit-test`)

5 testes unitários das regras `NORMAL`/`WARNING`/`CRITICAL` nos limites exatos (inclusivos), executados na imagem do consumidor; todos passam.

---

## 6. O que funcionou e o que não funcionou

### 6.1 Funcionou
- Cluster de 3 brokers KRaft sem ZooKeeper, com failover de líderes e do quórum, sem perda de mensagens na escrita idempotente com `acks=all`.
- Rebalanceamento automático de consumidores e elasticidade até o limite do número de partições.
- Escala de sensores sem alterar código, e alertas persistentes e compartilhados.
- Automação completa por `Makefile`, com testes que falham de verdade e evidências regeneráveis.

### 6.2 O que não funcionou de início (encontrado pelos testes e corrigido)
1. **Partição 2 sem dados** por causa da distribuição das chaves (§5.1). Só percebemos ao inspecionar os offsets por partição.
2. **Produtores e consumidores travados após a queda abrupta do líder do quórum** (§5.2). O `docker stop` não revelava isso; foi o SIGKILL que expôs.
3. **`make build` falhava**: os 4 serviços de sensor tentavam construir a mesma tag de imagem. Passou a existir um único serviço de build.
4. **Regime nominal do forno acima dos limites de aviso** (80 °C e 28 kW contra limites de 75 °C e 25 kW): gerava alerta a cada leitura. O perfil da fundição foi reajustado para 70 °C e 22 kW.
5. **Medições instáveis nos próprios testes**: o `perf-test` devolve exit 0 mesmo quando todos os envios expiram (passamos a validar a contagem de registros enviados); o T5 media a partida da JVM junto com o esvaziamento; o T4 dependia do pico de lag amostrado. Todos foram corrigidos para critérios factuais.
6. **Nomes das réplicas**: o Compose não reaproveita os números ao escalar (`consumer-8`, `-9`, `-10`); os scripts descobrem os containers dinamicamente.

### 6.3 Limitações que permanecem
- **Tolerância de exatamente 1 broker.** Com 2 de 3 fora não há quórum, e as escritas `acks=all` são rejeitadas (T7). É uma escolha consciente de consistência sobre disponibilidade.
- **Falha simulada em um único host.** Todos os containers compartilham a máquina e o Docker; não testamos partição de rede real, latência entre nós nem falha de disco.
- **Entrega *at-least-once* nos consumidores** (commit automático a cada 2 s): após queda abrupta, mensagens podem ser reprocessadas. Não há *exactly-once*.
- **Perda possível de telemetria dos sensores em indisponibilidade prolongada** (§5.5): o produtor Python só retém lotes por `request.timeout.ms`.
- **Cliente `kafka-python-ng`** (mantido pela comunidade): sob queda de broker registra muitos avisos `DNS lookup failed` (o Docker remove o nome do container parado). Um cliente Java teria comportamento mais robusto, mas o enunciado admite Python.
- **Escala do custo de processamento simulada** (`PROCESSING_DELAY_MS`): a elasticidade do T5 é medida com custo artificial por mensagem, pois o processamento real é trivial.
- **Distribuição por chave depende do conjunto de sensores.** Trocar os IDs pode deixar uma partição vazia; o `make test-balance` detecta isso.
- **Resultados variam entre execuções** (por exemplo a duração da eleição do controlador, 13 a 26 s, e os lotes descartados no T7). Os números acima são os da rodada de referência.

---

## 7. Conclusão

O sistema atende aos objetivos do enunciado: cluster multi-broker com partições e replicação, sensores e consumidores em containers, balanceamento e rebalanceamento automáticos, falha de broker e de consumidor com o sistema continuando, e demonstração via logs sob carga e falhas. O valor maior do trabalho está nos testes de falha "duros": eles revelaram dois problemas reais (partição sem dados e clientes presos a metadados desatualizados) que o `docker stop` simples e a inspeção superficial não mostravam, e ambos foram corrigidos e verificados. As limitações listadas em §6.3 delimitam o que **não** foi demonstrado.

---

## 8. Apêndice: perguntas para a arguição

O roteiro com respostas apoiadas em evidências está na Parte 6 de [`../docs/ROTEIRO_DE_EXECUCAO.md`](../docs/ROTEIRO_DE_EXECUCAO.md) (partições × replicação, 5 consumidores para 3 partições, ISR, detecção de queda de consumidor, por que 3 controllers, perda de mensagens, semântica de entrega).
