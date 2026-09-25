# Relatório Técnico: SmartFactory IoT com Apache Kafka
**Disciplina:** Distribuição e Concorrência (PUC - 2026/1)  
**Tema:** Balanceamento de Carga, Elasticidade e Failover com Kafka em Containers / Docker Compose  
**Autores:** Equipe de Engenharia de Software Distribuído  

---

## Sumário Executivo
Este relatório apresenta o projeto e a implementação de uma arquitetura de mensageria distribuída baseada em **Apache Kafka (modo KRaft)** para o cenário industrial da **SmartFactory IoT**. A solução aborda a ingestão contínua de telemetria de sensores fabris, particionamento escalável, replicação com alta disponibilidade, consumo concorrente balanceado, detecção em tempo real de anomalias operacionais e resiliência a falhas de nós consumidores e brokers de infraestrutura.

---

## 1. Introdução e Objetivo

### 1.1. O Cenário da SmartFactory
No contexto da Indústria 4.0, equipamentos fabris modernos são monitorados em tempo real por dezenas ou centenas de sensores IoT. Falhas mecânicas, sobreaquecimento ou picos de demanda elétrica não detectados podem resultar em paradas críticas de produção (*downtime*) e perdas financeiras substanciais.

O mini-mundo modelado neste trabalho contempla quatro setores industriais essenciais:
1. **Linha de Produção (`linha_producao`):** Sensor de torno/fresadora CNC (`sensor-usinagem-01`), monitorando aquecimento e esforços mecânicos de usinagem.
2. **Refrigeração Industrial (`refrigeracao`):** Sensor de unidade de resfriamento / chiller (`sensor-chiller-01`), controlando temperatura de fluidos e vibração de compressores.
3. **Empacotamento e Logística (`empacotamento`):** Sensor de esteira transportadora e braço robótico (`sensor-esteira-01`), medindo vibração e consumo.
4. **Fundição (`fundicao`):** Sensor de forno elétrico de alta potência (`sensor-forno-01`), operando sob regime de alta temperatura e alta demanda energética.

### 1.2. Objetivos de Concorrência e Tolerância a Falhas
Os principais objetivos de engenharia de software distribuído avaliados neste projeto incluem:
- **Particionamento e Paralelismo:** Dividir o fluxo de telemetria em partições lógicas independentes ($P=3$), permitindo processamento simultâneo e ordenado por chave de partição.
- **Replicação e Tolerância a Falhas de Infraestrutura:** Assegurar que os dados persistidos sobrevivam à queda súbita de qualquer nó broker no cluster através de replicação ativa ($R=3$, `min.insync.replicas=2`) e quórum KRaft com 3 votantes (maioria 2 de 3).
- **Concorrência e Rebalanceamento Dinâmico:** Organizar instâncias consumidoras em um *Consumer Group* único (`smartfactory-processors`), garantindo que a carga seja distribuída de forma balanceada e que partições órfãs sejam realocadas sem interrupção de serviço em caso de queda de réplicas.
- **Elasticidade Horizontal:** Demonstrar a capacidade de escalar para cima e para baixo a quantidade de processadores conforme a carga de trabalho.
- **Detecção de Anomalias em Tempo Real:** Classificar desvios de operação em níveis `WARNING` e `CRITICAL` e consolidá-los em repositório estruturado compartilhado.

---

## 2. Arquitetura da Solução

### 2.1. Topologia de Rede e Fluxo de Mensagens
A arquitetura foi implementada em containers Docker orquestrados via Docker Compose, utilizando Apache Kafka 3.7 em modo **KRaft (Kafka Raft Metadata Mode)** com 3 brokers, eliminando a dependência do Apache ZooKeeper e garantindo menor latência, quórum de metadados resiliente e alta disponibilidade.

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

### 2.2. Justificativa Técnica do Particionamento e Replicação

#### Estratégia de Particionamento ($P = 3$)
- **Paralelismo Máximo Efetivo:** No Apache Kafka, uma partição só pode ser atribuída a no máximo um consumidor dentro do mesmo *Consumer Group*. Com $P = 3$, o sistema suporta até 3 consumidores ativos em paralelismo total (relação 1:1).
- **Ordenação Local por Chave:** Os produtores utilizam `key = sensor_id`. O algoritmo de hash (`murmur2`) garante que todas as leituras de um mesmo sensor sejam sistematicamente encaminhadas para a mesma partição, assegurando consistência estrita de ordem cronológica na entrega ao consumidor.

#### Estratégia de Replicação ($R = 3$) e Quórum KRaft com 3 Votantes
- **Fator de Replicação ($R = 3$):** Cada mensagem gravada no líder de uma partição é replicada para todos os outros dois brokers do cluster, garantindo 3 cópias ativas dos dados em nós fisicamente independentes.
- **`min.insync.replicas = 2`:** Garante consistência estrita sem perda de disponibilidade. Com $R=3$ e `min.insync.replicas=2`, o cluster tolera a queda de 1 broker sem perder disponibilidade de escrita para produtores configurados com `acks = all`, pois restam ainda 2 réplicas ativas em sincronia (ISR).
- **Quórum KRaft Integrado (3 Votantes):** Todos os três nós (`kafka-1`, `kafka-2` e `kafka-3`) atuam com papéis combinados `broker,controller`, participando do quórum de consenso Raft para metadados e eleição de líderes (`KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"`).
- **Eliminação do Antipadrão de 2 Votantes:** Em algoritmos de consenso Raft, quórum significa maioria estrita ($\lfloor N/2 \rfloor + 1$). Com apenas 2 votantes, a maioria é 2 (100%), o que significa que a queda de 1 único broker já derrubava o quórum do controller, impedindo novas eleições de líderes e alterações de metadados. A migração para 3 votantes (maioria = 2 de 3) corrigiu esse antipadrão, alinhando a arquitetura à recomendação oficial do Apache Kafka de operar com número ímpar de controllers ($N \ge 3$) e conferindo tolerância a falhas real tanto ao plano de controle quanto ao plano de dados.

### 2.3. Especificação do Payload JSON
O contrato de dados trafegado no tópico `dados-sensores` segue o padrão estruturado abaixo:

```json
{
  "sensor_id": "sensor-usinagem-01",
  "setor": "linha_producao",
  "temperatura": 89.2,
  "vibracao": 4.1,
  "consumo_energia_kw": 18.5,
  "timestamp": "2026-09-24T10:15:20.123456Z"
}
```

---

## 3. Documentação de Instalação e Operação

### 3.1. Pré-Requisitos de Software
- **Docker Engine:** Versão 20.10 ou superior.
- **Docker Compose:** Versão v2 ou superior.
- **GNU Make:** Instalado no sistema operacional hospedeiro.

### 3.2. Ciclo de Vida Automatizado via `Makefile`

O `Makefile` centraliza todos os comandos operacionais sem intervenções manuais avulsas:

| Comando | Descrição Operacional |
| :--- | :--- |
| `make build` | Compila as imagens Docker personalizadas do produtor e do consumidor. |
| `make up` | Inicializa os 3 brokers Kafka, aguarda 10s, cria o tópico particionado e sobe todos os serviços. |
| `make down` | Encerra os containers mantendo a integridade dos volumes persistentes. |
| `make create-topic` | Cria explicitamente o tópico `dados-sensores` com $P=3$ e $R=3$ e exibe sua topologia. |
| `make logs` | Conecta-se à saída em tempo real dos consumidores de telemetria. |
| `make scale-up` | Escala o número de consumidores para 3 réplicas (1 partição por consumidor). |
| `make scale-down` | Reduz os consumidores para 1 réplica (absorve as 3 partições). |
| `make test-consumer-fail` | Executa o teste automatizado de rebalanceamento por queda de consumidor. |
| `make test-broker-fail` | Executa o teste de resiliência e failover com derrubada do broker líder. |
| `make health` | Gera diagnóstico completo do cluster, tópicos, consumer group e alertas. |
| `make clean` | Para todos os containers e remove os volumes temporários. |

---

## 4. Testes de Falha e Elasticidade (Evidências Práticas)

Os quatro experimentos a seguir foram executados e tiveram seus logs capturados no diretório `reports/logs/`.

### 4.1. Teste 1: Balanceamento Inicial de Partições
- **Cenário:** Tópico com 3 partições e 3 instâncias de consumidores registradas no grupo `smartfactory-processors`.
- **Resultado Observado:** O Kafka Coordinator efetuou a divisão perfeita 1:1 das partições entre os 3 membros.

```text
GROUP                   TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID      CLIENT-ID
smartfactory-processors dados-sensores 0          152             152             0    consumer-1-...   consumer-1
smartfactory-processors dados-sensores 1          148             148             0    consumer-2-...   consumer-2
smartfactory-processors dados-sensores 2          155             155             0    consumer-3-...   consumer-3
```

**Log do ConsumerRebalanceListener:**
```text
================================================================================
 [REBALANCE EVENT - PARTITIONS ASSIGNED]
 Consumidor ID       : consumer-1
 Partições Atribuídas: dados-sensores-P0
 Total Atribuído     : 1
 Timestamp           : 2026-09-24T10:15:29.412850Z
 Status Operacional  : ATIVO (Processando dados)
================================================================================
```

---

### 4.2. Teste 2: Falha de Consumidor e Rebalanceamento Automático
- **Cenário:** Queda forçada do container `smartfactory-consumer-3` (`docker stop smartfactory-consumer-3`).
- **Comportamento Observado:**
  1. A ausência de *heartbeats* fez o coordenador detectar a saída do consumidor.
  2. O `SmartFactoryRebalanceListener` registrou o disparo de `on_partitions_revoked` nos nós remanescentes.
  3. Em seguida, `on_partitions_assigned` foi executado: o `consumer-1` assumiu a partição órfã (P2), passando a processar P0 e P2 simultaneamente, enquanto o `consumer-2` permaneceu com P1.
  4. Nenhuma mensagem foi perdida e o fluxo de telemetria continuou estável.

**Log do Rebalanceamento no Consumidor 1:**
```text
================================================================================
 [REBALANCE EVENT - PARTITIONS ASSIGNED]
 Consumidor ID       : consumer-1
 Partições Atribuídas: dados-sensores-P0, dados-sensores-P2
 Total Atribuído     : 2
 Timestamp           : 2026-09-24T10:20:25.210450Z
 Status Operacional  : ATIVO (Processando dados)
================================================================================
```

---

### 4.3. Teste 3: Falha de Broker Kafka (Alta Disponibilidade com 3 Brokers)
- **Cenário:** O cluster operava com 3 brokers ativos. O broker `kafka-1` (líder da Partição 0) foi deliberadamente derrubado (`docker stop smartfactory-kafka-1`).
- **Comportamento Observado:**
  1. O quórum KRaft detectou a parada de `kafka-1`. Como o quórum de controllers possui 3 votantes, os nós sobreviventes (`kafka-2` e `kafka-3`) mantiveram maioria estrita ativa (2 de 3), preservando a integridade do quórum de metadados sem interrupção.
  2. Um dos brokers remanescentes no ISR (`kafka-2`) foi promovido a novo Líder da Partição 0 em milissegundos.
  3. Com `min.insync.replicas = 2` e 2 réplicas restantes em sincronia (`Isr: 2,3`), os produtores continuaram enviando telemetria com `acks=all` sem falhas.
  4. Os clientes (produtores e consumidores) reconectaram através da lista de bootstrap (`kafka-1:9092,kafka-2:9092,kafka-3:9092`), atualizando os metadados dinamicamente.
  5. Após a reinicialização do nó (`docker start smartfactory-kafka-1`), o broker sincronizou os logs pendentes e reintegrou o ISR de todas as partições (`Isr: 2,3,1`).

```text
# Estado durante a falha (Broker 1 parado; quórum de metadados e escritas mantidos por Broker 2 e 3):
Topic: dados-sensores  Partition: 0  Leader: 2  Replicas: 1,2,3  Isr: 2,3
Topic: dados-sensores  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3
Topic: dados-sensores  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,2

# Estado após recuperação (ISRs restauradas com os 3 brokers sincronizados):
Topic: dados-sensores  Partition: 0  Leader: 2  Replicas: 1,2,3  Isr: 2,3,1
Topic: dados-sensores  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Topic: dados-sensores  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,2,1
```

---

### 4.4. Teste 4: Elasticidade e Comportamento Acima do Limite de Partições
- **Cenário:** O grupo foi escalonado dinamicamente para 4 réplicas (`docker compose up -d --scale consumer=4`) em um tópico de apenas 3 partições.
- **Resultado Observado:** Como uma partição só pode ser consumida por um único membro do grupo ao mesmo tempo, o 4º consumidor entrou em modo **OCIOSO / STANDBY**.

```text
GROUP                   TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID      CLIENT-ID
smartfactory-processors dados-sensores 0          780             780             0    consumer-1-...   consumer-1
smartfactory-processors dados-sensores 1          775             775             0    consumer-2-...   consumer-2
smartfactory-processors dados-sensores 2          790             790             0    consumer-3-...   consumer-3
smartfactory-processors -              -          -               -               -    consumer-4-...   consumer-4
```

**Log do 4º Consumidor Ocioso:**
```text
================================================================================
 [REBALANCE EVENT - PARTITIONS ASSIGNED]
 Consumidor ID       : consumer-4
 Partições Atribuídas: Nenhuma (Ocioso/Standby)
 Total Atribuído     : 0
 Timestamp           : 2026-09-24T10:30:09.612450Z
 Status Operacional  : OCIOSO (Standby - Mais réplicas que partições)
================================================================================
```

#### Tabela Comparativa de Comportamento sob Escala

| Número de Consumidores | Partições por Consumidor | Taxa de Ocupação | Status do Grupo |
| :---: | :---: | :---: | :--- |
| **1 Consumidor** | P0, P1, P2 (3 partições) | 100% Ativo | Sem paralelismo; maior latência sob carga alta. |
| **2 Consumidores** | C1: P0, P2 \| C2: P1 | 100% Ativo | Carga assimétrica (relação 2:1). |
| **3 Consumidores** | C1: P0 \| C2: P1 \| C3: P2 | 100% Ativo | **Ponto ótimo de equilíbrio e paralelismo pleno.** |
| **4 Consumidores** | C1: P0 \| C2: P1 \| C3: P2 \| C4: Ocioso | 75% Ativo | C4 atua como reserva quente (*hot standby*). |

---

## 5. Análise Crítica: O que Funcionou e o que Não Funcionou

### 5.1. O que Funcionou com Pleno Sucesso
1. **KRaft Mode sem ZooKeeper:** O cluster Kafka 3.7 iniciou rapidamente com footprint reduzido de memória e sem a fragilidade operacional do ZooKeeper.
2. **RebalanceListener Estruturado:** Os callbacks `on_partitions_revoked` e `on_partitions_assigned` capturaram com clareza o ciclo de vida das partições, fornecendo visibilidade direta nos logs.
3. **Resiliência Transparente nos Clientes:** A especificação dos três nós na lista `KAFKA_BOOTSTRAP_SERVERS` (`kafka-1:9092,kafka-2:9092,kafka-3:9092`) permitiu que tanto produtores quanto consumidores reconectassem seus sockets aos brokers sobreviventes sem falhas irreversíveis.
4. **Volume Compartilhado de Alertas:** A gravação atômica em `/var/log/smartfactory/alerts.log` manteve o histórico consolidado de anomalias mesmo durante restarts de pods.

### 5.2. Desafios Enfrentados e Soluções Adotadas
1. **Temporização do Quórum KRaft na Inicialização:**
   - *Problema:* Em clusters multi-broker KRaft, os brokers precisam de alguns segundos para negociar o `CLUSTER_ID` e eleger o Metadata Quorum Leader antes de responder ao comando `kafka-topics.sh`.
   - *Solução:* Foi adicionada uma pausa de estabilização de 10 segundos no alvo `make up` e uma rotina de retry com backoff exponencial nos scripts Python.
2. **Janela de Detecção de Queda de Consumidor (`session.timeout.ms`):**
   - *Problema:* Com o valor padrão de 45 segundos, a detecção de queda forçada demorava excessivamente para testes rápidos.
   - *Solução:* Ajustou-se `session_timeout_ms = 10000` (10s) e `heartbeat_interval_ms = 3000` (3s), tornando a percepção de falha ágil sem gerar falso-positivos em rede local.
3. **Quórum KRaft e Migração de 2 para 3 Votantes:**
   - *Análise Teórica e Solução Implementada:* Em teoria de consenso distribuído (Raft), um quórum de $N$ votantes requer $\lfloor N/2 \rfloor + 1$ votos para constituir a maioria estrita. Com $N=2$, a maioria exigida é 2, o que implicava que a queda de qualquer um dos nós destruía o quórum de metadados do controller. Para sanar esse antipadrão e garantir tolerância a falhas real, a topologia foi migrada para $N=3$ votantes (`kafka-1`, `kafka-2` e `kafka-3`). Com 3 nós, a maioria necessária é de 2 nós ativos (maioria 2 de 3). Isso assegura tolerância a falhas completa: mesmo com a queda de 1 broker, o cluster mantém tanto o plano de controle (KRaft Controller Quorum operacional para novas eleições e metadados) quanto o plano de dados ($R=3$, `min.insync.replicas=2` garantindo quórum de escrita para `acks=all`), alinhando-se estritamente às boas práticas do Apache Kafka em ambientes de missão crítica.

---

## 6. Conclusão

O projeto atingiu com êxito todos os objetivos propostos na disciplina de Distribuição e Concorrência. A arquitetura implementada demonstrou como conceitos fundamentais de sistemas distribuídos — como particionamento de streams, grupos de consenso, replicação de logs e detecção de falhas por batimentos cardíacos (*heartbeats*) — operam na prática industrial com Apache Kafka.

A automação integral via `Makefile`, o código limpo com documentação completa em DocStrings e a externalização de configurações tornam o sistema robusto, manutenível e preparado para apresentação e avaliação técnica.

---

## 7. Apêndice: Preparação para a Arguição Oral da Banca

Para consulta rápida durante a defesa oral do trabalho, sintetizam-se as respostas-chave para os tópicos obrigatórios:

1. **"Qual a diferença entre o número de partições e o fator de replicação?"**  
   *Resposta:* O **número de partições** define o grau de concorrência e throughput (paralelismo de processamento entre consumidores), dividindo logicamente o fluxo de dados. O **fator de replicação** define a tolerância física a falhas e a alta disponibilidade, copiando os dados da partição entre nós brokers distintos.

2. **"O que acontece se tivermos 5 consumidores no mesmo grupo para um tópico com 3 partições?"**  
   *Resposta:* 3 consumidores processarão exatamente uma partição cada. Os 2 consumidores excedentes permanecerão em espera ociosa (*hot standby*), assumindo o processamento automaticamente caso qualquer um dos consumidores ativos sofra falha.

3. **"O que é uma réplica ISR (In-Sync Replica) no Kafka?"**  
   *Resposta:* É o conjunto de réplicas que estão totalmente sincronizadas com o líder da partição dentro da janela de tempo estipulada (`replica.lag.time.max.ms`). Apenas membros da lista ISR são elegíveis para promoção a novo líder em caso de falha do nó primário.

4. **"Como o consumidor sabe que outro consumidor caiu?"**  
   *Resposta:* O Group Coordinator (broker responsável pelo gerenciamento do grupo) monitora o envio periódico de *heartbeats* de cada consumidor. Se uma instância deixar de enviar heartbeats dentro do intervalo `session.timeout.ms`, o coordenador a declara inoperante e dispara um evento de rebalanceamento para os membros restantes.
