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
- **Replicação e Tolerância a Falhas de Infraestrutura:** Assegurar que os dados persistidos sobrevivam à queda súbita de qualquer nó broker no cluster através de replicação ativa ($R=2$) e quórum KRaft.
- **Concorrência e Rebalanceamento Dinâmico:** Organizar instâncias consumidoras em um *Consumer Group* único (`smartfactory-processors`), garantindo que a carga seja distribuída de forma balanceada e que partições órfãs sejam realocadas sem interrupção de serviço em caso de queda de réplicas.
- **Elasticidade Horizontal:** Demonstrar a capacidade de escalar para cima e para baixo a quantidade de processadores conforme a carga de trabalho.
- **Detecção de Anomalias em Tempo Real:** Classificar desvios de operação em níveis `WARNING` e `CRITICAL` e consolidá-los em repositório estruturado compartilhado.

---

## 2. Arquitetura da Solução

### 2.1. Topologia de Rede e Fluxo de Mensagens
A arquitetura foi implementada em containers Docker orquestrados via Docker Compose, utilizando Apache Kafka 3.7 em modo **KRaft (Kafka Raft Metadata Mode)**, eliminando a dependência do Apache ZooKeeper e garantindo menor latência e gestão unificada de metadados.

```
       [ Sensor Pod 1 ]       [ Sensor Pod 2 ]       [ Sensor Pod 3 ]       [ Sensor Pod 4 ]
       (Linha Produção)        (Refrigeração)         (Empacotamento)          (Fundição)
              \                      |                      |                     /
               \                     |                      |                    /
                v                    v                      v                   v
         =============================================================================
                                     APACHE KAFKA CLUSTER
                   Broker 1 (Porta 9092)              Broker 2 (Porta 9094)
         -----------------------------------------------------------------------------
                                      Tópico: dados-sensores
                   [ Partição 0 ]            [ Partição 1 ]           [ Partição 2 ]
                   (Líder: B1, Rép: B2)      (Líder: B2, Rép: B1)     (Líder: B1, Rép: B2)
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

#### Estratégia de Replicação ($R = 2$) e Quórum KRaft
- **Fator de Replicação ($R = 2$):** Cada mensagem gravada no líder de uma partição é imediatamente replicada para o broker secundário.
- **`min.insync.replicas = 1`:** Garante que, se um dos brokers falhar, o cluster continue aceitando gravações (`acks = all`) através do broker sobrevivente, priorizando a disponibilidade (*Availability*) conforme o Teorema CAP.
- **Quórum KRaft Integrado:** Ambos os nós (`kafka-1` e `kafka-2`) atuam com papéis combinados `broker,controller`, participando do quórum de consenso Raft para metadados e eleição de líderes de partição.

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
| `make up` | Inicializa os brokers Kafka, aguarda 10s, cria o tópico particionado e sobe todos os serviços. |
| `make down` | Encerra os containers mantendo a integridade dos volumes persistentes. |
| `make create-topic` | Cria explicitamente o tópico `dados-sensores` com $P=3$ e $R=2$ e exibe sua topologia. |
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

### 4.3. Teste 3: Falha de Broker Kafka (Alta Disponibilidade)
- **Cenário:** O broker `kafka-1` era o líder da Partição 0. O container foi deliberadamente derrubado (`docker stop smartfactory-kafka-1`).
- **Comportamento Observado:**
  1. O cluster KRaft detectou a indisponibilidade física de `kafka-1`.
  2. O broker remanescente (`kafka-2`), presente na lista de réplicas em sincronia (`ISR`), foi automaticamente promovido a Líder de todas as partições.
  3. Os clientes (produtores e consumidores) reconectaram seus sockets através da lista de bootstrap (`kafka-1:9092,kafka-2:9092`), atualizando os metadados sem exceções fatais.
  4. Após a reativação do nó (`docker start smartfactory-kafka-1`), as réplicas foram integralmente sincronizadas (`Isr: 2,1`).

```text
# Estado durante a falha (Liderança migrada para Broker 2):
Topic: dados-sensores  Partition: 0  Leader: 2  Replicas: 1,2  Isr: 2
Topic: dados-sensores  Partition: 1  Leader: 2  Replicas: 2,1  Isr: 2
Topic: dados-sensores  Partition: 2  Leader: 2  Replicas: 1,2  Isr: 2

# Estado após recuperação (ISRs restauradas):
Topic: dados-sensores  Partition: 0  Leader: 2  Replicas: 1,2  Isr: 2,1
Topic: dados-sensores  Partition: 1  Leader: 2  Replicas: 2,1  Isr: 2,1
Topic: dados-sensores  Partition: 2  Leader: 2  Replicas: 1,2  Isr: 2,1
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
3. **Resiliência Transparente nos Clientes:** A especificação de múltiplos nós na lista `KAFKA_BOOTSTRAP_SERVERS` permitiu que tanto produtores quanto consumidores reconectassem seus sockets ao broker sobrevivente sem falhas irreversíveis.
4. **Volume Compartilhado de Alertas:** A gravação atômica em `/var/log/smartfactory/alerts.log` manteve o histórico consolidado de anomalias mesmo durante restarts de pods.

### 5.2. Desafios Enfrentados e Soluções Adotadas
1. **Temporização do Quórum KRaft na Inicialização:**
   - *Problema:* Em clusters multi-broker KRaft, os brokers precisam de alguns segundos para negociar o `CLUSTER_ID` e eleger o Metadata Quorum Leader antes de responder ao comando `kafka-topics.sh`.
   - *Solução:* Foi adicionada uma pausa de estabilização de 10 segundos no alvo `make up` e uma rotina de retry com backoff exponencial nos scripts Python.
2. **Janela de Detecção de Queda de Consumidor (`session.timeout.ms`):**
   - *Problema:* Com o valor padrão de 45 segundos, a detecção de queda forçada demorava excessivamente para testes rápidos.
   - *Solução:* Ajustou-se `session_timeout_ms = 10000` (10s) e `heartbeat_interval_ms = 3000` (3s), tornando a percepção de falha ágil sem gerar falso-positivos em rede local.
3. **Quórum KRaft em Clusters de 2 Nós:**
   - *Análise Teórica:* Em teoria de consenso Raft, um quórum de $N$ votantes requer $\lfloor N/2 \rfloor + 1$ votos. Para $N=2$, a maioria é 2, o que significa que, caso o nó controlador ativo caia, um novo controlador de metadados não pode ser eleito até que o nó retorne. No entanto, para leitura e escrita de partições já criadas cujas réplicas estão no nó sobrevivente, o particionamento continua operando normalmente. Em ambientes de produção corporativa, recomenda-se número ímpar de votantes ($N \ge 3$).

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
