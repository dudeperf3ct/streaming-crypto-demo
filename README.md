# Streaming Data

Hands-on Kafka + Flink demo that streams Binance `BTCUSDT` trades into Kafka, consumes them in Python, and optionally processes them with Flink. Includes Kafka UI for quick inspection.

## Prereqs

- Docker + Docker Compose
- Python 3.10+ with [uv](https://github.com/astral-sh/uv) installed

## Local Kafka-only stack

### Services and Ports

- Kafka: external listener `localhost:9092` (host clients) and internal listener `kafka:29092` (containers)
- Kafka UI: `http://localhost:8080`
- Flink Dashboard (Flink stack only): `http://localhost:8081`

### Getting Started

Start Kafka + UI:

```bash
docker compose up
```

Inspect topics, messages, and consumer groups in Kafka UI at `http://localhost:8080`.

In separate terminal activate and install dependencies in virtual environment 

```bash
uv sync
source .venv/bin/activate
```

Produce trade data to Kafka (creates topic if missing):

```bash
python producer.py
```

Press `Ctrl+C` after 2 mins or keep it running to seed enough data.

Consume in batches (tweak `BATCH_SIZE` in `batch_consumer.py` to see latency effects):

```bash
python batch_consumer.py
```

### Key Concepts

* Kafka Cluster: Collection of brokers
* Controller: Keeps track of brokers and metadata
* Broker: Server that stores data (topics and partitions) and handles streaming requests from producers (writing data) and consumers (reading data)* Producer: Produces the data
* Consumer: Consumes the data
* Topics: Abstract concept that distinguishes various messages in a complex project such that producer pushes data to a topic and consumer subscribes to a topic
* Consumer Offsets: A special topic that keeps track of messages read by each consumer and topic
* Consumer Groups: Collection of consumers. Each consumer group has a ID and each consumer within a group has an ID. Kafka treats consumers within a group as single entity.
* Partitions: Topic can be partitioned. Partitions are assigned to consumer inside consumer groups. The message with same key will go to same partition.
* Replication: Partitions are replicated across multiple brokers -- fault tolerant design 
* Message : 3 components - key (used for identifying partitions), value (actual information) and timestamp
* Logs

Nice introduction to these concepts can be found here: https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/6_streaming.md#basic-kafka-components

I found the [design docs](https://kafka.apache.org/documentation/#design) useful in understanding the motivation behind some of the choices. For e.g. why is consumer pull and not push model, aren't filesystem's slow for storing logs?

## Apache Flink

Apache Flink is a distributed streaming processor that sits downstream of Kafka to do real-time transformations/analytics instead of just consuming raw messages. It creates a 

### Getting Started

Start the docker compose

```bash
docker compose -f docker-compose-flink.yml up
```

Dashboards:
- Flink: `http://localhost:8081`
- Kafka UI: `http://localhost:8080`

Run the producer to seed the data

```bash
python producer.py
```

Run the flink job

```bash
docker exec streaming-crypto-demo-jobmanager-1 /opt/flink/bin/flink run --jarfile /opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar -m http://localhost:8081 -py /opt/flink/usrlib/flink_job.py
```

> [!NOTE]
> Pyflink was a pain to setup.


### Key Concepts

* Job Manager: Coordinates jobs, parses the job graph, schedules tasks across TaskManagers, and drives checkpoints/restarts.
* Task Manager: Executes tasks, shuffles data between tasks, and reports metrics/backpressure.


## Tradeoffs to explore

- Websockets vs REST API for market data
- Kafka with ZooKeeper vs KRaft
- Kafka vs Redpanda
- Kafka Streams vs Flink for stream processing

A fun project to explore

* Production setup for Kafka and Apache Flink

Production considerations

* Controllers and Brokers at least 3 nodes
* Replication - 3, adjust as necessary
* Correct retention period
* Appropriate number of partitions
* `enable.idempotence` configuration parameter to true

Good guide on sizing the Kafka clusters: https://jbcodeforce.github.io/kafka-studies/sizing/

## Further readings

* [Data engineering zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/06-streaming)
* [Kafka Animated comic](https://www.gentlydownthe.stream/)
* [Kafka Design Docs](https://kafka.apache.org/documentation/#design)
