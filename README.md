# Kafka Multi-Cluster Disaster Recovery with Spring Cloud Stream

A production-ready example of **active-passive disaster recovery** across N Kafka clusters using Spring Boot 4.0.5 and Spring Cloud Stream 2025.1.1. The application automatically detects cluster failures, switches producers and consumers to the next healthy cluster, and fails back when the original cluster recovers.

## Architecture

```
                        +-------------------+
                        |   Application     |
                        |                   |
                        | ResilientProducer  |-----> Active Cluster
                        | IdempotentConsumer |<----- Active Cluster
                        |                   |
                        | ActiveCluster     |
                        |   Manager         |
                        |      |            |
                        | ClusterHealth     |
                        |   Checker         |
                        +------|------------+
                               |
              +----------------+----------------+
              |                |                |
        +-----v-----+   +-----v-----+   +------v----+
        |  Kafka     |   |  Kafka     |   |  Kafka    |
        |  Primary   |   |  Secondary |   |  Tertiary |
        | priority=1 |   | priority=2 |   | priority=3|
        +-----------+   +-----------+   +-----------+
```

## Features

- **N-cluster support** -- configure any number of Kafka clusters with priority-based failover
- **Automatic failover** -- health checker detects failures; producer triggers instant failover on send failure
- **Automatic failback** -- returns to the highest-priority healthy cluster
- **Consumer binding management** -- only the active cluster's consumers are running; others are stopped
- **Producer cache cleanup** -- dead cluster producers are closed to prevent reconnect noise
- **Idempotent message processing** -- Redis-based deduplication prevents duplicate processing during failover
- **Multi-format support** -- String, JSON, Avro, and raw bytes payloads with per-topic configuration
- **Fully dynamic configuration** -- clusters, consumers, and producers are defined in YAML; no code changes needed to add topics or clusters
- **Per-topic handler mapping** -- business logic methods are mapped to topics via configuration

## Project Structure

```
src/main/
  avro/
    PaymentEvent.avsc                  # Avro schema (generates Java class)
  java/com/example/kafkadr/
    KafkaDrExampleApplication.java     # Entry point
    config/
      KafkaClusterProperties.java      # Configuration model (clusters, consumers, producers)
      DynamicBindingRegistrar.java     # Generates binders, bindings, and consumer beans
    consumer/
      IdempotentConsumer.java          # Deduplication wrapper (Message<?>)
      MessageHandlerRegistry.java      # Maps topics to handler methods with payload conversion
      MessageProcessor.java            # Business logic methods (add your handlers here)
    producer/
      ResilientProducer.java           # Send with automatic failover across clusters
      MessageProducerController.java   # REST API for all payload types
    routing/
      ActiveClusterManager.java        # Cluster election state machine
      BindingLifecycleManager.java     # Start/stop consumer + producer bindings on switch
      ClusterSwitchedEvent.java        # Spring event published on failover/failback
    health/
      ClusterHealthChecker.java        # Periodic health probe for all clusters
    idempotency/
      IdempotencyStore.java            # Interface
      RedisIdempotencyStore.java       # Redis SET NX EX (default)
      InMemoryIdempotencyStore.java    # In-memory fallback (profile: in-memory-idempotency)
    model/
      OrderEvent.java                  # Sample JSON POJO
  resources/
    application.yml                    # All configuration in one place
docker-compose.yml                     # 3 Kafka clusters + Schema Registry + Redis
```

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.8+
- Docker & Docker Compose

### Run

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Build and run
mvn clean spring-boot:run
```

### Test All Payload Types

```bash
# String
curl -X POST 'localhost:8080/api/messages/demo-events?message=hello+world'

# JSON
curl -X POST localhost:8080/api/messages/order-events/json \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"ORD-001","product":"Laptop","quantity":2,"price":999.99}'

# Avro
curl -X POST 'localhost:8080/api/messages/payment-events/avro?paymentId=PAY-001&orderId=ORD-001&amount=1999.98&currency=USD&status=COMPLETED'

# Raw bytes
echo -n "sensor-data" | curl -X POST localhost:8080/api/messages/raw-telemetry/bytes \
  -H 'Content-Type: application/octet-stream' --data-binary @-

# Cluster status
curl -s localhost:8080/api/messages/status | jq
```

### Test Failover

```bash
# Kill primary cluster
docker-compose stop kafka-primary

# Send a message -- triggers instant failover to secondary
curl -X POST 'localhost:8080/api/messages/demo-events?message=after+failover'

# Verify switch
curl -s localhost:8080/api/messages/status | jq

# Restore primary -- auto failback after recovery threshold
docker-compose start kafka-primary

# Cascade failover -- kill two clusters
docker-compose stop kafka-primary kafka-secondary
curl -X POST 'localhost:8080/api/messages/demo-events?message=on+tertiary'
docker-compose start kafka-primary kafka-secondary
```

## Configuration

Everything is configured under the `kafka-dr` prefix. Binders, bindings, and function definitions are generated automatically at startup.

### Clusters

```yaml
kafka-dr:
  clusters:
    us-east:
      bootstrap-servers: kafka-us-east:9092
      priority: 1                    # Lowest value = highest priority
    eu-west:
      bootstrap-servers: kafka-eu-west:9092
      priority: 2
      environment:                   # Per-cluster binder overrides
        spring.cloud.stream.kafka.binder:
          configuration:
            ssl.truststore.location: /certs/eu-truststore.p12
```

### Default Binder Environment

Applied to all clusters. Per-cluster `environment` overrides these defaults. Structure is identical to `spring.cloud.stream.binders.<name>.environment`:

```yaml
kafka-dr:
  default-environment:
    spring.cloud.stream.kafka.binder:
      auto-create-topics: true
      replication-factor: 3
      configuration:
        security.protocol: SSL
        ssl.truststore.location: /certs/truststore.p12
        ssl.truststore.password: ${TRUSTSTORE_PASSWORD}
      consumer-properties:
        max.poll.records: 500
      producer-properties:
        acks: all
```

### Consumers

Each consumer gets an input binding on every cluster. Only the active cluster's bindings are running.

```yaml
kafka-dr:
  consumers:
    - topic: order-events
      group: my-group
      handler: processOrder          # Method name in MessageProcessor
      content-type: json             # json | string | bytes | native
      properties:                    # Per-topic Kafka consumer properties
        max.poll.records: "100"
```

**Content types:**

| Type | Conversion | Use case |
|---|---|---|
| `string` | `byte[]` -> `String` (UTF-8) | Plain text messages |
| `json` | `byte[]` -> POJO via Jackson | JSON payloads (default) |
| `native` | No conversion; Kafka deserializer handles it | Avro, Protobuf |
| `bytes` | No conversion; raw `byte[]` | Binary data |

### Producers

Independent from consumers. Each producer generates output binding properties for StreamBridge.

```yaml
kafka-dr:
  producers:
    - topic: order-events
      content-type: json

    - topic: payment-events
      content-type: native
      properties:
        value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
```

### Health Check & Failover Tuning

```yaml
kafka-dr:
  health-check:
    interval-ms: 5000       # How often to probe each cluster
    timeout-ms: 3000         # AdminClient timeout per probe
    failure-threshold: 3     # Consecutive failures before marking unhealthy
    recovery-threshold: 3    # Consecutive successes before marking healthy
```

### Idempotency

```yaml
kafka-dr:
  idempotency:
    ttl-seconds: 3600        # How long to remember processed message IDs
    key-prefix: idempotency  # Redis key prefix
```

To use in-memory idempotency (dev only, no Redis needed):

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=in-memory-idempotency
```

## Adding Business Logic

### 1. Add a handler method to `MessageProcessor`

```java
@Component
public class MessageProcessor {

    public void processOrder(Message<OrderEvent> message) {
        OrderEvent order = message.getPayload();
        orderService.process(order);
    }
}
```

### 2. Add consumer and producer in `application.yml`

```yaml
kafka-dr:
  consumers:
    - topic: order-events
      group: my-group
      handler: processOrder
      content-type: json

  producers:
    - topic: order-events
      content-type: json
```

### 3. Send messages via `ResilientProducer`

```java
@Service
public class OrderService {
    private final ResilientProducer producer;

    public void placeOrder(OrderEvent order) {
        producer.send("order-events", order, order.getOrderId());
    }
}
```

No bean registration, no binding configuration, no binder setup needed.

## How Failover Works

```
Normal operation:
  Producer  ---> primary (priority=1)
  Consumers: primary STARTED, secondary STOPPED, tertiary STOPPED

Primary goes down:
  1. Health checker detects failure (3 consecutive failures)
     OR producer send fails (instant failover)
  2. ActiveClusterManager.forceUnhealthy("primary")
  3. reelectActive() -> picks "secondary" (priority=2)
  4. ClusterSwitchedEvent published
  5. BindingLifecycleManager:
     - STOP primary consumer bindings
     - Clear StreamBridge producer cache for primary
     - START secondary consumer bindings
  6. Next producer.send() goes to secondary

Primary recovers:
  1. Health checker detects 3 consecutive successes
  2. reelectActive() -> picks "primary" (priority=1, healthy again)
  3. Bindings switch back automatically
```

## Tech Stack

- Java 17
- Spring Boot 4.0.5
- Spring Cloud 2025.1.1 (Kafka Binder)
- Apache Kafka 3.9 (KRaft, no ZooKeeper)
- Confluent Schema Registry 8.2.0
- Apache Avro 1.12.1
- Redis 7 (idempotency store)

## License

MIT
