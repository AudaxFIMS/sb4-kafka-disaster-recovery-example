# Kafka Multi-Cluster Disaster Recovery with Spring Cloud Stream

A production-ready example of **active-passive disaster recovery** across N Kafka clusters using Spring Boot 4.0.5 and Spring Cloud Stream 2025.1.1. The application automatically detects cluster failures, switches producers and consumers to the next healthy cluster, and fails back when the original cluster recovers.

> **Important: Cross-cluster replication is required.**
> This application handles failover at the *application level* -- switching producers and consumers between clusters. However, it does **not** replicate data between Kafka clusters. During a failover, there is a brief window where in-flight messages may still be delivered to the previous cluster before the switch completes. To ensure no messages are lost, **cross-cluster replication must be configured independently** using tools such as [MirrorMaker 2](https://kafka.apache.org/documentation/#georeplication), Confluent Cluster Linking, or Confluent Replicator. These tools continuously replicate topics, consumer group offsets, and ACLs between clusters, ensuring that the DR cluster has a complete copy of the data at all times.

## Architecture

```
                        +--------------------+
                        |   Application      |
                        |                    |
                        | ResilientProducer  |-----> Active Cluster
                        | IdempotentConsumer |<----- Active Cluster
                        |                    |
                        | ActiveCluster      |
                        |   Manager          |
                        |      |             |
                        | ClusterHealth      |
                        |   Checker          |
                        +------|-------------+
                               |
              +----------------+----------------+
              |                |                |
        +-----v------+   +-----v------+   +------v-----+
        |  Kafka     |   |  Kafka     |   |  Kafka     |
        |  Primary   |   |  Secondary |   |  Tertiary  |
        | priority=1 |   | priority=2 |   | priority=3 |
        +------------+   +------------+   +------------+
```

## Features

- **N-cluster support** -- configure any number of Kafka clusters with priority-based failover
- **Automatic failover** -- health checker detects failures; producer triggers instant failover on send failure with error classification (serialization / cluster unavailable / transient) and configurable retries
- **Automatic failback** -- returns to the highest-priority healthy cluster when it recovers
- **Resilient startup** -- application starts instantly even if some clusters are down; unreachable clusters are initialized dynamically when they come online (no restart required)
- **Late binding initialization** -- clusters that were unavailable at startup get binders, consumer bindings, and topic provisioning created automatically once reachable
- **Synchronous send with ACK** -- `sync: true` + `acks: all` ensures broker acknowledgement before returning success, preventing silent message loss
- **Consumer binding management** -- only the active cluster's consumers are running; others are stopped
- **Producer cache cleanup** -- dead cluster producers are closed to prevent reconnect noise
- **Idempotent message processing** -- Redis-based deduplication prevents duplicate processing during failover
- **Multi-format support** -- String, JSON, Avro, and raw bytes payloads with per-topic configuration
- **Fully dynamic configuration** -- clusters, consumers, and producers are defined in YAML; no code changes needed to add topics or clusters
- **Per-topic handler mapping** -- business logic methods are mapped to topics via configuration
- **Unified property model** -- consumers and producers use the same `default-*-properties` + per-topic `properties` merge pattern with `configuration:` for Kafka client properties
- **Conditional activation** -- all DR components are gated by `kafka-dr.enabled`; without it, the app is a standard Spring Boot application
- **SSL/SASL support** -- security properties from `default-environment.configuration` are applied to all AdminClient operations (probes, health checks, topic provisioning)
- **Custom headers** -- `ResilientProducer.send()` accepts optional user headers merged with system headers (`message-id`, `source-cluster`, `sent-at`)

## Project Structure

```
src/main/
  avro/
    PaymentEvent.avsc                  # Avro schema (generates Java class)
  java/com/example/kafkadr/
    KafkaDrExampleApplication.java     # Entry point
    config/
      KafkaClusterProperties.java      # Configuration model (clusters, consumers, producers)
      KafkaAdminHelper.java            # Shared AdminClient utilities (probe, topic provisioning)
      DynamicBindingRegistrar.java     # Generates binders, bindings, and consumer beans
      StartupClusterState.java         # Tracks which clusters were initialized
    consumer/
      IdempotentConsumer.java          # Deduplication wrapper (Message<?>)
      MessageHandlerRegistry.java      # Maps topics to handler methods with payload conversion
      MessageProcessor.java            # Business logic methods (add your handlers here)
    controller/
      MessageProducerController.java   # REST API for all payload types
    producer/
      ResilientProducer.java           # Send with automatic failover across clusters
    routing/
      ActiveClusterManager.java        # Cluster election state machine
      BindingLifecycleManager.java     # Start/stop consumer + producer bindings on switch
      LateBindingInitializer.java      # Dynamically creates bindings for recovered clusters
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
docker-compose.yml                     # 3 Kafka clusters + Schema Registry (all brokers) + Redis
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

### Test Startup with Dead Cluster

```bash
# Stop primary before starting the app
docker-compose stop kafka-primary

# Start the app -- starts instantly on secondary, no blocking
mvn clean spring-boot:run

# Verify: app is on secondary
curl -s localhost:8080/api/messages/status | jq

# Restore primary -- app auto-initializes and fails back
docker-compose start kafka-primary

# After ~15-20s (recovery threshold), verify failback
curl -s localhost:8080/api/messages/status | jq
```

## Configuration

Everything is configured under the `kafka-dr` prefix. Binders, bindings, and function definitions are generated automatically at startup. All DR components are conditional on `kafka-dr.enabled: true` — without it, the application starts as a standard Spring Boot app.

```yaml
kafka-dr:
  enabled: true                        # Activates all DR components
```

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
      auto-create-topics: false
      replication-factor: 3
      configuration:
        security.protocol: SSL
        ssl.truststore.location: /certs/truststore.p12
        ssl.truststore.password: ${TRUSTSTORE_PASSWORD}
        request.timeout.ms: 5000
        default.api.timeout.ms: 10000
        socket.connection.setup.timeout.ms: 3000
      consumer-properties:
        max.poll.records: 500
```

> **Note:** The binder-level `auto-create-topics` is set to `false` in `default-environment` by design. When set to `true`, the Kafka binder calls `AdminClient` during binding initialization for every cluster. If any cluster is unreachable at startup, the `AdminClient` call blocks and prevents the application from starting -- defeating the purpose of DR. Instead, use the application-level `kafka-dr.auto-create-topics: true` flag, which provisions topics asynchronously via `KafkaAdminHelper` -- at startup for reachable clusters and later for recovered clusters.

### Default Consumer Properties

Applied to all consumers. Per-consumer `properties` override these defaults. Structure maps to `spring.cloud.stream.kafka.bindings.<binding>.consumer.*`:

```yaml
kafka-dr:
  default-consumer-properties:
    configuration:
      max.poll.records: 500
```

### Consumers

Each consumer gets an input binding on every cluster. Only the active cluster's bindings are running. Per-consumer `properties` are merged on top of `default-consumer-properties`.

```yaml
kafka-dr:
  consumers:
    - topic: order-events
      group: my-group
      handler: processOrder          # Method name in MessageProcessor
      content-type: json             # json | string | bytes | native

    - topic: payment-events
      group: my-group
      handler: processPayment
      content-type: native
      properties:                    # Merged on top of default-consumer-properties
        configuration:               # Kafka client properties go under "configuration"
          value.deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
          specific.avro.reader: "true"
```

**Content types:**

| Type | Conversion | Use case |
|---|---|---|
| `string` | `byte[]` -> `String` (UTF-8) | Plain text messages |
| `json` | `byte[]` -> POJO via Jackson | JSON payloads (default) |
| `native` | No conversion; Kafka deserializer handles it | Avro, Protobuf |
| `bytes` | No conversion; raw `byte[]` | Binary data |

### Default Producer Properties

Applied to all producers. Per-producer `properties` override these defaults. Structure maps to `spring.cloud.stream.kafka.bindings.<binding>.producer.*`:

```yaml
kafka-dr:
  default-producer-properties:
    sync: true                        # Block until broker ACK (prevents silent message loss)
    configuration:
      acks: all                       # Wait for all in-sync replicas
      max.block.ms: 5000              # Max time to block on metadata fetch
      delivery.timeout.ms: 10000      # Max time for end-to-end delivery
      request.timeout.ms: 5000        # Max time for broker response
```

### Producers

Independent from consumers. Each producer generates output binding properties for StreamBridge. Per-producer `properties` are merged on top of `default-producer-properties`.

```yaml
kafka-dr:
  producers:
    - topic: order-events
      content-type: json

    - topic: payment-events
      content-type: native
      properties:                        # Merged on top of default-producer-properties
        configuration:                   # Kafka client properties go under "configuration"
          value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
```

### Topic Provisioning

Application-level topic creation, independent of the binder's `auto-create-topics`. Controlled by `kafka-dr.auto-create-topics` (default: `false`):

```yaml
kafka-dr:
  auto-create-topics: true    # false in production, true in development
```

When enabled, topics are created asynchronously:
- At startup: `DynamicBindingRegistrar` provisions topics on reachable clusters
- At runtime: `LateBindingInitializer` provisions topics on clusters that recover after startup

Both use `KafkaAdminHelper` to avoid code duplication.

### Health Check & Failover Tuning

```yaml
kafka-dr:
  health-check:
    interval-ms: 5000       # How often to probe each cluster
    timeout-ms: 3000         # AdminClient timeout per probe
    failure-threshold: 3     # Consecutive failures before marking unhealthy
                             # Also used as retry count for non-critical send errors
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
        // Simple: payload + messageId
        producer.send("order-events", order, order.getOrderId());

        // With custom headers
        producer.send("order-events", order, order.getOrderId(), Map.of(
            "correlation-id", correlationId,
            "source-service", "order-service"
        ));

        // Full control: pre-built Message<?>
        Message<OrderEvent> msg = MessageBuilder.withPayload(order)
                .setHeader("message-id", order.getOrderId())
                .setHeader("correlation-id", correlationId)
                .setHeader("priority", "high")
                .build();
        producer.send("order-events", msg);
    }
}
```

System headers `source-cluster` and `sent-at` are added automatically on each send attempt.

No bean registration, no binding configuration, no binder setup needed.

## How Failover Works

### Startup

```
1. DynamicBindingRegistrar probes all clusters (3s timeout each)
2. Reachable clusters: binders, bindings, and function beans created
3. Unreachable clusters: only environment properties generated (no binder child context)
4. All consumer bindings start with auto-startup=false
5. All clusters begin as UNHEALTHY
6. First health check: first healthy cluster elected immediately (no recovery threshold)
7. ClusterSwitchedEvent -> BindingLifecycleManager starts consumers on elected cluster
8. LateBindingInitializer monitors unreachable clusters in background
```

### Normal Failover

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

### Late Cluster Initialization

When a cluster was unreachable at startup and later becomes available:

```
1. LateBindingInitializer detects cluster is reachable (periodic probe)
2. Creates binder via BinderFactory (child context initialized)
3. Creates consumer bindings with proper Kafka properties (deserializers, native decoding)
4. Topics are provisioned if auto-create-topics is enabled
5. If cluster is already the active one -> starts consumer bindings immediately
6. Cluster is now fully available for failover/failback
```

### Producer Error Handling

`ResilientProducer` classifies send errors into three categories:

| Error type | Examples | Behavior |
|---|---|---|
| **Serialization** | `SerializationException` | Warn log, skip publish, cluster stays healthy |
| **Cluster unavailable** | `TimeoutException`, `NetworkException`, `DisconnectException`, `BrokerNotAvailableException`, `ConnectException` | Immediate `forceUnhealthy` + failover to next cluster |
| **Other errors** | Any non-serialization, non-connectivity exception | Retry up to `failure-threshold` times (default 3), then `forceUnhealthy` + failover |

Synchronous send (`sync: true`) ensures the broker ACK is received before returning success, preventing silent message loss when a broker goes down after the message enters the producer buffer.

## Key Design Decisions

| Decision | Rationale |
|---|---|
| `kafka-dr.enabled` conditional activation | All DR components use `@ConditionalOnProperty`; without the flag, the app starts as standard Spring Boot with full `KafkaAutoConfiguration` |
| Default `KafkaAdmin` removed when DR active | `DynamicBindingRegistrar` removes the `kafkaAdmin` bean to prevent Spring Boot's default from connecting to `localhost:9092` and blocking startup |
| `auto-create-topics: false` | Binder's `AdminClient` blocks during binding initialization if broker is unreachable; topic creation is handled asynchronously by `DynamicBindingRegistrar` (at startup) and `LateBindingInitializer` (for recovered clusters) when enabled |
| All consumers `auto-startup: false` | Prevents Kafka consumer metadata fetch from blocking the main thread during startup; `BindingLifecycleManager` starts consumers after health check elects an active cluster |
| All clusters start as `UNHEALTHY` | First health check immediately elects the first reachable cluster (no `recoveryThreshold` wait); subsequent recovery requires full threshold |
| Binder configs for all clusters, bindings only for reachable | Binder child context creation is what blocks; having config properties in the environment is safe |
| Kafka client timeouts in `default-environment` | `request.timeout.ms`, `socket.connection.setup.timeout.ms` etc. ensure fast failure instead of 60s+ default timeouts |
| Noisy Kafka client logs suppressed | `AdminMetadataManager`, `Metadata`, `NetworkClient` set to ERROR; `LoggingProducerListener` disabled to prevent payload leaking in logs |
| Payload excluded from API responses and logs | Prevents sensitive data leakage; only messageId and metadata are returned/logged |
| Schema Registry configured with all brokers | `KAFKASTORE_BOOTSTRAP_SERVERS` includes all clusters so SR can register schemas even when primary is down |
| `KafkaAdminHelper` shared utility | Consolidates AdminClient probe, topic provisioning, client creation, and Kafka client property extraction — eliminates duplication and ensures SSL/SASL properties are applied to all AdminClient operations |

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
