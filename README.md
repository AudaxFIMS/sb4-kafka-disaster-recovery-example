# Kafka Multi-Cluster Disaster Recovery with Spring Cloud Stream

A production-ready **Spring Boot starter** for **active-passive disaster recovery** across N Kafka clusters. The framework automatically detects cluster failures, switches producers and consumers to the next healthy cluster, and fails back when the original cluster recovers.

The project is structured as a multi-module Maven build:

- **`kafka-dr-spring-boot-starter`** — reusable framework (add as dependency)
- **`kafka-dr-example`** — example application demonstrating usage

> **Important: Cross-cluster replication is required.**
> This framework handles failover at the *application level* — switching producers and consumers between clusters. It does **not** replicate data between Kafka clusters. To ensure no messages are lost, configure cross-cluster replication independently using [MirrorMaker 2](https://kafka.apache.org/documentation/#georeplication), Confluent Cluster Linking, or Confluent Replicator.

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

- **N-cluster support** — configure any number of Kafka clusters with priority-based failover
- **Automatic failover** — health checker detects failures; producer triggers instant failover on send failure with error classification (serialization / cluster unavailable / transient) and configurable retries
- **Automatic failback** — returns to the highest-priority healthy cluster when it recovers
- **Resilient startup** — application starts instantly even if some clusters are down; unreachable clusters are initialized dynamically when they come online (no restart required)
- **Late binding initialization** — clusters that were unavailable at startup get binders, consumer bindings, and topic provisioning created automatically once reachable
- **Synchronous send with ACK** — `sync: true` + `acks: all` ensures broker acknowledgement before returning success, preventing silent message loss
- **Consumer binding management** — only the active cluster's consumers are running; others are stopped
- **Producer cache cleanup** — dead cluster producers are closed to prevent reconnect noise
- **Idempotent message processing** — pluggable deduplication via `IdempotencyStore` interface (in-memory default, Redis example included)
- **Multi-format support** — String, JSON, Avro, and raw bytes payloads with per-topic configuration
- **Fully dynamic configuration** — clusters, consumers, and producers are defined in YAML; no code changes needed
- **Per-topic handler mapping** — business logic methods are mapped to topics via configuration
- **Unified property model** — consumers and producers use the same `default-*-properties` + per-topic `properties` merge pattern
- **Conditional activation** — all DR components are gated by `kafka-dr.enabled`; without it, the app is a standard Spring Boot application
- **SSL/SASL support** — security properties from `default-environment.configuration` are applied to all AdminClient operations
- **Custom headers** — `ResilientProducer.send()` accepts optional user headers or pre-built `Message<?>`
- **Framework / application separation** — reusable starter JAR + application-specific handlers via `MessageProcessor` interface

## Project Structure

```
kafka-dr-spring-boot-starter/             # Framework (reusable JAR)
  src/main/java/dev/semeshin/kafkadr/
    KafkaDrAutoConfiguration.java          # Auto-config: @ComponentScan + @EnableScheduling
    config/
      KafkaClusterProperties.java          # Configuration model
      KafkaAdminHelper.java                # Shared AdminClient utilities
      DynamicBindingRegistrar.java         # Generates binders, bindings, consumer beans
      StartupClusterState.java             # Tracks initialized clusters
    consumer/
      MessageProcessor.java                # Marker interface — implement in your app
      MessageHandlerRegistry.java          # Discovers handlers across all MessageProcessor beans
      IdempotentConsumer.java              # Deduplication wrapper
    producer/
      ResilientProducer.java               # Send with automatic failover
    routing/
      ActiveClusterManager.java            # Cluster election state machine
      BindingLifecycleManager.java         # Start/stop bindings on cluster switch
      LateBindingInitializer.java          # Creates bindings for recovered clusters
      ClusterSwitchedEvent.java            # Spring event on failover/failback
    health/
      ClusterHealthChecker.java            # Periodic health probe
    idempotency/
      IdempotencyStore.java                # Interface — implement for custom backends
      InMemoryIdempotencyStore.java        # Default fallback (@ConditionalOnMissingBean)
  src/main/resources/
    META-INF/spring/
      ...AutoConfiguration.imports         # Spring Boot auto-configuration registration

kafka-dr-example/                          # Example application
  src/main/java/dev/semeshin/kafkadr/
    KafkaDrExampleApplication.java         # Entry point (@SpringBootApplication)
    handler/
      DemoAndOrderMessageProcessor.java    # Example: implements MessageProcessor
      PaymentAndRawDataMessageProcessor.java
    controller/
      MessageProducerController.java       # REST API (example)
    model/
      OrderEvent.java                      # Sample POJO
    idempotency/
      RedisIdempotencyStore.java           # Custom IdempotencyStore (replaces InMemory)
  src/main/avro/
    PaymentEvent.avsc                      # Avro schema
  src/main/resources/
    application.yml

docker-compose.yml                         # 3 Kafka clusters + Schema Registry + Redis
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
mvn clean install -DskipTests
cd kafka-dr-example
mvn spring-boot:run
```

### Using in Your Own Application

Add the starter dependency:

```xml
<dependency>
    <groupId>dev.semeshin</groupId>
    <artifactId>kafka-dr-spring-boot-starter</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

Implement `MessageProcessor`:

```java
@Component
public class MyProcessor implements MessageProcessor {
    public void handleOrder(Message<Order> message) {
        // your business logic
    }
}
```

Configure in `application.yml`:

```yaml
kafka-dr:
  enabled: true
  clusters:
    primary:
      bootstrap-servers: kafka-1:9092
      priority: 1
    secondary:
      bootstrap-servers: kafka-2:9092
      priority: 2
  consumers:
    - topic: orders
      group: my-group
      handler: handleOrder
      content-type: json
  producers:
    - topic: orders
      content-type: json
```

Optionally provide a custom `IdempotencyStore`:

```java
@Component
public class MyIdempotencyStore implements IdempotencyStore {
    @Override
    public boolean tryProcess(String consumerName, String messageId) {
        // your deduplication logic (database, Redis, etc.)
    }
}
```

If no custom `IdempotencyStore` is registered, the built-in `InMemoryIdempotencyStore` is used automatically.

No `@EnableScheduling`, no `KafkaAutoConfiguration` exclusion needed — the starter handles everything via auto-configuration.

### Test Failover

```bash
docker-compose stop kafka-primary
curl -X POST 'localhost:8080/api/messages/demo-events?message=after+failover'
curl -s localhost:8080/api/messages/status | jq
docker-compose start kafka-primary
```

### Test Startup with Dead Cluster

```bash
docker-compose stop kafka-primary
mvn spring-boot:run
curl -s localhost:8080/api/messages/status | jq   # running on secondary
docker-compose start kafka-primary                 # auto failback after ~15-20s
```

## Configuration

Everything is configured under the `kafka-dr` prefix.

### Enabling DR

All DR components are gated by `kafka-dr.enabled`. **Default is `false`** — if the property is absent or set to `false`, no DR beans are created and the application starts as a standard Spring Boot app.

```yaml
kafka-dr:
  enabled: true                        # Activates all DR components (default: false)
```

> **Important:** When `kafka-dr.enabled: true`, the framework:
> - Removes Spring Boot's default `kafkaAdmin` bean (prevents blocking on startup)
> - Creates its own binders, bindings, health checks, and failover logic
> - Manages all Kafka producer/consumer lifecycle
>
> When `kafka-dr.enabled` is absent or `false`:
> - No DR components are created
> - Spring Boot's standard `KafkaAutoConfiguration` is fully active
> - The application behaves as a regular Spring Boot + Kafka app

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

Applied to all clusters. Per-cluster `environment` overrides these defaults:

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

> **Note:** The binder-level `auto-create-topics` is set to `false` by design. Use the application-level `kafka-dr.auto-create-topics: true` flag instead — it provisions topics asynchronously via `KafkaAdminHelper`.

### Default Consumer / Producer Properties

```yaml
kafka-dr:
  default-consumer-properties:
    configuration:
      max.poll.records: 500

  default-producer-properties:
    sync: true
    configuration:
      acks: all
      max.block.ms: 5000
      delivery.timeout.ms: 10000
      request.timeout.ms: 5000
      key.serializer: org.apache.kafka.common.serialization.StringSerializer
```

Per-topic `properties` are merged on top. Kafka client properties go under `configuration:`.

> **Note:** `key.serializer` is set to `StringSerializer` because the default `ByteArraySerializer` fails when Spring Cloud Stream passes message keys as Strings.

### Consumers

```yaml
kafka-dr:
  consumers:
    - topic: order-events
      group: my-group
      handler: processOrder          # Method name in any MessageProcessor bean
      content-type: json             # json | string | bytes | native
      properties:
        configuration:
          value.deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
```

Topic names with dots (e.g. `ax123.test.event`) are fully supported — binding names are auto-converted to camelCase internally.

| Content type | Conversion | Use case |
|---|---|---|
| `string` | `byte[]` → `String` (UTF-8) | Plain text |
| `json` | `byte[]` → POJO via Jackson (default) | JSON payloads |
| `native` | No conversion; Kafka deserializer handles it | Avro, Protobuf |
| `bytes` | No conversion; raw `byte[]` | Binary data |

### Producers

```yaml
kafka-dr:
  producers:
    - topic: order-events
      content-type: json
    - topic: payment-events
      content-type: native
      properties:
        configuration:
          value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
```

### Topic Provisioning

```yaml
kafka-dr:
  auto-create-topics: true    # false in production (default), true in development
```

### Health Check & Failover Tuning

```yaml
kafka-dr:
  health-check:
    interval-ms: 5000       # How often to probe each cluster
    timeout-ms: 3000         # AdminClient timeout per probe
    failure-threshold: 3     # Consecutive failures → unhealthy (also retry count for send errors)
    recovery-threshold: 3    # Consecutive successes → healthy
```

### Idempotency

```yaml
kafka-dr:
  idempotency:
    ttl-seconds: 3600        # How long to remember processed message IDs
    key-prefix: idempotency  # Key prefix for store implementations
```

The framework provides `InMemoryIdempotencyStore` as default (`@ConditionalOnMissingBean`). To replace it, register any `@Component implements IdempotencyStore` in your application — the in-memory fallback is disabled automatically.

The example app includes `RedisIdempotencyStore` as a custom implementation.

## Adding Business Logic

### 1. Implement `MessageProcessor`

```java
@Component
public class OrderMessageProcessor implements MessageProcessor {
    public void processOrder(Message<OrderEvent> message) {
        OrderEvent order = message.getPayload();
        orderService.process(order);
    }
}
```

Handlers can be spread across any number of `MessageProcessor` beans.

### 2. Configure consumers and producers in `application.yml`

### 3. Send messages via `ResilientProducer`

```java
@Service
public class OrderService {
    private final ResilientProducer producer;

    public void placeOrder(OrderEvent order) {
        // Simple
        producer.send("order-events", order, order.getOrderId());

        // With custom headers
        producer.send("order-events", order, order.getOrderId(), Map.of(
            "correlation-id", correlationId
        ));

        // Pre-built Message<?>
        Message<OrderEvent> msg = MessageBuilder.withPayload(order)
                .setHeader("message-id", order.getOrderId())
                .setHeader("correlation-id", correlationId)
                .build();
        producer.send("order-events", msg);
    }
}
```

System headers `source-cluster` and `sent-at` are added automatically.

## How Failover Works

### Startup

1. `DynamicBindingRegistrar` probes all clusters (3s timeout)
2. Reachable clusters: binders, bindings, function beans created
3. Unreachable clusters: only environment properties generated (no blocking)
4. All consumers start with `auto-startup=false`
5. All clusters begin as `UNHEALTHY`
6. First health check elects first healthy cluster immediately
7. `BindingLifecycleManager` starts consumers on elected cluster
8. `LateBindingInitializer` monitors unreachable clusters in background

### Late Cluster Initialization

When a cluster recovers after startup:

1. `LateBindingInitializer` detects cluster is reachable
2. Creates binder via `BinderFactory`
3. Creates consumer bindings with proper Kafka properties
4. Provisions topics if `auto-create-topics` is enabled
5. If cluster is already active → starts consumers immediately

### Producer Error Handling

| Error type | Behavior |
|---|---|
| **Serialization** | Warn + skip, cluster stays healthy |
| **Cluster unavailable** | Immediate `forceUnhealthy` + failover |
| **Other errors** | Retry up to `failure-threshold` times, then failover |

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Multi-module (starter + example) | Reusable framework JAR; applications only implement `MessageProcessor` and `IdempotencyStore` |
| `kafka-dr.enabled` conditional activation | All DR components use `@ConditionalOnProperty`; without it, standard Spring Boot |
| `KafkaDrAutoConfiguration` with `@ComponentScan` | Starter works regardless of consuming app's base package |
| Default `KafkaAdmin` removed when DR active | Prevents blocking on `localhost:9092` at startup |
| `InMemoryIdempotencyStore` via `@ConditionalOnMissingBean` | Auto-replaced by any custom `IdempotencyStore` bean in the application |
| `MessageProcessor` as marker interface | Handler methods discovered across all implementing beans; no framework code changes needed |
| Binder configs for all clusters, bindings only for reachable | Binder child context creation blocks; environment properties alone are safe |
| Topic names converted to camelCase for binding names | Dots in topic names break Spring property binding |

## Tech Stack

- Java 17
- Spring Boot 4.0.5
- Spring Cloud 2025.1.1 (Kafka Binder)
- Apache Kafka 3.9 (KRaft, no ZooKeeper)
- Confluent Schema Registry 8.2.0
- Apache Avro 1.12.1
- Redis 7 (optional, for idempotency)

## License

MIT
