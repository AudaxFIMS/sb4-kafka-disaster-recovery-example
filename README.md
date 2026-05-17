# Kafka Multi-Cluster Disaster Recovery with Spring Cloud Stream

A production-ready **Spring Boot starter** for **active-passive disaster recovery** across N Kafka clusters. The framework automatically detects cluster failures, switches producers and consumers to the next healthy cluster, and fails back when the original cluster recovers.

The project is structured as a multi-module Maven build:

- **`kafka-dr-spring-boot-starter`** — reusable framework (add as dependency)
- **`kafka-dr-example`** — example application with Avro, Redis idempotency, REST API
- **`kafka-dr-example-timestamp-seek`** — example with timestamp-based seek on failover
- **`kafka-dr-example-multinode`** — example with multi-node clusters and deep probe health check
- **`kafka-dr-example-redis-state`** — example with Redis-backed `FailoverStateStore` so `failback-after` survives application restarts

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
- **Restart-safe failback gate** — pluggable `FailoverStateStore` persists which cluster the app is pinned to after a failover plus the failover timestamp; `failback-after` is honored across restarts (in-memory default, Redis example included)
- **Multi-format support** — String, JSON, Avro, and raw bytes payloads with per-topic configuration
- **Fully dynamic configuration** — clusters, consumers, and producers are defined in YAML; no code changes needed
- **Per-topic handler mapping** — business logic methods are mapped to topics via configuration
- **Unified property model** — consumers and producers use the same `default-*-properties` + per-topic `properties` merge pattern
- **Conditional activation** — all DR components are gated by `kafka-dr.enabled`; without it, the app is a standard Spring Boot application
- **SSL/SASL support** — security properties from `default-environment.configuration` are applied to all AdminClient operations
- **Custom headers** — `ResilientProducer.send()` accepts optional user headers or pre-built `Message<?>`
- **Standard Kafka key for idempotency** — uses `KafkaHeaders.KEY` / `KafkaHeaders.RECEIVED_KEY` instead of custom headers; Kafka key is always available and doubles as the idempotency key
- **Framework / application separation** — reusable starter JAR + application-specific handlers via `MessageProcessor` interface

## Project Structure

```
kafka-dr-spring-boot-starter/             # Framework (reusable JAR)
  src/main/java/dev/semeshin/kafkadr/
    KafkaDrAutoConfiguration.java          # Auto-config + InMemoryIdempotencyStore @Bean fallback
    config/
      KafkaClusterProperties.java          # Configuration model
      KafkaAdminHelper.java                # Shared AdminClient utilities
      DynamicBindingRegistrar.java         # Generates binders, bindings, consumer beans
      StartupClusterState.java             # Tracks initialized clusters
    consumer/
      MessageProcessor.java                # Marker interface — implement in your app
      MessageHandlerRegistry.java          # Discovers handlers across all MessageProcessor beans
      IdempotentConsumer.java              # Deduplication wrapper + timestamp tracking
      LastProcessedTimestampTracker.java   # Tracks last processed timestamp per topic
      TimestampSeekRebalanceListener.java  # Seeks consumer by timestamp on failover
      TimestampStore.java                  # Interface — implement to persist timestamps across restarts
    producer/
      ResilientProducer.java               # Send with automatic failover
    routing/
      ActiveClusterManager.java            # Cluster election state machine
      BindingLifecycleManager.java         # Start/stop bindings on cluster switch
      LateBindingInitializer.java          # Creates bindings for recovered clusters
      ClusterSwitchedEvent.java            # Spring event on failover/failback
      FailoverStateStore.java              # Interface — persist failover state (active cluster + Instant)
      InMemoryFailoverStateStore.java      # Default fallback (registered as @Bean in auto-config)
    health/
      ClusterHealthChecker.java            # Periodic health probe
    idempotency/
      IdempotencyStore.java                # Interface — implement for custom backends
      InMemoryIdempotencyStore.java        # Default fallback (registered as @Bean in auto-config)
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

kafka-dr-example-timestamp-seek/           # Example with timestamp-based seek on failover
  src/main/java/dev/semeshin/kafkadr/
    TimestampSeekExampleApp.java            # Entry point
    handler/
      EventMessageProcessor.java           # Simple string event handler
    store/
      RedisTimestampStore.java             # TimestampStore impl (persists across restarts)
  src/main/resources/
    application.yml                        # seek-by-timestamp: true

kafka-dr-example-multinode/                # Example with multi-node clusters + deep probe
  src/main/java/dev/semeshin/kafkadr/
    MultinodeExampleApp.java               # Entry point
    handler/
      EventProcessor.java                  # Simple event handler
    controller/
      TestController.java                  # REST API for testing
  src/main/resources/
    application.yml                        # deep-probe + min-isr config

kafka-dr-example-redis-state/              # Example: Redis-backed FailoverStateStore
  src/main/java/dev/semeshin/kafkadr/
    RedisStateExampleApp.java              # Entry point
    handler/
      EventMessageProcessor.java           # Simple string event handler
    controller/
      EventController.java                 # REST API + /status exposes persisted state
    store/
      RedisFailoverStateStore.java         # FailoverStateStore impl (survives restarts)
  src/test/java/.../store/
    RedisFailoverStateStoreTest.java       # Unit tests for the SPI contract
  src/main/resources/
    application.yml                        # failback-after: "22:00:00" + Redis config

docker-compose.yml                         # 3 single-node Kafka + MirrorMaker 2 + Schema Registry + Redis
docker-compose-multinode.yml               # 2 clusters × 3 nodes + MirrorMaker 2 + Schema Registry + Redis
mm2/
  mm2.properties                           # MirrorMaker 2 config (single-node clusters)
  mm2-multinode.properties                 # MirrorMaker 2 config (multi-node clusters)
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

# 2. Build and install the starter
cd kafka-dr-spring-boot-starter
mvn clean install -DskipTests

# 3. Run the example app
cd ../kafka-dr-example
mvn clean spring-boot:run
```

> **Note:** The two modules have independent POMs (no parent aggregator). Build the starter first — it installs the JAR into your local Maven repository. Then the example app resolves it as a regular dependency.

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

### Example: Timestamp-Based Seek with Cross-Cluster Replication

The `kafka-dr-example-timestamp-seek` module demonstrates failover with MirrorMaker 2 replication. When primary fails, the consumer on secondary seeks to the offset matching the last processed timestamp — skipping already-handled replicated messages.

**Infrastructure** includes MirrorMaker 2 (`mirror-maker` service) which replicates topics from the active cluster to standby clusters using `IdentityReplicationPolicy` (preserves original topic names).

> **Important:** Replication must be **one-directional** (active → standby) when using `IdentityReplicationPolicy`. Bidirectional replication with identity policy causes infinite message loops — a message is replicated from A to B, then back from B to A, and so on indefinitely.

```bash
# 1. Start all infrastructure including MirrorMaker 2
docker-compose up -d

# 2. Build starter
cd kafka-dr-spring-boot-starter && mvn clean install -DskipTests

# 3. Run the timestamp-seek example
cd ../kafka-dr-example-timestamp-seek && mvn clean spring-boot:run
```

**Test the timestamp seek:**

```bash
# Send messages to primary
curl -X POST 'localhost:8080/api/messages/events?message=msg1&messageId=key-1'
curl -X POST 'localhost:8080/api/messages/events?message=msg2&messageId=key-2'
curl -X POST 'localhost:8080/api/messages/events?message=msg3&messageId=key-3'

# Wait 10-15s for MirrorMaker to replicate to secondary
sleep 15

# Kill primary — triggers failover to secondary
docker-compose stop kafka-primary

# Check logs — you should see:
#   DR_EVENT [primary] -> [secondary] CLUSTER SWITCH
#   DR_EVENT [events] Seeked partition 0 to offset N (timestamp=...)
# Consumer skips already-processed replicated messages

# Send more messages — go to secondary
curl -X POST 'localhost:8080/api/messages/events?message=after-failover'

# Restore primary — auto failback
docker-compose start kafka-primary
```

Key configuration:
```yaml
kafka-dr:
  failover:
    seek-by-timestamp: true    # Seek by last processed timestamp on cluster switch
```

The app includes `RedisTimestampStore` to persist timestamps across restarts. MirrorMaker 2 config is in `mm2/mm2.properties`.

### Example: Restart-Safe `failback-after` (Redis-backed `FailoverStateStore`)

The `kafka-dr-example-redis-state` module demonstrates the pluggable `FailoverStateStore` SPI. The example pins the app to whatever cluster it failed over to until 22:00 local time **even if the app is restarted in between**.

```bash
# 1. Start infrastructure (Redis is included in docker-compose.yml)
docker-compose up -d

# 2. Build starter
cd kafka-dr-spring-boot-starter && mvn clean install -DskipTests

# 3. Run the example
cd ../kafka-dr-example-redis-state && mvn clean spring-boot:run
```

```bash
# Trigger a failover
docker-compose stop kafka-primary
curl -X POST 'localhost:8080/api/messages/events?message=after-failover'
curl -s localhost:8080/api/messages/status | jq
# .activeCluster == "secondary"
# .persistedFailoverState.activeCluster == "secondary"
# .persistedFailoverState.failoverAt == "<ISO-8601 instant>"

# Restart the app while still before failback-after — secondary is restored
# Restart the app after failback-after (or next day) — state is cleared, primary is elected
```

Run the SPI unit tests:

```bash
cd kafka-dr-example-redis-state && mvn test
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

Per-cluster binder overrides go under `environment`. Keys mirror `spring.cloud.stream.binders.{name}.environment.*`. Values are merged on top of `kafka-dr.default-environment` (per-cluster wins on conflict).

#### Per-cluster Schema Registry

When using Confluent Schema Registry, each cluster typically has its own registry (or an isolated logical context) so that Avro/Protobuf schemas evolve independently per region. Each binder runs in its own Spring child context, so the `KafkaAvroSerializer` / `KafkaAvroDeserializer` for each cluster resolves its own Schema Registry — there is no shared global registry client. **No code changes are needed**; everything is configured via the per-cluster `environment` map.

Schema Registry client properties use the **`schema.registry.`** prefix, which keeps them isolated from Kafka broker `ssl.*` settings — the two namespaces never collide. The Kafka client passes the full configuration map into the Avro (de)serializer's `configure()` method, and the (de)serializer routes `schema.registry.*` and `basic.auth.*` / `bearer.auth.*` keys to its internal HTTP client.

| Purpose | Property |
|---|---|
| URL | `schema.registry.url` |
| Truststore (TLS) | `schema.registry.ssl.truststore.location` / `.password` / `.type` |
| Keystore (mTLS) | `schema.registry.ssl.keystore.location` / `.password` / `.type` / `.key.password` |
| TLS protocol | `schema.registry.ssl.protocol` (e.g. `TLSv1.3`) |
| Hostname verification | `schema.registry.ssl.endpoint.identification.algorithm` |
| Basic auth | `basic.auth.credentials.source: USER_INFO` + `basic.auth.user.info: user:pass` |
| Bearer auth | `bearer.auth.credentials.source` + `bearer.auth.token` |

> Note: Kafka broker SSL uses the unprefixed `ssl.*` keys (`ssl.truststore.location`, etc.). The two namespaces are independent — a cluster can use SSL to the brokers and plain HTTP to its Schema Registry, or vice versa.

**Minimal example — different Schema Registry URL per cluster (no auth, plain HTTP):**

```yaml
kafka-dr:
  default-environment:
    spring.cloud.stream.kafka.binder:
      configuration:
        schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}   # global fallback

  clusters:
    primary:
      bootstrap-servers: ${KAFKA_PRIMARY_BROKERS:localhost:9092}
      priority: 1
      environment:
        spring.cloud.stream.kafka.binder:
          configuration:
            schema.registry.url: ${SR_PRIMARY_URL:http://sr-primary:8081}
    secondary:
      bootstrap-servers: ${KAFKA_SECONDARY_BROKERS:localhost:9094}
      priority: 2
      environment:
        spring.cloud.stream.kafka.binder:
          configuration:
            schema.registry.url: ${SR_SECONDARY_URL:http://sr-secondary:8081}
    tertiary:
      bootstrap-servers: ${KAFKA_TERTIARY_BROKERS:localhost:9096}
      priority: 3
      environment:
        spring.cloud.stream.kafka.binder:
          configuration:
            schema.registry.url: ${SR_TERTIARY_URL:http://sr-tertiary:8081}
```

How merging works for, say, `primary`: `getEffectiveEnvironment("primary")` flattens `default-environment` first (registers `schema.registry.url = http://localhost:8081`), then layers `clusters.primary.environment` on top (replaces it with `http://sr-primary:8081`). The resulting key lands under `spring.cloud.stream.binders.primary.environment.…schema.registry.url`, so the `primary` binder's Avro (de)serializer talks to `sr-primary:8081` while `secondary` and `tertiary` talk to theirs.

If a cluster's registry happens to match the default, omit its `environment` block entirely — the fallback in `default-environment` applies.

**Production example — independent Schema Registry security per cluster:**

```yaml
kafka-dr:
  default-environment:
    spring.cloud.stream.kafka.binder:
      configuration:
        schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}   # fallback

  clusters:
    us-east:
      bootstrap-servers: kafka-us-east:9093
      environment:
        spring.cloud.stream.kafka.binder:
          configuration:
            # Kafka broker SSL
            security.protocol: SSL
            ssl.truststore.location: /certs/kafka-us-east-truststore.p12
            ssl.truststore.password: ${KAFKA_US_EAST_TS_PASS}
            ssl.keystore.location: /certs/kafka-us-east-keystore.p12
            ssl.keystore.password: ${KAFKA_US_EAST_KS_PASS}
            # Schema Registry — URL + mTLS
            schema.registry.url: https://sr-us-east:8081
            schema.registry.ssl.truststore.location: /certs/sr-us-east-truststore.p12
            schema.registry.ssl.truststore.password: ${SR_US_EAST_TS_PASS}
            schema.registry.ssl.keystore.location: /certs/sr-us-east-keystore.p12
            schema.registry.ssl.keystore.password: ${SR_US_EAST_KS_PASS}

    eu-west:
      bootstrap-servers: kafka-eu-west:9093
      environment:
        spring.cloud.stream.kafka.binder:
          configuration:
            security.protocol: SSL
            ssl.truststore.location: /certs/kafka-eu-west-truststore.p12
            ssl.truststore.password: ${KAFKA_EU_WEST_TS_PASS}
            # Schema Registry — URL + basic auth instead of mTLS
            schema.registry.url: https://sr-eu-west:8081
            schema.registry.ssl.truststore.location: /certs/sr-eu-west-truststore.p12
            schema.registry.ssl.truststore.password: ${SR_EU_WEST_TS_PASS}
            basic.auth.credentials.source: USER_INFO
            basic.auth.user.info: ${SR_EU_WEST_USER}:${SR_EU_WEST_PASS}
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
    deep-probe: true         # default: false
    deep-probe-min-nodes: 2  # min unique active leader nodes (default: 1)
    deep-probe-min-isr: 2    # min in-sync replicas per partition (default: 0 — disabled)
```

**Health check modes:**

| Mode | Check | Detects | Writes data |
|---|---|---|---|
| `deep-probe: false` (default) | `describeCluster()` | Controller/broker process down, network unreachable | No |
| `deep-probe: true` | `describeCluster()` + `describeTopics()` + leaders + ISR | All of the above + not enough active nodes + under-replicated partitions | No |

With `deep-probe: true`, the health checker calls `describeTopics()` on all configured topics and verifies:
- **Active nodes** — counts unique partition leader nodes. If fewer than `deep-probe-min-nodes`, cluster is unhealthy.
- **ISR (in-sync replicas)** — checks `partition.isr().size()` per partition. If any partition has fewer than `deep-probe-min-isr` replicas in sync, cluster is unhealthy. Set to `0` (default) to disable ISR check.

This is a read-only metadata check — no test messages are produced.

**Deep probe examples:**

| Scenario | `min-nodes: 1` | `min-nodes: 2` | `min-isr: 2` |
|---|---|---|---|
| 3 nodes, all healthy | HEALTHY | HEALTHY | HEALTHY |
| 3 nodes, 1 down | HEALTHY | HEALTHY | Depends on replication |
| 3 nodes, 2 down | HEALTHY | UNHEALTHY | UNHEALTHY |
| Leader alive, 1 replica lagging (ISR=1) | HEALTHY | HEALTHY | UNHEALTHY |

> **Important:** `deep-probe-min-nodes` counts **unique leader nodes per topic**. A topic with 1 partition can have at most 1 leader — setting `min-nodes: 2` will always fail for single-partition topics regardless of cluster health. Use `min-nodes: 1` with `min-isr: 2` for single-partition topics. `min-nodes: 2+` is useful when topics have multiple partitions spread across different nodes.

> **Recommended for production.** Without deep probe, a scenario is possible where the cluster controller responds to metadata queries but brokers can't serve data. The basic probe reports "healthy" while all produce/consume operations fail, delaying failover.

### Failover

```yaml
kafka-dr:
  failover:
    seek-by-timestamp: true    # default: false
    failback-after: "23:59:59" # optional: failback only after this time of day
```

**`seek-by-timestamp`** — when `true` and cross-cluster replication (e.g. MirrorMaker 2) is active, consumers on the new cluster seek to the offset matching the timestamp of the last processed message. This skips already-processed replicated data instead of reprocessing from the committed offset.

**`failback-after`** — prevents **any** automatic failback until the specified time of day (HH:mm:ss). After a failover, the app stays on whatever healthy cluster it lands on — no failback to any higher-priority cluster until the clock reaches this time. Useful for deferring failback to a maintenance window.

| Step | Event | `failback-after` not set | `failback-after: "23:59:59"` |
|---|---|---|---|
| 1 | Primary down at 10:00 | Switch → secondary | Switch → secondary |
| 2 | Primary recovers at 10:05 | Instant failback → primary | **Stay on secondary** |
| 3 | Secondary down at 11:00 | Switch → tertiary | Switch → tertiary (failover is always instant) |
| 4 | Secondary recovers at 11:05 | Instant failback → secondary | **Stay on tertiary** |
| 5 | Primary recovers at 12:00 | Instant failback → primary | **Stay on tertiary** |
| 6 | Clock reaches 00:00 | — | Failback → primary (highest priority healthy) |

> **Note:** `failback-after` blocks **all** failback (return to any higher-priority cluster) while the current cluster is healthy. Failover (leaving an unhealthy cluster) is always immediate regardless of this setting. After a successful failback the gate resets — subsequent failovers will again be held until the configured time.

#### Surviving application restarts (`FailoverStateStore`)

The `failback-after` gate is enforced via a pluggable `FailoverStateStore`. On every cluster switch the manager records the active cluster name and the failover `Instant` to the store; on a successful failback (or whenever a non-failover initial selection happens) it clears the store.

```java
public interface FailoverStateStore {
    void save(FailoverState state);
    Optional<FailoverState> load();
    void clear();
    record FailoverState(String activeCluster, Instant failoverAt) {}
}
```

On startup `ActiveClusterManager` calls `load()`:

- **No persisted state** → standard initial election (priority order).
- **Persisted state, but `now` is at or past the next occurrence of `failback-after` after `failoverAt`** → state is cleared and the priority cluster is elected as usual. This is the date-aware part: if the app was down for hours or days, the gate has already expired and the priority cluster wins.
- **Persisted state, threshold not yet reached** → the persisted cluster is restored as active, `failoverOccurred` is set, and the gate continues to block any failback until the threshold passes — even if a higher-priority cluster reports healthy first.

The runtime gate (`isFailbackBlocked`) uses the same date-aware computation so the live behavior matches what the startup decision saw.

The framework provides `InMemoryFailoverStateStore` as the default `@Bean` (via `@ConditionalOnMissingBean(FailoverStateStore.class)` in `KafkaDrAutoConfiguration`). Restarting the app loses the in-memory state, so `failback-after` is best-effort across restarts unless you provide a durable implementation.

To make the gate restart-safe, register a `@Component implements FailoverStateStore`:

```java
@Component
public class MyFailoverStateStore implements FailoverStateStore {
    public void save(FailoverState state) { /* persist activeCluster + failoverAt */ }
    public Optional<FailoverState> load() { /* read */ }
    public void clear() { /* delete */ }
}
```

The `kafka-dr-example-redis-state` module includes `RedisFailoverStateStore` as a reference implementation (Redis hash `kafka-dr:failover-state` with `activeCluster` and `failoverAt` fields).

| Scenario | In-memory store (default) | Durable store (e.g. Redis) |
|---|---|---|
| Failover at 14:00, restart at 16:00, `failback-after: "22:00"` | App boots on priority cluster (gate lost) | App restores secondary, gate active until 22:00 |
| Failover at 14:00, restart next day at 09:00 | App boots on priority cluster | Threshold (yesterday 22:00) past → store cleared → priority cluster |
| Failover at 23:00, restart at 09:00 next day, `failback-after: "22:00"` | App boots on priority cluster | Threshold = next day 22:00 → still blocked → restores secondary |

How it works:
1. `IdempotentConsumer` tracks the latest `RECEIVED_TIMESTAMP` per topic via `LastProcessedTimestampTracker`
2. On cluster switch, the new consumer receives partition assignments
3. `TimestampSeekRebalanceListener` calls `consumer.offsetsForTimes()` with the last timestamp and seeks to the matching offset
4. `IdempotentConsumer` provides additional deduplication for messages in the timestamp boundary window

When `seek-by-timestamp: false` (default), consumers use standard Kafka offset management (committed offsets / `auto.offset.reset`).

**Timestamp storage:** `LastProcessedTimestampTracker` keeps timestamps in memory by default. This is sufficient for failover during normal operation — no additional setup needed. `TimestampStore` is an optional interface for persisting timestamps to an external store (Redis, DB, etc.).

| Setup | Failover (no restart) | After restart + failover |
|---|---|---|
| `seek-by-timestamp: true` (no `TimestampStore`) | Seek works (in-memory timestamps) | Fallback to committed offsets (timestamps lost, safe) |
| `seek-by-timestamp: true` + `TimestampStore` impl | Seek works (persisted timestamps) | Seek works (timestamps restored from store) |
| `seek-by-timestamp: false` | No seek, committed offsets | No seek, committed offsets |

To persist timestamps across restarts, implement `TimestampStore` and register as `@Component`:

```java
@Component
public class MyTimestampStore implements TimestampStore {
    @Override
    public void save(String topic, long timestamp) { /* persist */ }
    @Override
    public Long load(String topic) { /* read */ }
    @Override
    public Map<String, Long> loadAll() { /* read all */ }
}
```

The `kafka-dr-example-timestamp-seek` module includes `RedisTimestampStore` as a reference implementation.

### Idempotency

```yaml
kafka-dr:
  idempotency:
    ttl-seconds: 3600        # How long to remember processed message IDs
    key-prefix: idempotency  # Key prefix for store implementations
    # key-header: x-idempotency-key  # Optional: use a custom header instead of Kafka key
```

By default, idempotency uses **Kafka record key** (`KafkaHeaders.RECEIVED_KEY`) as the deduplication key — no custom headers required. To use a custom message header instead, set `key-header`:

| Configuration | Deduplication key source |
|---|---|
| *(default, no `key-header`)* | Kafka record key (`KafkaHeaders.RECEIVED_KEY`) |
| `key-header: x-idempotency-key` | Value of `x-idempotency-key` message header |

Messages without a key (or without the configured header) are processed without idempotency check (with a warning log).

The framework provides `InMemoryIdempotencyStore` as default fallback — it is registered as a `@Bean` in `KafkaDrAutoConfiguration` with `@ConditionalOnMissingBean(IdempotencyStore.class)`. This ensures proper ordering: Spring processes application `@Component` beans first, then auto-configuration `@Bean` methods. If any `IdempotencyStore` is already registered, the in-memory fallback is skipped.

To replace it, register any `@Component implements IdempotencyStore` in your application:

```java
@Component
public class MyIdempotencyStore implements IdempotencyStore {
    @Override
    public boolean tryProcess(String consumerName, String messageId) {
        // your deduplication logic
    }
}
```

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

        // Pre-built Message<?> with Kafka key
        Message<OrderEvent> msg = MessageBuilder.withPayload(order)
                .setHeader(KafkaHeaders.KEY, order.getOrderId())
                .setHeader("correlation-id", correlationId)
                .build();
        producer.send("order-events", msg);
    }
}
```

The `messageId` parameter (or `KafkaHeaders.KEY` header) is used as:
- **Kafka record key** — determines partition assignment
- **Idempotency key** — `IdempotentConsumer` deduplicates by `KafkaHeaders.RECEIVED_KEY` on the consumer side

No system headers are injected by the framework — only user-provided headers and `KafkaHeaders.KEY` are sent.

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

### Timestamp-Based Seek on Failover

When `kafka-dr.failover.seek-by-timestamp: true` and cross-cluster replication is active:

```
Cluster switch: primary -> secondary
  1. BindingLifecycleManager stops primary consumers, starts secondary consumers
  2. Secondary consumer receives partition assignments
  3. TimestampSeekRebalanceListener:
     - Gets last processed timestamp from LastProcessedTimestampTracker
     - Calls consumer.offsetsForTimes(timestamp) on each partition
     - Seeks to the offset matching that timestamp
  4. Consumer reads from the seek point, not from offset 0 or latest
  5. IdempotentConsumer deduplicates any overlap in the boundary window
```

```
DR_EVENT [demo-events] Seeked partition 0 to offset 1542 (timestamp=1714003200000)
```

### Producer Error Handling

| Error type | Behavior |
|---|---|
| **Serialization** | Warn + skip, cluster stays healthy |
| **Cluster unavailable** | Immediate `forceUnhealthy` + failover |
| **Other errors** | Retry up to `failure-threshold` times, then failover |

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Independent POMs (no parent aggregator) | Starter and example app are fully independent Maven projects; starter installs to local repo, apps depend on it like any other library |
| `kafka-dr.enabled` conditional activation | All DR components use `@ConditionalOnProperty`; without it, standard Spring Boot |
| `KafkaDrAutoConfiguration` with `@ComponentScan` | Starter works regardless of consuming app's base package |
| Default `KafkaAdmin` removed when DR active | Prevents blocking on `localhost:9092` at startup |
| `InMemoryIdempotencyStore` as `@Bean` in auto-configuration | `@ConditionalOnMissingBean` on `@Bean` in `@AutoConfiguration` is reliable (unlike on `@Component`); app-level `@Component` beans are always processed first |
| `MessageProcessor` as marker interface | Handler methods discovered across all implementing beans; no framework code changes needed |
| Binder configs for all clusters, bindings only for reachable | Binder child context creation blocks; environment properties alone are safe |
| Kafka key as idempotency key | Uses standard `KafkaHeaders.KEY` / `RECEIVED_KEY` instead of custom headers; always available, also drives partition assignment |
| Topic names converted to camelCase for binding names | Dots in topic names break Spring property binding |
| Timestamp-based seek via `ListenerContainerCustomizer` | `TimestampSeekRebalanceListener` uses `offsetsForTimes()` on partition assignment; combined with idempotency for boundary deduplication |
| `failback-after` time-of-day gate | Blocks all failback (not failover) until specified clock time; once a failover occurs, the app stays on the current cluster until the gate opens regardless of how many higher-priority clusters recover |
| `FailoverStateStore` SPI | Persists active cluster + failover `Instant` so the `failback-after` gate survives application restarts; threshold computation is date-aware (next occurrence of `failback-after` after `failoverAt`) so multi-day downtime correctly releases the gate; in-memory default keeps existing behavior unchanged |
| Deep probe via `describeTopics()` | Read-only check: partition leader count + ISR size; catches "controller alive, brokers dead" and under-replicated partitions without writing test data |
| One-directional MirrorMaker replication | `IdentityReplicationPolicy` with bidirectional replication causes infinite message loops; active → standby only |
| Kafka key `byte[]` → `String` conversion | `RECEIVED_KEY` arrives as `byte[]`; `IdempotentConsumer` converts to UTF-8 String for consistent idempotency key comparison |

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
