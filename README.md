# Kafka Multi-Cluster Disaster Recovery with Spring Cloud Stream

> ## ⚠️ BREAKING CHANGE — consumers/producers config format changed
>
> **The list-based form is no longer supported.** All `kafka-dr.consumers` and `kafka-dr.producers` entries must now be defined as a **map keyed by a logical name**.
>
> ```yaml
> # ❌ OLD — NO LONGER WORKS
> kafka-dr:
>   consumers:
>     - topic: orders
>       handler: processOrder
>   producers:
>     - topic: orders
>
> # ✅ NEW — REQUIRED
> kafka-dr:
>   consumers:
>     orders-consumer:        # logical name (you choose)
>       topic: orders
>       handler: processOrder
>   producers:
>     orders-producer:
>       topic: orders
> ```
>
> **Why:** the old list form required `kafka-dr.consumers[0].topic=...` keys when flattened, which break in Kubernetes ConfigMaps, AWS SSM Parameter Store, Vault, and any config source where `[`/`]` are not allowed in keys. The new map form is fully flat and works everywhere.
>
> **Side effects:**
> - Generated Spring Cloud Function bean names change: `ordersPrimary` → `ordersConsumerPrimary` (derived from the new logical name). If you override `spring.cloud.stream.bindings.<bindingName>.*` properties anywhere, update them.
> - `IdempotentConsumer` now scopes idempotency by consumer name (not topic) — multiple consumers on the same topic each have independent dedup state.
> - Each topic must have exactly one producer entry per [cluster group](#cluster-groups); duplicates fail-fast at startup.
>
> See [Consumers](#consumers) and [Producers](#producers) for the full migration guide.

> ## ℹ️ Idempotency is ENABLED by default
>
> The deduplication mechanism is active out of the box — every consumed message goes through an `IdempotencyStore` check (`InMemoryIdempotencyStore` unless you provide your own). To switch it off, set the flag explicitly:
>
> ```yaml
> # ✅ DEFAULT — idempotency is on, no configuration required
> kafka-dr:
>   enabled: true
>
> # ⛔ OPT OUT — disable deduplication entirely
> kafka-dr:
>   enabled: true
>   idempotency:
>     enabled: false
> ```
>
> **When disabled:**
> - No `IdempotencyStore` bean is created (including the in-memory fallback), and any user-defined store is ignored by the consumer chain.
> - Every message is processed without a deduplication check — duplicates from cross-cluster replication during failover will reach your handlers.
> - Timestamp tracking for `seek-by-timestamp` failover keeps working regardless of this flag.
>
> See [Idempotency](#idempotency) for details.

---

A production-ready **Spring Boot starter** for **active-passive disaster recovery** across N Kafka clusters. The framework automatically detects cluster failures, switches producers and consumers to the next healthy cluster, and fails back when the original cluster recovers.

The project is structured as a multi-module Maven build:

- **`kafka-dr-spring-boot-starter`** — reusable framework (add as dependency)
- **`kafka-dr-example`** — example application with Avro, Redis idempotency, REST API
- **`kafka-dr-example-timestamp-seek`** — example with timestamp-based seek on failover
- **`kafka-dr-example-multinode`** — example with multi-node clusters and deep probe health check
- **`kafka-dr-example-redis-state`** — example with Redis-backed `FailoverStateStore` so `failback-after` survives application restarts
- **`kafka-dr-example-mixed-batch`** — example with a batch consumer and a record consumer side by side
- **`kafka-dr-example-integration-flow`** — example with Spring Integration flows around the DR chain
- **`kafka-dr-example-multi-group`** — example with two independent Kafkas (cluster groups), bridged both ways with `depends-on`

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

With [cluster groups](#cluster-groups) one application works with several independent Kafkas, each a
set of interchangeable clusters with its own active cluster and its own failover:

```
                 +---------------------------------------------+
                 |                 Application                 |
                 |  orders-bridge --to("order-analytics")-->   |
                 |  <--to("order-scores")-- analytics-scorer   |
                 +--------|---------------------------|--------+
                          |                           |
          cluster group "core"           cluster group "analytics"
        +-------------+-------------+  +-------------+-------------+
        |   core-a    |   core-b    |  | analytics-a | analytics-b |
        |  (active)   |  (standby)  |  |  (active)   |  (standby)  |
        +------+------+------^------+  +------+------+------^------+
               |  MirrorMaker 2  |            |  MirrorMaker 2  |
               +-----------------+            +-----------------+
```

## Features

- **N-cluster support** — configure any number of Kafka clusters with priority-based failover
- **Automatic failover** — health checker detects failures; producer triggers instant failover on send failure with error classification (serialization / cluster unavailable / transient) and configurable retries
- **Automatic failback** — returns to the highest-priority healthy cluster when it recovers
- **Resilient startup** — application starts instantly even if some clusters are down; unreachable clusters are initialized dynamically when they come online (no restart required)
- **Late binding initialization** — clusters that were unavailable at startup get binders, consumer bindings, and topic provisioning created automatically once reachable
- **Synchronous send with ACK** — `sync: true` + `acks: all` ensures broker acknowledgement before returning success, preventing silent message loss
- **Consumer binding management** — only the active cluster's consumers are running; others are stopped
- **Cluster groups** — several independent Kafkas in one application, each with its own primary and standbys, health check, failover, failback window and persisted state; consumers and producers are bound to one group each
- **Bridging between Kafkas without loss** — `depends-on` pauses a consumer while a group it writes to is down and holds back records already in flight with a negative acknowledgment, so a dependency outage never spends the retry budget and skips records
- **Producers addressed by name** — `producer.to("order-analytics").send(...)` keeps topic names and Kafka topology out of business code; `orThrow()` turns a failed send into the exception that gets the source record redelivered
- **Idempotent message processing** — pluggable deduplication via `IdempotencyStore` interface; the store receives the full message, so custom implementations can dedup by any header or payload data (in-memory key-based default, Redis example included)
- **Batch consumption** — per-consumer `batch.enabled`; the starter unpacks the batch so existing `Message<T>` handlers, deduplication and timestamp watermarks keep working unchanged, or hands over the raw envelope in standard Spring Cloud Stream form
- **Manual acknowledgment on both paths** — in batch mode the starter owns the commit: it acknowledges the successful prefix, releases idempotency marks for the rest, and only then advances the watermark, so seek-by-timestamp never passes an uncommitted offset. On the record path the commit is the handler's by default, or the starter's with `ack.owner: starter`, and the watermark follows it either way
- **Batch producer** — `sendBatch()` makes one failover decision for the whole batch and moves only the unsent tail to the next cluster
- **Poison-pill protection** — wrap any deserializer in `ErrorHandlingDeserializer` via per-consumer `properties.configuration`; malformed messages are logged and skipped (or sent to a DLQ) without reaching your handler
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
      BindingPropertyRouter.java           # Routes properties into the core / Kafka namespaces
      ConsumerConfigValidator.java         # Startup checks for batch, acknowledgment and depends-on settings
      ClusterTopology.java                 # Clusters resolved into cluster groups; binder ids and bean names
      ClusterTopologyValidator.java        # Startup checks: shared brokers, names, collisions, Schema Registry
      StartupClusterState.java             # Tracks initialized clusters
    consumer/
      MessageProcessor.java                # Marker interface — implement in your app
      MessageHandlerRegistry.java          # Discovers handlers, resolves shapes, converts payloads
      IdempotentConsumer.java              # Deduplication wrapper + record-path commit and watermark
      AckPolicy.java                       # Who acknowledges on the record path, and when it is observable
      AckObserver.java                     # Makes a handler-owned commit observable to the watermark
      TrackingAcknowledgment.java          # Delegating Acknowledgment that reports whether it was used
      BatchIdempotentConsumer.java         # Batch consumer: dedup, commit point, watermark
      BatchPassThroughConsumer.java        # Batch consumer for mode: standard (raw envelope)
      BatchMessages.java                   # Unpacks the batch envelope into per-record messages
      BatchHandler.java                    # Seam between the registry and the batch consumer
      BatchOutcome.java                    # Per-record verdicts reported by a batch handler
      BatchConversionException.java        # Conversion failure carrying the record's position
      LastProcessedTimestampTracker.java   # Tracks last committed timestamp per (consumer, topic, partition)
      TimestampSeekRebalanceListener.java  # Seeks consumer by timestamp on failover
      TimestampStore.java                  # Interface — implement to persist timestamps across restarts
      DependencyGate.java                  # depends-on as seen by a consumer: block, or reclassify a failure
      DependencyNacks.java                 # Negative acknowledgments on behalf of the gate
    producer/
      ResilientProducer.java               # Send with automatic failover; to(name), orThrow()
      SendFailedException.java             # Raised by orThrow() when a message could not be sent
      ClusterGroupUnavailableException.java # Raised by orThrow() when no cluster of the group took it
    routing/
      ActiveClusterManager.java            # Cluster election state machine, one per cluster group
      BindingLifecycleManager.java         # Start/stop/pause bindings on cluster switch
      LateBindingInitializer.java          # Creates bindings for recovered clusters
      DependencyGuard.java                 # Pauses and resumes consumers for depends-on
      ClusterSwitchedEvent.java            # Spring event on failover/failback of a group
      ClusterGroupAvailabilityEvent.java   # Spring event: a group lost its last healthy cluster, or regained one
      FailoverStateStore.java              # Interface — persist failover state (active cluster + Instant)
      InMemoryFailoverStateStore.java      # Default fallback (registered as @Bean in auto-config)
    health/
      ClusterHealthChecker.java            # Periodic health probe, scheduled per cluster group
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
      LedgerMessageProcessor.java          # Example: manual commit with ack.owner: starter
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
  src/test/java/dev/semeshin/kafkadr/
    AcknowledgmentConfigurationTest.java   # Asserts the shipped YAML really demonstrates ack.owner: starter

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

kafka-dr-example-mixed-batch/              # Example: batch and record consumers side by side
  src/main/java/dev/semeshin/kafkadr/
    MixedBatchExampleApp.java              # Entry point
    model/
      OrderEvent.java                      # JSON payload for the batch topic
    handler/
      OrderBatchProcessor.java             # Batch handler returning BatchOutcome (per-record verdicts)
      AuditRecordProcessor.java            # Ordinary Message<String> handler, unchanged by batching
    controller/
      MixedBatchController.java            # REST API: sendBatch, single send, counters
  src/test/java/.../
    MixedBatchConfigurationTest.java       # Asserts the shipped YAML resolves to the intended shapes
  src/main/resources/
    application.yml                        # One batching consumer, one not

kafka-dr-example-integration-flow/         # Example: Spring Integration flows around the DR chain
  src/main/java/dev/semeshin/kafkadr/
    IntegrationFlowExampleApp.java         # Entry point (@IntegrationComponentScan for the gateway)
    model/
      OrderEvent.java                      # Flow input
      Invoice.java                         # Flow output, published through ResilientProducer
      UnprocessableOrderException.java     # Maps to a discard verdict in the batch path
    flow/
      OrderFlowConfig.java                 # Two DirectChannel flows: record path and billing path
      BillingGateway.java                  # Request/reply entry, unwraps exceptions for typed catches
      FlowMetrics.java                     # Counters surfaced by /api/status
    handler/
      OrderFlowProcessor.java              # Record handler: forwards into the flow, catches nothing
      BillingBatchProcessor.java           # Batch handler: gateway per record, one sendBatch at the end
      InvoiceProcessor.java                # Plain consumer of what the flows published
    controller/
      FlowController.java                  # REST API: feed both paths, read the counters
  src/test/java/.../
    IntegrationFlowConfigurationTest.java  # Asserts the shipped YAML resolves to the intended shapes
    OrderFlowTest.java                     # Same-thread execution, propagation, gateway unwrapping
  src/main/resources/
    application.yml                        # Record consumer + batch consumer + invoices consumer

kafka-dr-example-multi-group/              # Example: two independent cluster groups, bridged both ways
  src/main/java/dev/semeshin/kafkadr/
    MultiGroupExampleApp.java              # Entry point
    model/
      OrderEvent.java                      # Published to core, bridged to analytics
      OrderScore.java                      # Computed in analytics, sent back to core
    handler/
      OrderBridgeProcessor.java            # core -> analytics: to(name).send(...).orThrow(), depends-on analytics
      ScoringProcessor.java                # analytics -> core, depends-on core
      ScoreSinkProcessor.java              # End of the round trip, counts distinct orders
      AuditProcessor.java                  # Topic "audit" in both groups, same consumer group
    controller/
      MultiGroupController.java            # REST API: orders, audit per group, per-group status
  src/test/java/.../
    MultiGroupConfigurationTest.java       # Asserts the shipped YAML resolves to the intended topology
  src/main/resources/
    application.yml                        # cluster-groups core + analytics, depends-on both ways

docker-compose.yml                         # 3 single-node Kafka + MirrorMaker 2 + Schema Registry + Redis
docker-compose-multinode.yml               # 2 clusters × 3 nodes + MirrorMaker 2 + Schema Registry + Redis
docker-compose-multigroup.yml              # 2 cluster groups × 2 clusters + one MirrorMaker 2 per group
mm2/
  mm2.properties                           # MirrorMaker 2 config (single-node clusters)
  mm2-multinode.properties                 # MirrorMaker 2 config (multi-node clusters)
  mm2-multigroup-core.properties           # MirrorMaker 2 for group "core" (core-a -> core-b)
  mm2-multigroup-analytics.properties      # MirrorMaker 2 for group "analytics" (analytics-a -> analytics-b)
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
    orders-consumer:
      topic: orders
      group: my-group
      handler: handleOrder
      content-type: json
  producers:
    orders-producer:
      topic: orders
      content-type: json
```

Optionally provide a custom `IdempotencyStore`:

```java
@Component
public class MyIdempotencyStore implements IdempotencyStore {
    @Override
    public boolean tryProcess(String clusterName, String consumerName, Message<?> message) {
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

### Example: Batch and Record Consumers Side by Side

`kafka-dr-example-mixed-batch` runs two consumers in one application: `order-events` is
consumed in batches with per-record verdicts and manual acknowledgment, `audit-events` one
record at a time. Batching is a per-binding property, so neither consumer is aware of the
other's mode.

```bash
# 1. Infrastructure (two clusters are enough)
docker-compose up -d

# 2. Build the starter
mvn -f kafka-dr-spring-boot-starter/pom.xml clean install

# 3. Run
mvn -f kafka-dr-example-mixed-batch/pom.xml spring-boot:run
```

```bash
# 30 orders, published with sendBatch — one failover decision for the whole batch
curl -X POST 'localhost:8083/api/orders?count=30'

# One audit record, published and consumed the ordinary way
curl -X POST 'localhost:8083/api/audit?message=user-logged-in&messageId=a-1'

# Counters for both consumers
curl -s localhost:8083/api/status | jq
```

The batch handler exercises all three verdicts, so the interesting cases are reachable
from the REST API:

```bash
# discard: unprocessable, keeps its idempotency mark, never redelivered
curl -X POST 'localhost:8083/api/orders?count=5&amount=-1'

# retry: mark released, Kafka redelivers, orders after it are deduplicated on the way back
curl -X POST 'localhost:8083/api/orders?count=5&customer=flaky'
```

Watch the log for the commit point moving only as far as the first retried record, while
the records marked done behind it are redelivered and then skipped as duplicates.

Killing the primary mid-batch shows the producer side: only the unsent tail moves to the
secondary, and `clusters` in the response lists both.

```bash
docker stop kafka-primary
curl -X POST 'localhost:8083/api/orders?count=50'
```

### Example: Spring Integration Flows (`kafka-dr-example-integration-flow`)

Spring Cloud Stream is built on Spring Integration, so a flow needs no bridge — only the right
position. This example puts one in each of the two places that keep every DR guarantee intact:
**behind the handler** and **in front of the producer**.

```bash
# 1. Infrastructure (two clusters are enough)
docker-compose up -d kafka-primary kafka-secondary

# 2. Build the starter
cd kafka-dr-spring-boot-starter && mvn clean install -DskipTests

# 3. Run
cd ../kafka-dr-example-integration-flow && mvn clean spring-boot:run
```

```bash
# Record path: handler → DirectChannel → filter → transform → ResilientProducer
curl -X POST 'localhost:8084/api/orders?count=5&amount=100'

# amount=0 is filtered out inside the flow — consumed deliberately, no invoice, no failure
curl -X POST 'localhost:8084/api/orders?count=3&amount=0'

# Batch path: one gateway call per record, one sendBatch for the invoices
curl -X POST 'localhost:8084/api/billing?count=10&amount=100'

# A negative amount is discarded through the typed catch the gateway makes possible
curl -X POST 'localhost:8084/api/billing?count=4&amount=-1'

# Counters for both paths and the round trip back through Kafka
curl -s localhost:8084/api/status | jq
```

What the module encodes, and why:

| Choice | Reason |
|---|---|
| Every channel is a `DirectChannel` | The flow runs on the calling thread. A queue or executor channel would let the offset commit and the watermark advance while the message is still queued. |
| No `errorChannel`, no catching in the handler | A thrown exception is the starter's only signal that the record was not processed — it rolls back the idempotency mark and lets Kafka redeliver. |
| The batch path calls a **gateway**, not `channel.send()` | A gateway rethrows the original exception, so `catch (UnprocessableOrderException e)` matches and the record is discarded. `send()` wraps it in a `MessagingException` and the typed catch would miss. |
| The billing flow has **no filter** | It is request/reply: a filtered-out message produces no reply at all, and the call would block until `replyTimeout`. Records that must not be billed are rejected before the gateway. |
| The loop over the batch lives in the handler, not in a `.split()` | `BatchOutcome` is indexed; splitting inside the flow loses each record's position and with it the commit prefix. |
| The tail is `ResilientProducer`, never `Kafka.outboundChannelAdapter` | The adapter is bound to one `ProducerFactory` and would keep writing to a dead cluster after a failover. |
| `max-attempts: 1` on the record consumer | Handler failures propagate now, so the retry chain is chosen deliberately instead of inherited. |

`OrderFlowTest` pins these properties against a real Integration context rather than leaving them
as comments: same-thread execution, propagation out of the flow, filtering as a success, and the
gateway-versus-send difference in exception types.

### Example: Several Independent Kafkas (`kafka-dr-example-multi-group`)

Two [cluster groups](#cluster-groups) in one application. `core` carries the orders and receives
the scores back; `analytics` receives the orders and computes the scores. Each group has a
primary and a standby, its own MirrorMaker and its own failover. Both bridges declare the group
they write to under [`depends-on`](#bridging-between-groups-depends-on).

```bash
# 1. Infrastructure: 2 groups × 2 clusters + one MirrorMaker 2 per group
docker compose -f docker-compose-multigroup.yml up -d

# 2. Build the starter
mvn -f kafka-dr-spring-boot-starter/pom.xml clean install -DskipTests

# 3. Run (port 8085)
mvn -f kafka-dr-example-multi-group/pom.xml spring-boot:run
```

```bash
# Orders go core -> analytics -> core; the counters show the round trip
curl -X POST 'localhost:8085/api/orders?count=5'
curl -s localhost:8085/api/status | jq '.roundTrip'
# { "bridgedToAnalytics": 5, "scoredInAnalytics": 5, "completedInCore": 5 }

# Topic "audit" exists in both groups — only the producer name says which one
curl -X POST 'localhost:8085/api/audit?group=core&message=hello'
curl -X POST 'localhost:8085/api/audit?group=analytics&message=hello'
curl -s localhost:8085/api/status | jq '.audit'
# { "core": 1, "analytics": 1 }
```

**One group fails over, the other does not notice:**

```bash
docker stop mg-core-a
# DR_EVENT [core-a] -> [core-b] CLUSTER SWITCH
# DR_EVENT [orders-bridge] [orders] Seeked partition 0 to offset 4 (timestamp=...)
# [core-b][orders-bridge] Duplicate skipped: ...        <- the boundary record, filtered
curl -s localhost:8085/api/status | jq '.groups | map_values(.activeCluster)'
# { "core": "core-b", "analytics": "analytics-a" }
```

**A dependency is down entirely — the bridge waits instead of skipping:**

```bash
docker stop mg-analytics-a mg-analytics-b
# DR_EVENT [analytics] Cluster group 'analytics' UNAVAILABLE — no healthy cluster
# DR_EVENT [orders-bridge] Paused: dependency group 'analytics' has no healthy cluster
curl -X POST 'localhost:8085/api/orders?count=3'     # accepted by core, not bridged yet

docker start mg-analytics-a mg-analytics-b
# DR_EVENT [orders-bridge] Resumed: every dependency [analytics] is available again
curl -s localhost:8085/api/status | jq '.roundTrip'   # all orders complete, none skipped
```

A consumer returning to a cluster that was stopped hard may take up to `session.timeout.ms`
(45 s by default) to rejoin its consumer group: the coordinator first waits for the member it
lost to time out. That is Kafka's behaviour, not the starter's — the records are waiting, not
lost.

What the module encodes, and why:

| Choice | Reason |
|---|---|
| `to("order-analytics")`, never a topic name | The handler does not know which Kafka the topic lives in; moving it to another group is a YAML change |
| `.orThrow()` after every bridging send | A failed send that returns normally is acknowledged and lost; the exception is what lets `depends-on` hold the record back |
| The source key travels with the bridged record | No transaction spans two Kafkas, so a crash between send and commit sends twice; the consumer on the other side drops the copy by key |
| `ack-mode: MANUAL` + `ack.owner: starter` on the bridges | `depends-on` holds records back with a nack, which exists only under manual acknowledgment; the starter commits, the handler has no acknowledgment code |
| `depends-on-max-hold-ms: 120000` on the bridges | A record the target group is up for yet does not take (a missing ACL, say) is held two minutes, then takes the ordinary failure path; shorter than the 10-minute default so the demo shows it |
| One MirrorMaker per group, replicating only inside it | A group is one logical Kafka; replicating into another group's clusters would mix two independent systems |
| `audit` + consumer group `audit` in both groups | Two topics in two Kafkas; the starter tells their listener containers apart by binding name |

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

> **Separate registries need the same schema IDs.** The Confluent wire format stores a numeric
> schema ID in every record, not the schema. MirrorMaker copies those bytes unchanged, so after a
> failover a consumer on the standby looks the primary's ID up in the standby's registry. With
> independent registries the IDs differ: deserialization fails, or — worse — succeeds against the
> wrong schema. Registries of clusters that replicate into each other must share IDs: Confluent
> Schema Linking, a replica in `IMPORT` mode, or one registry with its own DR. Clusters in
> different [cluster groups](#cluster-groups) do not have this problem — a bridge deserializes in
> one group and serializes anew in the other.

> **A registry set in `properties` replaces the per-cluster ones.** Consumer and producer
> `properties` land on the binding, i.e. on every cluster of the group, so a
> `configuration.schema.registry.url` there sends all of them to one registry. The starter logs a
> warning at startup when that happens while clusters have registries of their own.

### Cluster Groups

A **cluster group** is one logical Kafka: a set of interchangeable clusters kept in sync by
replication, with its own active cluster, health check, failover, failback window and persisted
state. Several groups make one application work with several independent Kafkas — each with
its own primary and standbys — and move data between them.

```yaml
kafka-dr:
  cluster-groups:
    core:
      clusters:
        primary:   { bootstrap-servers: core-dc1:9092, priority: 1 }
        secondary: { bootstrap-servers: core-dc2:9092, priority: 2 }
      failover:
        failback-after: "23:00:00"

    analytics:
      health-check:                 # narrows the global health-check for this group only
        deep-probe: true
        failure-threshold: 1
      default-environment:          # between the global default-environment and the cluster's own
        spring.cloud.stream.kafka.binder.configuration:
          schema.registry.url: http://analytics-sr:8081
      clusters:
        dc1: { bootstrap-servers: an-dc1:9092, priority: 1 }
        dc2: { bootstrap-servers: an-dc2:9092, priority: 2 }

  consumers:
    orders-consumer:
      cluster-group: core           # every consumer and producer belongs to one group
      topic: orders
      handler: processOrder

  producers:
    order-analytics:
      cluster-group: analytics
      topic: order-analytics
```

Groups are independent: losing a cluster in `core` never moves a consumer or producer of
`analytics`, and each group may fail over at a different moment, to a different cluster.

**What can be set per group.** Every field is optional and narrows the global setting of the
same name; a field left unset inherits it.

| Setting | Notes |
|---|---|
| `health-check.*` | All fields, `interval-ms` included — each group is probed on its own schedule |
| `failover.seek-by-timestamp`, `failover.failback-after` | An empty `failback-after: ""` switches a global window off for the group |
| `default-environment` | Merged key by key: global → group → cluster, nearest wins |
| `auto-create-topics` | A group's clusters get only that group's topics |

`idempotency`, `debug` and `default-consumer/producer-properties` stay global.

**Names.** Binder names are global to Spring Cloud Stream, and two groups may both have a
`primary`, so a cluster's binder id is `<group>-<cluster>`; bean and binding names carry the
group too.

| | Group `default` | Group `core`, cluster `primary` |
|---|---|---|
| Binder id (also in logs, `SendResult.cluster()`, health) | `primary` | `core-primary` |
| Consumer function bean / binding | `ordersConsumerPrimary` / `…-in-0` | `ordersConsumerCorePrimary` / `…-in-0` |
| `FailoverStateStore` state | the single-state methods | `save/load/clear("core", …)` |

**The `clusters` form is the group `default`.** Existing configurations keep working unchanged
— same binder, binding and bean names, same persisted keys. To add a second Kafka, move the
existing clusters under `cluster-groups.default.clusters` (a group named `default` keeps the
historical names, so nothing is renamed) and add the new group next to it. `clusters` and
`cluster-groups` cannot be combined.

`cluster-group` may be omitted while there is only one group to fall back to; with several, a
consumer or producer without one is rejected.

**Checked at startup:**

| Rejected | Why |
|---|---|
| A broker address listed by two clusters — in different groups or the same one | Across groups the two Kafkas would not be independent; within a group a cluster would be its own failover target. Addresses are compared without scheme, case or whitespace; a DNS alias of the same broker is not caught |
| A group without clusters, an unknown or missing `cluster-group` | — |
| Two clusters resolving to the same binder id, two consumers to the same bean name | `core-primary` in group `default` and `primary` in group `core`, for one |
| Group or cluster names other than letters, digits, `-`, `_` | They become part of property keys and bean names |
| Two producers for one topic in one group | The same topic in two groups is fine — they are different Kafkas |
| Two consumers with the same topic and consumer group in one group | The same pair in two groups is fine — containers are told apart by binding name |

**Several groups at run time:**

- `ActiveClusterManager` keeps one election per group: `getActiveCluster(group)`,
  `getActiveClusters()`, `hasHealthyCluster(group)`, `getClustersByPriority(group)`,
  `getHealthStatuses(group)`, `getGroups()`. The methods without a group keep working with one
  group and throw with more, naming the method to use instead — except `getHealthStatuses()`,
  which returns every cluster of every group, keyed by binder id.
- `ClusterSwitchedEvent.getGroup()` names the group that switched;
  `ClusterGroupAvailabilityEvent` reports a group losing its last healthy cluster, or regaining
  one.
- The health endpoint keeps its flat layout with one group; with several every detail is
  prefixed, e.g. `group.core.activeCluster`, `group.core.cluster.primary: UP`.
- A custom `FailoverStateStore` must implement the per-group methods and return `true` from
  `supportsGroups()` — see
  [Surviving application restarts](#surviving-application-restarts-failoverstatestore).

> **Replicate inside a group, never across.** Each group needs its own MirrorMaker (or Cluster
> Link), replicating only between that group's clusters. Replicating one group's topics into
> another group's clusters mixes two independent Kafkas. `docker-compose-multigroup.yml` runs
> one MirrorMaker 2 per group.

#### Bridging between groups: `depends-on`

A consumer whose handler writes to another group cannot do its work while that group is down.
Throwing is the wrong signal there: every failed delivery counts against the retry budget, the
send fails fast so the budget is gone in seconds, and the container then **skips the record**.
`depends-on` makes the starter hold such records back instead:

```yaml
kafka-dr:
  consumers:
    orders-bridge:
      cluster-group: core
      depends-on: analytics              # several groups: comma-separated
      depends-on-nack-interval-ms: 1000  # optional, the default
      depends-on-max-hold-ms: 600000     # optional, the default (10 min)
      topic: orders
      handler: bridgeOrder
      ack:
        owner: starter                   # optional: the starter commits, the handler does not
      properties:
        ack-mode: MANUAL                 # required
```

```java
public void bridgeOrder(Message<OrderEvent> message) {
    producer.to("order-analytics")
            .send(message.getPayload(), IdempotencyStore.kafkaKey(message, null))  // keep the key
            .orThrow();                                                            // never swallow
}
```

Three mechanisms, each covering what the one before cannot:

1. **Pause.** While any dependency has no healthy cluster, the consumer's binding is paused. It
   keeps its partitions — no rebalance, nothing to seek — and the records wait in Kafka. It
   resumes once every dependency is back, and starts paused if its own group fails over in the
   meantime.
2. **Gate before the handler.** A record already polled when the pause takes effect does not
   reach the handler or the idempotency store; it is negatively acknowledged and redelivered after
   `depends-on-nack-interval-ms`. A nack does not count as a delivery attempt, so it can never
   run out.
3. **Reclassified failure.** A send that fails before any health check noticed — the record was
   already in the handler — marks the target clusters unhealthy on the way. When the handler's
   exception arrives the dependency is reported down, or the exception carries a
   `ClusterGroupUnavailableException` for it, and the record is nacked instead of rethrown, with
   its idempotency mark released. In `batch.mode: split` the nack starts at the record that stopped
   the batch, which also commits the records before it.

**How long a record is held.** The send itself fails at once — `orThrow()` throws, nothing
blocks. What is held is the *source* record: not committed, not sent to the DLQ, redelivered every
`depends-on-nack-interval-ms`. A nack re-seeks every partition of the poll and the consumer thread
sleeps the interval, so while a record is held its consumer thread makes no progress (other
threads of a `concurrency > 1` consumer do).

- **The dependency is down** (no healthy cluster): the binding is paused and records wait in Kafka
  for as long as the outage lasts. No limit — this is what `depends-on` is for.
- **The dependency is up, yet the record is not taken** — a missing ACL, a schema registry that
  cannot be reached, any failure that proves nothing against the message: the record is held for
  at most `depends-on-max-hold-ms` (10 minutes by default) of time the dependency was up. After
  that it is no longer nacked and takes the ordinary failure path — the binder's retries, then the
  DLQ or a skip — with an error in the log naming the record; it is not held again on those
  retries, only a later record of the partition starts a new count. Time the dependency spends
  down does not count, and does not reset the count either: a record cannot buy itself more time
  by taking the group down. A record without topic, partition and offset headers cannot be counted,
  so it is not held while the dependency is up.

**Checked at startup:**

| Mode | Required |
|---|---|
| record | `ack-mode: MANUAL` or `MANUAL_IMMEDIATE` |
| `batch.mode: split` | `ack-mode: MANUAL_IMMEDIATE`, `batch.error-policy: fail-batch` — `skip-failed` would skip and commit a record that failed because the dependency went down mid-batch |
| `batch.mode: standard` | `ack-mode: MANUAL` or `MANUAL_IMMEDIATE` and a `Message<List<T>>` handler (a `List<T>` handler has no `Acknowledgment` and is rejected). Only the pause and the gate apply: a send failing inside the handler is the handler's to hold back — nack the envelope's `Acknowledgment`; a thrown exception is retried and the batch then skipped. A warning at startup says so |

`ack.async-acks: true` is rejected — spring-kafka does not support a nack with out-of-order
commits. So are an unknown group, the consumer's own group (its own failover already covers
that), and a non-positive interval.

**Exactly-once across Kafkas is not possible** — no transaction spans two clusters. Between the
send and the commit a crash sends the record twice; pass the source key on, and the consumer on
the other side drops the copy through its `IdempotencyStore`.

> A handler that swallows a failed send — ignores the `SendResult`, or catches the exception —
> makes the record look processed, and it is committed. `depends-on` can hold back only what it
> is told about: end bridging sends with `orThrow()`. The same goes for a `BatchOutcome` handler:
> `discard(i, e)` is a verdict that the record is done, so never discard a record whose send
> failed — mark it `retry(i, e)`, which stops the batch there and lets `depends-on` hold it back.

### Default Binder Environment

Applied to all clusters. Per-cluster `environment` overrides these defaults:

```yaml
kafka-dr:
  default-environment:
    spring.cloud.stream.kafka.binder:
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

> **Note:** The starter sets the binder-level `auto-create-topics` to `false` for every cluster, so it does not need to appear here. Topic creation belongs to `kafka-dr.auto-create-topics` instead — see [Topic Provisioning](#topic-provisioning). Setting the binder-level flag explicitly, in `default-environment` or per cluster, still overrides the starter's default.

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

Per-topic `properties` are merged on top. Kafka client properties go under `configuration:`; everything else is routed by name into the core or Kafka-extension binding namespace — see [Consumers](#consumers).

> These defaults apply to **every** consumer and producer. Batch tuning such as `max.poll.records` belongs in the per-consumer `batch:` block instead, or consumers that do not batch inherit it too.

> **Note:** `key.serializer` is set to `StringSerializer` because the default `ByteArraySerializer` fails when Spring Cloud Stream passes message keys as Strings.

### Consumers

```yaml
kafka-dr:
  consumers:
    orders-consumer:                 # Logical consumer name (user-chosen key)
      topic: order-events
      group: my-group
      handler: processOrder          # Method name in any MessageProcessor bean
      content-type: json             # json | string | bytes | native
      ack:                           # Optional — takes effect only with ack-mode MANUAL*
        owner: starter               # who commits: handler (default) | starter
      properties:
        configuration:
          value.deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
```

Consumers are configured as a map keyed by an arbitrary logical name. The key is used for:

- Function bean naming (`ordersConsumerPrimary`, etc.)
- Spring Cloud Stream binding naming (`ordersConsumerPrimary-in-0`)
- Idempotency scoping (`{consumerName}:{messageId}`)
- Per-consumer timestamp tracking for seek-by-timestamp

Per-consumer `properties` are routed into the two namespaces Spring Cloud Stream actually uses. The two field sets are disjoint, and the routing table is derived from the Spring Cloud Stream classes themselves, so it cannot drift on upgrade:

| Namespace | Target | Examples |
|---|---|---|
| core (`ConsumerProperties`) | `spring.cloud.stream.bindings.{binding}.consumer.*` | `concurrency`, `max-attempts`, `back-off-*`, `retryable-exceptions`, `header-mode` |
| Kafka extension (`KafkaConsumerProperties`) | `spring.cloud.stream.kafka.bindings.{binding}.consumer.*` | `ack-mode`, `enable-dlq`, `dlq-name`, `start-offset`, `configuration.*` |

```yaml
      properties:
        concurrency: 3                 # -> core
        max-attempts: 1                # -> core
        ack-mode: MANUAL_IMMEDIATE     # -> Kafka extension
        enable-dlq: true               # -> Kafka extension
        configuration:                 # -> Kafka extension
          max.poll.interval.ms: 300000
```

A key that belongs to neither set is almost certainly a typo: it is routed to the Kafka namespace (where the binder ignores it) and reported with a warning naming the consumer, so it no longer disappears silently.

A few keys are owned by the starter and rejected if set by hand, because overriding them breaks binding lifecycle or payload handling: `auto-startup`, `batch-mode`, `use-native-decoding`, `destination`, `group`, `binder`. The error message names the setting to use instead.

Acknowledgment has a block of its own, `ack`, for the settings `properties` cannot carry — who commits on the record path, and the `ContainerProperties` knobs the binder does not expose. See [Manual Acknowledgment](#manual-acknowledgment).

This map form is also the format that works on Kubernetes / EKS without `[]` indexes:

```properties
kafka-dr.consumers.orders-consumer.topic=order-events
kafka-dr.consumers.orders-consumer.handler=processOrder
kafka-dr.consumers.orders-consumer.group=my-group
```

Topic names with dots (e.g. `ax123.test.event`) are fully supported — the logical consumer name (Map key) is independent of the topic, so dotted topic names don't pollute binding keys.

Multiple consumers may target the same topic (different groups / handlers) — use distinct map keys:

```yaml
kafka-dr:
  consumers:
    orders-main:
      topic: orders
      group: order-processor
      handler: processOrder
    orders-audit:
      topic: orders
      group: order-auditor
      handler: auditOrder
```

| Content type | Conversion | Use case |
|---|---|---|
| `string` | `byte[]` → `String` (UTF-8) | Plain text |
| `json` | `byte[]` → POJO via Jackson (default) | JSON payloads |
| `native` | No conversion; Kafka deserializer handles it | Avro, Protobuf |
| `bytes` | No conversion; raw `byte[]` | Binary data |

#### Handler failures

A handler that throws on the record path **propagates**, exactly as on the batch paths. The
starter rolls back the idempotency mark, leaves the seek-by-timestamp watermark where it was,
and lets the exception reach the listener container so Kafka redelivers the record.

Swallowing it — which is what the starter did before — meant the record was marked processed,
the watermark advanced and the offset was committed for a record the handler never handled;
the redelivery Kafka performs was then dropped as a duplicate. Silent loss, visible only as one
ERROR line.

What this changes in practice:

| | Before | Now |
|---|---|---|
| Idempotency mark | Kept — redelivery looks like a duplicate | Rolled back, redelivery is processed |
| Watermark (`seek-by-timestamp`) | Advanced past the failed record | Stays put, so a failover replays from it |
| Offset | Committed | Redelivered, then handled by the retry chain |

The retry chain is Spring's, not the starter's: the binding retries `max-attempts` times
(default **3**), and if the exception still escapes, the container's error handler delivers the
record up to **10** times before logging it and moving on. Bound it deliberately for topics that
can carry poison payloads:

```yaml
kafka-dr:
  consumers:
    order-events-consumer:
      topic: order-events
      group: my-group
      handler: processOrder
      properties:
        max-attempts: 1        # no in-binding retry, straight to the error handler
        enable-dlq: true       # poison records land in <topic>.DLQ instead of being logged away
```

Where a failure genuinely is not worth a redelivery — an unprocessable payload, a business
rule that rejects the record — catch it inside the handler. That is now an explicit decision
rather than the default.

> **A note on `content-type: json`.** Record-mode conversion stays lenient: a payload that
> does not parse is handed to the handler as its raw `String`. With a handler typed
> `Message<OrderEvent>` that surfaces as a `ClassCastException` — which now propagates instead
> of being logged away. `ErrorHandlingDeserializer` (below) is unaffected: those records never
> reach the handler at all.

#### Skipping malformed messages (`ErrorHandlingDeserializer`)

If a topic may contain messages your deserializer can't parse (e.g. you consume Avro but other producers occasionally write a different format), wrap the deserializer in Spring Kafka's `ErrorHandlingDeserializer` with the real deserializer as delegate. Kafka client properties pass through per-consumer `properties.configuration`, so no framework changes are needed:

```yaml
kafka-dr:
  consumers:
    payment-events-consumer:
      topic: payment-events
      group: my-group
      handler: processPayment
      content-type: native
      properties:
        configuration:
          value.deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
          spring.deserializer.value.delegate.class: io.confluent.kafka.serializers.KafkaAvroDeserializer
          specific.avro.reader: "true"
```

Behavior:

- **Valid messages** are deserialized by the delegate as usual and reach your handler (here as an Avro `SpecificRecord`).
- **Malformed messages** never reach your handler. The listener container detects the deserialization failure *before* invoking the consumer function, throws a `DeserializationException`, and the default error handler classifies it as fatal: no retries, the error is logged, the offset is committed, and consumption continues with the next record. The idempotency store and the last-processed-timestamp tracker are not touched.
- The configuration is applied to the consumer bindings of **every cluster**, so the behavior is identical after failover.

> **In batch mode this works differently.** The container calls `checkDeser` only on the record path, so with `batch.enabled: true` unreadable records are *not* filtered out before the listener runs. They reach the starter, which reports them as conversion failures at their position in the batch: everything before the bad record is committed, and redelivery resumes from it. The behaviour is safe, but it is not the "never reaches your handler" guarantee described above. The startup log warns when both are configured together.

If the message **key** can also be malformed, wrap it the same way: `key.deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer` + `spring.deserializer.key.delegate.class: <real key deserializer>`.

To capture skipped records instead of only logging them, enable the binder DLQ in the same `properties` block (outside `configuration`):

```yaml
      properties:
        enable-dlq: true
        dlq-name: payment-events-dlq
        configuration:
          value.deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
          spring.deserializer.value.delegate.class: io.confluent.kafka.serializers.KafkaAvroDeserializer
```

The DLQ producer belongs to the cluster's own binder, so each cluster gets its own DLQ topic.

#### DLQ with `content-type: native`

With native decoding the payload reaching the DLQ is no longer `byte[]`, so the binder
refuses to publish unless the **DLQ producer** carries its own serializer:

```
Native decoding is used on the consumer. Payload is not byte[] and no serializer is set on the DLQ producer.
```

The failure surfaces only after the retries are exhausted — at which point the record the
DLQ existed to preserve is dropped instead. The starter therefore rejects this combination
at startup. Configure the serializer under the same consumer:

```yaml
kafka-dr:
  consumers:
    payment-events-consumer:
      topic: payment-events
      group: my-group
      handler: processPayment
      content-type: native
      properties:
        enable-dlq: true
        dlq-name: payment-events-dlq
        dlq-producer-properties:
          configuration:
            value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
            key.serializer: org.apache.kafka.common.serialization.StringSerializer
            schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}
        configuration:
          value.deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
          spring.deserializer.value.delegate.class: io.confluent.kafka.serializers.KafkaAvroDeserializer
```

`dlq-producer-properties` is a field of `KafkaConsumerProperties`, so it routes to the
Kafka namespace automatically. `key.serializer` is only needed when the record key is not
`byte[]` either — the binder checks key and payload separately.

Quick test with the example app — send non-Avro garbage straight into `payment-events` and watch it being skipped while the app keeps consuming:

```bash
docker exec -it kafka-primary kafka-console-producer \
  --bootstrap-server localhost:9092 --topic payment-events <<< 'not-avro-garbage'
# App log: ErrorHandlingDeserializer / DeserializationException is logged, record skipped.
# Valid Avro messages sent via the REST API continue to be processed normally:
curl -X POST 'localhost:8080/api/messages/payment-events/avro?paymentId=pay-1&orderId=ord-1&amount=99.95'
```

### Batch Processing

Batching is per-consumer and off by default. Turning it on changes how records reach the handler, how often offsets are committed, and — with a Redis-backed store — how many round-trips deduplication costs.

```yaml
kafka-dr:
  consumers:
    orders-consumer:
      topic: order-events
      group: my-group
      handler: processOrders
      content-type: json
      batch:
        enabled: true
        mode: split            # split (default) | standard
        max-records: 500       # -> configuration.max.poll.records
        min-bytes: 1024        # -> configuration.fetch.min.bytes
        max-wait-ms: 250       # -> configuration.fetch.max.wait.ms
        error-policy: fail-batch   # fail-batch (default) | skip-failed
```

Batching is a per-consumer setting, so one application can mix batching and non-batching consumers freely — `batch-mode` is a binding property, and each consumer gets its own binding, container and function bean.

> Raise `max.poll.interval.ms` alongside `max-records`: that many records times the per-record processing time has to fit inside it, or the consumer is evicted from the group mid-batch and the whole batch is redelivered. The starter warns when `max-records` is raised and the interval is left at its default.
>
> Put batch tuning in the per-consumer `batch:` block, not in `default-consumer-properties` — the latter applies to every consumer, including the ones that do not batch.

#### Two modes

| Guarantee | `split` (default) | `standard` |
|---|---|---|
| Per-record deduplication | Yes, through `IdempotencyStore` | No — there are no per-record messages |
| Deduplication after failover | Automatic | Your handler's job |
| Watermark on partial failure | Up to the successful prefix | Batch maximum only |
| Idempotency rollback | Yes | Nothing to roll back |
| Existing handlers | Work unchanged | Rewrite |
| Signature | `List<Message<T>>` | `Message<List<T>>` |
| Familiar to Spring Cloud Stream users | Starter-specific | Fully |

`split` unpacks the batch envelope back into per-record messages, which is what lets deduplication, watermarks and existing `Message<T>` handlers keep working. `standard` hands over the raw envelope exactly as plain Spring Cloud Stream delivers it, including `kafka_acknowledgment` and `kafka_batchConvertedHeaders`.

The first four rows are the reason this starter exists, so `mode: standard` together with idempotency is rejected at startup rather than silently ignored — set `idempotency-enabled: false` on that consumer to acknowledge the trade-off:

```yaml
    telemetry-consumer:
      topic: raw-telemetry
      handler: processTelemetry
      idempotency-enabled: false   # required by mode: standard
      batch:
        enabled: true
        mode: standard
```

`idempotency-enabled` can only narrow the global `kafka-dr.idempotency.enabled` flag — the store bean itself is conditional on it — which is what lets a standard-mode consumer coexist with deduplicating ones in the same application.

#### Handler shapes

The shape is derived from the handler's signature; nothing declares it in configuration:

| Signature | Mode | Behaviour |
|---|---|---|
| `void h(Message<T>)` | `split` | Called per record. Failures are per record, so `error-policy` applies |
| `void h(List<Message<T>>)` | `split` | Called once with the deduplicated batch |
| `BatchOutcome h(List<Message<T>>)` | `split` | Called once; reports a verdict per record — see [Manual Acknowledgment](#manual-acknowledgment) |
| `void h(Message<List<T>>)` | `standard` | Raw envelope, headers included |
| `void h(List<T>)` | `standard` | Payloads only — so no `Acknowledgment`: rejected at startup with `ack-mode: MANUAL`/`MANUAL_IMMEDIATE`, under which offsets would never be committed |

Payloads are converted element by element to the declared type, honouring `content-type` exactly as in record mode. A mismatch between the shape and `batch.mode` fails at startup with the signature to use.

```java
@Component
public class OrderProcessor implements MessageProcessor {

    // split mode, existing handler — unchanged by enabling batching
    public void processOrder(Message<OrderEvent> message) { ... }

    // split mode, whole batch at once
    public void processOrders(List<Message<OrderEvent>> messages) { ... }

    // standard mode
    public void processRaw(Message<List<byte[]>> batch) {
        Acknowledgment ack = batch.getHeaders()
                .get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        ...
    }
}
```

#### Error policy

| Value | Behaviour |
|---|---|
| `fail-batch` *(default)* | Stops at the failing record and throws `BatchListenerFailedException` with its index. Whether that commits the successful prefix or replays the whole batch depends on `ack-mode` — see below |
| `skip-failed` | Logs the failure, releases that record's idempotency mark, and continues with the rest |

> **A partial commit needs `ack-mode: MANUAL_IMMEDIATE`.** With the container-managed
> modes the exception does not reach `DefaultErrorHandler` intact — Spring Integration
> wraps it in a `MessageHandlingException` on the way out of the function, and the handler
> logs *"Expected a BatchListenerFailedException; re-delivering full batch"* and replays
> from record 0. Nothing is lost: the already-processed prefix is filtered out by the
> idempotency store on redelivery. But it is processed-then-deduplicated rather than
> committed, and the timestamp watermark is deliberately left where it was, because
> nothing was committed. The startup log warns when `fail-batch` is combined with anything
> other than `MANUAL_IMMEDIATE`.

`skip-failed` breaks per-key ordering — a later record with the same key can be processed before the skipped one. Use it where processing is commutative (upsert by key, counters), not where the sequence of states for one entity matters.

`skip-failed` requires a per-record handler. With `List<Message<T>>` the handler is invoked once for the whole list, so an individual record cannot be skipped; that combination is rejected at startup, with `BatchOutcome` offered as the way to report per-record verdicts.

Conversion failures are treated the same way as handler failures and carry the record's index. Unlike record mode, a malformed payload is never substituted with its raw string: putting a `String` into a `List<OrderEvent>` would only surface as a `ClassCastException` deep inside business logic, far from the record that caused it.

#### What batching buys

- **Fewer commits and polls.** One commit per batch instead of one per record.
- **One deduplication round-trip.** `IdempotencyStore.filterProcessable` defaults to a loop, but a Redis-backed store overrides it with a pipeline — 500 sequential `SETNX` calls become one. The example `RedisIdempotencyStore` does exactly that.
- **Per-partition commits.** The starter enables `subBatchPerPartition` for batching consumers, so a failure in one partition does not truncate the commit prefix of the others.

### Manual Acknowledgment

`ack-mode` passes through to the container as any other Kafka binder property:

```yaml
      properties:
        ack-mode: MANUAL_IMMEDIATE
```

Everything below applies only when `ack-mode` is `MANUAL` or `MANUAL_IMMEDIATE`. Without it
the container commits as soon as the listener returns, `ack.owner` does nothing, and the
`kafka_acknowledgment` header is not even present — a handler has nothing to acknowledge and
must not try.

Who calls `acknowledge()` depends on the path, and only the record path leaves it open:

| Path | Owner |
|---|---|
| record, `ack.owner: handler` *(default)* | the handler, through the `kafka_acknowledgment` header |
| record, `ack.owner: starter` | the starter, once the handler has returned normally |
| batch `split` | the starter, which computes how far the batch may be committed |
| batch `standard` | the handler, which receives the raw envelope |

In `split` mode the **starter owns the commit** — it computes how far the batch may be acknowledged, acknowledges it, and only then advances the timestamp watermark. Handlers never touch `Acknowledgment`. In `standard` mode the handler owns it, and must call `acknowledge()` itself or offsets are never committed; the watermark for that batch waits for the call, and a handler that returns without acknowledging is reported once in the log.

#### Record mode

With `ack.owner: handler` the handler reads the header and commits when it decides to:

```yaml
kafka-dr:
  consumers:
    orders-consumer:
      topic: order-events
      group: orders
      handler: processOrder
      properties:
        ack-mode: MANUAL         # MANUAL_IMMEDIATE commits synchronously on every call
```

```java
public void processOrder(Message<OrderEvent> message) {
    handle(message.getPayload());
    message.getHeaders()
            .get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class)
            .acknowledge();
}
```

`ack.owner: starter` moves that call into the starter, which acknowledges after the handler
returns normally and only then advances the watermark — the record-path equivalent of what
the split batch consumer has always done:

```yaml
      ack:
        owner: starter
      properties:
        ack-mode: MANUAL
```

A running version of this is the `ledger-events-consumer` in `kafka-dr-example`: the topic
commits manually, and `LedgerMessageProcessor` contains no acknowledgment code at all.

```bash
curl -X POST "http://localhost:8080/api/messages/ledger-events?message=entry-1"
```

A handler that throws still propagates: the idempotency mark is rolled back, nothing is
acknowledged, and Kafka redelivers.

A record the idempotency store recognises as already processed is acknowledged by the
starter under either owner, because the handler never sees it and could not commit it. The
watermark stays where the earlier delivery left it. Without that, a partition whose tail is
all duplicates — the normal state right after a failover with replicated data — would keep
its committed offset behind those records until their marks expire, and the next redelivery
would be processed for real. The same applies to a fully deduplicated batch in `split` mode.

`MANUAL_IMMEDIATE` exists for partial batch commits and buys nothing on the record path — it
issues a synchronous commit per record. `MANUAL` queues the acknowledgment and the container
commits it on the next poll, one round trip per poll instead of per record.

#### What advances the watermark in record mode

| `ack-mode` | Commit | Timestamp watermark |
|---|---|---|
| `BATCH` *(default)* / `RECORD` | Container, as soon as the listener returns | Advanced |
| `MANUAL` / `MANUAL_IMMEDIATE` | Whoever owns the acknowledgment | Advanced **only** if `acknowledge()` was called |
| `TIME` / `COUNT` / `COUNT_TIME` | On the container's own schedule | **Not advanced** |

The watermark follows the commit, never the handler. A handler that returns without
acknowledging leaves it where it was, and says so once per consumer in the log:

```
[primary][orders-consumer] Handler returned without acknowledging under ack-mode=MANUAL.
Offsets are not committed and the timestamp watermark stays put.
```

Deliberate exceptions to that warning: `nack()`, which is a redelivery request rather than a
forgotten commit, and `ack.async-acks: true`, which is the supported way to acknowledge later
from another thread.

#### What each mode does in batch mode

| `ack-mode` | Commit | Timestamp watermark |
|---|---|---|
| `BATCH` *(default)* | Container, after the listener returns — all or nothing | Whole batch on success; **not advanced** on failure |
| `RECORD` | Not applied — the binder skips it in batch mode, leaving the `BATCH` default | Same as `BATCH` |
| `MANUAL` | Whole batch only | Frozen on partial failure — nothing was committed. In `standard` mode it follows the handler's own `acknowledge()` |
| `MANUAL_IMMEDIATE` | Successful prefix, immediately | Follows the acknowledged index |
| `TIME` / `COUNT` / `COUNT_TIME` | On the container's own schedule | **Not advanced** |

`BATCH` commits the batch as a unit: on failure nothing is committed, so the watermark
stays where it was and the batch is redelivered in full. `MANUAL_IMMEDIATE` is the only
mode in which the successful prefix is committed and the watermark moves with it — which
is why it is the one to choose when batches are large enough that reprocessing the prefix
costs something.

The last row is a deliberate choice, not a gap. These modes commit at a moment the starter cannot observe, so advancing the watermark would risk placing it ahead of the last committed offset. Leaving it alone makes seek-by-timestamp fall back to committed offsets after a failover: more redelivery, no loss.

#### Why an arbitrary subset cannot be acknowledged

Kafka commits a per-partition **watermark**, not a set of records. `Acknowledgment.acknowledge(int index)` commits a prefix, and spring-kafka enforces four constraints on it: `MANUAL_IMMEDIATE` only, the listener must receive a list, the call must happen on the consumer thread, and the index must strictly increase.

Sparse completion lives in the idempotency store instead. `BatchOutcome` reports a verdict per record and the starter translates it into the two mechanisms that do exist:

```java
public BatchOutcome processOrders(List<Message<OrderEvent>> messages) {
    BatchOutcome outcome = BatchOutcome.of(messages);
    for (int i = 0; i < messages.size(); i++) {
        try {
            handle(messages.get(i));
            outcome.done(i);
        } catch (PoisonPayloadException e) {
            outcome.discard(i, e);   // closed for good — do not redeliver
        } catch (TransientException e) {
            outcome.retry(i, e);     // hand back to Kafka
        }
    }
    return outcome;
}
```

- `done` and `discard` both **keep** the record's idempotency mark — one because it succeeded, the other because repeating it would fail again.
- `retry` **releases** the mark, so the redelivery is not dropped as a duplicate.
- A record left unmarked counts as `retry`, with a warning naming the handler. Assuming success would silently drop whatever the handler forgot; assuming failure costs one redelivery the store absorbs.

The starter commits up to the first `retry`, releases the marks of every retried record, and advances the watermark to the acknowledged index. Records marked `done` *after* a retried one are redelivered — offsets move as a watermark — and the store is what remembers they are already finished.

#### Container settings the binder cannot express

`asyncAcks`, `syncCommits`, `ackCount` and `ackTime` live on spring-kafka's
`ContainerProperties` and have no counterpart in `KafkaConsumerProperties`, so they cannot be
set through `properties`. They get their own block, applied by the starter's
`ListenerContainerCustomizer`:

```yaml
      ack:
        owner: starter        # handler (default) | starter — record path only
        async-acks: false     # allow the handler to acknowledge later, from another thread
        sync-commits: true    # false removes the commit round trip from the consumer thread
        count: 100            # ack-mode COUNT / COUNT_TIME
        time: 5000            # ack-mode TIME / COUNT_TIME, in ms
```

| Setting | Maps to | Applies with |
|---|---|---|
| `owner` | the starter itself | record path, `ack-mode: MANUAL` / `MANUAL_IMMEDIATE` |
| `async-acks` | `ContainerProperties.asyncAcks` | `MANUAL` / `MANUAL_IMMEDIATE` |
| `sync-commits` | `ConsumerProperties.syncCommits` | any mode |
| `count` | `ContainerProperties.ackCount` | `COUNT` / `COUNT_TIME` |
| `time` | `ContainerProperties.ackTime` | `TIME` / `COUNT_TIME` |

Anything unset keeps the spring-kafka default. A setting that cannot apply to the configured
`ack-mode` is reported at startup and ignored; `count` or `time` at zero or below is rejected
there, because spring-kafka's own assertion would otherwise fire inside a binder child
context — on a standby cluster, that is only built at failover.

`async-acks` has one consequence worth stating: the acknowledgment arrives after the handler
has returned, so the starter never observes the commit and the timestamp watermark is never
advanced for that consumer. Seek-by-timestamp then falls back to committed offsets after a
failover — more redelivery, no loss. The combination is reported at startup when
`failover.seek-by-timestamp` is on.

#### Constraints worth knowing

- **`MANUAL` + `fail-batch` is rejected at startup.** Partial acknowledgment requires `MANUAL_IMMEDIATE`; without it the successful prefix cannot be committed and every failure reprocesses the whole batch.
- **`ack.owner: starter` is rejected with batching enabled.** Ownership is not a choice there: `split` is acknowledged by the starter and `standard` by the handler that receives the envelope.
- **Acknowledgment happens on the consumer thread.** `parallelStream()` inside a handler is fine; handing the work to an executor and acknowledging later needs `ack.async-acks: true`.
- **`MANUAL_IMMEDIATE` commits synchronously on each call.** In batch mode that is one or two commits per poll — effectively free. On the record path it is one commit per record; prefer `MANUAL`.
- **`ack.async-acks: true` turns off the timestamp watermark.** The commit happens after the handler returns, so the starter cannot observe it; seek-by-timestamp falls back to committed offsets.
- **Auto-commit is already off.** spring-kafka sets `enable.auto.commit=false` unless it is configured explicitly, so a manual ack-mode moves the commit point rather than turning auto-commit off.

### Producers

```yaml
kafka-dr:
  producers:
    order-events-producer:           # Logical producer name (user-chosen key)
      topic: order-events
      content-type: json
    payment-events-producer:
      topic: payment-events
      content-type: native
      properties:
        configuration:
          value.serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
```

Producers are also a map keyed by logical name, and that name is how a producer is addressed:

```java
producer.to("order-events-producer").send(order, order.getOrderId());
producer.to("order-events-producer").sendBatch(messages);
```

The name says nothing about the topic or the Kafka behind it, so renaming a topic or moving it
to another [cluster group](#cluster-groups) is a configuration change only. `to(name)` is also the
only way to reach a topic that exists in several groups.

`ResilientProducer.send(topic, ...)` keeps working: it resolves the topic to its producer entry
as long as the topic is configured in one group, and fails with a pointer to `to(name)` when it
is not. A topic has exactly one producer per cluster group; duplicates fail at startup.

### Topic Provisioning

```yaml
kafka-dr:
  auto-create-topics: true    # false in production (default), true in development
```

One flag, deliberately not the binder's own. `KafkaAdminHelper` opens an AdminClient to
**every reachable cluster** at startup — and again from `LateBindingInitializer` when a
cluster comes back — and creates the topics of all configured consumers and producers.

The binder's lazy creation would only reach the cluster that currently holds bindings, so
the standby cluster would get its topics no earlier than the failover itself, which is the
worst possible moment and too late for MirrorMaker to have been replicating into them.
Lazy creation also routes through `KafkaTopicProvisioner`, which blocks on metadata lookups
against dead brokers (`max.block.ms`) instead of failing cleanly on send.

That is why the starter forces the binder-level flag to `false` on every cluster. Override
it in `default-environment` or per cluster if you deliberately want the binder to create
topics as well.

**A topic that appears after startup.** Spring Cloud Stream cannot create a consumer's binding
while its topic is missing; it retries every 30 s and creates the binding once the topic
exists. Every DR binding is created stopped, and the binding service's own record of such a
late binding is a placeholder that cannot start, stop or pause it. The starter keeps the real
binding from the binder's `BindingCreatedEvent`: it starts it at once when its cluster is the
active one of its group, and starts, stops, pauses and resumes it on later failovers like any
other — otherwise the consumer would sit idle until a restart, or keep reading a cluster the
group has left.

> **Known limitation — create topics before the application sends to them.** With
> `auto-create-topics: false`, the first send to a topic that does not exist yet blocks the
> sending thread for about 45 s: Spring Cloud Stream creates producer bindings lazily, on the
> first send, and its `KafkaTopicProvisioner` keeps retrying the partition lookup. On a
> consumer thread — a `depends-on` bridge — that can exceed `max.poll.interval.ms` and trigger
> a rebalance. Later sends fail at once and are held as described in
> [Producer Error Handling](#producer-error-handling). Create topics before the application
> starts, or keep `auto-create-topics: true`.

### Health Check & Failover Tuning

```yaml
kafka-dr:
  health-check:
    interval-ms: 5000       # How often to probe each cluster (wall-clock, fixed rate)
    timeout-ms: 2000         # AdminClient timeout per probe (default: 2000)
    failure-threshold: 2     # Consecutive failures → unhealthy (default: 2; also retry count for send errors)
    recovery-threshold: 3    # Consecutive successes → healthy (default: 3)
    deep-probe: true         # default: false
    deep-probe-min-nodes: 2  # min unique active leader nodes (default: 1)
    deep-probe-min-isr: 2    # min in-sync replicas per partition (default: 0 — disabled)
```

**Time to fail over** ≈ `failure-threshold × interval-ms` + the binding switch. With the defaults that is `2 × 5000ms` ≈ **10 s**. Lower `failure-threshold` to 1 for the fastest reaction (at the cost of reacting to transient blips), or raise it to debounce flapping clusters. The probe cadence is wall-clock (`fixedRate`): probes for all clusters run **in parallel**, each bounded by `timeout-ms` (the bound covers the initial socket connection setup too), so a slow or unreachable cluster never stretches the interval or delays the other clusters' probes. `recovery-threshold` is intentionally higher than `failure-threshold` — leave a failed cluster quickly, return to it cautiously.

With [cluster groups](#cluster-groups) every setting here can be narrowed per group, and each group is probed on its own schedule at its own `interval-ms`, from its own probe pool — probes hanging in one group never delay another group's past their deadline. Deep probe checks only the topics of the group being probed. A round cut short by application shutdown reports nothing, so stopping the app never fails a group over.

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
| 6 | Clock reaches 23:59:59 | — | Failback → primary (highest priority healthy) at the next health round |

> **Note:** `failback-after` blocks **all** failback (return to any higher-priority cluster) while the current cluster is healthy. Failover (leaving an unhealthy cluster) is always immediate regardless of this setting. After a successful failback the gate resets — subsequent failovers will again be held until the configured time. The value is checked at startup: anything `LocalTime.parse` rejects (`"11pm"`, say) fails startup rather than the first failback.

#### Surviving application restarts (`FailoverStateStore`)

The `failback-after` gate is enforced via a pluggable `FailoverStateStore`. On every cluster switch the manager records the active cluster name and the failover `Instant` to the store; on a successful failback, and when the preferred cluster is elected at startup — no failover, nothing to hold — it clears the store.

```java
public interface FailoverStateStore {
    void save(FailoverState state);
    Optional<FailoverState> load();
    void clear();

    // One state per cluster group. Defaults delegate to the single-state methods above.
    default void save(String group, FailoverState state) { save(state); }
    default Optional<FailoverState> load(String group) { return load(); }
    default void clear(String group) { clear(); }

    // True when the three methods above keep a separate state per group.
    default boolean supportsGroups() { return false; }

    record FailoverState(String activeCluster, Instant failoverAt) {}
}
```

Every [cluster group](#cluster-groups) fails over on its own, so its state is kept on its own.
With one group the defaults are exactly right and existing stores need no change. With several,
the defaults would let groups overwrite each other's state — and restore a cluster of one group
as the active cluster of another after a restart — so the starter **refuses to start** unless the
store implements the three per-group methods with a key per group and says so by returning `true`
from `supportsGroups()`. An explicit declaration rather than a guess from the class's methods, which
proxies and wrappers would make unreliable. `activeCluster` is the binder id (`core-primary`).

When an existing store gains the per-group methods, map the group `default` to the key it used
before — as `RedisFailoverStateStore.keyOf` does. A new key for it would make the first restart
after the change find no state and elect the primary inside a failback window.

On startup `ActiveClusterManager` calls `load(group)` for every group:

- **No persisted state** → standard initial election (priority order).
- **Persisted state, but `now` is at or past the next occurrence of `failback-after` after `failoverAt`** → state is cleared and the priority cluster is elected as usual. This is the date-aware part: if the app was down for hours or days, the gate has already expired and the priority cluster wins.
- **Persisted state, threshold not yet reached** → the persisted cluster is restored as active, `failoverOccurred` is set, and the gate continues to block any failback until the threshold passes — even if a higher-priority cluster reports healthy first. The initial election waits for the restored cluster's report in the first health round: healthy, it is elected at once (no `recovery-threshold` wait) and its consumers start; down, the best healthy cluster is elected instead.

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

The `kafka-dr-example-redis-state` module includes `RedisFailoverStateStore` as a reference implementation: a Redis hash with `activeCluster` and `failoverAt` fields, keyed `kafka-dr:failover-state` for the `default` group — the key it always used — and `kafka-dr:failover-state:<group>` for every other group.

| Scenario | In-memory store (default) | Durable store (e.g. Redis) |
|---|---|---|
| Failover at 14:00, restart at 16:00, `failback-after: "22:00"` | App boots on priority cluster (gate lost) | App restores secondary, gate active until 22:00 |
| Failover at 14:00, restart next day at 09:00 | App boots on priority cluster | Threshold (yesterday 22:00) past → store cleared → priority cluster |
| Failover at 23:00, restart at 09:00 next day, `failback-after: "22:00"` | App boots on priority cluster | Threshold = next day 22:00 → still blocked → restores secondary |

How it works:
1. Each consumer tracks the latest committed `RECEIVED_TIMESTAMP` per topic partition via `LastProcessedTimestampTracker`
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
    enabled: true            # Master switch; true by default — set false to disable deduplication
    ttl-seconds: 3600        # How long to remember processed message IDs
    key-prefix: idempotency  # Key prefix for store implementations
    # key-header: x-idempotency-key  # Optional: use a custom header instead of Kafka key
```

Idempotency is **enabled by default**. Setting `kafka-dr.idempotency.enabled=false` switches the mechanism off entirely: no `IdempotencyStore` bean is created (including the in-memory fallback), any user-defined store is ignored by the consumer chain, and every message is processed without a deduplication check. Timestamp tracking for `seek-by-timestamp` failover keeps working regardless of this flag.

By default, idempotency uses **Kafka record key** (`KafkaHeaders.RECEIVED_KEY`) as the deduplication key — no custom headers required. To use a custom message header instead, set `key-header`:

| Configuration | Deduplication key source |
|---|---|
| *(default, no `key-header`)* | Kafka record key (`KafkaHeaders.RECEIVED_KEY`) |
| `key-header: x-idempotency-key` | Value of `x-idempotency-key` message header |

Messages without a key (or without the configured header) are processed without idempotency check (with a warning log).

Marks are scoped by consumer name and never by cluster: a record that MirrorMaker replicated
and the standby redelivers after a failover is a duplicate on any cluster of the group. Consumer
names are unique application-wide and each consumer belongs to one [cluster group](#cluster-groups),
so groups never see each other's marks. `ttl-seconds` applies to the in-memory store too, and a
mark past its TTL no longer counts as a duplicate even before the periodic eviction removes it.

The framework provides `InMemoryIdempotencyStore` as default fallback — it is registered as a `@Bean` in `KafkaDrAutoConfiguration` with `@ConditionalOnMissingBean(IdempotencyStore.class)`. This ensures proper ordering: Spring processes application `@Component` beans first, then auto-configuration `@Bean` methods. If any `IdempotencyStore` is already registered, the in-memory fallback is skipped.

#### Custom `IdempotencyStore`

The SPI receives the **full message** — headers and payload — so the deduplication decision can be based on anything: the Kafka key, any header, or data extracted from the payload itself. Two customization points:

**Override `extractKey` only** — keep the storage logic of an existing implementation, change just how the key is derived. The built-in stores call `extractKey(message)` from `tryProcess`, so this is the lightest way to customize:

```java
@Component
public class PayloadKeyedStore extends InMemoryIdempotencyStore {   // or RedisIdempotencyStore
    @Override
    public String extractKey(Message<?> message) {
        // any header or payload data
        return ((OrderEvent) message.getPayload()).getOrderId();
    }
}
```

The default `extractKey` uses the Kafka record key (`KafkaHeaders.RECEIVED_KEY`, then `KafkaHeaders.KEY`; `byte[]` → UTF-8); the built-in stores honor the configured `key-header`. Returning `null` means "no key" — built-in stores then process the message without idempotency check.

**Implement the whole store** — full control over both key extraction and storage. Return `true` to process the message, `false` to skip it as a duplicate:

```java
@Component
public class MyIdempotencyStore implements IdempotencyStore {
    @Override
    public boolean tryProcess(String clusterName, String consumerName, Message<?> message) {
        String key = extractKey(message);   // default Kafka-key logic, or override it
        return markAsProcessedIfFirstTime(consumerName, key);
    }
}
```

The static helper `IdempotencyStore.kafkaKey(message, customKeyHeader)` exposes the default key-based extraction (including custom-header support) for reuse. The example app includes `RedisIdempotencyStore` built on it.

**Two optional methods** cover batch consumption and failure recovery. Both have defaults, so existing stores keep compiling and working:

```java
// Batch check. Default is a loop over tryProcess; override it when the store is remote.
default List<Message<?>> filterProcessable(String clusterName, String consumerName,
                                           List<Message<?>> messages);

// Release marks for messages that were accepted but never processed. Default is a no-op.
default void rollback(String clusterName, String consumerName, List<Message<?>> messages);
```

`filterProcessable` must return **the same message instances** as the input, not copies: callers map records back to their position in the batch by identity, and that is what makes partial commits land on the right offset. `RedisIdempotencyStore` overrides it with a pipeline, turning one round-trip per record into one per batch.

`rollback` matters beyond batching. `tryProcess` marks a message *before* the handler runs, so a failure between the two steps leaves it recorded as done and the redelivery Kafka performs is dropped as a duplicate. The window is narrow with auto-commit and as wide as the application wants it with manual acknowledgment. Implement it wherever keys can be deleted — `InMemoryIdempotencyStore` removes the entry, `RedisIdempotencyStore` issues a batched `DEL`.

> **Migration note:** a `List<T>` handler in `batch.mode: standard` with `ack-mode: MANUAL` or `MANUAL_IMMEDIATE` now fails startup. It never committed an offset — payloads carry no `Acknowledgment` — so the consumer replayed everything on each restart. Declare it `Message<List<T>>` and acknowledge through its `KafkaHeaders.ACKNOWLEDGMENT` header, or use an automatic ack-mode.

> **Migration note:** two settings are now checked at startup instead of later. `kafka-dr.idempotency.ttl-seconds` is applied by the in-memory store — it used to be ignored there, always 3600 s — so a value of 0 or less now fails startup, and a large one really keeps marks that long in heap. A `failback-after` that is not a time of day (`"11pm"`) now fails startup instead of the first failback.

> **Migration note:** when a send proves the active cluster down, the failover it forces — `ClusterSwitchedEvent` and the binding switch — is now published on a `kafka-dr-failover-events-<group>` thread, after `send()` has returned, no longer on the sending thread. The active cluster itself changes before `send()` returns. A listener that relied on running on the sender's thread, or on the switch being complete when `send()` returns, must not.

> **Migration note:** producer bindings are now always synchronous. A `sync: false` under `default-producer-properties` or a producer's `properties` fails startup; without the setting the starter sets `sync: true` itself.

> **Migration note:** code that records watermarks itself must do so through `LastProcessedTimestampTracker.forConsumer(consumerName)`. Calling `update`/`advance` on the injected bean records under the old unscoped key, which no seek reads any more; the first such call logs a warning.

> **Migration note:** `TimestampStore` keys are now `<consumer>:topic-partition` — each consumer has its own watermark. The interface is unchanged. Entries written under the previous `topic-partition` format are never read again: they were the position of whichever consumer wrote them, and a consumer added later would seek by it and skip records it never processed. So, as with the previous key change, the first failover after upgrading falls back to committed offsets once; the idempotency store drops what that replays. `TimestampStore` has no delete, so the old entries stay in the store until removed there.

> **Migration note:** `TimestampStore` keys changed from a bare topic name to `topic-partition`. The interface itself is unchanged — the key stays an opaque `String` — so implementations such as the example `RedisTimestampStore` need no edits. Entries written under the old format are simply never read again, so the first start after upgrading falls back to committed offsets once.

> **Migration note:** the SPI changed from `tryProcess(String consumerName, String messageId)` to `tryProcess(String clusterName, String consumerName, Message<?> message)`. Key extraction moved from `IdempotentConsumer` into the store: existing key-based implementations should call `extractKey(message)` (or the static `IdempotencyStore.kafkaKey(message, keyHeader)`) and handle the `null` (no key) case by returning `true`.

### Diagnostic Logging

```yaml
kafka-dr:
  debug:
    enable: true   # default: false
```

Off by default, and deliberately so: during a failover a probe failure *is* the expected signal, and printing a stack trace for every one of them (each cluster, every `interval-ms`) would bury the `DR_EVENT` lines that actually matter. Turn it on when the question is **why** a cluster is considered down rather than *that* it is.

What the flag changes:

| Where | Off (default) | On |
|---|---|---|
| `KafkaAdminHelper.probeCluster` | Failure swallowed, `false` returned | `WARN` with brokers, timeout and the full stack trace |
| `KafkaAdminHelper.provisionTopics` | `WARN` with `e.getMessage()` | Same line with the stack trace |
| `ClusterHealthChecker` — basic probe, deep probe, probe timeout | `DEBUG` with `e.getMessage()` | `WARN` with the stack trace |
| `ResilientProducer` — cluster unavailable, retry attempt, serialization error | `WARN` with `e.getMessage()` | `WARN` with the stack trace |
| `ResilientProducer` — retries exhausted, all clusters unavailable (single and batch) | Message and counts only, no cause | Same line plus the stack trace of the exception that ended the retry ladder |
| `BindingLifecycleManager` — start/stop/pause/resume binding | `ERROR` with `e.getMessage()` | `ERROR` with the stack trace |

The flag is read once at startup: `DynamicBindingRegistrar` pushes it into `KafkaAdminHelper` (a static utility, so there is nothing for Spring to inject into) before the first probe runs, and the bean-side users read it from `KafkaClusterProperties`.

Everything above is the *cause* of a probe failure. Kafka's own client chatter is a separate axis and stays under `logging.level` — the examples silence it explicitly:

```yaml
logging:
  level:
    dev.semeshin.kafkadr: INFO
    org.apache.kafka.clients.NetworkClient: ERROR   # raise to WARN to see connection attempts
```

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

For many messages at once, `sendBatch` makes **one** failover decision for the whole batch:

```java
List<Message<?>> messages = orders.stream()
        .map(o -> (Message<?>) MessageBuilder.withPayload(o)
                .setHeader(KafkaHeaders.KEY, o.getOrderId())
                .build())
        .toList();

BatchSendResult result = producer.sendBatch("order-events", messages);

if (!result.allSent()) {
    result.failures().forEach(f -> log.warn("not sent: {}", f.messageId()));
}
```

Sending in a loop would re-run the retry ladder for every message against a cluster that is already gone — 500 messages times `failure-threshold` doomed attempts before the failover. Here the first message that reports the cluster unavailable ends the attempt for the entire remainder.

On failover only the **unsent tail** moves to the next cluster; resending the whole batch would duplicate everything the previous cluster already acknowledged. A message proven at fault — unserializable, an invalid schema, refused by the broker for what it is — is marked failed and the batch continues on the same cluster; see [Producer Error Handling](#producer-error-handling) for what counts as proof.

`BatchSendResult` holds one `SendResult` per input message, in order, plus `sent()`, `failed()`, `allSent()`, `failures()` and `clusters()`. There is deliberately no single `cluster` field — a batch that failed over mid-way was written to more than one, and that is exactly the case a single field would misreport.

Sends stay synchronous. `StreamBridge.send` returns a boolean rather than a future, so going async would cost the very failure signal that drives the failover; throughput belongs to `linger.ms` and `batch.size` in per-producer `properties.configuration`.

`StreamBridge` caches one output channel per cluster and producer it has used, and once the cache is full it unbinds the eldest — possibly a producer still in use. After a failover and a failback every producer has used two clusters, so the starter sets `spring.cloud.stream.dynamic-destination-cache-size` to at least the number of producers times the clusters of their group, unless it is set explicitly (a warning is logged if an explicit value is too small).

The `messageId` parameter (or `KafkaHeaders.KEY` header) is used as:
- **Kafka record key** — determines partition assignment
- **Idempotency key** — `IdempotentConsumer` deduplicates by `KafkaHeaders.RECEIVED_KEY` on the consumer side

No system headers are injected by the framework — only user-provided headers and `KafkaHeaders.KEY` are sent.

**Results and failures.** `SendResult` reports `success()`, `cluster()` (binder id), `messageId()`,
`group()` and, on failure, `failure()`:

| `Failure` | Meaning | `orThrow()` throws |
|---|---|---|
| `NO_HEALTHY_CLUSTER` | No cluster of the group was healthy; nothing was attempted | `ClusterGroupUnavailableException` |
| `ALL_CLUSTERS_FAILED` | No cluster of the group took the message, and nothing proves the message at fault: the clusters were unreachable or broken, or did not take it for a reason that is no verdict on the message (a missing ACL, a topic missing on a cluster, a schema registry down, an exception that is not Kafka's) | `ClusterGroupUnavailableException` |
| `SERIALIZATION` | The message itself could not be serialized — including an invalid schema (a registry 422) or Jackson failing on the payload; another cluster would fail the same way. Not the registry's own state or health, which every cluster may have its own of: 404 (subject not found), 409 or "Incompatible schema" (against that registry's history), error code 42205 (a registry in `READONLY`/`IMPORT` mode, as a standby replica is), and an unreachable or failing registry — a timeout, a reset connection, a proxy's page, 401/403/408/429/5xx | `SendFailedException` |
| `REJECTED` | The broker refused the record for what it is — too large, invalid, an invalid timestamp — or the topic name is invalid; no failover is triggered | `SendFailedException` |

The rule behind it: **a message is failed only on proof that the message is at fault, and a
cluster is marked down only on proof that the cluster is broken.** A failure that proves neither
gets neither verdict, so a `depends-on` bridge holds the record back rather than losing it — for
at most `depends-on-max-hold-ms` while the group is up. Missing ACLs belong there: ACLs are kept
per cluster and MirrorMaker does not copy them, so they say nothing about the message.

`orThrow()` returns the result when the message was sent, and throws otherwise —
`BatchSendResult.orThrow()` does the same for a batch, reporting a group failure ahead of single
messages. From a consumer handler that exception is the point: it is what gets the source record
redelivered instead of committed, and what [`depends-on`](#bridging-between-groups-depends-on)
recognizes as the dependency being down.

## How Failover Works

### Startup

1. `DynamicBindingRegistrar` probes all clusters (3s timeout)
2. Reachable clusters: binders, bindings, function beans created
3. Unreachable clusters: only environment properties generated (no blocking)
4. All consumers start with `auto-startup=false`
5. All clusters begin as `UNHEALTHY`
6. First health check elects first healthy cluster immediately — except in a group restored from
   the `FailoverStateStore`, whose election waits for the restored cluster's report and prefers it
   while it is healthy
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
     - Gets the last committed timestamp for each (topic, partition)
     - Calls consumer.offsetsForTimes(timestamp) on each partition
     - Seeks to the offset matching that timestamp
  4. Consumer reads from the seek point, not from offset 0 or latest
  5. IdempotentConsumer deduplicates any overlap in the boundary window
```

Watermarks are tracked **per (consumer, topic, partition)**. A per-topic watermark is the maximum across partitions, which would seek a lagging partition past records it never processed — and a watermark shared by two consumers of the same topic is the maximum of the two, which would seek the one that lags past records *it* never processed. Since a consumer belongs to one [cluster group](#cluster-groups), the same topic name in two groups never shares a watermark either. Each group fails over on its own, and only the consumers of the group that switched seek.

The watermark follows what was **committed**, not what was processed. With manual acknowledgment a batch can be handled and acknowledged at different moments; advancing the watermark first would make the seek skip records whose offsets never landed, and nothing would redeliver them. When a partition has no watermark — nothing processed yet, or an `ack-mode` whose commits the starter cannot observe, such as `TIME` or `COUNT` on either path — the seek is skipped and the consumer falls back to committed offsets.

> This mechanism assumes topic names are identical across clusters. The bundled MirrorMaker 2 configuration uses `IdentityReplicationPolicy` for that reason; switching to `DefaultReplicationPolicy`, which prefixes topics with the source cluster alias, silently breaks the lookup.

```
DR_EVENT [orders-consumer] [demo-events] Seeked partition 0 to offset 1542 (timestamp=1714003200000)
```

### Producer Error Handling

| Attempt ended with | Proof of | Behavior |
|---|---|---|
| **Serialization** (not the schema registry's connection or state), **record refused** (`RecordTooLargeException`, `RecordBatchTooLargeException`, `InvalidRecordException`, `InvalidTimestampException`), **invalid topic** | the message | Not retried. That message fails (`SERIALIZATION`/`REJECTED`), the cluster stays healthy, the rest goes on |
| **Cluster unavailable** (`StreamBridge` false, timeout, network; a "topic not present in metadata" timeout only when a quick `describeCluster` probe confirms the cluster does not answer — otherwise the topic is missing there — or a broker that has just restarted has not loaded it yet — which proves nothing, and for 10 s sends fail at once instead of waiting `max.block.ms` each. The same for an expired record ("Expiring N record(s)") when a `describeTopics` probe finds the cluster answering without the topic: deleted while the producer still knew it, which would otherwise read as an outage, fail the group over and back on every send) | the cluster | Immediate `forceUnhealthy` + failover. The active cluster changes at once, so the next send goes to the new one; the binding switch is published on a dedicated thread, never on the sender's — usually a listener thread that would otherwise stop its own container |
| **Retriable Kafka error** (`NotEnoughReplicas`, `CorruptRecord`…) outlasting `failure-threshold` retries | the cluster | `forceUnhealthy` + failover — also on the last cluster, which leaves the group unavailable |
| **Anything else** — a non-retriable broker answer (missing ACL), a topic missing on a cluster that answers (both not retried), the schema registry failing (retried; recognized in the shapes Confluent 8.x throws, including a Kafka `TimeoutException` or `DisconnectException` carrying the registry's `RestClientException` or an I/O error, which must not read as the broker), an exception that is not Kafka's (retried) | neither | `ALL_CLUSTERS_FAILED` for it and, untried, for the rest of the batch — the cause is likely the same for all. Nothing is marked, the group does not fail over, and the message is not written to another cluster (below): a `depends-on` bridge holds it until the cause is gone, or for at most `depends-on-max-hold-ms` |

Messages are written to the group's **active cluster only** — that is where its consumers read,
and replication runs from it to the standbys, not back. A message written to a standby while
another cluster is active would sit there unread until a failover made that standby active. So a
cluster that does not take a message is either proven broken, and the whole group — consumers
included — fails over, or the message is held.

One engine serves `send` and `sendBatch`. A cluster lost during a batch stays lost for the rest of
it, so a dead cluster costs one attempt rather than one per message.

Sends are always synchronous: the starter sets `sync: true` on every DR producer binding, and a
`sync: false` in `default-producer-properties` or a producer's `properties` fails startup —
asynchronously, `StreamBridge` reports success before the broker answers, and none of the above
could happen.

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
| Binding properties routed by reflecting over Spring Cloud Stream's own classes | Core and Kafka-extension property sets are disjoint; deriving the routing table from the classes means it cannot drift on upgrade, and a key in neither set is reported as a probable typo instead of vanishing |
| Batch envelope unpacked back into per-record messages | Keeps `IdempotencyStore`, key extraction, watermarks and existing `Message<T>` handlers working unchanged; batching becomes a transport setting rather than a second API |
| Partial commits only under `MANUAL_IMMEDIATE` | Spring Integration wraps the exception before `DefaultErrorHandler` sees it, so `BatchListenerFailedException` cannot drive a partial commit from inside a Spring Cloud Stream function; verified against a live broker. Under container-managed ack modes the batch is replayed in full and the watermark stays put |
| Duplicates acknowledged by the starter | A deduplicated record never reaches the handler, so under a manual ack-mode nobody else can commit it; leaving it unacknowledged stalls the committed offset on the all-duplicate stretch a failover produces, and once the marks expire the redelivery is processed for real |
| Watermark advanced by commit, never by processing | With manual acknowledgment the two happen at different moments; a watermark ahead of the committed offset makes seek-by-timestamp skip records nothing will redeliver. Applies to the record path too: the acknowledgment is wrapped so the commit is observable, and a handler that returns without acknowledging leaves the watermark alone |
| Acknowledgment ownership is explicit (`ack.owner`) | The record path is the only one where it is a choice; making it configuration rather than convention lets the starter acknowledge and advance the watermark in one place, and lets a forgotten `acknowledge()` be reported instead of silently stalling the consumer group |
| Container-only settings exposed as `ack.*` | `asyncAcks`, `syncCommits`, `ackCount` and `ackTime` exist on `ContainerProperties` but not in `KafkaConsumerProperties`, so binder YAML cannot reach them; the starter already owns the single `ListenerContainerCustomizer` the binder accepts |
| Sparse completion kept in the idempotency store, not in offsets | Kafka commits a per-partition watermark, so an arbitrary subset cannot be acknowledged; `BatchOutcome` verdicts map onto a contiguous commit plus the store as the "already done" set |
| Conversion failures in batch mode throw instead of falling back | Substituting a raw `String` would put a foreign type into a `List<T>` and surface as a `ClassCastException` inside business logic, far from the record that caused it |
| `sendBatch` abandons a dead cluster after the first failed message | One message proves the cluster is gone; retrying the ladder for the rest costs `size × failure-threshold` doomed attempts before the failover |
| Cluster groups, each with its own election | Independent Kafkas fail independently; one election per group keeps a failover in one from moving the consumers and producers of another |
| `clusters` resolved into the group `default` with unchanged names | Existing deployments keep binder, binding and bean names and persisted keys; moving clusters under `cluster-groups.default` adds groups without renaming anything |
| Binder id `<group>-<cluster>` | Binder names are global to Spring Cloud Stream and two groups may both have a `primary` |
| One broker address, one cluster entry | Shared brokers make two groups fail together, or a cluster its own standby; rejected at startup rather than discovered during a failover |
| Containers matched to consumers by binding name | Destination and consumer group may repeat across groups; the binding name passed to a `KafkaListenerContainerCustomizer` is unique per consumer and cluster |
| `depends-on` holds records back with a nack, not an exception | A nack does not count as a delivery attempt; a fast-failing send would otherwise exhaust the retries in seconds and the container would skip the record |
| `depends-on` requires manual acknowledgment | A nack exists only there; accepting it elsewhere would leave a configuration that reads as protected and loses records exactly when the dependency fails |
| Producers addressed by name | Business code does not encode which Kafka a topic lives in; a topic present in several groups stays addressable |
| Watermark per consumer | Two consumers of one topic progress independently; a shared watermark is the maximum, and the one that lags would seek past its own unprocessed records |

## Tech Stack

- Java 17
- Spring Boot 4.0.7
- Spring Cloud 2025.1.1 (Kafka Binder)
- Apache Kafka 3.9 (KRaft, no ZooKeeper)
- Confluent Schema Registry 8.2.0
- Apache Avro 1.12.1
- Redis 7 (optional, for idempotency)

## License

MIT
