package dev.semeshin.kafkadr.concurrent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class DaemonExecutorsTest {

    @Test
    void threadsAreNamedDaemons() throws Exception {
        ExecutorService single = DaemonExecutors.singleThread("kafka-dr-test");
        ExecutorService pool = DaemonExecutors.fixedPool("kafka-dr-test-pool-", 2);
        try {
            Thread singleThread = CompletableFuture.supplyAsync(Thread::currentThread, single).get(1, TimeUnit.SECONDS);
            Thread pooled = CompletableFuture.supplyAsync(Thread::currentThread, pool).get(1, TimeUnit.SECONDS);

            assertThat(singleThread.getName()).isEqualTo("kafka-dr-test");
            assertThat(singleThread.isDaemon()).isTrue();
            assertThat(pooled.getName()).startsWith("kafka-dr-test-pool-");
            assertThat(pooled.isDaemon()).isTrue();
        } finally {
            single.shutdownNow();
            pool.shutdownNow();
        }
    }

    @Test
    void gracefulShutdownLetsQueuedWorkFinish() {
        ExecutorService executor = DaemonExecutors.singleThread("kafka-dr-test");
        AtomicBoolean done = new AtomicBoolean();
        executor.execute(() -> sleep(100));
        executor.execute(() -> done.set(true));

        DaemonExecutors.shutdownGracefully(executor, Duration.ofSeconds(2));

        assertThat(done).isTrue();
        assertThat(executor.isTerminated()).isTrue();
    }

    @Test
    void gracefulShutdownInterruptsWhatOutlastsTheGracePeriod() throws Exception {
        ExecutorService executor = DaemonExecutors.singleThread("kafka-dr-test");
        java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch interrupted = new java.util.concurrent.CountDownLatch(1);
        executor.execute(() -> {
            started.countDown();
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                interrupted.countDown();
            }
        });
        started.await(1, TimeUnit.SECONDS);

        DaemonExecutors.shutdownGracefully(executor, Duration.ofMillis(100));

        assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
