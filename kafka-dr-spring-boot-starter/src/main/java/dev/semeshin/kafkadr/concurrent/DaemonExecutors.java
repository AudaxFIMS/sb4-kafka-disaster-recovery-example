package dev.semeshin.kafkadr.concurrent;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * The starter's own background threads. All of them are daemons — none may keep the JVM alive
 * on its own — and all are named, so a thread dump says which component and which cluster group
 * a stuck thread belongs to.
 */
public final class DaemonExecutors {

    private DaemonExecutors() {
    }

    /** One named daemon thread, created on first use. */
    public static ExecutorService singleThread(String name) {
        return Executors.newSingleThreadExecutor(daemon(name, false));
    }

    /**
     * A fixed pool of daemon threads, created on first use and named {@code <prefix><thread id>}.
     *
     * @param size number of threads; at least one is created
     */
    public static ExecutorService fixedPool(String namePrefix, int size) {
        return Executors.newFixedThreadPool(Math.max(1, size), daemon(namePrefix, true));
    }

    /**
     * Lets work already queued finish within {@code grace} — a failover state still to be persisted,
     * say — then interrupts whatever is left.
     */
    public static void shutdownGracefully(ExecutorService executor, Duration grace) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(grace.toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory daemon(String name, boolean suffixWithId) {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(suffixWithId ? name + thread.getId() : name);
            thread.setDaemon(true);
            return thread;
        };
    }
}
