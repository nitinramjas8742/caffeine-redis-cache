package com.hybridcache.runtime.storage;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.jboss.logging.Logger;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * One Redis pub/sub subscription shared by every cache region in the JVM.
 * <p>
 * Pub/sub is only an eager-eviction optimization; correctness comes from the
 * timestamp comparison. Still, each subscription holds a Redis connection for
 * its lifetime, so subscribing once per region would exhaust the client pool
 * (the query, update-timestamp, entity and natural-id regions easily exceed the
 * default pool size) and make unrelated {@code HGET}s time out.
 * <p>
 * Messages are published on a single channel as {@code <region>|<payload>} and
 * dispatched to the region that registered a listener.
 * <p>
 * The subscription is <b>not</b> permanent. Redis restarts, failovers and network
 * drops end the connection, and a dead subscription used to be invisible because
 * the near-cache hot path never consults Redis. This class therefore:
 * <ul>
 *   <li>resets its state when the subscription terminates and retries with a delay;</li>
 *   <li>runs a heartbeat round-trip on the channel, so a silently dead subscription
 *       is detected even when the client does not report the close;</li>
 *   <li>clears every local near-cache once when the gap is detected and once when
 *       the subscription is confirmed live again, so updates missed during the
 *       outage are re-read from Redis/database instead of being served stale
 *       indefinitely. The clear is intentionally not repeated on every reconnect
 *       attempt: a node running in local-only mode must keep the entries it
 *       cached while Redis was down.</li>
 * </ul>
 */
@ApplicationScoped
public class HybridCachePubSub {

    private static final Logger LOG = Logger.getLogger(HybridCachePubSub.class);

    public static final String CHANNEL = "hybridcache.invalidation";

    /**
     * Payload handed to every region listener when the subscription is considered
     * lost. The map parses it exactly like a remote {@code CLEAR_ALL_CACHE}
     * invalidation, which also evicts the local entity payloads.
     */
    private static final String RECONNECT_CLEAR = "__reconnect__|CLEAR_ALL_CACHE";
    /**
     * Sent once the new subscription has actually delivered a message (proof that
     * invalidations can arrive again). Regions additionally force revalidation for a
     * short grace period on this marker, because messages published while the new
     * SUBSCRIBE was still being established may have been lost.
     */
    private static final String RECONNECT_LIVE_CLEAR = "__reconnect_live__|CLEAR_ALL_CACHE";
    private static final String HEARTBEAT_REGION = "__heartbeat__";

    private static final long RECONNECT_DELAY_MS =
            Long.getLong("hybridcache.pubsub.reconnect-delay-ms", 2_000L);
    private static final long HEARTBEAT_INTERVAL_MS =
            Long.getLong("hybridcache.pubsub.heartbeat-interval-ms", 2_000L);
    private static final long HEARTBEAT_TIMEOUT_MS =
            Long.getLong("hybridcache.pubsub.heartbeat-timeout-ms", 6_000L);

    /**
     * Redisson's {@code ReconnectionStrategy.CLEAR}: after the pub/sub topic comes
     * back, drop the local near-cache so updates missed during the outage are
     * re-read from Redis. Set to {@code false} to keep the raw
     * {@code ReconnectionStrategy.NONE} behaviour.
     */
    private static final boolean CLEAR_ON_RECONNECT = Boolean.parseBoolean(
            System.getProperty("hybridcache.pubsub.clear-on-reconnect", "true"));

    private final ConcurrentHashMap<String, List<Consumer<String>>> listeners = new ConcurrentHashMap<>();
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
    /** Invalidates callbacks from an older subscription attempt. */
    private final AtomicLong subscriptionGeneration = new AtomicLong();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "hybrid-cache-pubsub");
                thread.setDaemon(true);
                return thread;
            });

    /**
     * {@code subscribed} is true from the moment an attempt is started until it ends;
     * {@code subscriptionConfirmed} becomes true only once the subscription actually
     * delivers a message (a heartbeat echo is enough). The distinction matters because
     * a connection attempt against a down Redis fails asynchronously: clearing the
     * near-caches on every retry would evict the entries a local-only node stores
     * while Redis is unavailable.
     */
    private volatile boolean subscribed = false;
    private volatile boolean subscriptionConfirmed = false;
    private volatile boolean closed = false;
    /** True between the loss of a subscription and its first confirmed message. */
    private volatile boolean outage = false;
    /** A clear is owed once the subscription is confirmed again after an outage. */
    private volatile boolean recoveryClearPending = false;
    private volatile long lastHeartbeatAckAt = 0L;
    private volatile ScheduledFuture<?> heartbeatTask;

    @Inject
    ReactiveRedisDataSource reactiveRedis;

    @PostConstruct
    void start() {
        heartbeatTask = scheduler.scheduleAtFixedRate(
                this::heartbeat,
                HEARTBEAT_INTERVAL_MS,
                HEARTBEAT_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    public void register(String region, Consumer<String> listener) {
        listeners.computeIfAbsent(region, key -> new CopyOnWriteArrayList<>()).add(listener);
        ensureSubscribed();
    }

    private void ensureSubscribed() {
        if (closed || subscribed) {
            return;
        }
        synchronized (this) {
            if (closed || subscribed) {
                return;
            }
            subscribed = true;
            subscriptionConfirmed = false;
            long generation = subscriptionGeneration.incrementAndGet();
            lastHeartbeatAckAt = System.currentTimeMillis();
            try {
                reactiveRedis.pubsub(String.class)
                        .subscribe(CHANNEL)
                        .subscribe().with(
                                message -> {
                                    if (generation == subscriptionGeneration.get()) {
                                        dispatch(message);
                                    }
                                },
                                failure -> onSubscriptionEnded(generation, failure),
                                () -> onSubscriptionEnded(generation, null));
                LOG.debugf("Subscribing to Redis pub/sub channel %s", CHANNEL);
                // Ask for an immediate echo; the periodic heartbeat alone would add
                // up to one interval of delay before a reconnect is confirmed.
                heartbeat();
            } catch (Throwable t) {
                onSubscriptionEnded(generation, t);
            }
        }
    }

    private void onSubscriptionEnded(long generation, Throwable failure) {
        if (generation != subscriptionGeneration.get()) {
            // Delayed callback from a subscription attempt that has been superseded.
            return;
        }
        boolean hadAttempt;
        synchronized (this) {
            hadAttempt = subscribed;
            subscribed = false;
            subscriptionConfirmed = false;
        }
        if (closed || !hadAttempt) {
            // Duplicate callback for an attempt that already ended.
            return;
        }
        if (!outage) {
            // First loss of a working connection: this is the only point where the
            // caches from before the gap have to be dropped. Later retries must not
            // clear again, otherwise local-only entries stored while Redis is down
            // would be evicted on every retry.
            outage = true;
            recoveryClearPending = true;
            if (failure != null) {
                LOG.warnf("Redis pub/sub connection lost: %s. Reconnecting in %d ms.",
                        failure.toString(), RECONNECT_DELAY_MS);
            } else {
                LOG.warnf("Redis pub/sub subscription ended. Reconnecting in %d ms.",
                        RECONNECT_DELAY_MS);
            }
            if (CLEAR_ON_RECONNECT) {
                LOG.warn("Redis pub/sub gap detected; clearing local near-caches once to recover "
                        + "invalidation messages missed during the outage.");
                clearAllRegions(RECONNECT_CLEAR);
            }
        } else {
            LOG.debugf("Redis pub/sub reconnect attempt failed: %s",
                    failure == null ? "subscription ended" : failure.toString());
        }
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (closed || !reconnectScheduled.compareAndSet(false, true)) {
            return;
        }
        scheduler.schedule(() -> {
            reconnectScheduled.set(false);
            ensureSubscribed();
        }, RECONNECT_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Round-trip heartbeat: publish a token on the same channel and wait for our
     * own subscription to deliver it back. Redis delivers a published message to
     * the publisher's subscription too, so this proves the subscription is alive.
     */
    private void heartbeat() {
        if (closed || !subscribed) {
            return;
        }
        long generation = subscriptionGeneration.get();
        long now = System.currentTimeMillis();
        if (now - lastHeartbeatAckAt > HEARTBEAT_TIMEOUT_MS) {
            onSubscriptionEnded(generation, new IllegalStateException(
                    "pub/sub heartbeat timed out after " + HEARTBEAT_TIMEOUT_MS + " ms"));
            return;
        }
        String token = HEARTBEAT_REGION + '|' + instanceId() + '|' + System.nanoTime();
        try {
            reactiveRedis.pubsub(String.class)
                    .publish(CHANNEL, token)
                    .subscribe().with(ignored -> {
                    }, failure -> onHeartbeatPublishFailed(generation, failure));
        } catch (Throwable t) {
            onHeartbeatPublishFailed(generation, t);
        }
    }

    private void onHeartbeatPublishFailed(long generation, Throwable failure) {
        if (closed || !subscribed) {
            return;
        }
        onSubscriptionEnded(generation, failure);
    }

    private String instanceId() {
        // Stable per JVM; used only to recognise our own heartbeat echo.
        return HOLDER.instanceId;
    }

    /**
     * Reconnect/gap detected: every listener is told to drop its local timestamp
     * mirror and payloads. The next read goes back to Redis/database.
     */
    private void clearAllRegions(String message) {
        listeners.forEach((region, regionListeners) -> {
            for (Consumer<String> listener : regionListeners) {
                try {
                    listener.accept(message);
                } catch (RuntimeException e) {
                    LOG.warnf(e, "Failed to clear local near-cache for region %s after reconnect", region);
                }
            }
        });
    }

    private void dispatch(String message) {
        if (!subscriptionConfirmed) {
            // A message can only arrive on a live subscription, so this is the
            // authoritative "connection is back" signal.
            subscriptionConfirmed = true;
            LOG.infof("Subscribed to Redis pub/sub channel %s", CHANNEL);
            if (recoveryClearPending) {
                recoveryClearPending = false;
                outage = false;
                if (CLEAR_ON_RECONNECT) {
                    LOG.warn("Redis pub/sub reconnected; clearing local near-caches once more so "
                            + "entries cached during the outage are revalidated.");
                    clearAllRegions(RECONNECT_LIVE_CLEAR);
                }
            }
        }
        int separator = message == null ? -1 : message.indexOf('|');
        if (separator < 0) {
            LOG.debugf("Ignoring malformed pub/sub message: %s", message);
            return;
        }
        String region = message.substring(0, separator);
        String payload = message.substring(separator + 1);
        if (HEARTBEAT_REGION.equals(region)) {
            if (payload.startsWith(instanceId() + '|')) {
                lastHeartbeatAckAt = System.currentTimeMillis();
            }
            return;
        }
        for (Consumer<String> listener : listeners.getOrDefault(region, List.of())) {
            try {
                listener.accept(payload);
            } catch (RuntimeException e) {
                LOG.warnf(e, "Failed to apply pub/sub invalidation for region %s", region);
            }
        }
    }

    @PreDestroy
    void shutdown() {
        closed = true;
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        scheduler.shutdownNow();
    }

    /** Lazy holder so the instance id is created without static-init ordering issues. */
    private static final class HOLDER {
        private static final String instanceId = java.util.UUID.randomUUID().toString();
    }
}
