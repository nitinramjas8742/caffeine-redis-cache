package com.hybridcache.runtime.nearcache;

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

import com.hybridcache.runtime.config.HybridCacheConfig;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.pubsub.ReactivePubSubCommands;
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
 *   <li>always clears every local near-cache once when the gap is detected and once
 *       when the subscription is confirmed live again, so updates missed during the
 *       outage are re-read from Redis/database instead of being served stale
 *       indefinitely. The clear is intentionally not repeated on every reconnect
 *       attempt: a node waiting for Redis must not churn its local state on each
 *       failed retry; the definitive clear happens when the subscription is
 *       actually restored.</li>
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

    private final ConcurrentHashMap<String, List<Consumer<String>>> listeners = new ConcurrentHashMap<>();
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
    /**
     * Publish failures are non-fatal (invalidations are an eager-eviction
     * optimization). The first one is reported at WARN, the rest at DEBUG so a
     * broker hiccup cannot flood the application log.
     */
    private final AtomicBoolean publishFailureReported = new AtomicBoolean();
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
     * near-caches on every retry would churn local state while Redis is unavailable.
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

    @Inject
    HybridCacheConfig config;

    /**
     * Lazily created and then reused: {@code reactiveRedis.pubsub(...)} allocates a
     * command facade on every call, and both the heartbeat and every invalidation
     * publish go through here.
     */
    private volatile ReactivePubSubCommands<String> pubsubCommands;

    private long reconnectDelayMs;
    private long heartbeatIntervalMs;
    private long heartbeatTimeoutMs;

    @PostConstruct
    void start() {
        reconnectDelayMs = config.pubsub().reconnectDelayMs();
        heartbeatIntervalMs = config.pubsub().heartbeatIntervalMs();
        heartbeatTimeoutMs = config.pubsub().heartbeatTimeoutMs();
        heartbeatTask = scheduler.scheduleAtFixedRate(
                this::heartbeat,
                heartbeatIntervalMs,
                heartbeatIntervalMs,
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
                pubsubCommands()
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
            // clear again, otherwise local state would be churned on every retry.
            outage = true;
            recoveryClearPending = true;
            if (failure != null) {
                LOG.warnf("Redis pub/sub connection lost: %s. Reconnecting in %d ms.",
                        failure.toString(), reconnectDelayMs);
            } else {
                LOG.warnf("Redis pub/sub subscription ended. Reconnecting in %d ms.",
                        reconnectDelayMs);
            }
            LOG.warn("Redis pub/sub gap detected; clearing local near-caches once to recover "
                    + "invalidation messages missed during the outage.");
            clearAllRegions(RECONNECT_CLEAR);
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
        }, reconnectDelayMs, TimeUnit.MILLISECONDS);
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
        if (now - lastHeartbeatAckAt > heartbeatTimeoutMs) {
            onSubscriptionEnded(generation, new IllegalStateException(
                    "pub/sub heartbeat timed out after " + heartbeatTimeoutMs + " ms"));
            return;
        }
        String token = HEARTBEAT_REGION + '|' + instanceId() + '|' + System.nanoTime();
        try {
            pubsubCommands()
                    .publish(CHANNEL, token)
                    .subscribe().with(ignored -> {
                    }, failure -> onHeartbeatPublishFailed(generation, failure));
        } catch (Throwable t) {
            onHeartbeatPublishFailed(generation, t);
        }
    }

    /**
     * Publishes an invalidation without blocking the caller.
     * <p>
     * Hibernate cache callbacks can be reached from a reactive request thread,
     * where the blocking {@code RedisDataSource} would throw
     * {@code BlockingNotAllowedException}. Nothing waits for the result, so a
     * write never pays a pub/sub round trip.
     * <p>
     * A failed publish is logged but does <b>not</b> mark the data connection as
     * unavailable: a broken/denied PUBLISH must not stop HGET/HSET from working.
     * Correctness still comes from the Redis timestamp comparison; this message is
     * only the eager-eviction optimization.
     */
    public void publish(String message) {
        if (closed) {
            return;
        }
        try {
            pubsubCommands()
                    .publish(CHANNEL, message)
                    .subscribe().with(
                            ignored -> publishFailureReported.set(false),
                            this::onInvalidationPublishFailed);
        } catch (Throwable t) {
            onInvalidationPublishFailed(t);
        }
    }

    private void onInvalidationPublishFailed(Throwable failure) {
        if (publishFailureReported.compareAndSet(false, true)) {
            LOG.warnf("Failed to publish an invalidation on Redis channel %s (%s). "
                            + "Peer near-caches can stay stale until the next invalidation, "
                            + "a reconnect, or the near-cache TTL; further publish failures "
                            + "are logged at DEBUG.",
                    CHANNEL, failure.toString());
        } else {
            LOG.debugf(failure, "Failed to publish an invalidation on Redis channel %s", CHANNEL);
        }
    }

    private ReactivePubSubCommands<String> pubsubCommands() {
        ReactivePubSubCommands<String> commands = pubsubCommands;
        if (commands == null) {
            synchronized (this) {
                commands = pubsubCommands;
                if (commands == null) {
                    commands = reactiveRedis.pubsub(String.class);
                    pubsubCommands = commands;
                }
            }
        }
        return commands;
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
                LOG.warn("Redis pub/sub reconnected; clearing local near-caches once more so "
                        + "entries cached during the outage are revalidated.");
                clearAllRegions(RECONNECT_LIVE_CLEAR);
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
