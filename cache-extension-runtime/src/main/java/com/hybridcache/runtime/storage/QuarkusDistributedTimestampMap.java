package com.hybridcache.runtime.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hybridcache.runtime.config.HybridCacheConfig;
import com.hybridcache.runtime.config.ReconnectionStrategy;
import io.quarkus.arc.Arc;
import io.quarkus.redis.datasource.RedisDataSource;
import org.jboss.logging.Logger;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Near-cache over the global Redis timestamp hash.
 * <p>
 * The behaviour mirrors Redisson's {@code RLocalCachedMap} with
 * {@code LocalCachedMapOptions.defaults()}:
 * <ul>
 *   <li>{@code storeMode = LOCALCACHE_REDIS}: the authoritative value lives in Redis,
 *       a local mirror lives in {@link #localMap};</li>
 *   <li>{@code syncStrategy = INVALIDATE}: writes publish an invalidation and other
 *       nodes drop their local entry;</li>
 *   <li>{@code storeCacheMiss = false}: a Redis miss is never remembered locally;</li>
 *   <li>{@code timeToLive = 0}, {@code maxIdle = 0}, {@code cacheSize = 0}: the local
 *       mirror does not expire or evict on this level;</li>
 *   <li>{@code reconnectionStrategy = NONE} by default (see
 *       {@code quarkus.hybrid-cache.near-cache.reconnection-strategy}).</li>
 * </ul>
 * A repeated read therefore needs no Redis round trip: only the first read of a key,
 * the first read after an invalidation, and writes talk to Redis.
 * <p>
 * This intentionally gives Redisson's eventual consistency: if an invalidation message
 * is lost (pub/sub disabled or disconnected) and the reconnection strategy is
 * {@code NONE}, the near-cache can serve a stale value until the entry is invalidated
 * or the node reconnects with a clearing strategy.
 */
public class QuarkusDistributedTimestampMap {

    private static final Logger LOG = Logger.getLogger(QuarkusDistributedTimestampMap.class);
    private static final String CLEAR_ALL = "CLEAR_ALL_CACHE";
    /** Sender id used when the new subscription is confirmed live; arms the revalidation window. */
    private static final String RECONNECT_LIVE_SENDER = "__reconnect_live__";
    /** Sender id used when a subscription loss is detected; starts the pending-recovery window. */
    private static final String RECONNECT_PENDING_SENDER = "__reconnect__";
    private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

    /**
     * Marker stored in the global registry while a READ_WRITE transaction holds a SoftLock.
     * The full marker is {@code NO_DATA:<owner>:<createdAt>} so that a crashed node cannot
     * leave a permanent lock behind.
     */
    public static final String LOCK_MARKER = "NO_DATA";

    /** Soft locks older than this are treated as abandoned. */
    public static final long LOCK_TTL_MS = 60_000L;

    /**
     * Defaults used by the programmatic (test/back-compat) constructors. Production
     * values come from {@link HybridCacheConfig}.
     */
    private static final long DEFAULT_RETRY_DELAY_MS = 5_000L;
    private static final long DEFAULT_RECOVERY_GRACE_MS = 8_000L;

    private final String regionName;
    private final ReconnectionStrategy reconnectionStrategy;
    private final long nearCacheTtlMs;
    private final long recoveryGraceMs;
    private final long retryDelayMs;
    private final boolean pubsubEnabled;
    private final ConcurrentHashMap<String, Object> localMap;
    /** When each near-cache entry was fetched, used only when {@link #nearCacheTtlMs} > 0. */
    private final ConcurrentHashMap<String, Long> localMapFetchedAt;
    /**
     * Identifies this JVM so that pub/sub events published by this node are not
     * applied twice (Redis delivers a message to the publisher's own subscription too).
     */
    private final String instanceId = UUID.randomUUID().toString();

    // Lazily loaded to avoid Quarkus boot-time CDI crashes with Hibernate
    private volatile RedisDataSource redis;
    private volatile ObjectMapper mapper = DEFAULT_MAPPER;
    private volatile Consumer<String> invalidationListener;
    private volatile boolean subscribed = false;
    private volatile boolean initialized = false;
    private volatile boolean remoteAvailable = true;
    private volatile long retryAt = 0L;
    private volatile boolean redisFailureReported = false;

    /**
     * Until this timestamp local near-cache hits are treated as misses because the
     * pub/sub subscription has just been (re)established and may still lose
     * invalidations. Set when the shared subscriber reports a reconnect.
     */
    private volatile long forceRevalidateUntil = 0L;

    /**
     * True from the moment a pub/sub subscription loss is detected until the new
     * subscription is confirmed live. During this window invalidations are certainly
     * being lost, so even if Redis itself is reachable again, local hits must not be
     * trusted (the recovery interval can be shorter than the reconnect delay).
     */
    private volatile boolean reconnectPending = false;

    /**
     * Creates a map with default settings. Quarkus applications use
     * {@link #QuarkusDistributedTimestampMap(String, HybridCacheConfig)} instead so
     * that {@code quarkus.hybrid-cache.*} is honoured.
     */
    public QuarkusDistributedTimestampMap(String regionName) {
        this(regionName, ReconnectionStrategy.NONE, 0L,
                DEFAULT_RECOVERY_GRACE_MS, true, DEFAULT_RETRY_DELAY_MS);
    }

    /**
     * Creates a map configured from the Quarkus runtime configuration.
     */
    public QuarkusDistributedTimestampMap(String regionName, HybridCacheConfig config) {
        this(regionName,
                config.nearCache().reconnectionStrategy(),
                config.nearCache().ttlMs(),
                config.nearCache().recoveryGraceMs(),
                config.pubsub().enabled(),
                config.redis().retryDelayMs());
    }

    private QuarkusDistributedTimestampMap(String regionName,
                                           ReconnectionStrategy reconnectionStrategy,
                                           long nearCacheTtlMs,
                                           long recoveryGraceMs,
                                           boolean pubsubEnabled,
                                           long retryDelayMs) {
        this.regionName = regionName;
        this.reconnectionStrategy = reconnectionStrategy;
        this.nearCacheTtlMs = Math.max(0L, nearCacheTtlMs);
        this.recoveryGraceMs = Math.max(0L, recoveryGraceMs);
        this.pubsubEnabled = pubsubEnabled;
        this.retryDelayMs = Math.max(1L, retryDelayMs);
        this.localMap = new ConcurrentHashMap<>();
        this.localMapFetchedAt = new ConcurrentHashMap<>();
    }

    QuarkusDistributedTimestampMap(String regionName, String reconnectionStrategy) {
        this(regionName, ReconnectionStrategy.parse(reconnectionStrategy), 0L,
                DEFAULT_RECOVERY_GRACE_MS, true, DEFAULT_RETRY_DELAY_MS);
    }

    QuarkusDistributedTimestampMap(String regionName, String reconnectionStrategy, long nearCacheTtlMs) {
        this(regionName, ReconnectionStrategy.parse(reconnectionStrategy), nearCacheTtlMs,
                DEFAULT_RECOVERY_GRACE_MS, true, DEFAULT_RETRY_DELAY_MS);
    }

    /**
     * Registers the callback invoked when another node invalidates a key.
     * {@code null} is passed when the whole region must be cleared.
     */
    public void setInvalidationListener(Consumer<String> listener) {
        this.invalidationListener = listener;
    }

    protected void init() {
        if (initialized) {
            return;
        }
        synchronized (this) {
            if (initialized) {
                return;
            }
            initialized = true;

            HybridCacheRedisProvider provider = Arc.container()
                    .instance(HybridCacheRedisProvider.class).get();
            if (provider == null) {
                remoteAvailable = false;
                LOG.warnf("Redis clients are not available; region %s will run in local-only mode.", regionName);
                return;
            }
            this.redis = provider.redisDataSource();
            ObjectMapper bean = Arc.container().instance(ObjectMapper.class).get();
            if (bean != null) {
                this.mapper = bean;
            }
            try {
                setupSubscriber();
            } catch (Throwable t) {
                LOG.warnf("Unable to subscribe to Redis pub/sub for region %s; "
                        + "cross-node invalidation is disabled: %s", regionName, t.toString());
            }
        }
    }

    private void setupSubscriber() {
        if (!pubsubEnabled) {
            LOG.infof("Redis pub/sub disabled for region %s; the near-cache will not receive "
                    + "invalidation events.", regionName);
            return;
        }
        if (subscribed) {
            return;
        }
        HybridCachePubSub pubsub = Arc.container().instance(HybridCachePubSub.class).get();
        if (pubsub == null) {
            LOG.warnf("Shared pub/sub subscriber is not available; region %s will not receive "
                    + "invalidation events.", regionName);
            return;
        }
        pubsub.register(regionName, this::onPubSubMessage);
        subscribed = true;
    }

    /**
     * Applies an {@code INVALIDATE} message from another node: drop the local mirror
     * entry so the next read refreshes it from Redis.
     */
    protected void onPubSubMessage(String message) {
        // Payload format: "<senderInstanceId>|<key or CLEAR_ALL_CACHE>"
        int separator = message == null ? -1 : message.indexOf('|');
        if (separator < 0) {
            LOG.debugf("Ignoring malformed pub/sub message in region %s: %s", regionName, message);
            return;
        }
        String sender = message.substring(0, separator);
        if (instanceId.equals(sender)) {
            // Our own invalidation, the local state is already up to date.
            return;
        }
        String key = message.substring(separator + 1);
        if (CLEAR_ALL.equals(key)) {
            localMap.clear();
            localMapFetchedAt.clear();
            if (RECONNECT_PENDING_SENDER.equals(sender)) {
                // From now until the subscription is confirmed, invalidations can be
                // lost. Redis may come back before pub/sub does, so stop trusting
                // local hits immediately.
                reconnectPending = true;
                LOG.infof("Redis pub/sub outage detected for region %s; near-cache hits "
                        + "will revalidate until the subscription is confirmed.", regionName);
            }
            if (RECONNECT_LIVE_SENDER.equals(sender)) {
                reconnectPending = false;
                // A live pub/sub channel proves the Redis host is reachable again,
                // so drop the data-connection cooldown too. Otherwise a node whose
                // last command failed just before the restart would keep writing in
                // local-only mode for up to retryDelayMs without publishing.
                remoteAvailable = true;
                redisFailureReported = false;
                retryAt = 0L;
                // Re-subscribing is asynchronous; until the subscription is proven
                // live, invalidation messages can still be lost. Force the next few
                // seconds of reads to revalidate against Redis.
                forceRevalidateUntil = System.currentTimeMillis() + recoveryGraceMs;
                LOG.infof("Redis pub/sub reconnect for region %s; forcing revalidation of "
                        + "near-cache hits for %d ms.", regionName, recoveryGraceMs);
            }
            LOG.debugf("PubSub: Cleared local near-cache for region %s", regionName);
            notifyInvalidation(null);
        } else {
            localMap.remove(key);
            localMapFetchedAt.remove(key);
            LOG.debugf("PubSub: Invalidated key %s in region %s", key, regionName);
            notifyInvalidation(key);
        }
    }

    private void notifyInvalidation(String key) {
        Consumer<String> listener = invalidationListener;
        if (listener != null) {
            try {
                listener.accept(key);
            } catch (RuntimeException e) {
                LOG.warnf(e, "Failed to evict local cache data for region %s", regionName);
            }
        }
    }

    private void publish(String payload) {
        if (!pubsubEnabled) {
            return;
        }
        publishMessage(regionName + '|' + instanceId + '|' + payload);
    }

    protected void publishMessage(String message) {
        RedisDataSource source = redis;
        if (source == null) {
            return;
        }
        try {
            source.pubsub(String.class).publish(HybridCachePubSub.CHANNEL, message);
        } catch (Exception e) {
            markRemoteFailure("PUBLISH", e);
        }
    }

    /**
     * Reads the global registry with Redisson {@code RLocalCachedMap} semantics:
     * the local mirror is served first and only a local miss reaches Redis. A Redis
     * miss is not cached ({@code storeCacheMiss = false}), so a key that appears
     * later is picked up by the following read.
     */
    public Object getRemote(String key) {
        init();

        // Hot path: near-cache hit, no Redis round trip. If Redis is known to be down,
        // probe after the cooldown so CLEAR/LOAD can be applied even when every read
        // is served locally.
        Object cached = localMap.get(key);
        if (cached != null && (mustRevalidate() || !isNearCacheEntryFresh(key))) {
            // TTL safety net: the connection may have died without a detectable close.
            localMap.remove(key);
            localMapFetchedAt.remove(key);
            cached = null;
        }
        if (cached != null) {
            maybeRecover();
            cached = localMap.get(key);
            if (cached != null && !mustRevalidate() && isNearCacheEntryFresh(key)) {
                return cached;
            }
        }

        if (!canUseRedis()) {
            return null;
        }
        try {
            String jsonValue = readRemote(key);
            // Handle reconnection before caching the value we just read.
            onRedisSuccess();
            if (jsonValue == null) {
                // storeCacheMiss = false: do not remember the miss.
                return null;
            }
            Object parsedValue = parseValue(jsonValue);
            nearCachePut(key, parsedValue);
            return parsedValue;
        } catch (Exception e) {
            markRemoteFailure("GET", e);
            return null;
        }
    }

    /**
     * @return whether Redis was usable for the most recent operation. {@code false} means
     *         callers should fall back to the local tier.
     */
    public boolean isRemoteAvailable() {
        init();
        return redis != null && remoteAvailable;
    }

    /** Builds the lock marker published by this node. */
    public String lockMarker() {
        return LOCK_MARKER + ':' + instanceId + ':' + System.currentTimeMillis();
    }

    public boolean isLockMarker(Object value) {
        return value instanceof String text && text.startsWith(LOCK_MARKER);
    }

    public boolean isOwnLockMarker(Object value) {
        return value instanceof String text && text.startsWith(LOCK_MARKER + ':' + instanceId + ':');
    }

    /**
     * A lock whose node died mid-transaction must not block the entry forever.
     * Markers without the extended format are treated as active (legacy behaviour).
     */
    public boolean isExpiredLock(Object value) {
        if (!(value instanceof String text) || !text.startsWith(LOCK_MARKER)) {
            return false;
        }
        int lastSeparator = text.lastIndexOf(':');
        if (lastSeparator < 0 || lastSeparator == text.length() - 1) {
            return false;
        }
        try {
            long createdAt = Long.parseLong(text.substring(lastSeparator + 1));
            return System.currentTimeMillis() - createdAt > LOCK_TTL_MS;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public void put(String key, Object value) {
        init();
        if (!canUseRedis()) {
            // Local-only mode: keep the mirror coherent so this node can still read it.
            nearCachePut(key, value);
            return;
        }
        try {
            String jsonValue = mapper.writeValueAsString(value);
            writeRemote(key, jsonValue);
            // Apply a reconnect strategy before publishing the new value locally.
            onRedisSuccess();
            nearCachePut(key, value);
            publish(key);
        } catch (Exception e) {
            markRemoteFailure("PUT", e);
            nearCachePut(key, value);
        }
    }

    public Object get(String key) {
        return getRemote(key);
    }

    public void remove(String key) {
        init();
        if (!canUseRedis()) {
            nearCacheRemove(key);
            return;
        }
        try {
            deleteRemote(key);
            onRedisSuccess();
            nearCacheRemove(key);
            publish(key);
        } catch (Exception e) {
            markRemoteFailure("REMOVE", e);
            nearCacheRemove(key);
        }
    }

    public void clear() {
        init();
        if (!canUseRedis()) {
            clearNearCache();
            return;
        }
        try {
            deleteRemoteRegion();
            onRedisSuccess();
            clearNearCache();
            publish(CLEAR_ALL);
        } catch (Exception e) {
            markRemoteFailure("CLEAR", e);
            clearNearCache();
        }
    }

    /**
     * Applies Redisson's {@code ReconnectionStrategy} on the transition from an
     * unavailable Redis back to a usable one.
     * <ul>
     *   <li>{@code NONE}: keep the local mirror (can stay stale after an outage);</li>
     *   <li>{@code CLEAR}: drop the local mirror so entries are re-read from Redis;</li>
     *   <li>{@code LOAD}: like {@code CLEAR} but reloaded lazily on the next read.</li>
     * </ul>
     */
    protected void applyReconnectionStrategy() {
        switch (reconnectionStrategy) {
            case CLEAR, LOAD -> {
                clearNearCache();
                notifyInvalidation(null);
                LOG.infof("Redis reconnected for region %s; local near-cache cleared (%s).",
                        regionName, reconnectionStrategy);
            }
            default -> LOG.debugf("Redis reconnected for region %s; keeping near-cache (NONE).",
                    regionName);
        }
    }

    private void nearCachePut(String key, Object value) {
        localMap.put(key, value);
        localMapFetchedAt.put(key, System.currentTimeMillis());
    }

    private void nearCacheRemove(String key) {
        localMap.remove(key);
        localMapFetchedAt.remove(key);
    }

    private void clearNearCache() {
        localMap.clear();
        localMapFetchedAt.clear();
    }

    /**
     * @return whether this map is currently forcing reads to bypass local state after
     *         a Redis/pub-sub reconnect. Callers that keep a separate payload cache
     *         (the Hibernate storage access) use this to force a database reload
     *         instead of trusting a payload whose timestamp update may still be in
     *         flight on the writer.
     */
    public boolean isForcedRevalidationActive() {
        return mustRevalidate();
    }

    private boolean isNearCacheEntryFresh(String key) {
        if (nearCacheTtlMs <= 0L) {
            return true;
        }
        Long fetchedAt = localMapFetchedAt.get(key);
        return fetchedAt != null && System.currentTimeMillis() - fetchedAt < nearCacheTtlMs;
    }

    /**
     * @return true during the short window after a pub/sub reconnect in which local
     *         hits are not trusted. Only forces a revalidation when Redis is usable;
     *         otherwise a node in local-only mode would turn every read into a failed
     *         network call.
     */
    private boolean mustRevalidate() {
        // While the subscription is lost, invalidation messages are not being
        // delivered at all. Do not trust local hits regardless of the Redis
        // command cooldown: the database is the only safe source until the
        // subscription is confirmed again.
        if (reconnectPending) {
            return true;
        }
        return System.currentTimeMillis() < forceRevalidateUntil && canUseRedis();
    }

    private void onRedisSuccess() {
        if (!remoteAvailable) {
            remoteAvailable = true;
            redisFailureReported = false;
            applyReconnectionStrategy();
            // Redis is usable again. Besides the pub/sub-driven window, arm a local
            // revalidation window so the first reads after a data-connection recovery
            // do not trust near-cache entries cached while invalidations were lost.
            if (recoveryGraceMs > 0L) {
                forceRevalidateUntil = System.currentTimeMillis() + recoveryGraceMs;
            }
        }
    }

    /**
     * Detects the transition from an unavailable Redis back to a usable one on the
     * near-cache hot path. Without this, a node whose reads are all local would never
     * notice the recovery and {@code CLEAR}/{@code LOAD} would never run.
     */
    private void maybeRecover() {
        if (remoteAvailable || redis == null || System.currentTimeMillis() < retryAt) {
            return;
        }
        try {
            // A missing probe key returns null but still proves the connection works.
            readRemote("__hybridcache_reconnect_probe__");
            onRedisSuccess();
        } catch (Exception e) {
            markRemoteFailure("RECONNECT", e);
        }
    }

    /**
     * @return whether a Redis operation may be attempted. After a failure the next
     *         {@link #retryDelayMs} are skipped, then the next operation retries.
     */
    protected boolean canUseRedis() {
        if (redis == null) {
            remoteAvailable = false;
            return false;
        }
        if (!remoteAvailable && System.currentTimeMillis() < retryAt) {
            return false;
        }
        return true;
    }

    protected String readRemote(String key) {
        return redis.hash(String.class, String.class, String.class).hget(regionName, key);
    }

    protected void writeRemote(String key, String jsonValue) {
        redis.hash(String.class, String.class, String.class).hset(regionName, key, jsonValue);
    }

    protected void deleteRemote(String key) {
        redis.hash(String.class, String.class, String.class).hdel(regionName, key);
    }

    protected void deleteRemoteRegion() {
        redis.key().del(regionName);
    }

    /**
     * Reports the first Redis failure at WARN, then keeps subsequent ones at DEBUG so
     * a missing/offline Redis does not flood the log on every cache operation.
     */
    private void markRemoteFailure(String operation, Exception failure) {
        remoteAvailable = false;
        retryAt = System.currentTimeMillis() + retryDelayMs;
        if (!redisFailureReported) {
            redisFailureReported = true;
            LOG.warnf("Redis %s failed for region %s (%s). Continuing in local-only mode; "
                            + "further Redis failures for this region are logged at DEBUG.",
                    operation, regionName, failure.toString());
        } else {
            LOG.debugf(failure, "Redis %s failed for region %s", operation, regionName);
        }
    }

    public boolean containsKey(String key) {
        return get(key) != null;
    }

    private Object parseValue(String json) {
        try {
            // Handle SoftLocks ("NO_DATA") vs Timestamps (Long)
            if (json.startsWith("\"") && json.endsWith("\"")) {
                return mapper.readValue(json, String.class);
            }
            return mapper.readValue(json, Long.class);
        } catch (JsonProcessingException e) {
            return json;
        }
    }
}
