package com.hybridcache.runtime.nearcache;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies that {@link QuarkusDistributedTimestampMap} behaves like Redisson's
 * {@code RLocalCachedMap} with {@code LocalCachedMapOptions.defaults()}:
 * local mirror on the hot path, Redis only on a local miss, no negative caching,
 * pub/sub invalidation, and always-clear recovery.
 *
 * <p>A simulated Redis backend is used so the semantics can be tested without a
 * running server; the production code paths (local mirror, invalidation, strategy)
 * are the real ones.</p>
 */
class QuarkusDistributedTimestampMapNearCacheTest {

    /**
     * Replaces only the network boundary with a map. {@code canUseRedis()} always
     * returns {@code true} so an unavailable backend throws from the read/write
     * methods, exactly like a failed Redis call, and the base class can mark the
     * connection as down and later detect the recovery.
     */
    static final class SimulatedMap extends QuarkusDistributedTimestampMap {

        private final Map<String, String> redisStore;
        boolean available = true;
        int reads = 0;

        SimulatedMap(String region, Map<String, String> redisStore) {
            super(region);
            this.redisStore = redisStore;
        }

        SimulatedMap(String region, Map<String, String> redisStore, long nearCacheTtlMs) {
            super(region, nearCacheTtlMs);
            this.redisStore = redisStore;
        }

        @Override
        protected void init() {
            // No Quarkus/Arc needed; the Redis boundary is simulated below.
        }

        @Override
        protected boolean canUseRedis() {
            return true;
        }

        @Override
        public boolean isRemoteAvailable() {
            return available;
        }

        @Override
        protected String readRemote(String key) {
            reads++;
            requireAvailable();
            return redisStore.get(key);
        }

        @Override
        protected void writeRemote(String key, String jsonValue) {
            requireAvailable();
            redisStore.put(key, jsonValue);
        }

        @Override
        protected void deleteRemote(String key) {
            requireAvailable();
            redisStore.remove(key);
        }

        @Override
        protected void deleteRemoteRegion() {
            requireAvailable();
            redisStore.clear();
        }

        @Override
        protected void publishMessage(String message) {
            // No peers in this test; invalidation is delivered explicitly.
        }

        private void requireAvailable() {
            if (!available) {
                throw new IllegalStateException("Redis unavailable");
            }
        }
    }

    private final Map<String, String> redis = new ConcurrentHashMap<>();

    private SimulatedMap map() {
        return new SimulatedMap("test.region", redis);
    }

    @Test
    void repeatedReadsUseTheLocalMirrorWithoutRedis() {
        redis.put("key", "100");
        SimulatedMap map = map();

        assertEquals(100L, map.getRemote("key"));
        assertEquals(1, map.reads);

        // Simulate another node changing Redis without an invalidation message.
        redis.put("key", "200");

        // Redisson's near-cache serves its local copy; this is the stale-read window.
        assertEquals(100L, map.getRemote("key"));
        assertEquals(1, map.reads, "Repeated read must not call Redis");
    }

    @Test
    void invalidationForcesTheNextReadToRefreshFromRedis() {
        redis.put("key", "100");
        SimulatedMap map = map();
        assertEquals(100L, map.getRemote("key"));

        redis.put("key", "200");
        // Delivered by HybridCachePubSub after another node's write.
        map.onPubSubMessage("other-node|key");

        assertEquals(200L, map.getRemote("key"));
        assertEquals(2, map.reads);
    }

    @Test
    void clearInvalidationDropsEveryLocalEntry() {
        redis.put("a", "1");
        redis.put("b", "2");
        SimulatedMap map = map();
        assertEquals(1L, map.getRemote("a"));
        assertEquals(2L, map.getRemote("b"));

        redis.put("a", "10");
        redis.put("b", "20");
        map.onPubSubMessage("other-node|CLEAR_ALL_CACHE");

        assertEquals(10L, map.getRemote("a"));
        assertEquals(20L, map.getRemote("b"));
        assertEquals(4, map.reads);
    }

    @Test
    void missesAreNotCached() {
        SimulatedMap map = map();

        assertNull(map.getRemote("key"));
        assertEquals(1, map.reads);

        // storeCacheMiss = false: a value that appears later must be picked up.
        redis.put("key", "300");
        assertEquals(300L, map.getRemote("key"));
        assertEquals(2, map.reads);
    }

    @Test
    void putWritesThroughAndUpdatesTheLocalMirror() {
        SimulatedMap map = map();

        map.put("key", 42L);
        assertEquals("42", redis.get("key"), "put must write through to Redis");

        // Change Redis behind the map's back; the local mirror still wins.
        redis.put("key", "99");
        assertEquals(42L, map.getRemote("key"));
        assertEquals(0, map.reads);
    }

    @Test
    void removeDeletesRemoteAndLocalEntries() {
        redis.put("key", "1");
        SimulatedMap map = map();
        assertEquals(1L, map.getRemote("key"));

        map.remove("key");

        assertFalse(redis.containsKey("key"));
        assertNull(map.getRemote("key"));
        assertEquals(2, map.reads);
    }

    @Test
    void reconnectAlwaysClearsTheLocalMirror() {
        redis.put("key", "100");
        SimulatedMap map = map();
        assertEquals(100L, map.getRemote("key"));

        map.available = false;
        // A read against the unavailable backend marks the connection as down.
        assertNull(map.getRemote("missing"));

        // The value changes during the outage: another node wrote the database, but
        // its timestamp never reached Redis while the data connection was down.
        redis.put("key", "200");
        map.available = true;

        // The first successful call detects the recovery and clears all local state.
        assertNull(map.getRemote("trigger"));

        assertEquals(200L, map.getRemote("key"),
                "Recovery must drop entries cached before the outage");
    }

    @Test
    void ttlBoundStalenessEvenWithoutInvalidation() throws InterruptedException {
        redis.put("key", "100");
        SimulatedMap map = new SimulatedMap("test.region.ttl", redis, 40L);

        assertEquals(100L, map.getRemote("key"));
        assertEquals(1, map.reads);

        // Within the TTL the near-cache still answers locally.
        redis.put("key", "200");
        assertEquals(100L, map.getRemote("key"));
        assertEquals(1, map.reads);

        // After the TTL the entry is treated as a miss and re-read from Redis.
        Thread.sleep(80);
        assertEquals(200L, map.getRemote("key"));
        assertEquals(2, map.reads);
    }
}
