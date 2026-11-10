package com.hybridcache.runtime.hibernate;

import com.hybridcache.runtime.nearcache.TimestampRegistry;

import org.hibernate.cache.spi.access.SoftLock;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Exercises the Redis extension's Hibernate interceptor directly with two "nodes"
 * that share only the global timestamp registry. The registry double always
 * reflects the shared state, which models the state after the near-cache entry
 * has been invalidated or refreshed. The near-cache itself is covered by
 * {@code QuarkusDistributedTimestampMapNearCacheTest}.
 */
class TimestampValidationTest {

    private static final String LOCK_MARKER = "NO_DATA";

    /**
     * In-memory stand-in for the distributed registry. Two instances share one map,
     * which is exactly what two pods would see through Redis. The test therefore
     * exercises the Redis extension's Hibernate layer only.
     */
    static class SharedTimestampMap implements TimestampRegistry {

        private final Map<String, Object> shared;
        private final String instanceId = java.util.UUID.randomUUID().toString();
        private Consumer<String> listener;
        private boolean available = true;

        SharedTimestampMap(String regionName, Map<String, Object> shared) {
            this.shared = shared;
        }

        @Override
        public void setInvalidationListener(Consumer<String> listener) {
            this.listener = listener;
        }

        /** Simulates the recovery broadcast that invalidates every local payload. */
        void invalidateAll() {
            if (listener != null) {
                listener.accept(null);
            }
        }

        @Override
        public Object getRemote(String key) {
            return available ? shared.get(key) : null;
        }

        @Override
        public boolean isRemoteAvailable() {
            return available;
        }

        @Override
        public boolean isForcedRevalidationActive() {
            return false;
        }

        @Override
        public String lockMarker() {
            return LOCK_MARKER + ':' + instanceId + ':' + System.currentTimeMillis();
        }

        @Override
        public boolean isLockMarker(Object value) {
            return value instanceof String text && text.startsWith(LOCK_MARKER);
        }

        @Override
        public boolean isOwnLockMarker(Object value) {
            return value instanceof String text
                    && text.startsWith(LOCK_MARKER + ':' + instanceId + ':');
        }

        @Override
        public boolean isExpiredLock(Object value) {
            return false;
        }

        @Override
        public void put(String key, Object value) {
            shared.put(key, value);
        }

        @Override
        public void remove(String key) {
            shared.remove(key);
        }

        @Override
        public void clear() {
            shared.clear();
        }
    }

    private static final Object KEY = new Object() {
        @Override
        public String toString() {
            return "Customer#1";
        }
    };

    private static final Object OTHER_KEY = new Object() {
        @Override
        public String toString() {
            return "Customer#2";
        }
    };

    private final Map<String, Object> sharedRedis = new ConcurrentHashMap<>();
    private final SharedTimestampMap nodeARegistry = new SharedTimestampMap("Customer", sharedRedis);
    private final SharedTimestampMap nodeBRegistry = new SharedTimestampMap("Customer", sharedRedis);
    private final DomainDataTimestampsRedisStorage nodeA =
            new DomainDataTimestampsRedisStorage("Customer", nodeARegistry, new CaffeineStorageAccess());
    private final DomainDataTimestampsRedisStorage nodeB =
            new DomainDataTimestampsRedisStorage("Customer", nodeBRegistry, new CaffeineStorageAccess());

    @Test
    void cacheFillAdoptsGlobalTimestampInsteadOfBumpingIt() {
        nodeA.putFromLoad(KEY, "Ada", null);
        Object timestampAfterA = sharedRedis.get("Customer#1");
        assertNotNull(timestampAfterA);
        assertEquals("Ada", nodeA.getFromCache(KEY, null));

        // Node B has no payload yet: it must miss and then adopt A's timestamp.
        assertNull(nodeB.getFromCache(KEY, null));
        nodeB.putFromLoad(KEY, "Ada", null);
        assertEquals("Ada", nodeB.getFromCache(KEY, null));

        // B's fill must not invalidate A (no cross-node churn).
        assertEquals(timestampAfterA, sharedRedis.get("Customer#1"));
        assertEquals("Ada", nodeA.getFromCache(KEY, null));
    }

    @Test
    void newerGlobalTimestampInvalidatesLocalPayload() throws InterruptedException {
        nodeA.putFromLoad(KEY, "Ada", null);
        assertEquals("Ada", nodeA.getFromCache(KEY, null));

        // Simulate a READ_WRITE commit on node B: acquire, store the committed value,
        // release. The successful Hibernate path does not call unlockItem, but this
        // exercises both the value write and the release branch.
        Thread.sleep(5);
        TestSoftLock lock = new TestSoftLock();
        nodeB.putIntoCache(KEY, lock, null);
        nodeB.putIntoCache(KEY, "Ada Updated", null);
        lock.release();
        nodeB.putIntoCache(KEY, lock, null);

        // The registry double reflects the shared state, as it would after an
        // invalidation message (or after the near-cache entry was refreshed).
        assertNull(nodeA.getFromCache(KEY, null));
        assertEquals("Ada Updated", nodeB.getFromCache(KEY, null));

        nodeA.putFromLoad(KEY, "Ada Updated", null);
        assertEquals("Ada Updated", nodeA.getFromCache(KEY, null));
    }

    @Test
    void removedGlobalEntryInvalidatesLocalPayload() {
        nodeA.putFromLoad(KEY, "Ada", null);
        assertEquals("Ada", nodeA.getFromCache(KEY, null));

        nodeB.removeFromCache(KEY, null);

        assertNull(sharedRedis.get("Customer#1"));
        assertNull(nodeA.getFromCache(KEY, null));
    }

    @Test
    void softLockMarkerForcesMiss() {
        nodeA.putFromLoad(KEY, "Ada", null);
        assertEquals("Ada", nodeA.getFromCache(KEY, null));

        sharedRedis.put("Customer#1", LOCK_MARKER + ":dead-node:" + System.currentTimeMillis());

        assertNull(nodeA.getFromCache(KEY, null));
    }

    @Test
    void unavailableRegistryForcesDatabaseRead() {
        nodeA.putFromLoad(KEY, "Ada", null);
        assertEquals("Ada", nodeA.getFromCache(KEY, null));

        nodeARegistry.available = false;

        // Redis is down: the timestamp cannot prove the payload current, so the
        // cache must miss and Hibernate reads the database.
        assertNull(nodeA.getFromCache(KEY, null));

        // The unverifiable payload is evicted, not kept for when Redis returns.
        nodeARegistry.available = true;
        assertNull(nodeA.getFromCache(KEY, null));
    }

    @Test
    void cacheFillIsSkippedWhileRegistryUnavailable() {
        nodeARegistry.available = false;

        nodeA.putFromLoad(KEY, "Ada", null);

        // Nothing was cached while the registry was down.
        assertNull(nodeA.getFromCache(KEY, null));

        nodeARegistry.available = true;
        assertNull(nodeA.getFromCache(KEY, null));
    }

    @Test
    void softLockBlocksOtherNodesAndReleasesCleanly() {
        TestSoftLock lock = new TestSoftLock();

        // READ_WRITE acquire: Hibernate stores the SoftLock through putIntoCache.
        nodeA.putIntoCache(KEY, lock, null);

        // The owner still sees its own lock; the other node must not cache anything.
        assertNotNull(nodeA.getFromCache(KEY, null));
        assertNull(nodeB.getFromCache(KEY, null));
        nodeB.putFromLoad(KEY, "mid-transaction", null);
        assertNull(nodeB.getFromCache(KEY, null));

        // Value written while locked, then the lock is released.
        nodeA.putIntoCache(KEY, "committed", null);
        lock.release();
        nodeA.putIntoCache(KEY, lock, null);

        // The other node misses once, refills and sees the committed value.
        assertNull(nodeB.getFromCache(KEY, null));
        nodeB.putFromLoad(KEY, "committed", null);
        assertEquals("committed", nodeB.getFromCache(KEY, null));
        assertEquals("committed", nodeA.getFromCache(KEY, null));
    }

    @Test
    void recoveryInvalidationPreservesOwnedSoftLocksAndEvictsTheRest() {
        nodeA.putFromLoad(KEY, "Ada", null);
        nodeA.putFromLoad(OTHER_KEY, "Grace", null);
        TestSoftLock lock = new TestSoftLock();
        nodeA.putIntoCache(KEY, lock, null);

        // A reconnect/recovery broadcast evicts every local payload except the
        // transactions this node is currently mutating. The node must still remember
        // that it owns this SoftLock, otherwise the committed value below would be
        // treated as a peer write and dropped.
        nodeARegistry.invalidateAll();

        assertNull(nodeA.getFromCache(OTHER_KEY, null));
        assertNotNull(nodeA.getFromCache(KEY, null),
                "The in-flight transaction's lock payload must survive the recovery clear");

        nodeA.putIntoCache(KEY, "committed", null);

        assertEquals("committed", nodeA.getFromCache(KEY, null));
        assertNotNull(sharedRedis.get("Customer#1"));
    }

    @Test
    void rolledBackSoftLockDoesNotLeaveAPermanentMarker() {
        TestSoftLock lock = new TestSoftLock();
        nodeA.putIntoCache(KEY, lock, null);

        // Rollback: released without any value being written.
        lock.release();
        nodeA.putIntoCache(KEY, lock, null);

        assertNull(sharedRedis.get("Customer#1"));
        assertNull(nodeB.getFromCache(KEY, null));
    }

    /**
     * Mirrors the state Hibernate's {@code SoftLockImpl} exposes through its private
     * {@code unlockTimestamp} field. Zero means the lock is still held.
     */
    static class TestSoftLock implements SoftLock {
        private long unlockTimestamp;

        void release() {
            unlockTimestamp = System.currentTimeMillis();
        }
    }
}
