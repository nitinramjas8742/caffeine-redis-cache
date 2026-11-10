package com.hybridcache.runtime.hibernate;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.hibernate.cache.spi.support.StorageAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local payload tier backed by Caffeine with Window TinyLFU eviction.
 * <p>
 * The cache is bounded by entry count; use
 * {@code quarkus.hybrid-cache.storage.maximum-size} to size it for the working
 * set and {@code storage.record-stats} to expose hit/miss counters.
 */
public class CaffeineStorageAccess implements StorageAccess {

    /** Kept so payload caches can still be created programmatically (tests, SPI users). */
    public static final long DEFAULT_MAXIMUM_SIZE = 10_000L;

    private final Cache<Object, Object> cache;

    /**
     * Maps the string form carried by Redis pub/sub ({@code key.toString()}) back
     * to the actual Hibernate cache key. Without it, every invalidation would scan
     * the whole payload cache to find the key to evict.
     */
    private final ConcurrentHashMap<String, Object> keyIndex = new ConcurrentHashMap<>();

    public CaffeineStorageAccess() {
        this(DEFAULT_MAXIMUM_SIZE, false);
    }

    public CaffeineStorageAccess(long maximumSize, boolean recordStats) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(Math.max(1L, maximumSize))
                // Run maintenance on the evicting thread: the key index must be
                // consistent by the time the cache operation returns.
                .executor(Runnable::run);
        if (recordStats) {
            builder.recordStats();
        }
        // Keep the index bounded too: drop entries Caffeine evicts by size.
        builder.removalListener((key, value, cause) -> {
            if (key != null) {
                keyIndex.remove(String.valueOf(key), key);
            }
        });
        this.cache = builder.build();
    }

    @Override
    public Object getFromCache(Object key, SharedSessionContractImplementor session) {
        return cache.getIfPresent(key);
    }

    @Override
    public void putIntoCache(Object key, Object value, SharedSessionContractImplementor session) {
        cache.put(key, value);
        keyIndex.put(String.valueOf(key), key);
    }

    @Override
    public boolean contains(Object key) {
        return cache.getIfPresent(key) != null;
    }

    @Override
    public void evictData() {
        cache.invalidateAll();
        keyIndex.clear();
    }

    @Override
    public void evictData(Object key) {
        if (key != null) {
            keyIndex.remove(String.valueOf(key), key);
        }
        cache.invalidate(key);
    }

    /**
     * Clears every payload except the keys whose string form is in
     * {@code preserveStringKeys}. A recovery broadcast must not destroy the pending
     * value of a READ_WRITE transaction this node currently owns.
     */
    public void evictDataExcept(Set<String> preserveStringKeys) {
        if (preserveStringKeys.isEmpty()) {
            evictData();
            return;
        }
        for (Object key : cache.asMap().keySet()) {
            String stringKey = String.valueOf(key);
            if (!preserveStringKeys.contains(stringKey)) {
                cache.invalidate(key);
                keyIndex.remove(stringKey, key);
            }
        }
    }

    /**
     * Evicts the payload by the string form carried over the invalidation channel,
     * in O(1), instead of scanning the whole Caffeine key set.
     */
    public void evictDataByStringKey(String key) {
        Object actualKey = keyIndex.remove(key);
        if (actualKey != null) {
            cache.invalidate(actualKey);
        }
    }

    @Override
    public void release() {
        cache.invalidateAll();
        cache.cleanUp();
        keyIndex.clear();
    }
}
