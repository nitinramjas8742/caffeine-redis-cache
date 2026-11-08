package com.hybridcache.runtime.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.hibernate.cache.spi.support.StorageAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

/**
 * Local payload tier backed by Caffeine with Window TinyLFU eviction.
 * <p>
 * The cache is bounded by entry count; use {@code quarkus.hybrid-cache.storage.maximum-size}
 * to size it for the working set and {@code storage.record-stats} to expose hit/miss
 * counters.
 */
public class CaffeineStorageAccess implements StorageAccess {

    /** Kept so payload caches can still be created programmatically (tests, SPI users). */
    public static final long DEFAULT_MAXIMUM_SIZE = 10_000L;

    private final Cache<Object, Object> cache;

    public CaffeineStorageAccess() {
        this(DEFAULT_MAXIMUM_SIZE, false);
    }

    public CaffeineStorageAccess(long maximumSize, boolean recordStats) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .maximumSize(Math.max(1L, maximumSize));
        if (recordStats) {
            builder.recordStats();
        }
        this.cache = builder.build();
    }

    @Override
    public Object getFromCache(Object key, SharedSessionContractImplementor session) {
        return cache.getIfPresent(key);
    }

    @Override
    public void putIntoCache(Object key, Object value, SharedSessionContractImplementor session) {
        cache.put(key, value);
    }

    @Override
    public boolean contains(Object key) {
        return cache.asMap().containsKey(key);
    }

    @Override
    public void evictData() {
        cache.invalidateAll();
    }

    @Override
    public void evictData(Object key) {
        cache.invalidate(key);
    }

    /**
     * Evicts the entry whose key stringifies to {@code key}. Remote invalidation
     * messages only carry the string form of the Hibernate cache key, while the
     * local payload is keyed by the actual key object.
     */
    public void evictDataByStringKey(String key) {
        cache.asMap().keySet().removeIf(candidate -> key.equals(String.valueOf(candidate)));
    }

    @Override
    public void release() {
        cache.invalidateAll();
        cache.cleanUp();
    }
}
