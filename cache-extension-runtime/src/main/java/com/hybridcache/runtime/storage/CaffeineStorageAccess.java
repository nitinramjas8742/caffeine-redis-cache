package com.hybridcache.runtime.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.hibernate.cache.spi.support.StorageAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

public class CaffeineStorageAccess implements StorageAccess {

    private final Cache<Object, Object> cache;

    public CaffeineStorageAccess() {
        // You can later make this maximumSize configurable via Quarkus properties
        this.cache = Caffeine.newBuilder()
                .maximumSize(10000)
                .build();
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