package com.hybridcache.runtime.hibernate;

import com.hybridcache.runtime.nearcache.TimestampRegistry;

import org.hibernate.cache.spi.support.StorageAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

public class DomainQueryTimestampsRedisStorage implements StorageAccess {

    private final TimestampRegistry remoteKeyTimestampCache;
    private final StorageAccess localDataStorage;

    public DomainQueryTimestampsRedisStorage(TimestampRegistry remoteKeyTimestampCache,
                                             StorageAccess localDataStorage) {
        this.remoteKeyTimestampCache = remoteKeyTimestampCache;
        this.localDataStorage = localDataStorage;
    }

    private String getCacheKey(Object key) {
        return key != null ? key.toString() : "null";
    }

    @Override
    public Object getFromCache(Object key, SharedSessionContractImplementor session) {
        // Query timestamps live in the shared timestamp map. With the near-cache the
        // hot path is local; a pub/sub invalidation removes the entry so the next read
        // refreshes it from Redis. Do not write a sentinel on a miss: that would turn
        // every read into a Redis write plus a pub/sub broadcast.
        Object timeStamp = remoteKeyTimestampCache.getRemote(getCacheKey(key));
        if (!remoteKeyTimestampCache.isRemoteAvailable() || timeStamp == null) {
            return null;
        }
        if (remoteKeyTimestampCache.isLockMarker(timeStamp)) {
            return null;
        }
        return timeStamp;
    }

    @Override
    public void putIntoCache(Object key, Object value, SharedSessionContractImplementor session) {
        // Write table timestamps directly to Redis
        remoteKeyTimestampCache.put(getCacheKey(key), value);
    }

    @Override
    public boolean contains(Object key) {
        return remoteKeyTimestampCache.getRemote(getCacheKey(key)) != null;
    }

    @Override
    public void evictData() {
        remoteKeyTimestampCache.clear();
    }

    @Override
    public void evictData(Object key) {
        remoteKeyTimestampCache.remove(getCacheKey(key));
    }

    @Override
    public void release() {
        localDataStorage.release();
    }
}
