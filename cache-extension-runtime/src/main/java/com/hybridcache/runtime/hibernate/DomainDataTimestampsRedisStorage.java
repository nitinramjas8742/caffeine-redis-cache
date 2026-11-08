package com.hybridcache.runtime.hibernate;

import com.hybridcache.runtime.nearcache.TimestampRegistry;

import org.hibernate.cache.spi.support.StorageAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

public class DomainDataTimestampsRedisStorage extends AbstractTimestampAwareStorage {

    public DomainDataTimestampsRedisStorage(String regionName,
                                            TimestampRegistry remoteKeyTimestampCache,
                                            StorageAccess localDataStorage) {
        super(regionName, remoteKeyTimestampCache, localDataStorage);
    }

    @Override
    public Object getFromCache(Object key, SharedSessionContractImplementor session) {
        DistributedCacheLockWrapper response = validateCacheEntryBasedOnTimeStamp(key, session);
        if (response == null) {
            // Validation passed, fetch from local Caffeine cache
            return getLocalValue(key, session);
        }
        // Validation failed (stale or locked), force database read
        return response.getLock();
    }

    @Override
    public void putIntoCache(Object key, Object value, SharedSessionContractImplementor session) {
        storeWrite(key, value, session);
    }

    @Override
    public void putFromLoad(Object key, Object value, SharedSessionContractImplementor session) {
        // A DB read must not bump the global timestamp; see storeFromLoad.
        storeFromLoad(key, value, session);
    }

    @Override
    public void removeFromCache(Object key, SharedSessionContractImplementor session) {
        removeStored(key, session);
    }
}
