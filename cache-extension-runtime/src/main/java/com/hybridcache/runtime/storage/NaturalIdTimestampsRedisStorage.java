package com.hybridcache.runtime.storage;

import org.hibernate.cache.spi.support.StorageAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

public class NaturalIdTimestampsRedisStorage extends AbstractTimestampAwareStorage {

    public NaturalIdTimestampsRedisStorage(String regionName,
                                           QuarkusDistributedTimestampMap remoteKeyTimestampCache,
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
        storeFromLoad(key, value, session);
    }

    @Override
    public void removeFromCache(Object key, SharedSessionContractImplementor session) {
        removeStored(key, session);
    }
}
