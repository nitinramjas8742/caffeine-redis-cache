package com.hybridcache.runtime.hibernate;

import com.hybridcache.runtime.nearcache.TimestampRegistry;

import org.hibernate.cache.spi.access.SoftLock;
import org.hibernate.cache.spi.support.DomainDataStorageAccess;
import org.hibernate.cache.spi.support.StorageAccess;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.jboss.logging.Logger;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractTimestampAwareStorage implements DomainDataStorageAccess {

    private static final Logger LOG = Logger.getLogger(AbstractTimestampAwareStorage.class);

    protected final String regionName;
    protected final TimestampRegistry remoteKeyTimestampCache;
    protected final StorageAccess localDataStorage;

    /**
     * Local timestamp registry: when this node cached the payload for a key.
     * It is compared against the near-cached global timestamp. With the Redisson-style
     * near-cache the comparison is a local operation; pub/sub invalidation removes the
     * global entry locally so the next read goes back to Redis.
     */
    private final ConcurrentHashMap<String, Long> localTimestamps = new ConcurrentHashMap<>();

    /**
     * Keys for which this node currently holds a READ_WRITE SoftLock. Hibernate calls
     * {@code putIntoCache} twice (acquire, then release), so this distinguishes the two.
     */
    private final Set<String> locallyLockedKeys = ConcurrentHashMap.newKeySet();

    public AbstractTimestampAwareStorage(String regionName,
                                         TimestampRegistry remoteKeyTimestampCache,
                                         StorageAccess localDataStorage) {
        this.regionName = regionName;
        this.remoteKeyTimestampCache = remoteKeyTimestampCache;
        this.localDataStorage = localDataStorage;
        // When another node invalidates a key, drop the corresponding local payload.
        remoteKeyTimestampCache.setInvalidationListener(this::onRemoteInvalidation);
    }

    /**
     * Invoked from the Redis pub/sub subscriber when another node invalidated an entry.
     * {@code null} means the whole region was cleared.
     */
    private void onRemoteInvalidation(String key) {
        if (key == null) {
            // Deliberately keep the payload and SoftLock ownership of keys this node
            // is mutating: a recovery broadcast can arrive while this node is inside a
            // READ_WRITE transaction. Evicting the pending value here would lose the
            // write, and forgetting the ownership would make the next validation treat
            // this node's own distributed marker as a peer's lock.
            if (localDataStorage instanceof CaffeineStorageAccess caffeineStorage) {
                caffeineStorage.evictDataExcept(locallyLockedKeys);
            } else {
                localDataStorage.evictData();
            }
            localTimestamps.keySet().retainAll(locallyLockedKeys);
            return;
        }
        localTimestamps.remove(key);
        locallyLockedKeys.remove(key);
        // Pub/sub messages only carry the string form of the Hibernate cache key,
        // while the local payload is keyed by the actual CacheKey object.
        if (localDataStorage instanceof CaffeineStorageAccess caffeineStorage) {
            caffeineStorage.evictDataByStringKey(key);
        } else {
            localDataStorage.evictData(key);
        }
    }

    protected String getCacheKey(Object key) {
        // Simple stringification; can be expanded based on your custom CacheKey structure
        return key != null ? key.toString() : "null";
    }

    protected void updateTimeStampInAll(Object key, Object value) {
        String cacheKey = getCacheKey(key);
        long timestamp = System.currentTimeMillis();
        localTimestamps.put(cacheKey, timestamp);
        remoteKeyTimestampCache.put(cacheKey, timestamp);
        LOG.debugf("Updated timestamp for %s in %s to %d", key, regionName, timestamp);
    }

    protected void removeTimeStampFromAll(Object key) {
        String cacheKey = getCacheKey(key);
        localTimestamps.remove(cacheKey);
        locallyLockedKeys.remove(cacheKey);
        remoteKeyTimestampCache.remove(cacheKey);
    }

    protected DistributedCacheLockWrapper validateCacheEntryBasedOnTimeStamp(Object key, SharedSessionContractImplementor session) {
        String cacheKey = getCacheKey(key);

        // Near-cache read: served from the local mirror on the hot path, otherwise
        // HGET from Redis. Pub/sub removes the mirror entry on another node's write,
        // so the read after an invalidation refreshes the timestamp from Redis.
        Object remoteTimeStamp = remoteKeyTimestampCache.getRemote(cacheKey);

        if (remoteKeyTimestampCache.isForcedRevalidationActive()) {
            // Recovery window after a Redis pub/sub outage: invalidation messages are
            // lost or a peer's just-committed write may not have published its new
            // timestamp yet, so local state cannot be trusted. Reload from the
            // database even if Redis is currently unreachable.
            evictLocal(cacheKey, key);
            return new DistributedCacheLockWrapper(null);
        }

        if (!remoteKeyTimestampCache.isRemoteAvailable()) {
            // The timestamp registry is the only authority that can prove a payload is
            // current. Without it, fail closed: evict the local payload and let
            // Hibernate read the database.
            evictLocal(cacheKey, key);
            return new DistributedCacheLockWrapper(null);
        }


        if (remoteTimeStamp == null) {
            // Never cached globally, or removed by another node. Fail closed.
            locallyLockedKeys.remove(cacheKey);
            evictLocal(cacheKey, key);
            return new DistributedCacheLockWrapper(null);
        }

        if (remoteKeyTimestampCache.isLockMarker(remoteTimeStamp)) {
            if (remoteKeyTimestampCache.isExpiredLock(remoteTimeStamp)) {
                // The locking node died mid-transaction; do not block the entry forever.
                remoteKeyTimestampCache.remove(cacheKey);
                locallyLockedKeys.remove(cacheKey);
                evictLocal(cacheKey, key);
                return new DistributedCacheLockWrapper(null);
            }
            if (locallyLockedKeys.contains(cacheKey)) {
                // This node owns the lock; let Hibernate see its own SoftLock or pending value.
                return null;
            }
            // A SoftLock is present. Actively clear the local payload to prevent dirty reads.
            evictLocal(cacheKey, key);
            return new DistributedCacheLockWrapper(null);
        }

        if (remoteTimeStamp instanceof Long || remoteTimeStamp instanceof Integer) {
            long timeFromRemote = ((Number) remoteTimeStamp).longValue();
            Long timeFromLocal = localTimestamps.get(cacheKey);

            // The global timestamp is newer than the payload this pod holds.
            if (timeFromLocal == null || timeFromRemote > timeFromLocal) {
                evictLocal(cacheKey, key);
                return new DistributedCacheLockWrapper(null);
            }
        }
        return null; // Valid
    }

    private void evictLocal(String cacheKey, Object key) {
        localDataStorage.evictData(key);
        localTimestamps.remove(cacheKey);
    }

    /**
     * Reads from the local payload tier, unwrapping a stored SoftLock wrapper so Hibernate
     * receives the actual {@link SoftLock} object it expects.
     */
    protected Object getLocalValue(Object key, SharedSessionContractImplementor session) {
        Object local = localDataStorage.getFromCache(key, session);
        if (local instanceof DistributedCacheLockWrapper wrapper) {
            return wrapper.getLock();
        }
        return local;
    }

    /**
     * Cache fill after a database read. It adopts an existing global timestamp instead of
     * generating a new one; generating a new timestamp on every fill would make nodes
     * invalidate each other in a loop for data that never changed.
     */
    protected void storeFromLoad(Object key, Object value, SharedSessionContractImplementor session) {
        storeFromLoad(key, value, session, remoteKeyTimestampCache.getRemote(getCacheKey(key)));
    }

    private void storeFromLoad(Object key, Object value,
                               SharedSessionContractImplementor session, Object remoteTimeStamp) {
        String cacheKey = getCacheKey(key);

        if (!remoteKeyTimestampCache.isRemoteAvailable()) {
            // Reads fail closed while the registry is unavailable, so caching this
            // payload would only leave unverifiable state behind for the recovery
            // clear to remove. Do not populate the local tier.
            evictLocal(cacheKey, key);
            return;
        }

        if (remoteKeyTimestampCache.isLockMarker(remoteTimeStamp)) {
            // A lock (from any node) is present; do not cache the value we just read.
            evictLocal(cacheKey, key);
            return;
        }
        if (remoteTimeStamp instanceof Number) {
            localDataStorage.putIntoCache(key, value, session);
            localTimestamps.put(cacheKey, ((Number) remoteTimeStamp).longValue());
            return;
        }

        // The key has no timestamp yet; mint one so the payload can be validated
        // by this and other nodes.
        localDataStorage.putIntoCache(key, value, session);
        updateTimeStampInAll(key, value);
    }

    /**
     * Stores a value coming through {@code putIntoCache}. Hibernate uses this for
     * READ_WRITE/READ_ONLY cache fills as well as for committed writes, so the only way
     * to tell them apart is the local SoftLock state:
     * <ul>
     *   <li>a value while this node holds the lock = committed write: publish a new timestamp;</li>
     *   <li>a value without a lock = cache fill: adopt the existing timestamp.</li>
     * </ul>
     * Without that distinction, READ_WRITE nodes would invalidate each other on every fill.
     */
    protected void storeWrite(Object key, Object value, SharedSessionContractImplementor session) {
        String cacheKey = getCacheKey(key);

        if (value instanceof SoftLock || value instanceof DistributedCacheLockWrapper) {
            Object lock = value instanceof DistributedCacheLockWrapper wrapper ? wrapper.getLock() : value;

            if (isReleased(lock)) {
                // Hibernate stores the SoftLock again after releasing it (rollback or
                // unlockItem). Clear the global marker and publish a pending value if one
                // was written before the release.
                locallyLockedKeys.remove(cacheKey);
                Long pendingTimestamp = localTimestamps.get(cacheKey);
                if (pendingTimestamp != null) {
                    remoteKeyTimestampCache.put(cacheKey, pendingTimestamp);
                } else {
                    localDataStorage.evictData(key);
                    localTimestamps.remove(cacheKey);
                    remoteKeyTimestampCache.remove(cacheKey);
                }
                return;
            }

            // Active lock (first acquisition or additional multiplicity on this node).
            locallyLockedKeys.add(cacheKey);
            localDataStorage.putIntoCache(key, new DistributedCacheLockWrapper(lock), session);
            localTimestamps.remove(cacheKey);
            remoteKeyTimestampCache.put(cacheKey, remoteKeyTimestampCache.lockMarker());
            return;
        }

        if (locallyLockedKeys.remove(cacheKey)) {
            // A committed value arrived while this node held the lock: publish it now.
            if (remoteKeyTimestampCache.isRemoteAvailable()) {
                localDataStorage.putIntoCache(key, value, session);
                updateTimeStampInAll(key, value);
            } else {
                // The new timestamp cannot be published, so the payload is not
                // verifiable anywhere. Leave the local tier empty.
                localDataStorage.evictData(key);
                localTimestamps.remove(cacheKey);
            }
            return;
        }

        Object remoteTimeStamp = remoteKeyTimestampCache.getRemote(cacheKey);
        if (remoteKeyTimestampCache.isRemoteAvailable()
                && remoteKeyTimestampCache.isLockMarker(remoteTimeStamp)
                && !remoteKeyTimestampCache.isOwnLockMarker(remoteTimeStamp)) {
            // Another node is mid-transaction: never cache this value.
            evictLocal(cacheKey, key);
            return;
        }

        // READ_WRITE and READ_ONLY fills also arrive here; adopt, do not bump.
        storeFromLoad(key, value, session, remoteTimeStamp);
    }

    /**
     * Hibernate's SoftLockImpl exposes the release state only through a private field:
     * {@code unlockTimestamp == 0} means the lock is still held. If the field cannot be
     * read (different Hibernate version), the lock is treated as active and the TTL
     * marker eventually clears it.
     */
    private static boolean isReleased(Object lock) {
        if (lock == null) {
            return false;
        }
        try {
            java.lang.reflect.Field field = lock.getClass().getDeclaredField("unlockTimestamp");
            field.setAccessible(true);
            return field.getLong(lock) > 0;
        } catch (ReflectiveOperationException | RuntimeException e) {
            return false;
        }
    }

    protected void removeStored(Object key, SharedSessionContractImplementor session) {
        localDataStorage.evictData(key);
        removeTimeStampFromAll(key);
    }

    @Override
    public boolean contains(Object key) {
        return localDataStorage.contains(key);
    }

    @Override
    public void evictData() {
        localDataStorage.evictData();
        localTimestamps.clear();
        locallyLockedKeys.clear();
        remoteKeyTimestampCache.clear();
    }

    @Override
    public void evictData(Object key) {
        localDataStorage.evictData(key);
        removeTimeStampFromAll(key);
    }

    @Override
    public void release() {
        localDataStorage.release();
    }
}
