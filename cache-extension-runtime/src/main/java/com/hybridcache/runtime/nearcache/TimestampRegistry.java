package com.hybridcache.runtime.nearcache;

import java.util.function.Consumer;

/**
 * Contract for the distributed timestamp registry consumed by the Hibernate
 * integration.
 *
 * <p>Keeping this interface in the {@code nearcache} package means the Hibernate
 * layer depends only on timestamp semantics, not on the near-cache implementation.
 * The whole {@code nearcache} package is Hibernate-free and can be extracted into
 * a standalone extension later without touching the cache provider.</p>
 */
public interface TimestampRegistry {

    /**
     * Reads the current timestamp for {@code key}, serving a local mirror when possible.
     */
    Object getRemote(String key);

    /**
     * Writes a timestamp (or lock marker) for {@code key}.
     */
    void put(String key, Object value);

    /**
     * Removes the timestamp for {@code key}.
     */
    void remove(String key);

    /**
     * Clears the whole region.
     */
    void clear();

    /**
     * @return whether Redis is currently usable for this region.
     */
    boolean isRemoteAvailable();

    /**
     * @return whether local hits must be revalidated after a reconnect.
     */
    boolean isForcedRevalidationActive();

    /**
     * Registers the callback invoked when another node invalidates a key; {@code null}
     * means the whole region was cleared.
     */
    void setInvalidationListener(Consumer<String> listener);

    /** Builds the lock marker published by this node. */
    String lockMarker();

    boolean isLockMarker(Object value);

    boolean isOwnLockMarker(Object value);

    boolean isExpiredLock(Object value);
}
