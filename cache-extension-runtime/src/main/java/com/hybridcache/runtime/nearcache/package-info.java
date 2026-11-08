/**
 * Near-cache primitives: the distributed timestamp map, the shared Redis pub/sub
 * subscriber and the Redis client provider.
 *
 * <p>This package must not depend on Hibernate. The Hibernate integration lives in
 * {@code com.hybridcache.runtime.hibernate} and talks to this package only through
 * {@link com.hybridcache.runtime.nearcache.TimestampRegistry}.</p>
 */
package com.hybridcache.runtime.nearcache;
