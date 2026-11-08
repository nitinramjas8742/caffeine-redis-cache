package com.hybridcache.runtime.config;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Configuration of the hybrid second-level cache extension.
 *
 * <p>All properties are overridable at runtime in {@code application.properties},
 * for example {@code quarkus.hybrid-cache.near-cache.ttl-ms=30000}.</p>
 */
@ConfigMapping(prefix = "quarkus.hybrid-cache")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface HybridCacheConfig {

    /**
     * Redis pub/sub invalidation settings.
     */
    PubSubConfig pubsub();

    /**
     * Local near-cache settings.
     */
    NearCacheConfig nearCache();

    /**
     * Redis data-connection settings.
     */
    RedisConfig redis();

    /**
     * Local payload (Caffeine) storage settings.
     */
    StorageConfig storage();

    interface PubSubConfig {

        /**
         * Whether cross-node invalidation over Redis pub/sub is enabled. Disabling it
         * keeps the cache functional but leans on timestamps and the reconnection
         * strategy for coherence.
         */
        @WithDefault("true")
        boolean enabled();

        /**
         * Delay before retrying a lost pub/sub subscription.
         */
        @WithDefault("2000")
        long reconnectDelayMs();

        /**
         * Interval of the pub/sub heartbeat that detects a silently dead subscription.
         */
        @WithDefault("2000")
        long heartbeatIntervalMs();

        /**
         * How long a heartbeat may go unanswered before the subscription is considered lost.
         */
        @WithDefault("6000")
        long heartbeatTimeoutMs();

        /**
         * Whether the local near-caches are cleared when a pub/sub gap is detected and
         * when the subscription is confirmed live again.
         */
        @WithDefault("true")
        boolean clearOnReconnect();
    }

    interface NearCacheConfig {

        /**
         * What to do with the local timestamp mirror after Redis becomes reachable again.
         */
        @WithDefault("NONE")
        ReconnectionStrategy reconnectionStrategy();

        /**
         * Optional safety net: after this many milliseconds a near-cache entry is treated
         * as a miss. {@code 0} keeps Redisson's {@code timeToLive = 0} behaviour.
         */
        @WithDefault("0")
        long ttlMs();

        /**
         * After a reconnect, near-cache hits are ignored for this long so the first reads
         * revalidate against Redis. {@code 0} disables the window.
         */
        @WithDefault("8000")
        long recoveryGraceMs();
    }

    interface RedisConfig {

        /**
         * Cooldown before retrying Redis after a failed command, during which cache reads
         * are served from the local tier only.
         */
        @WithDefault("5000")
        long retryDelayMs();
    }

    interface StorageConfig {

        /**
         * Maximum number of entity payload entries kept per Hibernate cache region.
         */
        @WithDefault("10000")
        long maximumSize();

        /**
         * Whether Caffeine collects hit/miss/eviction statistics for the payload tier.
         */
        @WithDefault("false")
        boolean recordStats();
    }
}
