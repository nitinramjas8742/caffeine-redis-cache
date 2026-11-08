package com.hybridcache.runtime.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;

/**
 * Verifies that the extension configuration is a regular SmallRye {@code @ConfigMapping}:
 * absent properties fall back to the documented defaults and application properties
 * override them under the {@code quarkus.hybrid-cache} prefix.
 */
class HybridCacheConfigTest {

    @Test
    void exposesDocumentedDefaults() {
        HybridCacheConfig config = config(Map.of());

        assertTrue(config.pubsub().enabled());
        assertEquals(2_000L, config.pubsub().reconnectDelayMs());
        assertEquals(2_000L, config.pubsub().heartbeatIntervalMs());
        assertEquals(6_000L, config.pubsub().heartbeatTimeoutMs());
        assertTrue(config.pubsub().clearOnReconnect());

        assertEquals(ReconnectionStrategy.NONE, config.nearCache().reconnectionStrategy());
        assertEquals(0L, config.nearCache().ttlMs());
        assertEquals(8_000L, config.nearCache().recoveryGraceMs());

        assertEquals(5_000L, config.redis().retryDelayMs());

        assertEquals(10_000L, config.storage().maximumSize());
        assertFalse(config.storage().recordStats());
    }

    @Test
    void mapsApplicationProperties() {
        HybridCacheConfig config = config(Map.of(
                "quarkus.hybrid-cache.pubsub.enabled", "false",
                "quarkus.hybrid-cache.pubsub.reconnect-delay-ms", "1500",
                "quarkus.hybrid-cache.near-cache.reconnection-strategy", "CLEAR",
                "quarkus.hybrid-cache.near-cache.ttl-ms", "30000",
                "quarkus.hybrid-cache.near-cache.recovery-grace-ms", "0",
                "quarkus.hybrid-cache.redis.retry-delay-ms", "1000",
                "quarkus.hybrid-cache.storage.maximum-size", "42",
                "quarkus.hybrid-cache.storage.record-stats", "true"));

        assertFalse(config.pubsub().enabled());
        assertEquals(1_500L, config.pubsub().reconnectDelayMs());
        assertEquals(ReconnectionStrategy.CLEAR, config.nearCache().reconnectionStrategy());
        assertEquals(30_000L, config.nearCache().ttlMs());
        assertEquals(0L, config.nearCache().recoveryGraceMs());
        assertEquals(1_000L, config.redis().retryDelayMs());
        assertEquals(42L, config.storage().maximumSize());
        assertTrue(config.storage().recordStats());
    }

    private static HybridCacheConfig config(Map<String, String> overrides) {
        SmallRyeConfig config = new SmallRyeConfigBuilder()
                .withMapping(HybridCacheConfig.class)
                .withDefaultValues(overrides)
                .build();
        return config.getConfigMapping(HybridCacheConfig.class);
    }
}
