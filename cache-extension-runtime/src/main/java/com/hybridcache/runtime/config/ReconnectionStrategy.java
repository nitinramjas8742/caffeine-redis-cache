package com.hybridcache.runtime.config;

import java.util.Locale;

/**
 * Mirrors Redisson's {@code LocalCachedMapOptions.ReconnectionStrategy}: what a
 * node does with its local near-cache once Redis becomes reachable again.
 */
public enum ReconnectionStrategy {
    /** Keep the local mirror as-is (Redisson default). */
    NONE,
    /** Clear the local mirror when Redis becomes reachable again. */
    CLEAR,
    /** Reload missed updates when Redis becomes reachable again; implemented lazily. */
    LOAD;

    /**
     * Lenient parser for programmatic/legacy configuration. Quarkus configuration
     * itself uses the enum converter and therefore fails fast on invalid values.
     */
    public static ReconnectionStrategy parse(String value) {
        if (value == null || value.isBlank()) {
            return NONE;
        }
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return NONE;
        }
    }
}
