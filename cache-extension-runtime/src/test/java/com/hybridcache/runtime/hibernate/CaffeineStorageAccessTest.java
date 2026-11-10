package com.hybridcache.runtime.hibernate;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the payload tier's key index, which lets a pub/sub invalidation evict
 * by the string form of a Hibernate cache key in O(1) instead of scanning the
 * whole cache.
 */
class CaffeineStorageAccessTest {

    /** Same equals/hashCode as a Hibernate key, but with a stable string form. */
    private record Key(String id) {
        @Override
        public String toString() {
            return id;
        }
    }

    @Test
    void evictsByStringFormWithoutScanning() {
        CaffeineStorageAccess storage = new CaffeineStorageAccess(100, false);
        Key key = new Key("com.example.Customer#1");
        storage.putIntoCache(key, "value", null);

        assertTrue(storage.contains(new Key("com.example.Customer#1")));

        // The pub/sub message only carries key.toString().
        storage.evictDataByStringKey("com.example.Customer#1");

        assertFalse(storage.contains(key));
    }

    @Test
    void unknownStringKeyIsIgnored() {
        CaffeineStorageAccess storage = new CaffeineStorageAccess(100, false);
        Key key = new Key("a");
        storage.putIntoCache(key, "value", null);

        storage.evictDataByStringKey("missing");

        assertTrue(storage.contains(key));
    }

    @Test
    void stringEvictionOnlyRemovesTheMatchingEntry() {
        CaffeineStorageAccess storage = new CaffeineStorageAccess(100, false);
        storage.putIntoCache(new Key("a"), "A", null);
        storage.putIntoCache(new Key("b"), "B", null);

        storage.evictDataByStringKey("a");

        assertFalse(storage.contains(new Key("a")));
        assertTrue(storage.contains(new Key("b")));
    }

    @Test
    void indexFollowsSizeEvictionAndReinsertion() {
        CaffeineStorageAccess storage = new CaffeineStorageAccess(1, false);
        storage.putIntoCache(new Key("first"), "1", null);
        storage.putIntoCache(new Key("second"), "2", null);

        // "first" was evicted by Caffeine; evicting it again must be harmless.
        storage.evictDataByStringKey("first");
        assertTrue(storage.contains(new Key("second")));

        // Re-inserting a previously seen key updates the index.
        storage.putIntoCache(new Key("first"), "1b", null);
        assertEquals("1b", storage.getFromCache(new Key("first"), null));
        storage.evictDataByStringKey("first");
        assertFalse(storage.contains(new Key("first")));
    }

    @Test
    void evictsEverythingExceptPreservedKeys() {
        CaffeineStorageAccess storage = new CaffeineStorageAccess(100, false);
        storage.putIntoCache(new Key("a"), "A", null);
        storage.putIntoCache(new Key("b"), "B", null);
        storage.putIntoCache(new Key("c"), "C", null);

        // Recovery clear: keys this node is currently mutating must survive.
        storage.evictDataExcept(Set.of("b"));

        assertFalse(storage.contains(new Key("a")));
        assertTrue(storage.contains(new Key("b")));
        assertFalse(storage.contains(new Key("c")));

        // The key index must still resolve the preserved entry.
        storage.evictDataByStringKey("b");
        assertFalse(storage.contains(new Key("b")));
    }

    @Test
    void evictDataExceptWithEmptySetClearsEverything() {
        CaffeineStorageAccess storage = new CaffeineStorageAccess(100, false);
        storage.putIntoCache(new Key("a"), "A", null);
        storage.putIntoCache(new Key("b"), "B", null);

        storage.evictDataExcept(Set.of());

        assertFalse(storage.contains(new Key("a")));
        assertFalse(storage.contains(new Key("b")));
    }

    @Test
    void objectEvictionAndClearKeepTheIndexConsistent() {
        CaffeineStorageAccess storage = new CaffeineStorageAccess(100, false);
        Key a = new Key("a");
        Key b = new Key("b");
        storage.putIntoCache(a, "A", null);
        storage.putIntoCache(b, "B", null);

        storage.evictData(a);
        assertFalse(storage.contains(a));
        assertTrue(storage.contains(b));

        storage.evictData();
        assertFalse(storage.contains(b));
        storage.evictDataByStringKey("b");
        assertFalse(storage.contains(b));
    }
}
