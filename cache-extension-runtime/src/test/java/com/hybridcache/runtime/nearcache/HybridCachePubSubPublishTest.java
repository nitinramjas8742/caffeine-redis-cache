package com.hybridcache.runtime.nearcache;

import io.quarkus.redis.datasource.pubsub.ReactivePubSubCommands;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * The invalidation publish must be fire-and-forget: a broker error must neither
 * propagate into the Hibernate write nor tear down the subscription.
 */
class HybridCachePubSubPublishTest {

    @SuppressWarnings("unchecked")
    private static ReactivePubSubCommands<String> commandsReturning(Uni<Void> result) {
        return (ReactivePubSubCommands<String>) Proxy.newProxyInstance(
                ReactivePubSubCommands.class.getClassLoader(),
                new Class<?>[]{ReactivePubSubCommands.class},
                (proxy, method, args) -> {
                    if ("publish".equals(method.getName())) {
                        return result;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static HybridCachePubSub pubSubWith(ReactivePubSubCommands<String> commands) throws Exception {
        HybridCachePubSub pubSub = new HybridCachePubSub();
        Field field = HybridCachePubSub.class.getDeclaredField("pubsubCommands");
        field.setAccessible(true);
        field.set(pubSub, commands);
        return pubSub;
    }

    @Test
    void failedPublishDoesNotReachTheCaller() throws Exception {
        HybridCachePubSub pubSub = pubSubWith(
                commandsReturning(Uni.createFrom().failure(new IllegalStateException("publish denied"))));

        assertDoesNotThrow(() -> pubSub.publish("region|node|key"));
    }

    @Test
    void successfulPublishIsAccepted() throws Exception {
        HybridCachePubSub pubSub = pubSubWith(commandsReturning(Uni.createFrom().item((Void) null)));

        assertDoesNotThrow(() -> pubSub.publish("region|node|key"));
    }
}
