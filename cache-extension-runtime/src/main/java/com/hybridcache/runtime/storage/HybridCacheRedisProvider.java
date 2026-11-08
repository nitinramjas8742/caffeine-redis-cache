package com.hybridcache.runtime.storage;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Holds the (lazily connecting) Quarkus Redis clients.
 * <p>
 * The Quarkus Redis extension only creates its synthetic {@code RedisDataSource}
 * beans when it detects an injection point. Looking the datasource up
 * programmatically ({@code Arc.container().instance(...)}) is not enough, so this
 * bean exists purely to declare the injection points. Registering it as an
 * additional bean in the deployment module keeps it in the container.
 */
@ApplicationScoped
public class HybridCacheRedisProvider {

    @Inject
    RedisDataSource redisDataSource;

    @Inject
    ReactiveRedisDataSource reactiveRedisDataSource;

    public RedisDataSource redisDataSource() {
        return redisDataSource;
    }

    public ReactiveRedisDataSource reactiveRedisDataSource() {
        return reactiveRedisDataSource;
    }
}
