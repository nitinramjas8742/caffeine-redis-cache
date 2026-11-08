package com.hybridcache.runtime;

import java.util.Map;

import org.eclipse.microprofile.config.ConfigProvider;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.cfg.spi.DomainDataRegionBuildingContext;
import org.hibernate.cache.cfg.spi.DomainDataRegionConfig;
import org.hibernate.cache.spi.support.DomainDataStorageAccess;
import org.hibernate.cache.spi.support.RegionFactoryTemplate;
import org.hibernate.cache.spi.support.StorageAccess;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.jboss.logging.Logger;

import com.hybridcache.runtime.config.HybridCacheConfig;
import com.hybridcache.runtime.storage.CaffeineStorageAccess;
import com.hybridcache.runtime.storage.DomainDataTimestampsRedisStorage;
import com.hybridcache.runtime.storage.DomainQueryTimestampsRedisStorage;
import com.hybridcache.runtime.storage.NaturalIdTimestampsRedisStorage;
import com.hybridcache.runtime.storage.QuarkusDistributedTimestampMap;

import io.smallrye.config.SmallRyeConfig;

/**
 * Hibernate second-level cache region factory that combines a local Caffeine
 * payload tier with Redis-coordinated timestamp invalidation.
 * <p>
 * Configuration lives in {@link HybridCacheConfig} under the
 * {@code quarkus.hybrid-cache} prefix.
 */
public class QuarkusHybridCacheRegionFactory extends RegionFactoryTemplate {

    private static final Logger LOG = Logger.getLogger(QuarkusHybridCacheRegionFactory.class);

    @Override
    protected void prepareForUse(SessionFactoryOptions settings, Map<String, Object> configValues) {
        LOG.info(">>> Initializing QuarkusHybridCacheRegionFactory (Caffeine + Redis timestamp invalidation) <<<");
    }

    @Override
    protected void releaseFromUse() {
        LOG.info(">>> Shutting down QuarkusHybridCacheRegionFactory <<<");
    }

    @Override
    protected DomainDataStorageAccess createDomainDataStorageAccess(DomainDataRegionConfig regionConfig,
                                                                    DomainDataRegionBuildingContext buildingContext) {
        HybridCacheConfig config = config();
        String regionName = regionConfig.getRegionName();

        // Tier 3: Local Payload Storage
        CaffeineStorageAccess localStorage = new CaffeineStorageAccess(
                config.storage().maximumSize(), config.storage().recordStats());

        // Tiers 1 & 2: Distributed Timestamp & Local Timestamp Registry
        QuarkusDistributedTimestampMap remoteStorage = new QuarkusDistributedTimestampMap(regionName, config);

        // Detect if this region is configured for Natural ID caching
        if (!regionConfig.getNaturalIdCaching().isEmpty()) {
            return new NaturalIdTimestampsRedisStorage(regionName, remoteStorage, localStorage);
        }

        // Otherwise, it's a standard Entity or Collection region
        return new DomainDataTimestampsRedisStorage(regionName, remoteStorage, localStorage);
    }

    @Override
    protected StorageAccess createQueryResultsRegionStorageAccess(String regionName,
                                                                  SessionFactoryImplementor sessionFactory) {
        HybridCacheConfig config = config();
        return new CaffeineStorageAccess(
                config.storage().maximumSize(), config.storage().recordStats());
    }

    @Override
    protected StorageAccess createTimestampsRegionStorageAccess(String regionName,
                                                                SessionFactoryImplementor sessionFactory) {
        HybridCacheConfig config = config();
        CaffeineStorageAccess localStorage = new CaffeineStorageAccess(
                config.storage().maximumSize(), config.storage().recordStats());
        QuarkusDistributedTimestampMap remoteStorage = new QuarkusDistributedTimestampMap(regionName, config);

        return new DomainQueryTimestampsRedisStorage(remoteStorage, localStorage);
    }

    private static HybridCacheConfig config() {
        return ConfigProvider.getConfig().unwrap(SmallRyeConfig.class)
                .getConfigMapping(HybridCacheConfig.class);
    }
}
