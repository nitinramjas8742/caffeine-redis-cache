package com.hybridcache.deployment;

import java.util.List;

import org.jboss.logging.Logger;

import com.hybridcache.runtime.HybridCacheRecorder;
import com.hybridcache.runtime.QuarkusHybridCacheRegionFactory;
import com.hybridcache.runtime.storage.HybridCachePubSub;
import com.hybridcache.runtime.storage.HybridCacheRedisProvider;

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.hibernate.orm.deployment.PersistenceUnitDescriptorBuildItem;
import io.quarkus.hibernate.orm.deployment.integration.HibernateOrmIntegrationRuntimeConfiguredBuildItem;
import io.quarkus.hibernate.orm.deployment.integration.HibernateOrmIntegrationStaticConfiguredBuildItem;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.RedisDataSource;

public class HybridCacheProcessor {

    private static final String FEATURE = "hybrid-distributed-cache";
    private static final Logger LOG = Logger.getLogger(HybridCacheProcessor.class);

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    /**
     * Registers a Hibernate ORM static-init listener for every persistence unit.
     * The listener injects {@code hibernate.cache.region.factory_class} after
     * Quarkus' own second-level cache defaults, so the custom factory wins.
     */
    @BuildStep
    @Record(ExecutionTime.STATIC_INIT)
    void integrateWithHibernateOrm(HybridCacheRecorder recorder,
            List<PersistenceUnitDescriptorBuildItem> persistenceUnits,
            BuildProducer<HibernateOrmIntegrationStaticConfiguredBuildItem> producer) {

        for (PersistenceUnitDescriptorBuildItem persistenceUnit : persistenceUnits) {
            producer.produce(new HibernateOrmIntegrationStaticConfiguredBuildItem(FEATURE,
                    persistenceUnit.getPersistenceUnitName())
                    .setInitListener(recorder.createStaticInitListener()));
            LOG.infof("Hybrid cache will replace the Hibernate ORM second-level cache region factory "
                    + "for persistence unit '%s'.", persistenceUnit.getPersistenceUnitName());
        }
    }

    /**
     * Also registers a runtime-init listener: Quarkus' Hibernate ORM extension
     * installs the local-cache region factory in the runtime settings, which would
     * otherwise override the factory configured during static initialization.
     */
    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void integrateWithHibernateOrmRuntime(HybridCacheRecorder recorder,
            List<PersistenceUnitDescriptorBuildItem> persistenceUnits,
            BuildProducer<HibernateOrmIntegrationRuntimeConfiguredBuildItem> producer) {

        for (PersistenceUnitDescriptorBuildItem persistenceUnit : persistenceUnits) {
            producer.produce(new HibernateOrmIntegrationRuntimeConfiguredBuildItem(FEATURE,
                    persistenceUnit.getPersistenceUnitName())
                    .setInitListener(recorder.createRuntimeInitListener()));
        }
    }

    /**
     * Keeps the Redis client beans even if they end up being used only through
     * programmatic lookup ({@code Arc.container().instance(...)}), which ArC cannot
     * detect. The provider registered below declares the actual injection points.
     */
    @BuildStep
    UnremovableBeanBuildItem keepRedisClientBeans() {
        return UnremovableBeanBuildItem.beanTypes(
                RedisDataSource.class,
                ReactiveRedisDataSource.class);
    }

    /**
     * Registers the CDI bean that declares the Redis client injection points.
     * Without an injection point the Quarkus Redis extension does not create its
     * datasource beans at all. The pub/sub bean is kept for the same reason: it
     * holds the single shared subscription used by every cache region.
     */
    @BuildStep
    AdditionalBeanBuildItem registerRedisProvider() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClasses(HybridCacheRedisProvider.class, HybridCachePubSub.class)
                .setUnremovable()
                .build();
    }

    @BuildStep
    ReflectiveClassBuildItem registerForReflection() {
        // Hibernate instantiates the region factory by reflection; nothing else needs
        // to be registered, so keep the native-image footprint minimal.
        return ReflectiveClassBuildItem.builder(QuarkusHybridCacheRegionFactory.class)
                .constructors()
                .build();
    }
}
