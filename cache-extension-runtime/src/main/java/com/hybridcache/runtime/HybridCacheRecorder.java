package com.hybridcache.runtime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.cache.internal.RegionFactoryInitiator;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import io.quarkus.hibernate.orm.runtime.integration.HibernateOrmIntegrationRuntimeInitListener;
import io.quarkus.hibernate.orm.runtime.integration.HibernateOrmIntegrationStaticInitListener;
import io.quarkus.runtime.annotations.Recorder;

import com.hybridcache.runtime.hibernate.QuarkusHybridCacheRegionFactory;

/**
 * Contributes the Hibernate ORM properties that install
 * {@link QuarkusHybridCacheRegionFactory} as the second-level cache region factory.
 * <p>
 * The properties are contributed both during static initialization (metadata model
 * validation) and at runtime. The runtime contribution is required because Quarkus'
 * Hibernate ORM extension installs its own default local-cache region factory in
 * the runtime settings, which would otherwise override our factory.
 */
@Recorder
public class HybridCacheRecorder {

    public HibernateOrmIntegrationStaticInitListener createStaticInitListener() {
        return new StaticInitListener();
    }

    public HibernateOrmIntegrationRuntimeInitListener createRuntimeInitListener() {
        return new RuntimeInitListener();
    }

    private static void contributeRegionFactory(BiConsumer<String, Object> propertyCollector) {
        org.jboss.logging.Logger.getLogger(HybridCacheRecorder.class)
                .debug("Hybrid cache contributing hibernate.cache.region.factory_class");
        propertyCollector.accept(AvailableSettings.CACHE_REGION_FACTORY,
                QuarkusHybridCacheRegionFactory.class.getName());
        propertyCollector.accept(AvailableSettings.USE_SECOND_LEVEL_CACHE, Boolean.TRUE.toString());
        propertyCollector.accept(AvailableSettings.USE_QUERY_CACHE, Boolean.TRUE.toString());
    }

    private static final class StaticInitListener implements HibernateOrmIntegrationStaticInitListener {

        @Override
        public void contributeBootProperties(BiConsumer<String, Object> propertyCollector) {
            contributeRegionFactory(propertyCollector);
        }

        @Override
        public void onMetadataInitialized(Metadata metadata, BootstrapContext bootstrapContext,
                BiConsumer<String, Object> propertyCollector) {
            // nothing to contribute
        }
    }

    private static final class RuntimeInitListener implements HibernateOrmIntegrationRuntimeInitListener {

        @Override
        public void contributeRuntimeProperties(BiConsumer<String, Object> propertyCollector) {
            contributeRegionFactory(propertyCollector);
        }

        /**
         * Quarkus pre-builds the {@code RegionFactory} service during static init
         * (with its own local-cache implementation) and re-adds that service at
         * runtime. Re-registering Hibernate's standard initiator makes the service
         * be re-created from the runtime setting contributed above.
         */
        @Override
        public List<StandardServiceInitiator<?>> contributeServiceInitiators() {
            return List.of(new HybridRegionFactoryInitiator());
        }
    }

    /**
     * Hibernate's {@link RegionFactoryInitiator} copies the whole configuration map into
     * a {@link java.util.Properties} instance, which throws a {@link NullPointerException}
     * when the map contains a {@code null} value (Quarkus' runtime settings do contain
     * them). This initiator simply drops {@code null} entries before delegating, so the
     * standard resolution logic runs unchanged.
     */
    static final class HybridRegionFactoryInitiator extends RegionFactoryInitiator {

        @Override
        public RegionFactory initiateService(Map<String, Object> configurationValues,
                ServiceRegistryImplementor registry) {
            Map<String, Object> sanitized = new HashMap<>(configurationValues.size());
            configurationValues.forEach((key, value) -> {
                if (key != null && value != null) {
                    sanitized.put(String.valueOf(key), value);
                }
            });
            return super.initiateService(sanitized, registry);
        }
    }
}
