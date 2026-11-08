package com.example.app;

import java.util.List;

import org.hibernate.SessionFactory;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.stat.CacheRegionStatistics;
import org.hibernate.stat.Statistics;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * Demo endpoints for the hybrid second-level cache.
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class CustomerResource {

    @Inject
    CustomerRepository customers;

    @Inject
    EntityManager em;

    @GET
    @Path("/customers")
    @Transactional
    public List<CustomerView> list() {
        return customers.listAll().stream()
                .map(c -> new CustomerView(c.id, c.customerCode, c.name))
                .toList();
    }

    @GET
    @Path("/customers/{code}")
    @Transactional
    public CustomerView get(@PathParam("code") String code) {
        Customer customer = customers.findByCustomerCode(code);
        if (customer == null) {
            throw new NotFoundException("No customer with code " + code);
        }
        return new CustomerView(customer.id, customer.customerCode, customer.name);
    }

    /**
     * Renames a customer. Used to demonstrate write invalidation across nodes.
     */
    @PUT
    @Path("/customers/{code}")
    @Transactional
    public CustomerView rename(@PathParam("code") String code, RenameRequest request) {
        Customer customer = customers.findByCustomerCode(code);
        if (customer == null) {
            throw new NotFoundException("No customer with code " + code);
        }
        customer.name = request.name();
        em.flush();
        return new CustomerView(customer.id, customer.customerCode, customer.name);
    }

    /**
     * Reports which region factory Hibernate ORM is actually using.
     */
    @GET
    @Path("/cache/info")
    @Transactional
    public CacheInfo info() {
        SessionFactory sessionFactory = em.getEntityManagerFactory().unwrap(SessionFactory.class);
        RegionFactory regionFactory = regionFactory(sessionFactory);
        Statistics stats = sessionFactory.getStatistics();

        return new CacheInfo(
                regionFactory.getClass().getName(),
                String.valueOf(sessionFactory.getProperties().get("hibernate.cache.region.factory_class")),
                stats.isStatisticsEnabled(),
                List.of(stats.getSecondLevelCacheRegionNames()));
    }

    /**
     * Loads the same customer three times through its natural id, clearing the
     * persistence context in between, and returns the Hibernate cache counters.
     * <p>
     * The first load must miss the cache; the following loads should produce
     * second-level and/or natural-id cache hits.
     */
    @GET
    @Path("/cache/demo/{code}")
    @Transactional
    public CacheDemoResult demo(@PathParam("code") String code) {
        SessionFactory sessionFactory = em.getEntityManagerFactory().unwrap(SessionFactory.class);
        Statistics stats = sessionFactory.getStatistics();
        stats.clear();

        Customer first = customers.findByCustomerCode(code);
        if (first == null) {
            throw new NotFoundException("No customer with code " + code);
        }
        em.clear();

        Customer second = customers.findByCustomerCode(code);
        em.clear();

        Customer third = customers.findByCustomerCode(code);

        return new CacheDemoResult(
                code,
                first.name,
                second == null ? null : second.name,
                third == null ? null : third.name,
                regionFactory(sessionFactory).getClass().getName(),
                stats.getSecondLevelCacheHitCount(),
                stats.getSecondLevelCacheMissCount(),
                stats.getSecondLevelCachePutCount(),
                stats.getNaturalIdCacheHitCount(),
                stats.getNaturalIdCacheMissCount(),
                stats.getNaturalIdCachePutCount(),
                stats.getEntityLoadCount(),
                stats.getPrepareStatementCount());
    }

    private static RegionFactory regionFactory(SessionFactory sessionFactory) {
        return sessionFactory.unwrap(SessionFactoryImplementor.class)
                .getServiceRegistry()
                .getService(RegionFactory.class);
    }

    /**
     * Per-region second-level cache counters, used by the strategy tests.
     */
    @GET
    @Path("/cache/stats/{region}")
    @Transactional
    public RegionStats regionStats(@PathParam("region") String region) {
        SessionFactory sessionFactory = em.getEntityManagerFactory().unwrap(SessionFactory.class);
        CacheRegionStatistics statistics = sessionFactory.getStatistics()
                .getCacheRegionStatistics(region);
        if (statistics == null) {
            throw new NotFoundException("Unknown cache region " + region);
        }
        return new RegionStats(
                region,
                statistics.getHitCount(),
                statistics.getMissCount(),
                statistics.getPutCount(),
                statistics.getElementCountInMemory());
    }

    public record CustomerView(Long id, String customerCode, String name) {
    }

    public record RenameRequest(String name) {
    }

    public record CacheInfo(String regionFactory, String regionFactorySetting, boolean statisticsEnabled,
            List<String> regions) {
    }

    public record RegionStats(String region, long hits, long misses, long puts, long elementsInMemory) {
    }

    public record CacheDemoResult(String customerCode,
                                  String nameFirstLoad,
                                  String nameSecondLoad,
                                  String nameThirdLoad,
                                  String regionFactory,
                                  long secondLevelCacheHits,
                                  long secondLevelCacheMisses,
                                  long secondLevelCachePuts,
                                  long naturalIdCacheHits,
                                  long naturalIdCacheMisses,
                                  long naturalIdCachePuts,
                                  long entityLoadCount,
                                  long prepareStatementCount) {
    }
}
