package com.example.app;

import java.util.List;

import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

/**
 * Demo endpoint for Hibernate's query cache.
 * <p>
 * Runs the same cacheable JPQL query twice, clearing the persistence context
 * between the executions. The first execution misses the query cache and stores
 * its result; the second one is served from the query results region without
 * touching the database. The response exposes the Hibernate query-cache
 * counters so the behaviour is visible from the outside.
 * <p>
 * This lives in its own resource so the README's customer endpoints stay
 * unchanged.
 */
@Path("/cache/query")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
public class QueryCacheDemoResource {

    @Inject
    EntityManager em;

    /**
     * @param name  the customer name to look up
     * @param reset when {@code true} (the default), clear the query cache and the
     *              statistics before running, so the counters describe this call
     *              only; pass {@code false} to observe an existing query cache
     */
    @GET
    @Path("/demo/{name}")
    @Transactional
    public QueryCacheDemoResult demo(@PathParam("name") String name, @QueryParam("reset") Boolean reset) {
        SessionFactory sessionFactory = em.getEntityManagerFactory().unwrap(SessionFactory.class);
        Statistics statistics = sessionFactory.getStatistics();

        if (reset == null || reset) {
            statistics.clear();
            // A previous test (or a previous call) may have warmed the same query.
            // Evict the query result region so this call starts from a cold cache.
            sessionFactory.getCache().evictQueryRegions();
        }

        List<String> firstResult = query(name);
        em.clear();
        List<String> secondResult = query(name);

        return new QueryCacheDemoResult(
                name,
                firstResult,
                secondResult,
                statistics.getQueryCacheMissCount(),
                statistics.getQueryCachePutCount(),
                statistics.getQueryCacheHitCount());
    }

    private List<String> query(String name) {
        return em.createQuery(
                "select c.customerCode from Customer c where c.name = :name order by c.customerCode",
                String.class)
                .setHint("org.hibernate.cacheable", true)
                .setParameter("name", name)
                .getResultList();
    }

    public record QueryCacheDemoResult(String name,
                                       List<String> firstResult,
                                       List<String> secondResult,
                                       long queryCacheMisses,
                                       long queryCachePuts,
                                       long queryCacheHits) {
    }
}
