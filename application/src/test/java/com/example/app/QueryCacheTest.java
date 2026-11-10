package com.example.app;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;

/**
 * Verifies Hibernate's query cache through the hybrid extension.
 * <ul>
 *   <li>The first execution of a cacheable named query misses and populates
 *       the query results region; the second one hits.</li>
 *   <li>An entity update invalidates the cached query result: after the
 *       update, the same query must miss again and no longer return the
 *       renamed row.</li>
 * </ul>
 * The query result payload lives in the local Caffeine query region, while the
 * query-space timestamps are kept in the shared Redis timestamps region by
 * {@code DomainQueryTimestampsRedisStorage}.
 */
@QuarkusTest
class QueryCacheTest {

    private static final String CUSTOMER_CODE = "CUST-001";
    private static final String ORIGINAL_NAME = "Ada Lovelace";
    private static final String UPDATED_NAME = "Ada Lovelace Updated";

    @Test
    void namedQueryIsServedFromTheQueryCache() {
        given()
                .pathParam("name", ORIGINAL_NAME)
                .when().get("/cache/query/demo/{name}")
                .then().statusCode(200)
                .body("name", is(ORIGINAL_NAME))
                .body("firstResult", contains(CUSTOMER_CODE))
                .body("secondResult", contains(CUSTOMER_CODE))
                // first execution misses, second one (after em.clear) hits
                .body("queryCacheMisses", is(1))
                .body("queryCachePuts", greaterThanOrEqualTo(1))
                .body("queryCacheHits", is(1));
    }

    @Test
    void entityUpdateInvalidatesTheCachedQueryResult() {
        try {
            // 1. Warm the query cache for the original name.
            given()
                    .pathParam("name", ORIGINAL_NAME)
                    .when().get("/cache/query/demo/{name}")
                    .then().statusCode(200)
                    .body("firstResult", contains(CUSTOMER_CODE))
                    .body("queryCacheMisses", is(1))
                    .body("queryCacheHits", is(1));

            // 2. Commit a real change to the query space.
            given()
                    .pathParam("code", CUSTOMER_CODE)
                    .contentType(ContentType.JSON)
                    .body(new CustomerResource.RenameRequest(UPDATED_NAME))
                    .when().put("/customers/{code}")
                    .then().statusCode(200)
                    .body("name", is(UPDATED_NAME));

            // 3. The previously cached result must now be stale and re-executed.
            given()
                    .pathParam("name", ORIGINAL_NAME)
                    .queryParam("reset", false)
                    .when().get("/cache/query/demo/{name}")
                    .then().statusCode(200)
                    .body("firstResult", empty())
                    .body("secondResult", empty())
                    .body("queryCacheMisses", greaterThanOrEqualTo(1))
                    .body("queryCacheHits", greaterThanOrEqualTo(1));
        } finally {
            // Restore the seed row for the other tests.
            given()
                    .pathParam("code", CUSTOMER_CODE)
                    .contentType(ContentType.JSON)
                    .body(new CustomerResource.RenameRequest(ORIGINAL_NAME))
                    .when().put("/customers/{code}")
                    .then().statusCode(200);
        }
    }
}
