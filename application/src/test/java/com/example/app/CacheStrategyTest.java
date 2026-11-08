package com.example.app;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;

/**
 * Verifies that the same hybrid storage wrapper works for READ_WRITE (SoftLock
 * path) and READ_ONLY (no lock) entities, through both natural-id and
 * primary-id lookups.
 */
@QuarkusTest
class CacheStrategyTest {

    private static final String ACCOUNT_REGION = "com.example.app.Account";
    private static final String ACCOUNT_NATURAL_ID_REGION = "com.example.app.Account##NaturalId";
    private static final String COUNTRY_REGION = "com.example.app.Country";
    private static final String COUNTRY_NATURAL_ID_REGION = "com.example.app.Country##NaturalId";

    @Test
    void readWriteNaturalIdLookupIsCached() {
        long hitsBefore = hits(ACCOUNT_NATURAL_ID_REGION);

        given().when().get("/accounts/ACC-002")
                .then().statusCode(200)
                .body("accountCode", is("ACC-002"));
        given().when().get("/accounts/ACC-002")
                .then().statusCode(200)
                .body("accountCode", is("ACC-002"));

        assertHitsIncreased(ACCOUNT_NATURAL_ID_REGION, hitsBefore);
    }

    @Test
    void readWritePrimaryIdLookupIsCached() {
        long hitsBefore = hits(ACCOUNT_REGION);

        given().when().get("/accounts/byId/1")
                .then().statusCode(200)
                .body("accountCode", is("ACC-001"));
        given().when().get("/accounts/byId/1")
                .then().statusCode(200)
                .body("accountCode", is("ACC-001"));

        assertHitsIncreased(ACCOUNT_REGION, hitsBefore);
    }

    @Test
    void readWriteUpdateInvalidatesBothRegions() {
        given().when().get("/accounts/ACC-001")
                .then().statusCode(200);

        given().contentType(ContentType.JSON)
                .body("{\"name\":\"Renamed Account\"}")
                .when().put("/accounts/ACC-001")
                .then().statusCode(200)
                .body("name", is("Renamed Account"));

        // Primary-id read after the natural-id transaction committed.
        long hitsBefore = hits(ACCOUNT_REGION);
        given().when().get("/accounts/byId/1")
                .then().statusCode(200)
                .body("name", is("Renamed Account"));
        given().when().get("/accounts/byId/1")
                .then().statusCode(200)
                .body("name", is("Renamed Account"));

        assertHitsIncreased(ACCOUNT_REGION, hitsBefore);
    }

    @Test
    void readOnlyNaturalIdLookupIsCached() {
        long hitsBefore = hits(COUNTRY_NATURAL_ID_REGION);

        given().when().get("/countries/US")
                .then().statusCode(200)
                .body("isoCode", is("US"))
                .body("name", is("United States"));
        given().when().get("/countries/US")
                .then().statusCode(200)
                .body("name", is("United States"));

        assertHitsIncreased(COUNTRY_NATURAL_ID_REGION, hitsBefore);
    }

    @Test
    void readOnlyPrimaryIdLookupIsCached() {
        long hitsBefore = hits(COUNTRY_REGION);

        given().when().get("/countries/byId/1")
                .then().statusCode(200)
                .body("isoCode", is("IN"))
                .body("name", is("India"));
        given().when().get("/countries/byId/1")
                .then().statusCode(200)
                .body("name", is("India"));

        assertHitsIncreased(COUNTRY_REGION, hitsBefore);
    }

    private static long hits(String region) {
        return given().pathParam("region", region)
                .when().get("/cache/stats/{region}")
                .then().statusCode(200)
                .extract().jsonPath().getLong("hits");
    }

    private static void assertHitsIncreased(String region, long hitsBefore) {
        long hitsAfter = hits(region);
        assertTrue(hitsAfter > hitsBefore,
                "Expected at least one cache hit in " + region + " (before=" + hitsBefore
                        + ", after=" + hitsAfter + ")");
    }
}
