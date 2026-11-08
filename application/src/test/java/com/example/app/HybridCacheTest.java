package com.example.app;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class HybridCacheTest {

    @Test
    void naturalIdLookupWorks() {
        given().when().get("/customers/CUST-001")
                .then().statusCode(200)
                .body("customerCode", is("CUST-001"))
                .body("name", is("Ada Lovelace"));
    }

    @Test
    void extensionInstallsItsRegionFactory() {
        given().when().get("/cache/info")
                .then().statusCode(200)
                .body("regionFactory", containsString("QuarkusHybridCacheRegionFactory"))
                .body("statisticsEnabled", is(true))
                .body("regions.size()", greaterThan(0));
    }

    @Test
    void secondLevelCacheServesRepeatedNaturalIdLoads() {
        given().when().get("/cache/demo/CUST-001")
                .then().statusCode(200)
                .body("nameFirstLoad", is("Ada Lovelace"))
                .body("nameSecondLoad", is("Ada Lovelace"))
                .body("nameThirdLoad", is("Ada Lovelace"))
                .body("secondLevelCacheHits", greaterThanOrEqualTo(1))
                .body("naturalIdCacheHits", greaterThanOrEqualTo(1));
    }
}
