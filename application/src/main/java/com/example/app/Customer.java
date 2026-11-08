package com.example.app;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.NaturalIdCache;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/**
 * Customer entity whose business key ({@code customerCode}) is mapped as a
 * natural id, so lookups can go through Hibernate's natural-id cache.
 */
@Entity
@Table(name = "customer")
@jakarta.persistence.Cacheable
@Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
@NaturalIdCache
public class Customer extends PanacheEntity {

    @NaturalId(mutable = false)
    @Column(name = "customer_code", nullable = false, unique = true, length = 64)
    public String customerCode;

    @Column(nullable = false, length = 128)
    public String name;

    public Customer() {
    }

    public Customer(String customerCode, String name) {
        this.customerCode = customerCode;
        this.name = name;
    }
}
