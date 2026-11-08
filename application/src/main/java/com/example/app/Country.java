package com.example.app;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.NaturalIdCache;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/**
 * Immutable reference data cached with {@link CacheConcurrencyStrategy#READ_ONLY}.
 */
@Entity
@Table(name = "country")
@jakarta.persistence.Cacheable
@Immutable
@Cache(usage = CacheConcurrencyStrategy.READ_ONLY)
@NaturalIdCache
public class Country extends PanacheEntity {

    @NaturalId(mutable = false)
    @Column(name = "iso_code", nullable = false, unique = true, length = 8)
    public String isoCode;

    @Column(nullable = false, length = 128)
    public String name;

    public Country() {
    }

    public Country(String isoCode, String name) {
        this.isoCode = isoCode;
        this.name = name;
    }
}
