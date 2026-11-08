package com.example.app;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.NaturalIdCache;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * Entity cached with {@link CacheConcurrencyStrategy#READ_WRITE}, so Hibernate
 * exercises the SoftLock path through the hybrid storage.
 */
@Entity
@Table(name = "account")
@jakarta.persistence.Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@NaturalIdCache
public class Account extends PanacheEntity {

    @NaturalId(mutable = false)
    @Column(name = "account_code", nullable = false, unique = true, length = 64)
    public String accountCode;

    @Column(nullable = false, length = 128)
    public String name;

    @Version
    public long version;

    public Account() {
    }

    public Account(String accountCode, String name) {
        this.accountCode = accountCode;
        this.name = name;
    }
}
