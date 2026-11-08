package com.example.app;

import org.hibernate.Session;

import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CustomerRepository implements PanacheRepository<Customer> {

    /**
     * Resolves the entity through Hibernate's natural-id path. With
     * {@code @NaturalIdCache} on the entity, this can be served from the
     * second-level cache instead of the database.
     */
    public Customer findByCustomerCode(String customerCode) {
        Session session = getEntityManager().unwrap(Session.class);
        return session.bySimpleNaturalId(Customer.class).load(customerCode);
    }
}
