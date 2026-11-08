package com.example.app;

import org.hibernate.Session;

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
 * Endpoints for the READ_WRITE and READ_ONLY cache strategies, so both can be
 * exercised against the same extension.
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class StrategyResource {

    @Inject
    EntityManager em;

    @GET
    @Path("/accounts/{code}")
    @Transactional
    public AccountView getAccount(@PathParam("code") String code) {
        Account account = session().bySimpleNaturalId(Account.class).load(code);
        if (account == null) {
            throw new NotFoundException("No account with code " + code);
        }
        return new AccountView(account.id, account.accountCode, account.name, account.version);
    }

    @GET
    @Path("/accounts/byId/{id}")
    @Transactional
    public AccountView getAccountById(@PathParam("id") Long id) {
        Account account = em.find(Account.class, id);
        if (account == null) {
            throw new NotFoundException("No account with id " + id);
        }
        return new AccountView(account.id, account.accountCode, account.name, account.version);
    }

    @PUT
    @Path("/accounts/{code}")
    @Transactional
    public AccountView renameAccount(@PathParam("code") String code, RenameRequest request) {
        Account account = session().bySimpleNaturalId(Account.class).load(code);
        if (account == null) {
            throw new NotFoundException("No account with code " + code);
        }
        account.name = request.name();
        em.flush();
        return new AccountView(account.id, account.accountCode, account.name, account.version);
    }

    @GET
    @Path("/countries/{code}")
    @Transactional
    public CountryView getCountry(@PathParam("code") String code) {
        Country country = session().bySimpleNaturalId(Country.class).load(code);
        if (country == null) {
            throw new NotFoundException("No country with code " + code);
        }
        return new CountryView(country.id, country.isoCode, country.name);
    }

    @GET
    @Path("/countries/byId/{id}")
    @Transactional
    public CountryView getCountryById(@PathParam("id") Long id) {
        Country country = em.find(Country.class, id);
        if (country == null) {
            throw new NotFoundException("No country with id " + id);
        }
        return new CountryView(country.id, country.isoCode, country.name);
    }

    private Session session() {
        return em.unwrap(Session.class);
    }

    public record AccountView(Long id, String accountCode, String name, long version) {
    }

    public record CountryView(Long id, String isoCode, String name) {
    }

    public record RenameRequest(String name) {
    }
}
