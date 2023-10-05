package com.maximyasn.dao;

import com.maximyasn.domain.City;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

import java.util.List;

public class CityDao {

    private final SessionFactory sessionFactory;
    public CityDao(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public List<City> getItems(int offset, int limit) {
        Query<City> query = sessionFactory
                .getCurrentSession()
                .createQuery("select c from City c", City.class)
                .setFirstResult(offset)
                .setMaxResults(limit);
        return query.list();
    }

    public int getTotalCount() {
        Query<Long> query = sessionFactory
                .getCurrentSession()
                .createQuery("select count(c) from City c", Long.class);

        return Math.toIntExact(query.getSingleResult());
    }

    public City getById(Integer id) {
        Query<City> query = sessionFactory
                .getCurrentSession()
                .createQuery("select c from City c join fetch c.country where c.id = :ID", City.class)
                .setParameter("ID", id);
        return query.getSingleResult();
    }
}

