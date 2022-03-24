package dev.vality.payout.manager.dao;

import dev.vality.dao.DaoException;
import dev.vality.dao.GenericDao;
import dev.vality.payout.manager.domain.tables.pojos.Source;

import java.util.Optional;

public interface SourceDao extends GenericDao {

    Optional<Long> save(Source source) throws DaoException;

    Source get(String sourceId) throws DaoException;

    Source getByCurrencyCode(String currencyCode) throws DaoException;

}
