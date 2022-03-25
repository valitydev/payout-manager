package dev.vality.payout.manager.dao.impl;

import dev.vality.dao.DaoException;
import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.mapper.RecordRowMapper;
import dev.vality.payout.manager.dao.SourceDao;
import dev.vality.payout.manager.domain.enums.SourceStatus;
import dev.vality.payout.manager.domain.tables.pojos.Source;
import dev.vality.payout.manager.domain.tables.records.SourceRecord;
import dev.vality.payout.manager.exception.NotFoundException;
import org.jooq.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Optional;

import static dev.vality.payout.manager.domain.tables.Source.SOURCE;

@Component
public class SourceDaoImpl extends AbstractGenericDao implements SourceDao {

    private final RowMapper<Source> sourceRowMapper;

    @Autowired
    public SourceDaoImpl(DataSource dataSource) {
        super(dataSource);
        sourceRowMapper = new RecordRowMapper<>(SOURCE, Source.class);
    }

    @Override
    public Optional<Long> save(Source source) throws DaoException {
        SourceRecord record = getDslContext().newRecord(SOURCE, source);
        Query query = getDslContext()
                .insertInto(SOURCE)
                .set(record)
                .onConflict(SOURCE.SOURCE_ID)
                .doUpdate()
                .set(record)
                .returning(SOURCE.ID);
        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        execute(query, keyHolder);
        return Optional.ofNullable(keyHolder.getKey()).map(Number::longValue);
    }

    @Override
    public Source get(String sourceId) throws DaoException {
        Query query = getDslContext().selectFrom(SOURCE)
                .where(SOURCE.SOURCE_ID.eq(sourceId));
        return Optional.ofNullable(fetchOne(query, sourceRowMapper))
                .orElseThrow(() -> new NotFoundException(String.format("Source not found, sourceId='%s'", sourceId)));
    }

    @Override
    public Source getAuthorizedByCurrencyCode(String currencyCode) throws DaoException {
        Query query = getDslContext().selectFrom(SOURCE)
                .where(SOURCE.CURRENCY_CODE.eq(currencyCode)
                        .and(SOURCE.STATUS.eq(SourceStatus.AUTHORIZED)));
        return fetch(query, sourceRowMapper).stream()
                .findFirst()
                .orElseThrow(() -> new NotFoundException(
                        String.format("Source not found, currencyCode='%s'", currencyCode)));
    }
}
