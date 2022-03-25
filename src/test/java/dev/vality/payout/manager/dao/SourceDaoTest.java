package dev.vality.payout.manager.dao;

import dev.vality.fistful.source.*;
import dev.vality.payout.manager.config.PostgresqlSpringBootITest;
import dev.vality.payout.manager.domain.enums.SourceStatus;
import dev.vality.payout.manager.domain.tables.pojos.Source;
import dev.vality.payout.manager.exception.NotFoundException;
import dev.vality.payout.manager.handler.StatusChangedSourceHandlerImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

import static dev.vality.payout.manager.domain.tables.Source.SOURCE;
import static dev.vality.payout.manager.util.ValuesGenerator.generateSourceId;
import static dev.vality.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@PostgresqlSpringBootITest
public class SourceDaoTest {

    private static final String TABLE_NAME = SOURCE.getSchema().getName() + "." + SOURCE.getName();

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private SourceDao sourceDao;

    @Test
    public void testSaveAndGet() {
        var source = random(Source.class, "id");
        source.setId(sourceDao.save(source).get());
        Source second = new Source(source);
        second.setId(null);
        second.setSourceId(generateSourceId());
        second.setId(sourceDao.save(second).get());
        assertEquals(source, sourceDao.get(source.getSourceId()));
        assertEquals(second, sourceDao.get(second.getSourceId()));
    }

    @Test
    public void testUpdate() {
        var source = new Source();
        source.setSourceId(generateSourceId());
        source.setStatus(SourceStatus.UNAUTHORIZED);
        source.setId(sourceDao.save(source).get());
        assertEquals(source, sourceDao.get(source.getSourceId()));
        var change = new TimestampedChange();
        change.setChange(Change.status(new StatusChange(Status.authorized(new Authorized()))));
        Source update = sourceDao.get(source.getSourceId());
        update.setCurrencyCode("USD");
        update.setStatus(StatusChangedSourceHandlerImpl.getStatus(change));
        update.setId(sourceDao.save(update).get());
        assertEquals(update, sourceDao.get(update.getSourceId()));
        assertEquals(1, JdbcTestUtils.countRowsInTable(jdbcTemplate, TABLE_NAME));
        assertEquals(update, sourceDao.getAuthorizedByCurrencyCode(update.getCurrencyCode()));
        assertThrows(NotFoundException.class, () -> sourceDao.getAuthorizedByCurrencyCode("RUB"));
    }
}
