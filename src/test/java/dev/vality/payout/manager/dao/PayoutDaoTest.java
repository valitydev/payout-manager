package dev.vality.payout.manager.dao;

import dev.vality.payout.manager.config.PostgresqlSpringBootITest;
import dev.vality.payout.manager.domain.enums.PayoutStatus;
import dev.vality.payout.manager.domain.tables.pojos.Payout;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static dev.vality.payout.manager.util.ValuesGenerator.generatePayoutId;
import static dev.vality.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.*;

@PostgresqlSpringBootITest
public class PayoutDaoTest {

    @Autowired
    private PayoutDao payoutDao;

    @Test
    public void testSaveAndGet() {
        Payout payout = random(Payout.class, "id");
        payout.setPayoutId(generatePayoutId());
        payout.setId(payoutDao.save(payout));
        Payout second = new Payout(payout);
        second.setId(null);
        second.setPayoutId(generatePayoutId());
        second.setId(payoutDao.save(second));
        assertEquals(payout, payoutDao.get(payout.getPayoutId()));
        assertEquals(second, payoutDao.get(second.getPayoutId()));
    }

    @Test
    public void shouldSetCancelDetails() {
        Payout payout = random(Payout.class, "id", "cancelDetails");
        payout.setPayoutId(generatePayoutId());
        payout.setId(payoutDao.save(payout));
        assertNull(payoutDao.get(payout.getPayoutId()).getCancelDetails());
        payoutDao.changeStatus(payout.getPayoutId(), PayoutStatus.CANCELLED, "asd");
        assertNotNull(payoutDao.get(payout.getPayoutId()).getCancelDetails());
    }

    @Test
    public void shouldIncrementSequenceIdAtChangeStatus() {
        Payout payout = random(Payout.class, "id", "sequenceId");
        payout.setPayoutId(generatePayoutId());
        payout.setSequenceId(0);
        payoutDao.save(payout);
        assertEquals(0, payoutDao.get(payout.getPayoutId()).getSequenceId());
        payoutDao.changeStatus(payout.getPayoutId(), PayoutStatus.UNPAID);
        payoutDao.changeStatus(payout.getPayoutId(), PayoutStatus.CANCELLED);
        payoutDao.changeStatus(payout.getPayoutId(), PayoutStatus.CONFIRMED);
        assertEquals(
                PayoutStatus.CONFIRMED,
                payoutDao.get(payout.getPayoutId()).getStatus());
        assertEquals(
                3,
                payoutDao.get(payout.getPayoutId()).getSequenceId());

    }
}
