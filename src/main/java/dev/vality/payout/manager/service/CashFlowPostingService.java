package dev.vality.payout.manager.service;

import dev.vality.dao.DaoException;
import dev.vality.payout.manager.dao.CashFlowPostingDao;
import dev.vality.payout.manager.domain.tables.pojos.CashFlowPosting;
import dev.vality.payout.manager.exception.NotFoundException;
import dev.vality.payout.manager.exception.StorageException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class CashFlowPostingService {

    private final CashFlowPostingDao cashFlowPostingDao;

    @Transactional(propagation = Propagation.REQUIRED)
    public void save(List<CashFlowPosting> cashFlowPostings) {
        log.info("Trying to save a CashFlowPosting, cashFlowPostings='{}'", cashFlowPostings.size());
        try {
            cashFlowPostingDao.save(cashFlowPostings);
        } catch (DaoException ex) {
            throw new StorageException(
                    String.format("Failed to save CashFlowPostings, cashFlowPostings='%s'", cashFlowPostings.size()),
                    ex);
        }
    }

    public List<CashFlowPosting> getCashFlowPostings(String payoutId) {
        log.info("Trying to get a CashFlowPosting, payoutId='{}'", payoutId);
        List<CashFlowPosting> cashFlowPostings;
        try {
            cashFlowPostings = cashFlowPostingDao.getByPayoutId(payoutId);
        } catch (DaoException ex) {
            throw new StorageException(String.format("Failed to get a CashFlowPosting, payoutId='%s'", payoutId), ex);
        }
        if (cashFlowPostings.isEmpty()) {
            throw new NotFoundException(
                    String.format("CashFlowPosting not found, payoutId='%s'", payoutId));
        }
        return cashFlowPostings;
    }
}
