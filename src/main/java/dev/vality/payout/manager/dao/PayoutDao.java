package dev.vality.payout.manager.dao;

import com.rbkmoney.dao.DaoException;
import dev.vality.payout.manager.domain.enums.PayoutStatus;
import dev.vality.payout.manager.domain.tables.pojos.Payout;

public interface PayoutDao {

    Payout get(String payoutId) throws DaoException;

    Payout getForUpdate(String payoutId) throws DaoException;

    long save(Payout payout) throws DaoException;

    default void changeStatus(String payoutId, PayoutStatus payoutStatus) throws DaoException {
        changeStatus(payoutId, payoutStatus, null);
    }

    void changeStatus(String payoutId, PayoutStatus payoutStatus, String cancelDetails) throws DaoException;

}
