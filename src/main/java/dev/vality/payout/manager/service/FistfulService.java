package dev.vality.payout.manager.service;

import dev.vality.fistful.SourceNotFound;
import dev.vality.fistful.WalletNotFound;
import dev.vality.fistful.base.Cash;
import dev.vality.fistful.base.CurrencyRef;
import dev.vality.fistful.deposit.DepositParams;
import dev.vality.fistful.deposit.DepositState;
import dev.vality.fistful.deposit.ManagementSrv;
import dev.vality.payout.manager.dao.SourceDao;
import dev.vality.payout.manager.exception.NotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

import java.util.HashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class FistfulService {

    private final ManagementSrv.Iface fistfulDepositClient;
    private final SourceDao sourceDao;

    public DepositState createDeposit(String payoutId, String walletId, long amount, String currencyCode) {
        DepositParams depositParams = new DepositParams();
        depositParams.setId(toDepositId(payoutId));
        depositParams.setWalletId(walletId);
        depositParams.setSourceId(sourceDao.getAuthorizedByCurrencyCode(currencyCode).getSourceId());
        depositParams.setBody(new Cash(amount, new CurrencyRef(currencyCode)));
        log.info("Trying to create deposit, depositParams='{}'", depositParams);
        try {
            DepositState depositState = fistfulDepositClient.create(depositParams, new HashMap<>());
            log.info("Deposit have been created, deposit='{}'", depositState);
            return depositState;
        } catch (WalletNotFound | SourceNotFound ex) {
            throw new NotFoundException(ex);
        } catch (TException ex) {
            throw new RuntimeException(ex);
        }
    }

    private String toDepositId(String payoutId) {
        return "payout_" + payoutId;
    }
}
