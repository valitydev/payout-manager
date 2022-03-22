package dev.vality.payout.manager.service;

import dev.vality.fistful.DestinationNotFound;
import dev.vality.fistful.SourceNotFound;
import dev.vality.fistful.admin.DepositParams;
import dev.vality.fistful.admin.FistfulAdminSrv;
import dev.vality.fistful.base.Cash;
import dev.vality.fistful.base.CurrencyRef;
import dev.vality.fistful.deposit.Deposit;
import dev.vality.payout.manager.exception.NotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class FistfulService {

    private final FistfulAdminSrv.Iface fistfulAdminClient;

    private final RetryTemplate retryTemplate;

    @Value("${service.fistful.sourceId}")
    private String defaultSourceId;

    public Deposit createDeposit(String payoutId, String walletId, long amount, String currencyCode) {
        DepositParams depositParams = new DepositParams();
        depositParams.setId(toDepositId(payoutId));
        depositParams.setSource(defaultSourceId);
        depositParams.setDestination(walletId);
        depositParams.setBody(new Cash(amount, new CurrencyRef(currencyCode)));
        log.info("Trying to create deposit, depositParams='{}'", depositParams);
        try {
            Deposit deposit = retryTemplate.execute(
                    context -> fistfulAdminClient.createDeposit(depositParams));
            log.info("Deposit have been created, deposit='{}'", deposit);
            return deposit;
        } catch (SourceNotFound | DestinationNotFound ex) {
            throw new NotFoundException(ex);
        } catch (TException ex) {
            throw new RuntimeException(ex);
        }
    }

    private String toDepositId(String payoutId) {
        return "payout_" + payoutId;
    }
}
