package dev.vality.payout.manager.service;

import dev.vality.damsel.accounter.PostingPlanLog;
import dev.vality.damsel.domain.Cash;
import dev.vality.damsel.domain.Party;
import dev.vality.damsel.domain.Shop;
import dev.vality.dao.DaoException;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.payout.manager.dao.PayoutDao;
import dev.vality.payout.manager.domain.enums.PayoutStatus;
import dev.vality.payout.manager.domain.tables.pojos.Payout;
import dev.vality.payout.manager.exception.*;
import dev.vality.payout.manager.util.CashFlowType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static dev.vality.payout.manager.util.ThriftUtil.parseCashFlow;
import static dev.vality.payout.manager.util.ThriftUtil.toDomainCashFlows;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutService {

    private final ShumwayService shumwayService;
    private final PartyManagementService partyManagementService;
    private final CashFlowPostingService cashFlowPostingService;

    private final PayoutDao payoutDao;

    @Transactional(propagation = Propagation.REQUIRED)
    public String create(String partyId, String shopId, Cash cash, String payoutId, String payoutToolId) {
        log.info("Trying to create a payout, partyId='{}', shopId='{}', payoutId='{}', payoutToolId='{}'",
                partyId, shopId, payoutId, payoutToolId);
        if (cash.getAmount() <= 0) {
            throw new InsufficientFundsException("Available amount must be greater than 0");
        }
        if (payoutId == null) {
            payoutId = UUID.randomUUID().toString();
        } else {
            validatePayoutId(payoutId);
        }
        var party = partyManagementService.getParty(partyId);
        var shop = party.getShops().get(shopId);
        if (shop == null) {
            throw new NotFoundException(String.format("Shop not found, shopId='%s'", shopId));
        }
        if (payoutToolId == null) {
            if (!shop.isSetPayoutToolId()) {
                throw new InvalidRequestException(
                        String.format("PayoutToolId is null with partyId=%s, shopId=%s", partyId, shopId));
            }
            payoutToolId = shop.getPayoutToolId();
        } else {
            validatePayoutToolId(payoutToolId, shop, party);
        }

        var localDateTime = LocalDateTime.now(ZoneOffset.UTC);
        var createdAt = TypeUtil.temporalToString(localDateTime.toInstant(ZoneOffset.UTC));
        var finalCashFlowPostings = partyManagementService.computePayoutCashFlow(
                partyId,
                shopId,
                cash,
                payoutToolId,
                createdAt);
        var cashFlow = parseCashFlow(finalCashFlowPostings);
        var cashFlowAmount = cashFlow.getOrDefault(CashFlowType.PAYOUT_AMOUNT, 0L);
        var cashFlowPayoutFee = cashFlow.getOrDefault(CashFlowType.PAYOUT_FIXED_FEE, 0L);
        var cashFlowFee = cashFlow.getOrDefault(CashFlowType.FEE, 0L);
        var amount = cashFlowAmount - cashFlowPayoutFee;
        var fee = cashFlowFee + cashFlowPayoutFee;
        if (amount <= 0) {
            throw new InsufficientFundsException(
                    String.format("Negative amount in payout cash flow, amount='%d', fee='%d'", amount, fee));
        }
        save(payoutId, localDateTime, partyId, shopId, payoutToolId, amount, fee, cash.getCurrency().getSymbolicCode());
        var cashFlowPostings = toDomainCashFlows(payoutId, localDateTime, finalCashFlowPostings);
        cashFlowPostingService.save(cashFlowPostings);
        var postingPlanLog = shumwayService.hold(payoutId, cashFlowPostings);
        validateAccount(shopId, payoutId, party, postingPlanLog);
        log.info("Payout has been created, payoutId='{}'", payoutId);
        return payoutId;
    }

    private void validatePayoutId(String payoutId) {
        if (payoutDao.get(payoutId) != null) {
            throw new PayoutAlreadyExistsException(String.format("Payout already exists, payoutId='%s'", payoutId));
        }
    }

    private void validatePayoutToolId(String payoutToolId, Shop shop, Party party) {
        var contractId = shop.getContractId();
        var contract = party.getContracts().get(contractId);
        if (contract == null) {
            throw new NotFoundException(String.format("Contract not found, contractId='%s'", contractId));
        }
        contract.getPayoutTools().stream()
                .filter(p -> p.getId().equals(payoutToolId))
                .findAny()
                .orElseThrow(() ->
                        new NotFoundException(String.format("PayoutTool not found, payoutToolId='%s'", payoutToolId)));
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void save(
            String payoutId,
            LocalDateTime createdAt,
            String partyId,
            String shopId,
            String payoutToolId,
            long amount,
            long fee,
            String symbolicCode) {
        log.info("Trying to save a Payout, payoutId='{}'", payoutId);
        try {
            var payout = new Payout();
            payout.setSequenceId(0);
            payout.setPayoutId(payoutId);
            payout.setCreatedAt(createdAt);
            payout.setPartyId(partyId);
            payout.setShopId(shopId);
            payout.setStatus(PayoutStatus.UNPAID);
            payout.setPayoutToolId(payoutToolId);
            payout.setAmount(amount);
            payout.setFee(fee);
            payout.setCurrencyCode(symbolicCode);
            payoutDao.save(payout);
        } catch (DaoException ex) {
            throw new StorageException(String.format("Failed to save Payout, payoutId='%s'", payoutId), ex);
        }
    }

    public Payout get(String payoutId) {
        log.info("Trying to get a Payout, payoutId='{}'", payoutId);
        try {
            var payout = payoutDao.get(payoutId);
            if (payout == null) {
                throw new NotFoundException(
                        String.format("Payout not found, payoutId='%s'", payoutId));
            }
            return payout;
        } catch (DaoException ex) {
            throw new StorageException(String.format("Failed to get a Payout, payoutId='%s'", payoutId), ex);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void confirm(String payoutId) {
        log.info("Trying to confirm a payout, payoutId='{}'", payoutId);
        try {
            var payout = getForUpdate(payoutId);
            if (payout.getStatus() == PayoutStatus.CONFIRMED) {
                log.info("Payout already confirmed, payoutId='{}'", payoutId);
                return;
            } else if (payout.getStatus() != PayoutStatus.UNPAID) {
                throw new InvalidStateException(
                        String.format("Invalid status for 'confirm' action, payoutId='%s', currentStatus='%s'",
                                payoutId, payout.getStatus())
                );
            }
            payoutDao.changeStatus(payoutId, PayoutStatus.CONFIRMED);
            shumwayService.commit(payoutId);
            log.info("Payout has been confirmed, payoutId='{}'", payoutId);
        } catch (DaoException ex) {
            throw new StorageException(String.format("Failed to confirm a payout, payoutId='%s'", payoutId), ex);
        }
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void cancel(String payoutId, String details) {
        log.info("Trying to cancel a payout, payoutId='{}'", payoutId);
        try {
            var payout = getForUpdate(payoutId);
            if (payout.getStatus() == PayoutStatus.CANCELLED) {
                log.info("Payout already cancelled, payoutId='{}'", payoutId);
                return;
            }
            payoutDao.changeStatus(payoutId, PayoutStatus.CANCELLED, details);
            switch (payout.getStatus()) {
                case UNPAID, PAID -> shumwayService.rollback(payoutId);
                case CONFIRMED -> shumwayService.revert(payoutId);
                default -> throw new InvalidStateException(String.format("Invalid status for 'cancel' action, " +
                        "payoutId='%s', currentStatus='%s'", payoutId, payout.getStatus()));
            }
            log.info("Payout has been cancelled, payoutId='{}'", payoutId);
        } catch (DaoException ex) {
            throw new StorageException(String.format("Failed to cancel a payout, payoutId='%s'", payoutId), ex);
        }
    }

    private void validateAccount(String shopId, String payoutId, Party party, PostingPlanLog postingPlanLog) {
        var accountId = party.getShops().get(shopId).getAccount().getSettlement();
        var account = postingPlanLog.getAffectedAccounts().get(accountId);
        if (account == null || account.getMinAvailableAmount() < 0) {
            shumwayService.rollback(payoutId);
            throw new InsufficientFundsException(
                    String.format("Invalid available amount in shop account, account='%s'", account));
        }
    }

    private Payout getForUpdate(String payoutId) {
        log.info("Trying to get a Payout, payoutId='{}'", payoutId);
        try {
            var payout = payoutDao.getForUpdate(payoutId);
            if (payout == null) {
                throw new NotFoundException(
                        String.format("Payout not found, payoutId='%s'", payoutId));
            }
            return payout;
        } catch (DaoException ex) {
            throw new StorageException(String.format("Failed to get a Payout, payoutId='%s'", payoutId), ex);
        }
    }
}
