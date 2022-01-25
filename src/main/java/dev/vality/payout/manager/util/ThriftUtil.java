package dev.vality.payout.manager.util;

import dev.vality.damsel.domain.*;
import dev.vality.geck.common.util.TypeUtil;
import dev.vality.payout.manager.*;
import dev.vality.payout.manager.domain.enums.AccountType;
import dev.vality.payout.manager.domain.tables.pojos.CashFlowPosting;
import dev.vality.payout.manager.exception.NotFoundException;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ThriftUtil {

    public static Map<CashFlowType, Long> parseCashFlow(List<FinalCashFlowPosting> finalCashFlow) {
        return finalCashFlow.stream()
                .collect(Collectors.groupingBy(CashFlowType::getCashFlowType,
                        Collectors.summingLong(cashFlow -> cashFlow.getVolume().getAmount())));
    }

    public static Event createEvent(
            com.rbkmoney.payout.manager.domain.tables.pojos.Payout payout,
            List<CashFlowPosting> cashFlowPostings) {
        Integer sequenceId = payout.getSequenceId();
        PayoutChange payoutChange;
        if (sequenceId == 0) {
            payoutChange = PayoutChange.created(
                    new PayoutCreated(toThriftPayout(payout, cashFlowPostings)));
        } else {
            payoutChange = PayoutChange.status_changed(
                    new PayoutStatusChanged(
                            toThriftPayoutStatus(payout.getStatus(), payout.getCancelDetails())));
        }
        return new Event()
                .setPayoutId(payout.getPayoutId())
                .setSequenceId(sequenceId)
                .setCreatedAt(TypeUtil.temporalToString(LocalDateTime.now(ZoneOffset.UTC).toInstant(ZoneOffset.UTC)))
                .setPayoutChange(payoutChange)
                .setPayout(toThriftPayout(payout, cashFlowPostings));
    }

    public static Payout toThriftPayout(
            com.rbkmoney.payout.manager.domain.tables.pojos.Payout payout,
            List<CashFlowPosting> cashFlowPostings) {
        return new Payout()
                .setPayoutId(payout.getPayoutId())
                .setCreatedAt(TypeUtil.temporalToString(payout.getCreatedAt().toInstant(ZoneOffset.UTC)))
                .setPartyId(payout.getPartyId())
                .setShopId(payout.getShopId())
                .setStatus(toThriftPayoutStatus(payout.getStatus(), payout.getCancelDetails()))
                .setCashFlow(toThriftCashFlows(cashFlowPostings))
                .setPayoutToolId(payout.getPayoutToolId())
                .setAmount(payout.getAmount())
                .setFee(payout.getFee())
                .setCurrency(new CurrencyRef(payout.getCurrencyCode()));
    }

    public static List<CashFlowPosting> toDomainCashFlows(
            String payoutId,
            LocalDateTime createdAt,
            List<FinalCashFlowPosting> cashFlowPostings) {
        return cashFlowPostings.stream()
                .map(finalCashFlowPosting -> {
                    CashFlowPosting cashFlowPosting = new CashFlowPosting();
                    cashFlowPosting.setPayoutId(payoutId);
                    cashFlowPosting.setCreatedAt(createdAt);
                    FinalCashFlowAccount source = finalCashFlowPosting.getSource();
                    cashFlowPosting.setFromAccountId(source.getAccountId());
                    cashFlowPosting.setFromAccountType(toAccountType(source.getAccountType()));
                    FinalCashFlowAccount destination = finalCashFlowPosting.getDestination();
                    cashFlowPosting.setToAccountId(destination.getAccountId());
                    cashFlowPosting.setToAccountType(toAccountType(destination.getAccountType()));
                    cashFlowPosting.setAmount(finalCashFlowPosting.getVolume().getAmount());
                    cashFlowPosting.setCurrencyCode(finalCashFlowPosting.getVolume().getCurrency().getSymbolicCode());
                    cashFlowPosting.setDescription(finalCashFlowPosting.getDetails());
                    return cashFlowPosting;
                })
                .collect(Collectors.toList());
    }

    private static PayoutStatus toThriftPayoutStatus(
            com.rbkmoney.payout.manager.domain.enums.PayoutStatus payoutStatus,
            String cancelDetails) {
        switch (payoutStatus) {
            case UNPAID:
                return PayoutStatus.unpaid(new PayoutUnpaid());
            case PAID:
                return PayoutStatus.paid(new PayoutPaid());
            case CONFIRMED:
                return PayoutStatus.confirmed(new PayoutConfirmed());
            case CANCELLED:
                return PayoutStatus.cancelled(new PayoutCancelled(cancelDetails));
            default:
                throw new NotFoundException(String.format("Payout status not found, status = %s", payoutStatus));
        }
    }

    private static List<FinalCashFlowPosting> toThriftCashFlows(
            List<CashFlowPosting> cashFlowPostings) {
        return cashFlowPostings.stream()
                .map(cfp -> new FinalCashFlowPosting(
                        new FinalCashFlowAccount(
                                toAccountType(cfp.getFromAccountType()), cfp.getFromAccountId()),
                        new FinalCashFlowAccount(
                                toAccountType(cfp.getToAccountType()), cfp.getToAccountId()),
                        new Cash(cfp.getAmount(),
                                new CurrencyRef(cfp.getCurrencyCode())))
                        .setDetails(cfp.getDescription()))
                .collect(Collectors.toList());
    }

    private static AccountType toAccountType(CashFlowAccount cashFlowAccount) {
        var cashFlowAccountType = cashFlowAccount.getSetField();
        switch (cashFlowAccountType) {
            case SYSTEM:
                if (cashFlowAccount.getSystem() == SystemCashFlowAccount.settlement) {
                    return AccountType.SYSTEM_SETTLEMENT;
                }
                throw new IllegalArgumentException();
            case EXTERNAL:
                switch (cashFlowAccount.getExternal()) {
                    case income:
                        return AccountType.EXTERNAL_INCOME;
                    case outcome:
                        return AccountType.EXTERNAL_OUTCOME;
                    default:
                        throw new IllegalArgumentException();
                }
            case MERCHANT:
                switch (cashFlowAccount.getMerchant()) {
                    case settlement:
                        return AccountType.MERCHANT_SETTLEMENT;
                    case guarantee:
                        return AccountType.MERCHANT_GUARANTEE;
                    case payout:
                        return AccountType.MERCHANT_PAYOUT;
                    default:
                        throw new IllegalArgumentException();
                }
            case PROVIDER:
                if (cashFlowAccount.getProvider() == ProviderCashFlowAccount.settlement) {
                    return AccountType.PROVIDER_SETTLEMENT;
                }
                throw new IllegalArgumentException();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static CashFlowAccount toAccountType(AccountType accountType) {
        switch (accountType) {
            case EXTERNAL_INCOME:
                return CashFlowAccount.external(
                        ExternalCashFlowAccount.income);
            case EXTERNAL_OUTCOME:
                return CashFlowAccount.external(
                        ExternalCashFlowAccount.outcome);
            case MERCHANT_PAYOUT:
                return CashFlowAccount.merchant(
                        MerchantCashFlowAccount.payout);
            case MERCHANT_GUARANTEE:
                return CashFlowAccount.merchant(
                        MerchantCashFlowAccount.guarantee);
            case MERCHANT_SETTLEMENT:
                return CashFlowAccount.merchant(
                        MerchantCashFlowAccount.settlement);
            case SYSTEM_SETTLEMENT:
                return CashFlowAccount.system(
                        SystemCashFlowAccount.settlement);
            case PROVIDER_SETTLEMENT:
                return CashFlowAccount.provider(
                        ProviderCashFlowAccount.settlement);
            default:
                throw new IllegalArgumentException();
        }
    }
}
