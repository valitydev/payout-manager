package dev.vality.payout.manager.service;

import dev.vality.damsel.accounter.Account;
import dev.vality.damsel.accounter.AccounterSrv;
import dev.vality.damsel.accounter.InvalidPostingParams;
import dev.vality.damsel.accounter.PostingPlanLog;
import dev.vality.payout.manager.config.PostgresqlSpringBootITest;
import dev.vality.payout.manager.domain.tables.pojos.CashFlowPosting;
import dev.vality.payout.manager.exception.AccounterException;
import dev.vality.payout.manager.exception.NotFoundException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static dev.vality.payout.manager.util.ValuesGenerator.generatePayoutId;
import static dev.vality.testcontainers.annotations.util.RandomBeans.randomStreamOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@PostgresqlSpringBootITest
public class ShumwayServiceTest {

    @MockBean
    private AccounterSrv.Iface shumwayClient;

    @Autowired
    private CashFlowPostingService cashFlowPostingService;
    @Autowired
    private ShumwayService shumwayService;

    @Test
    public void shouldHold() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        var postingPlanLog = getPostingPlanLog();
        when(shumwayClient.hold(any())).thenReturn(postingPlanLog);
        assertEquals(
                postingPlanLog,
                shumwayService.hold(payoutId, cashFlowPostings));
    }

    @Test
    public void shouldThrowExceptionAtHoldWhenClientIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        when(shumwayClient.hold(any())).thenThrow(InvalidPostingParams.class);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.hold(payoutId, cashFlowPostings));
    }

    @Test
    public void shouldCommit() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.commitPlan(any())).thenReturn(getPostingPlanLog());
        shumwayService.commit(payoutId);
        verify(shumwayClient, times(1)).commitPlan(any());
    }

    @Test
    public void shouldThrowExceptionAtCommitWhenCashFlowPostingIsNull() {
        String payoutId = generatePayoutId();
        assertThrows(
                NotFoundException.class,
                () -> shumwayService.commit(payoutId));
    }

    @Test
    public void shouldThrowExceptionAtCommitWhenClientIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.commitPlan(any())).thenThrow(InvalidPostingParams.class);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.commit(payoutId));
    }

    @Test
    public void shouldRollback() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.rollbackPlan(any())).thenReturn(getPostingPlanLog());
        shumwayService.rollback(payoutId);
        verify(shumwayClient, times(1)).rollbackPlan(any());
    }

    @Test
    public void shouldThrowExceptionAtRollbackWhenCashFlowPostingIsNull() {
        String payoutId = generatePayoutId();
        assertThrows(
                NotFoundException.class,
                () -> shumwayService.rollback(payoutId));
    }

    @Test
    public void shouldThrowExceptionAtRollbackWhenClientIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.rollbackPlan(any())).thenThrow(InvalidPostingParams.class);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.rollback(payoutId));
    }

    @Test
    public void shouldRevert() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.hold(any())).thenReturn(getPostingPlanLog());
        when(shumwayClient.commitPlan(any())).thenReturn(getPostingPlanLog());
        shumwayService.revert(payoutId);
        verify(shumwayClient, times(0)).rollbackPlan(any());
        verify(shumwayClient, times(1)).hold(any());
        verify(shumwayClient, times(1)).commitPlan(any());
    }

    @Test
    public void shouldThrowExceptionAtRevertWhenCashFlowPostingIsNull() {
        String payoutId = generatePayoutId();
        assertThrows(
                NotFoundException.class,
                () -> shumwayService.revert(payoutId));
    }

    @Test
    public void shouldThrowExceptionAtRevertWhenClientIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.hold(any())).thenThrow(InvalidPostingParams.class);
        when(shumwayClient.commitPlan(any())).thenThrow(InvalidPostingParams.class);
        when(shumwayClient.rollbackPlan(any())).thenThrow(InvalidPostingParams.class);
        assertThrows(
                AccounterException.class,
                () -> shumwayService.revert(payoutId));
    }

    @Test
    public void shouldThrowExceptionAndRollbackAtRevertWhenHoldIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.hold(any())).thenThrow(InvalidPostingParams.class);
        when(shumwayClient.rollbackPlan(any())).thenReturn(getPostingPlanLog());
        assertThrows(
                AccounterException.class,
                () -> shumwayService.revert(payoutId));
        verify(shumwayClient, times(1)).rollbackPlan(any());
    }

    @Test
    public void shouldThrowExceptionAndRollbackAtRevertWhenCommitIssue() throws TException {
        String payoutId = generatePayoutId();
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        cashFlowPostingService.save(cashFlowPostings);
        when(shumwayClient.hold(any())).thenReturn(getPostingPlanLog());
        when(shumwayClient.commitPlan(any())).thenThrow(InvalidPostingParams.class);
        when(shumwayClient.rollbackPlan(any())).thenReturn(getPostingPlanLog());
        assertThrows(
                AccounterException.class,
                () -> shumwayService.revert(payoutId));
        verify(shumwayClient, times(1)).rollbackPlan(any());
    }

    private PostingPlanLog getPostingPlanLog() {
        var account = new Account(1, 1, 1, 1, "RUB");
        return new PostingPlanLog(Map.of(1L, account));
    }
}
