package dev.vality.payout.manager.service;

import dev.vality.damsel.accounter.*;
import dev.vality.damsel.base.InvalidRequest;
import dev.vality.payout.manager.domain.tables.pojos.CashFlowPosting;
import dev.vality.payout.manager.exception.AccounterException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShumwayService {

    private final AccounterSrv.Iface shumwayClient;
    private final RetryTemplate retryTemplate;
    private final CashFlowPostingService cashFlowPostingService;

    public PostingPlanLog hold(String payoutId, List<CashFlowPosting> cashFlowPostings) {
        log.debug("Trying to hold payout postings, payoutId='{}', cashFlowPostings='{}'",
                payoutId, cashFlowPostings);
        try {
            var postingPlanId = toPlanId(payoutId);
            var postingBatch = toPostingBatch(cashFlowPostings);
            var postingPlanLog = hold(postingPlanId, postingBatch);
            log.info("Payout has been held, payoutId='{}', postingBatch='{}', postingPlanLog='{}'",
                    payoutId, postingBatch, postingPlanLog);
            return postingPlanLog;
        } catch (Exception ex) {
            throw new AccounterException(String.format("Failed to hold payout, payoutId='%s'", payoutId), ex);
        }
    }

    private PostingPlanLog hold(String postingPlanId, PostingBatch postingBatch) throws TException {
        try {
            log.debug("Start hold operation, postingPlanId='{}', postingBatch='{}'", postingPlanId, postingBatch);
            return retryTemplate.execute(
                    context -> shumwayClient.hold(new PostingPlanChange(postingPlanId, postingBatch)));
        } finally {
            log.debug("End hold operation, postingPlanId='{}', postingBatch='{}'", postingPlanId, postingBatch);
        }
    }

    public void commit(String payoutId) {
        log.debug("Trying to commit payout postings, payoutId='{}'", payoutId);
        var cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payoutId);
        try {
            var postingPlanId = toPlanId(payoutId);
            var postingBatches = List.of(toPostingBatch(cashFlowPostings));
            commit(postingPlanId, postingBatches);
            log.info("Payout has been committed, payoutId='{}', postingBatches='{}'", payoutId, postingBatches);
        } catch (Exception ex) {
            throw new AccounterException(String.format("Failed to commit payout, payoutId='%s'", payoutId), ex);
        }
    }

    public void commit(String postingPlanId, List<PostingBatch> postingBatches) throws TException {
        try {
            log.debug("Start commit operation, postingPlanId='{}', postingBatches='{}'",
                    postingPlanId, postingBatches);
            retryTemplate.execute(
                    context -> shumwayClient.commitPlan(new PostingPlan(postingPlanId, postingBatches)));
        } finally {
            log.debug("End commit operation, postingPlanId='{}', postingBatches='{}'",
                    postingPlanId, postingBatches);
        }
    }

    public void rollback(String payoutId) {
        log.debug("Trying to rollback payout postings, payoutId='{}'", payoutId);
        var cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payoutId);
        try {
            var postingPlanId = toPlanId(payoutId);
            var postingBatches = List.of(toPostingBatch(cashFlowPostings));
            rollback(postingPlanId, postingBatches);
            log.info("Payout has been rolled back, payoutId='{}', postingBatches='{}'", payoutId, postingBatches);
        } catch (Exception ex) {
            throw new AccounterException(String.format("Failed to rollback payout, payoutId='%s'", payoutId), ex);
        }
    }

    public void rollback(String postingPlanId, List<PostingBatch> postingBatches) throws TException {
        try {
            log.debug("Start rollback operation, postingPlanId='{}', postingBatches='{}'",
                    postingPlanId, postingBatches);
            retryTemplate.execute(
                    context -> shumwayClient.rollbackPlan(new PostingPlan(postingPlanId, postingBatches)));
        } finally {
            log.debug("End rollback operation, postingPlanId='{}', postingBatches='{}'",
                    postingPlanId, postingBatches);
        }
    }

    public void revert(String payoutId) {
        log.debug("Trying to revert payout, payoutId='{}'", payoutId);
        var cashFlowPostings = cashFlowPostingService.getCashFlowPostings(payoutId);
        try {
            var revertPlanId = toRevertPlanId(payoutId);
            var revertPostingBatch = revertPostingBatch(
                    toPostingBatch(cashFlowPostings),
                    posting -> {
                        var revertPosting = new Posting();
                        revertPosting.setFromId(posting.getToId());
                        revertPosting.setToId(posting.getFromId());
                        revertPosting.setAmount(posting.getAmount());
                        revertPosting.setCurrencySymCode(posting.getCurrencySymCode());
                        revertPosting.setDescription(posting.getDescription());
                        revertPosting.setDescription("Revert payout: " + payoutId);
                        return revertPosting;
                    });
            revert(revertPlanId, revertPostingBatch);
            log.info("Payout has been reverted, " +
                            "payoutId='{}', revertPostingBatch='{}'",
                    payoutId, revertPostingBatch);
        } catch (Exception ex) {
            throw new AccounterException(String.format("Failed to revert payout, payoutId='%s'", payoutId), ex);
        }
    }

    private void revert(String revertPlanId, PostingBatch revertPostingBatch) throws Exception {
        try {
            log.debug("Start revert operation, revertPlanId='{}', revertPostingBatch='{}'",
                    revertPlanId, revertPostingBatch);
            hold(revertPlanId, revertPostingBatch);
            commit(revertPlanId, List.of(revertPostingBatch));
        } catch (Exception ex) {
            processRollbackRevertWhenError(revertPlanId, List.of(revertPostingBatch), ex);
        } finally {
            log.debug("End revert operation, revertPlanId='{}', revertPostingBatch='{}'",
                    revertPlanId, revertPostingBatch);
        }
    }

    private PostingBatch revertPostingBatch(
            PostingBatch postingBatch,
            Function<Posting, Posting> howToRevert) {
        return new PostingBatch(
                postingBatch.getId(),
                postingBatch.getPostings().stream()
                        .map(howToRevert)
                        .collect(Collectors.toList()));
    }

    private void processRollbackRevertWhenError(
            String revertPlanId,
            List<PostingBatch> revertPostingBatches,
            Exception parent) throws Exception {
        try {
            rollback(revertPlanId, revertPostingBatches);
        } catch (Exception ex) {
            if (!(ex instanceof InvalidRequest)) {
                log.error("Inconsistent state of postings in shumway, revertPlanId='{}', revertPostingBatches='{}'",
                        revertPlanId, revertPostingBatches, ex);
            }
            var rollbackEx = new RuntimeException(
                    String.format("Failed to rollback postings from revert action, " +
                                    "revertPlanId='%s', revertPostingBatches='%s'",
                            revertPlanId, revertPostingBatches),
                    ex);
            rollbackEx.addSuppressed(parent);
            throw rollbackEx;
        }
        throw parent;
    }

    private PostingBatch toPostingBatch(List<CashFlowPosting> postings) {
        return new PostingBatch(
                1L,
                postings.stream()
                        .map(this::toPosting)
                        .collect(Collectors.toList()));
    }

    private Posting toPosting(CashFlowPosting cashFlowPosting) {
        var posting = new Posting();
        posting.setFromId(cashFlowPosting.getFromAccountId());
        posting.setToId(cashFlowPosting.getToAccountId());
        posting.setAmount(cashFlowPosting.getAmount());
        posting.setCurrencySymCode(cashFlowPosting.getCurrencyCode());
        posting.setDescription(buildPostingDescription(
                cashFlowPosting.getPayoutId(),
                cashFlowPosting.getDescription()));
        return posting;
    }

    private String buildPostingDescription(String payoutId, String description) {
        if (description == null) {
            return "PAYOUT-" + payoutId;
        }
        return description;
    }

    private String toPlanId(String payoutId) {
        return "payout_" + payoutId;
    }

    private String toRevertPlanId(String payoutId) {
        return "revert_" + toPlanId(payoutId);
    }
}
