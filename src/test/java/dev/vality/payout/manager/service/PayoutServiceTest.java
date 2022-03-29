package dev.vality.payout.manager.service;

import dev.vality.damsel.accounter.Account;
import dev.vality.damsel.accounter.PostingPlanLog;
import dev.vality.damsel.domain.*;
import dev.vality.geck.serializer.kit.mock.MockMode;
import dev.vality.geck.serializer.kit.mock.MockTBaseProcessor;
import dev.vality.payout.manager.config.PostgresqlSpringBootITest;
import dev.vality.payout.manager.domain.enums.PayoutStatus;
import dev.vality.payout.manager.domain.tables.pojos.Payout;
import dev.vality.payout.manager.exception.*;
import dev.vality.testcontainers.annotations.util.RandomBeans;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static dev.vality.payout.manager.util.ValuesGenerator.generatePayoutId;
import static dev.vality.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@PostgresqlSpringBootITest
public class PayoutServiceTest {

    private static final String DETAILS = "details";

    @MockBean
    private ShumwayService shumwayService;
    @MockBean
    private PartyManagementService partyManagementService;
    @MockBean
    private FistfulService fistfulService;

    @Autowired
    private CashFlowPostingService cashFlowPostingService;
    @Autowired
    private PayoutService payoutService;

    private MockTBaseProcessor mockTBaseProcessor;

    @BeforeEach
    public void setUp() {
        mockTBaseProcessor = new MockTBaseProcessor(MockMode.ALL, 15, 1);
        mockTBaseProcessor.addFieldHandler(
                structHandler -> structHandler.value(Instant.now().toString()),
                "created_at", "at", "due");
    }

    @Test
    public void shouldCreateAndSave() {
        PayoutTool payoutTool = getPayoutTool();
        payoutTool.setPayoutToolInfo(PayoutToolInfo.wallet_info(new WalletInfo("id12s")));
        Contract contract = getContract();
        contract.getPayoutTools().add(payoutTool);
        String partyId = "partyId";
        Party party = getParty();
        party.setId(partyId);
        party.getContracts().put(contract.getId(), contract);
        String shopId = "shopId";
        Shop shop = getShop();
        shop.setId(shopId);
        shop.setContractId(contract.getId());
        shop.setPayoutToolId(payoutTool.getId());
        shop.setAccount(RandomBeans.randomThriftOnlyRequiredFields(ShopAccount.class));
        party.setShops(Map.of(shopId, shop));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(party);
        FinalCashFlowPosting finalCashFlowPosting = getFinalCashFlowPosting();
        finalCashFlowPosting.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        finalCashFlowPosting.getDestination().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        finalCashFlowPosting.getVolume().setAmount(5L);
        FinalCashFlowPosting returnedPayoutFixedFee = new FinalCashFlowPosting(finalCashFlowPosting);
        returnedPayoutFixedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutFixedFee.getDestination().setAccountType(
                CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutFixedFee.getVolume().setAmount(1L);
        FinalCashFlowPosting returnedFee = new FinalCashFlowPosting(finalCashFlowPosting);
        returnedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedFee.getDestination().setAccountType(CashFlowAccount.system(SystemCashFlowAccount.settlement));
        returnedFee.getVolume().setAmount(1L);
        when(partyManagementService.computePayoutCashFlow(
                eq(partyId),
                eq(shopId),
                any(),
                anyString(),
                anyString()))
                .thenReturn(List.of(finalCashFlowPosting, returnedPayoutFixedFee, returnedFee));
        when(shumwayService.hold(anyString(), anyList())).thenReturn(getPostingPlanLog(shop));
        String payoutId = payoutService.create(
                partyId,
                shopId,
                buildCash(), null, null);
        Payout payout = payoutService.get(payoutId);
        assertEquals(4L, payout.getAmount());
        assertEquals(2L, payout.getFee());
        assertEquals(PayoutStatus.UNPAID, payout.getStatus());
        assertEquals(party.getShops().get(shopId).getPayoutToolId(), payout.getPayoutToolId());
        assertEquals(3L, cashFlowPostingService.getCashFlowPostings(payout.getPayoutId()).size());
        assertNotNull(cashFlowPostingService.getCashFlowPostings(payout.getPayoutId()).stream()
                .filter(cashFlowPosting ->
                        cashFlowPosting.getToAccountId().equals(returnedFee.getDestination().getAccountId()))
                .findFirst()
                .orElse(null));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenNotFound() {
        PayoutTool payoutTool = getPayoutTool();
        payoutTool.setPayoutToolInfo(PayoutToolInfo.wallet_info(new WalletInfo("id12s")));
        Contract contract = getContract();
        contract.getPayoutTools().add(payoutTool);
        String partyId = "partyId";
        Party party = getParty();
        party.setId(partyId);
        party.getContracts().put(contract.getId(), contract);
        String shopId = "shopId";
        Shop shop = getShop();
        shop.setId(shopId);
        shop.setContractId(contract.getId());
        shop.setPayoutToolId(payoutTool.getId());
        shop.setAccount(RandomBeans.randomThriftOnlyRequiredFields(ShopAccount.class));
        party.setShops(Map.of(shopId, shop));
        when(partyManagementService.getParty(eq(partyId))).thenThrow(NotFoundException.class);
        assertThrows(
                NotFoundException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        buildCash(), null, null));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(party);
        when(partyManagementService.computePayoutCashFlow(
                eq(partyId),
                eq(shopId),
                any(),
                anyString(),
                anyString()))
                .thenThrow(NotFoundException.class);
        assertThrows(
                NotFoundException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        buildCash(), null, null));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(buildNullShopParty(partyId));
        assertThrows(
                NotFoundException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        buildCash(), null, null));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(buildNullPayoutTool(partyId));
        assertThrows(
                NotFoundException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        buildCash(), null, "payoutToolId"));

        String payoutId = "payoutId";
        Payout payout = random(Payout.class, "payoutToolInfo");
        payout.setPayoutId(payoutId);
        saveRandomPayout(payout);
        when(partyManagementService.getParty(eq(partyId))).thenReturn(buildParty(partyId));
        assertThrows(
                PayoutAlreadyExistsException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        buildCash(),
                        payoutId,
                        null
                )
        );
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenContractNotFound() {
        PayoutTool payoutTool = getPayoutTool();
        payoutTool.setPayoutToolInfo(PayoutToolInfo.wallet_info(new WalletInfo("id12s")));
        String partyId = "partyId";
        Party party = getParty();
        party.setId(partyId);
        String shopId = "shopId";
        Shop shop = getShop();
        shop.setId(shopId);
        shop.setPayoutToolId(payoutTool.getId());
        shop.setAccount(RandomBeans.randomThriftOnlyRequiredFields(ShopAccount.class));
        party.setShops(Map.of(shopId, shop));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(party);
        FinalCashFlowPosting finalCashFlowPosting = getFinalCashFlowPosting();
        finalCashFlowPosting.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        finalCashFlowPosting.getDestination().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        finalCashFlowPosting.getVolume().setAmount(5L);
        FinalCashFlowPosting returnedPayoutFixedFee = new FinalCashFlowPosting(finalCashFlowPosting);
        returnedPayoutFixedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutFixedFee.getDestination().setAccountType(
                CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutFixedFee.getVolume().setAmount(1L);
        FinalCashFlowPosting returnedFee = new FinalCashFlowPosting(finalCashFlowPosting);
        returnedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedFee.getDestination().setAccountType(CashFlowAccount.system(SystemCashFlowAccount.settlement));
        returnedFee.getVolume().setAmount(1L);
        when(partyManagementService.computePayoutCashFlow(
                eq(partyId),
                eq(shopId),
                any(),
                anyString(),
                anyString()))
                .thenReturn(List.of(finalCashFlowPosting, returnedPayoutFixedFee, returnedFee));
        when(shumwayService.hold(anyString(), anyList())).thenReturn(getPostingPlanLog(shop));
        assertThrows(
                NotFoundException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        buildCash(), null, null));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenPayoutToolIdIsNull() {
        String partyId = "partyId";
        Party party = getParty();
        party.setId(partyId);
        String shopId = "shopId";
        Shop shop = getShop();
        shop.setId(shopId);
        shop.setPayoutToolId(null);
        party.setShops(Map.of(shopId, shop));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(party);
        assertThrows(
                InvalidRequestException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        buildCash(), null, null));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenComputedAmountIsNull() {
        PayoutTool payoutTool = getPayoutTool();
        payoutTool.setPayoutToolInfo(PayoutToolInfo.wallet_info(new WalletInfo("id12s")));
        Contract contract = getContract();
        contract.getPayoutTools().add(payoutTool);
        String partyId = "partyId";
        Party party = getParty();
        party.setId(partyId);
        party.getContracts().put(contract.getId(), contract);
        String shopId = "shopId";
        Shop shop = getShop();
        shop.setId(shopId);
        shop.setContractId(contract.getId());
        shop.setPayoutToolId(payoutTool.getId());
        shop.setAccount(RandomBeans.randomThriftOnlyRequiredFields(ShopAccount.class));
        party.setShops(Map.of(shopId, shop));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(party);
        FinalCashFlowPosting finalCashFlowPosting = getFinalCashFlowPosting();
        finalCashFlowPosting.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        finalCashFlowPosting.getDestination().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        finalCashFlowPosting.getVolume().setAmount(1L);
        FinalCashFlowPosting returnedPayoutFixedFee = new FinalCashFlowPosting(finalCashFlowPosting);
        returnedPayoutFixedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutFixedFee.getDestination().setAccountType(
                CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutFixedFee.getVolume().setAmount(1L);
        FinalCashFlowPosting returnedFee = new FinalCashFlowPosting(finalCashFlowPosting);
        returnedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedFee.getDestination().setAccountType(CashFlowAccount.system(SystemCashFlowAccount.settlement));
        returnedFee.getVolume().setAmount(1L);
        when(partyManagementService.computePayoutCashFlow(
                eq(partyId),
                eq(shopId),
                any(),
                anyString(),
                anyString()))
                .thenReturn(List.of(finalCashFlowPosting, returnedPayoutFixedFee, returnedFee));
        assertThrows(
                InsufficientFundsException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        buildCash(), null, null));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenBalanceAmountIsNegative() {
        PayoutTool payoutTool = getPayoutTool();
        payoutTool.setPayoutToolInfo(PayoutToolInfo.wallet_info(new WalletInfo("id12s")));
        Contract contract = getContract();
        contract.getPayoutTools().add(payoutTool);
        String partyId = "partyId";
        Party party = getParty();
        party.setId(partyId);
        party.getContracts().put(contract.getId(), contract);
        String shopId = "shopId";
        Shop shop = getShop();
        shop.setId(shopId);
        shop.setContractId(contract.getId());
        shop.setPayoutToolId(payoutTool.getId());
        shop.setAccount(RandomBeans.randomThriftOnlyRequiredFields(ShopAccount.class));
        party.setShops(Map.of(shopId, shop));
        when(partyManagementService.getParty(eq(partyId))).thenReturn(party);
        FinalCashFlowPosting finalCashFlowPosting = getFinalCashFlowPosting();
        finalCashFlowPosting.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        finalCashFlowPosting.getDestination().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        finalCashFlowPosting.getVolume().setAmount(5L);
        FinalCashFlowPosting returnedPayoutFixedFee = new FinalCashFlowPosting(finalCashFlowPosting);
        returnedPayoutFixedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.payout));
        returnedPayoutFixedFee.getDestination().setAccountType(
                CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedPayoutFixedFee.getVolume().setAmount(1L);
        FinalCashFlowPosting returnedFee = new FinalCashFlowPosting(finalCashFlowPosting);
        returnedFee.getSource().setAccountType(CashFlowAccount.merchant(MerchantCashFlowAccount.settlement));
        returnedFee.getDestination().setAccountType(CashFlowAccount.system(SystemCashFlowAccount.settlement));
        returnedFee.getVolume().setAmount(1L);
        when(partyManagementService.computePayoutCashFlow(
                eq(partyId),
                eq(shopId),
                any(),
                anyString(),
                anyString()))
                .thenReturn(List.of(finalCashFlowPosting, returnedPayoutFixedFee, returnedFee));
        var account = new Account(shop.getAccount().getSettlement(), 1, 1, 1, "RUB");
        when(shumwayService.hold(anyString(), anyList()))
                .thenReturn(new PostingPlanLog(Map.of(shop.getAccount().getSettlement() - 1L, account)));
        doNothing().when(shumwayService).rollback(anyString());
        assertThrows(
                InsufficientFundsException.class,
                () -> payoutService.create(
                        partyId,
                        shopId,
                        buildCash(), null, null));
    }

    @Test
    public void shouldSaveAndGet() {
        Payout payout = random(Payout.class, "payoutToolInfo");
        saveRandomPayout(payout);
        assertEquals(PayoutStatus.UNPAID, payoutService.get(payout.getPayoutId()).getStatus());
    }

    @Test
    public void shouldThrowExceptionAtGetWhenPayoutNotFound() {
        assertThrows(
                NotFoundException.class,
                () -> payoutService.get(generatePayoutId()));
    }

    @Test
    public void shouldConfirm() {
        Payout payout = random(Payout.class, "payoutToolInfo");
        saveRandomPayout(payout);
        doNothing().when(shumwayService).commit(anyString());
        payoutService.confirm(payout.getPayoutId());
        assertEquals(PayoutStatus.CONFIRMED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).commit(anyString());
        payoutService.confirm(payout.getPayoutId());
        assertEquals(PayoutStatus.CONFIRMED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).commit(anyString());
    }

    @Test
    public void shouldOnConfirmCreateDeposit() {
        Payout payout = random(Payout.class, "payoutToolInfo");
        payout.setPayoutToolInfo(dev.vality.payout.manager.domain.enums.PayoutToolInfo.WALLET_INFO);
        saveRandomPayout(payout);
        doNothing().when(shumwayService).commit(anyString());
        payoutService.confirm(payout.getPayoutId());
        assertEquals(PayoutStatus.CONFIRMED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).commit(anyString());
        payoutService.confirm(payout.getPayoutId());
        assertEquals(PayoutStatus.CONFIRMED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).commit(anyString());
        verify(fistfulService, times(1)).createDeposit(anyString(), anyString(),
                anyLong(), anyString());
    }

    @Test
    public void shouldThrowCreateDepositOnConfirm() {
        Payout payout = random(Payout.class, "payoutToolInfo");
        payout.setPayoutToolInfo(dev.vality.payout.manager.domain.enums.PayoutToolInfo.WALLET_INFO);
        saveRandomPayout(payout);
        doNothing().when(shumwayService).commit(anyString());
        doThrow(RuntimeException.class).when(fistfulService).createDeposit(anyString(), anyString(),
                anyLong(), anyString());
        payoutService.confirm(payout.getPayoutId());
        verify(shumwayService, times(1)).commit(anyString());
        verify(shumwayService, times(1)).revert(anyString());
        Payout payout1 = payoutService.get(payout.getPayoutId());
        assertEquals(PayoutStatus.FAILED, payout1.getStatus());
    }

    @Test
    public void shouldThrowExceptionAtConfirmWhenPayoutNotFound() {
        assertThrows(
                NotFoundException.class,
                () -> payoutService.confirm(generatePayoutId()));
    }

    @Test
    public void shouldThrowExceptionAtConfirmWhenStateIsCancelled() {
        Payout payout = random(Payout.class, "payoutToolInfo");
        saveRandomPayout(payout);
        doNothing().when(shumwayService).commit(anyString());
        doNothing().when(shumwayService).rollback(anyString());
        payoutService.cancel(payout.getPayoutId(), DETAILS);
        assertEquals(PayoutStatus.CANCELLED, payoutService.get(payout.getPayoutId()).getStatus());
        assertThrows(
                InvalidStateException.class,
                () -> payoutService.confirm(payout.getPayoutId()));
    }

    @Test
    public void shouldCancel() {
        Payout payout = random(Payout.class, "payoutToolInfo");
        saveRandomPayout(payout);
        doNothing().when(shumwayService).rollback(anyString());
        payoutService.cancel(payout.getPayoutId(), DETAILS);
        assertEquals(PayoutStatus.CANCELLED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).rollback(anyString());
        verify(shumwayService, times(0)).revert(anyString());
        payoutService.cancel(payout.getPayoutId(), DETAILS);
        assertEquals(PayoutStatus.CANCELLED, payoutService.get(payout.getPayoutId()).getStatus());
        verify(shumwayService, times(1)).rollback(anyString());
        verify(shumwayService, times(0)).revert(anyString());
    }

    @Test
    public void shouldCancelAfterConfirm() {
        Payout payout = random(Payout.class, "payoutToolInfo");
        saveRandomPayout(payout);
        doNothing().when(shumwayService).commit(anyString());
        payoutService.confirm(payout.getPayoutId());
        assertEquals(PayoutStatus.CONFIRMED, payoutService.get(payout.getPayoutId()).getStatus());
        doNothing().when(shumwayService).revert(anyString());
        assertThrows(InvalidStateException.class, () -> payoutService.cancel(payout.getPayoutId(), DETAILS));
    }

    @Test
    public void shouldCancelAfterConfirmWalletPayout() {
        Payout payout = random(Payout.class, "payoutToolInfo");
        payout.setPayoutToolInfo(dev.vality.payout.manager.domain.enums.PayoutToolInfo.WALLET_INFO);
        saveRandomPayout(payout);
        doNothing().when(shumwayService).commit(anyString());
        payoutService.confirm(payout.getPayoutId());
        assertEquals(PayoutStatus.CONFIRMED, payoutService.get(payout.getPayoutId()).getStatus());
        assertThrows(InvalidStateException.class, () -> payoutService.cancel(payout.getPayoutId(), DETAILS));
        verify(shumwayService, times(0)).revert(anyString());
    }

    @Test
    public void shouldThrowExceptionAtCancelWhenPayoutNotFound() {
        assertThrows(
                NotFoundException.class,
                () -> payoutService.cancel(generatePayoutId(), DETAILS));
    }

    private Payout saveRandomPayout(Payout payout) {
        payoutService.save(
                payout.getPayoutId(),
                payout.getCreatedAt(),
                payout.getPartyId(),
                payout.getShopId(),
                payout.getPayoutToolId(),
                payout.getAmount(),
                payout.getFee(),
                payout.getCurrencyCode(),
                payout.getPayoutToolInfo(),
                payout.getWalletId());
        return payout;
    }

    private Shop getShop() {
        return RandomBeans.randomThriftOnlyRequiredFields(Shop.class);
    }

    private Party getParty() {
        return RandomBeans.randomThriftOnlyRequiredFields(Party.class);
    }

    private Contract getContract() {
        return RandomBeans.randomThriftOnlyRequiredFields(Contract.class);
    }

    private PayoutTool getPayoutTool() {
        return RandomBeans.randomThriftOnlyRequiredFields(PayoutTool.class);
    }

    private FinalCashFlowPosting getFinalCashFlowPosting() {
        return RandomBeans.randomThriftOnlyRequiredFields(FinalCashFlowPosting.class);
    }

    private PostingPlanLog getPostingPlanLog(Shop shop) {
        var account = new Account(shop.getAccount().getSettlement(), 1, 1, 1, "RUB");
        return new PostingPlanLog(Map.of(shop.getAccount().getSettlement(), account));
    }

    private Cash buildCash() {
        return new Cash(100L, new CurrencyRef("RUB"));
    }

    private Party buildNullShopParty(String partyId) {
        return new Party().setId(partyId).setShops(Map.of());
    }

    private Party buildNullPayoutTool(String partyId) {
        return new Party()
                .setId(partyId)
                .setShops(Map.of(
                        "shopId",
                        new Shop().setContractId("contractId")))
                .setContracts(Map.of(
                        "contractId",
                        new Contract().setPayoutTools(List.of(new PayoutTool().setId("wrongToolId")))
                ));
    }

    private Party buildParty(String partyId) {
        return new Party()
                .setId(partyId)
                .setShops(Map.of("shopId", new Shop().setPayoutToolId("payoutToolId")));
    }
}
