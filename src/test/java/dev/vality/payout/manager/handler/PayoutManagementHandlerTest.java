package dev.vality.payout.manager.handler;

import dev.vality.damsel.base.InvalidRequest;
import dev.vality.damsel.domain.Cash;
import dev.vality.damsel.domain.CurrencyRef;
import com.rbkmoney.kafka.common.exception.KafkaProduceException;
import dev.vality.payout.manager.InsufficientFunds;
import dev.vality.payout.manager.PayoutParams;
import dev.vality.payout.manager.ShopParams;
import dev.vality.payout.manager.domain.tables.pojos.CashFlowPosting;
import dev.vality.payout.manager.domain.tables.pojos.Payout;
import dev.vality.payout.manager.exception.InsufficientFundsException;
import dev.vality.payout.manager.exception.InvalidRequestException;
import dev.vality.payout.manager.service.CashFlowPostingService;
import dev.vality.payout.manager.service.PayoutKafkaProducerService;
import dev.vality.payout.manager.service.PayoutService;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.stream.Collectors;

import static com.rbkmoney.payout.manager.util.ValuesGenerator.generatePayoutId;
import static dev.vality.testcontainers.annotations.util.RandomBeans.random;
import static dev.vality.testcontainers.annotations.util.RandomBeans.randomStreamOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
        classes =
                PayoutManagementHandler.class,
        initializers = PayoutManagementHandlerTest.Initializer.class
)
@TestPropertySource("classpath:application.yml")
@DirtiesContext
public class PayoutManagementHandlerTest {

    @MockBean
    private PayoutService payoutService;
    @MockBean
    private CashFlowPostingService cashFlowPostingService;
    @MockBean
    private PayoutKafkaProducerService payoutKafkaProducerService;

    @Autowired
    private PayoutManagementHandler payoutManagementHandler;

    @Test
    public void shouldCreate() throws TException {
        String payoutId = generatePayoutId();
        when(payoutService.create(anyString(), anyString(), any(), isNull(), isNull())).thenReturn(payoutId);
        Payout payout = random(Payout.class);
        when(payoutService.get(eq(payoutId))).thenReturn(payout);
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        when(cashFlowPostingService.getCashFlowPostings(anyString())).thenReturn(cashFlowPostings);
        doNothing().when(payoutKafkaProducerService).send(any());
        PayoutParams payoutParams = new PayoutParams(
                new ShopParams("partyId", "shopId"),
                new Cash(100L,
                        new CurrencyRef("RUB")));
        assertNotNull(payoutManagementHandler.createPayout(payoutParams));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenInsufficientFundsIssue() {
        when(payoutService.create(anyString(), anyString(), any(), isNull(), isNull()))
                .thenThrow(InsufficientFundsException.class);
        PayoutParams payoutParams = new PayoutParams(
                new ShopParams("partyId", "shopId"),
                new Cash(100L,
                        new CurrencyRef("RUB")));
        assertThrows(
                InsufficientFunds.class,
                () -> payoutManagementHandler.createPayout(payoutParams));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenPayoutToolIdIsNull() {
        when(payoutService.create(anyString(), anyString(), any(), isNull(), isNull()))
                .thenThrow(InvalidRequestException.class);
        PayoutParams payoutParams = new PayoutParams(
                new ShopParams("partyId", "shopId"),
                new Cash(100L,
                        new CurrencyRef("RUB")));
        assertThrows(
                InvalidRequest.class,
                () -> payoutManagementHandler.createPayout(payoutParams));
    }

    @Test
    public void shouldThrowExceptionAtCreateWhenKafkaIssue() {
        String payoutId = generatePayoutId();
        when(payoutService.create(anyString(), anyString(), any(), isNull(), isNull())).thenReturn(payoutId);
        Payout payout = random(Payout.class);
        when(payoutService.get(eq(payoutId))).thenReturn(payout);
        List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class, "id")
                .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payoutId))
                .collect(Collectors.toList());
        when(cashFlowPostingService.getCashFlowPostings(anyString())).thenReturn(cashFlowPostings);
        doThrow(KafkaProduceException.class).when(payoutKafkaProducerService).send(any());
        PayoutParams payoutParams = new PayoutParams(
                new ShopParams("partyId", "shopId"),
                new Cash(100L,
                        new CurrencyRef("RUB")));
        assertThrows(
                KafkaProduceException.class,
                () -> payoutManagementHandler.createPayout(payoutParams));
    }

    public static class Initializer extends ConfigDataApplicationContextInitializer {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            super.initialize(configurableApplicationContext);
        }
    }
}
