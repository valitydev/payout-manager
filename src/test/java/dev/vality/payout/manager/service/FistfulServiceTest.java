package dev.vality.payout.manager.service;

import dev.vality.fistful.SourceNotFound;
import dev.vality.fistful.deposit.DepositState;
import dev.vality.fistful.deposit.ManagementSrv;
import dev.vality.payout.manager.dao.SourceDao;
import dev.vality.payout.manager.domain.enums.SourceStatus;
import dev.vality.payout.manager.domain.tables.pojos.Source;
import dev.vality.payout.manager.exception.NotFoundException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(
        classes = {
                FistfulService.class,
                RetryTemplate.class},
        initializers = FistfulServiceTest.Initializer.class
)
@TestPropertySource("classpath:application.yml")
@DirtiesContext
public class FistfulServiceTest {

    @MockBean
    private ManagementSrv.Iface fistfulDepositClient;
    @MockBean
    private SourceDao sourceDao;

    @Autowired
    private FistfulService fistfulService;

    @Test
    public void shouldCreate() throws TException {
        when(fistfulDepositClient.create(any(), any())).thenReturn(new DepositState());
        when(sourceDao.getAuthorizedByCurrencyCode(any())).thenReturn(
                new Source(0L, "id", SourceStatus.AUTHORIZED, "id"));
        assertNotNull(fistfulService.createDeposit("payoutId", "walletId", 1L, "currencyCode"));
    }

    @Test
    public void shouldThrowException() throws TException {
        when(fistfulDepositClient.create(any(), any())).thenThrow(SourceNotFound.class);
        when(sourceDao.getAuthorizedByCurrencyCode(any())).thenReturn(
                new Source(0L, "id", SourceStatus.AUTHORIZED, "id"));
        assertThrows(NotFoundException.class, () ->
                fistfulService.createDeposit("payoutId", "walletId", 1L, "currencyCode"));
    }

    public static class Initializer extends ConfigDataApplicationContextInitializer {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            super.initialize(configurableApplicationContext);
        }
    }
}
