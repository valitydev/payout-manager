package dev.vality.payout.manager.config;

import dev.vality.damsel.accounter.AccounterSrv;
import dev.vality.damsel.payment_processing.PartyManagementSrv;
import dev.vality.fistful.deposit.ManagementSrv;
import dev.vality.woody.thrift.impl.http.THSpawnClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;

@Configuration
public class ApplicationConfig {

    @Bean
    public AccounterSrv.Iface shumwayClient(
            @Value("${service.shumway.url}") Resource resource,
            @Value("${service.shumway.networkTimeout}") int networkTimeout
    ) throws IOException {
        return new THSpawnClientBuilder()
                .withAddress(resource.getURI())
                .withNetworkTimeout(networkTimeout)
                .build(AccounterSrv.Iface.class);
    }

    @Bean
    public PartyManagementSrv.Iface partyManagementClient(
            @Value("${service.partyManagement.url}") Resource resource,
            @Value("${service.partyManagement.networkTimeout}") int networkTimeout
    ) throws IOException {
        return new THSpawnClientBuilder()
                .withNetworkTimeout(networkTimeout)
                .withAddress(resource.getURI()).build(PartyManagementSrv.Iface.class);
    }

    @Bean
    public ManagementSrv.Iface fistfulDepositClient(
            @Value("${service.fistful.deposit.url}") Resource resource,
            @Value("${service.fistful.deposit.networkTimeout}") int networkTimeout
    ) throws IOException {
        return new THSpawnClientBuilder()
                .withNetworkTimeout(networkTimeout)
                .withAddress(resource.getURI()).build(ManagementSrv.Iface.class);
    }
}
