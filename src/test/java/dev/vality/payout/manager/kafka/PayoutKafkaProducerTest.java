package dev.vality.payout.manager.kafka;

import dev.vality.payout.manager.Event;
import dev.vality.payout.manager.config.KafkaConsumerConfig;
import dev.vality.payout.manager.domain.tables.pojos.CashFlowPosting;
import dev.vality.payout.manager.domain.tables.pojos.Payout;
import dev.vality.payout.manager.service.PayoutKafkaProducerService;
import dev.vality.payout.manager.util.ThriftUtil;
import dev.vality.testcontainers.annotations.KafkaSpringBootTest;
import dev.vality.testcontainers.annotations.kafka.KafkaTestcontainerSingleton;
import dev.vality.testcontainers.annotations.kafka.config.KafkaConsumer;
import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static dev.vality.testcontainers.annotations.util.RandomBeans.random;
import static dev.vality.testcontainers.annotations.util.RandomBeans.randomStreamOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlTestcontainerSingleton
@KafkaTestcontainerSingleton(
        properties = {"kafka.topic.pm-events-payout.produce.enabled=true", "kafka.topic.source.consume.enabled=true"},
        topicsKeys = {"kafka.topic.pm-events-payout.name", "kafka.topic.source.name"})
@KafkaSpringBootTest
@Import(KafkaConsumerConfig.class)
public class PayoutKafkaProducerTest {

    private static final int TIMEOUT = 5;

    @Value("${kafka.topic.pm-events-payout.name}")
    private String topicName;

    @Autowired
    private PayoutKafkaProducerService payoutKafkaProducerService;

    @Autowired
    private KafkaConsumer<Event> testPayoutEventKafkaConsumer;

    @Test
    public void shouldProduceEvents() {
        int expected = 4;
        for (int i = 0; i < expected; i++) {
            Integer id = i + 1;
            Payout payout = random(Payout.class);
            payout.setPayoutId(String.valueOf(id));
            payout.setSequenceId(i);
            List<CashFlowPosting> cashFlowPostings = randomStreamOf(4, CashFlowPosting.class)
                    .peek(cashFlowPosting -> cashFlowPosting.setPayoutId(payout.getPayoutId()))
                    .collect(Collectors.toList());
            Event event = ThriftUtil.createEvent(payout, cashFlowPostings);
            payoutKafkaProducerService.send(event);
        }
        List<Event> readEvents = new ArrayList<>();
        testPayoutEventKafkaConsumer.read(topicName, data -> readEvents.add(data.value()));
        Unreliables.retryUntilTrue(TIMEOUT, TimeUnit.SECONDS, () -> readEvents.size() == expected);
        for (int i = 0; i < expected; i++) {
            Integer id = i + 1;
            assertEquals(String.valueOf(id), readEvents.get(i).getPayoutId());
        }
    }
}
