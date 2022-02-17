package dev.vality.payout.manager.config;

import dev.vality.kafka.common.serialization.AbstractThriftDeserializer;
import dev.vality.payout.manager.Event;
import dev.vality.testcontainers.annotations.kafka.config.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Bean
    public KafkaConsumer<Event> testPayoutEventKafkaConsumer() {
        return new KafkaConsumer<>(bootstrapAddress, new PayoutEventDeserializer());
    }

    public static class PayoutEventDeserializer extends AbstractThriftDeserializer<Event> {

        @Override
        public Event deserialize(String s, byte[] bytes) {
            return super.deserialize(bytes, new Event());
        }
    }
}