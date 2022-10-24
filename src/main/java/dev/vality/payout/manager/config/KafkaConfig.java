package dev.vality.payout.manager.config;

import dev.vality.kafka.common.serialization.ThriftSerializer;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.payout.manager.Event;
import dev.vality.payout.manager.serde.SinkEventDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;

import java.util.Map;

import static org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST;

@Configuration
public class KafkaConfig {

    @Value("${kafka.topic.source.consume.concurrency}")
    private int sourceConcurrency;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SinkEvent> sourceContainerFactory(
            KafkaProperties kafkaProperties) {
        var containerFactory = new ConcurrentKafkaListenerContainerFactory<String, SinkEvent>();
        configureContainerFactory(
                containerFactory,
                new SinkEventDeserializer(),
                kafkaProperties);
        containerFactory.setConcurrency(sourceConcurrency);
        return containerFactory;
    }

    @Bean
    public KafkaTemplate<String, Event> kafkaTemplate(KafkaProperties kafkaProperties) {
        return new KafkaTemplate<>(producerFactory(kafkaProperties));
    }

    private <T> void configureContainerFactory(
            ConcurrentKafkaListenerContainerFactory<String, T> containerFactory,
            Deserializer<T> deserializer,
            KafkaProperties kafkaProperties) {
        var consumerFactory = createKafkaConsumerFactory(
                deserializer,
                kafkaProperties);
        containerFactory.setConsumerFactory(consumerFactory);
        containerFactory.setBatchErrorHandler(new SeekToCurrentBatchErrorHandler());
        containerFactory.setBatchListener(true);
        containerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    }

    private ProducerFactory<String, Event> producerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> config = kafkaProperties.buildProducerProperties();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ThriftSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    private <T> DefaultKafkaConsumerFactory<String, T> createKafkaConsumerFactory(
            Deserializer<T> deserializer,
            KafkaProperties kafkaProperties) {
        Map<String, Object> properties = defaultProperties(kafkaProperties);
        return new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), deserializer);
    }

    private Map<String, Object> defaultProperties(KafkaProperties kafkaProperties) {
        Map<String, Object> properties = kafkaProperties.buildConsumerProperties();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaProperties.getConsumer().getClientId() + "-source");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST.name().toLowerCase());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return properties;
    }
}
