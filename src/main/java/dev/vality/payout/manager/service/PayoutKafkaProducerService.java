package dev.vality.payout.manager.service;

import dev.vality.kafka.common.exception.KafkaProduceException;
import dev.vality.payout.manager.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutKafkaProducerService {

    private final KafkaTemplate<String, Event> kafkaTemplate;

    @Value("${kafka.topic.pm-events-payout.name}")
    private String topicName;

    @Value("${kafka.topic.pm-events-payout.produce.enabled}")
    private boolean producerEnabled;

    public void send(Event event) {
        if (producerEnabled) {
            sendPayout(event);
        }
    }

    private void sendPayout(Event event) {
        try {
            log.info("Try to send payout data to kafka: topicName={}, payoutId={}",
                    topicName, event.getPayoutId());
            kafkaTemplate.send(topicName, event.getPayoutId(), event).get();
            log.info("Payout data to kafka was sent: topicName={}, payoutId={}",
                    topicName, event.getPayoutId());
        } catch (InterruptedException e) {
            log.error("InterruptedException command: {}", event, e);
            Thread.currentThread().interrupt();
            throw new KafkaProduceException(e);
        } catch (Exception e) {
            log.error("Error while sending command: {}", event, e);
            throw new KafkaProduceException(e);
        }
    }
}
