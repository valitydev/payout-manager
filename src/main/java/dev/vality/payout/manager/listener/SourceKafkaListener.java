package dev.vality.payout.manager.listener;

import dev.vality.kafka.common.util.LogUtil;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.payout.manager.service.SourceHandlerService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class SourceKafkaListener {

    @Value("${kafka.topic.source.consume.throttling-timeout-ms}")
    private int throttlingTimeout;

    private final SourceHandlerService sourceService;

    @KafkaListener(
            autoStartup = "${kafka.topic.source.consume.enabled}",
            topics = "${kafka.topic.source.name}",
            containerFactory = "sourceContainerFactory")
    public void handle(List<ConsumerRecord<String, SinkEvent>> messages, Acknowledgment ack) {
        log.info("SourceKafkaListener listen offsets, size={}, {}",
                messages.size(), LogUtil.toSummaryStringWithSinkEventValues(messages));
        List<MachineEvent> machineEvents = messages.stream()
                .map(ConsumerRecord::value)
                .map(SinkEvent::getEvent)
                .collect(Collectors.toList());
        handleMessages(machineEvents);
        ack.acknowledge();
        log.info("SourceKafkaListener Records have been committed, size={}, {}",
                messages.size(), LogUtil.toSummaryStringWithSinkEventValues(messages));
    }

    @SneakyThrows
    public void handleMessages(List<MachineEvent> sinkEvents) {
        try {
            sourceService.handleEvents(sinkEvents);
        } catch (Exception e) {
            log.error("Error when SourceKafkaListener listen e: ", e);
            Thread.sleep(throttlingTimeout);
            throw e;
        }
    }
}
