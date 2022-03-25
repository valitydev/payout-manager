package dev.vality.payout.manager.kafka;

import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.machinegun.eventsink.SinkEvent;
import dev.vality.payout.manager.service.SourceHandlerService;
import dev.vality.testcontainers.annotations.KafkaSpringBootTest;
import dev.vality.testcontainers.annotations.kafka.KafkaTestcontainerSingleton;
import dev.vality.testcontainers.annotations.kafka.config.KafkaProducer;
import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@PostgresqlTestcontainerSingleton
@KafkaTestcontainerSingleton(
        properties = {"kafka.topic.pm-events-payout.produce.enabled=true", "kafka.topic.source.consume.enabled=true"},
        topicsKeys = {"kafka.topic.pm-events-payout.name", "kafka.topic.source.name"})
@KafkaSpringBootTest
public class SourceListenerTest {

    @Value("${kafka.topic.source.name}")
    private String sourceTopicName;

    @MockBean
    private SourceHandlerService sourceService;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @Captor
    private ArgumentCaptor<List<MachineEvent>> arg;

    @Test
    public void shouldSourceEventListen() {
        var message = new MachineEvent();
        message.setCreatedAt(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
        message.setEventId(1L);
        message.setSourceId("source_id");
        message.setSourceNs("source_ns");
        var data = new dev.vality.machinegun.msgpack.Value();
        data.setBin(new byte[0]);
        message.setData(data);
        var sinkEvent = new SinkEvent();
        sinkEvent.setEvent(message);
        testThriftKafkaProducer.send(sourceTopicName, sinkEvent);
        verify(sourceService, timeout(5000).times(1)).handleEvents(arg.capture());
        Assertions.assertEquals(message, arg.getValue().get(0));
    }
}
