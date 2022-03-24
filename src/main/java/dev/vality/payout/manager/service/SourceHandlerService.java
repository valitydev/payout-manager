package dev.vality.payout.manager.service;

import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.payout.manager.handler.SourceHandler;
import dev.vality.payout.manager.serde.SourceChangeMachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SourceHandlerService {

    private final SourceChangeMachineEventParser parser;
    private final List<SourceHandler> payoutHandlers;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleEvents(List<MachineEvent> machineEvents) {
        machineEvents.forEach(this::handleIfAccept);
    }

    private void handleIfAccept(MachineEvent event) {
        var timestampedChange = parser.parse(event);
        payoutHandlers.stream()
                .filter(handler -> handler.accept(timestampedChange))
                .forEach(handler -> handler.handle(timestampedChange, event));
    }
}
