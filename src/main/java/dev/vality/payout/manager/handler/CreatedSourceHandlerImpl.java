package dev.vality.payout.manager.handler;

import dev.vality.fistful.source.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;
import dev.vality.payout.manager.dao.SourceDao;
import dev.vality.payout.manager.domain.enums.SourceStatus;
import dev.vality.payout.manager.domain.tables.pojos.Source;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class CreatedSourceHandlerImpl implements SourceHandler {

    private final SourceDao sourceDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        String sourceId = change.getChange().getCreated().getId();
        Source source = new Source();
        source.setSourceId(sourceId);
        source.setStatus(SourceStatus.UNAUTHORIZED);
        sourceDao.save(source).ifPresentOrElse(
                dbContractId -> log.info("Source created has been saved, sourceId={}", sourceId),
                () -> log.info("Source created bound duplicated, sourceId={}", sourceId));
    }
}
