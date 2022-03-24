package dev.vality.payout.manager.handler;

import dev.vality.fistful.source.TimestampedChange;
import dev.vality.geck.common.util.TBaseUtil;
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
public class StatusChangedSourceHandlerImpl implements SourceHandler {

    private final SourceDao sourceDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetStatus();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        String sourceId = event.getSourceId();
        Source source = sourceDao.get(sourceId);
        source.setStatus(TBaseUtil.unionFieldToEnum(change.getChange().getStatus().getStatus(), SourceStatus.class));
        sourceDao.save(source).ifPresentOrElse(
                dbContractId -> log.info("Source by status change has been saved, sourceId={}", sourceId),
                () -> log.info("Source by status change  created bound duplicated, sourceId={}", sourceId));
    }
}
