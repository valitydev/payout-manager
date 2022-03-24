package dev.vality.payout.manager.handler;

import dev.vality.fistful.source.TimestampedChange;
import dev.vality.machinegun.eventsink.MachineEvent;

public interface SourceHandler {

    boolean accept(TimestampedChange change);

    void handle(TimestampedChange change, MachineEvent event);

}
