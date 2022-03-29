package dev.vality.payout.manager.serde;

import dev.vality.fistful.source.TimestampedChange;
import dev.vality.sink.common.parser.impl.MachineEventParser;
import org.springframework.stereotype.Service;

@Service
public class SourceChangeMachineEventParser extends MachineEventParser<TimestampedChange> {

    public SourceChangeMachineEventParser(TimestampedChangeDeserializer deserializer) {
        super(deserializer);
    }
}