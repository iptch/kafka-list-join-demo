package ch.ipt.jkl.listjoindemo.timestamp.operator;

import ch.ipt.jkl.listjoindemo.proto.Outer;
import com.google.protobuf.Timestamp;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.time.Instant;

public class OuterMapper implements ValueMapper<Outer, Outer> {

    @Override
    public Outer apply(Outer value) {
        if (value == null) return null;

        Instant now = Instant.now();

        return value.toBuilder()
                .setLastEdited(Timestamp.newBuilder()
                        .setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano())
                        .build())
                .build();
    }
}
