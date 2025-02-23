package ch.ipt.jkl.listjoindemo.timestamp.operator;

import ch.ipt.jkl.listjoindemo.proto.Outer;
import com.google.protobuf.Timestamp;
import org.apache.kafka.streams.kstream.Reducer;

public class OuterReducer implements Reducer<Outer> {

    @Override
    public Outer apply(Outer value, Outer other) {
        // return the newer one, or merge if they have the same timestamp
        if (compareTimestamp(value.getLastEdited(), other.getLastEdited()) > 0) {
            return value;
        } else if (compareTimestamp(value.getLastEdited(), other.getLastEdited()) < 0) {
            return other;
        } else {
            return value.toBuilder()
                    .addAllInner(other.getInnerList())
                    .build();
        }
    }

    private static int compareTimestamp(Timestamp timestamp1, Timestamp timestamp2) {
        int secondsDiff = Long.compare(timestamp1.getSeconds(), timestamp2.getSeconds());
        return (secondsDiff != 0) ? secondsDiff : Integer.compare(timestamp1.getNanos(), timestamp2.getNanos());
    }
}
