package ch.ipt.jkl.listjoindemo.timestamp.operator;

import ch.ipt.jkl.listjoindemo.proto.Inner;
import ch.ipt.jkl.listjoindemo.proto.Outer;
import com.google.protobuf.Timestamp;
import org.apache.kafka.streams.kstream.Reducer;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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
                    .clearInner()
                    .addAllInner(deduplicate(List.of(value.getInnerList(), other.getInnerList())))
                    .build();
        }
    }

    private static int compareTimestamp(Timestamp timestamp1, Timestamp timestamp2) {
        int secondsDiff = Long.compare(timestamp1.getSeconds(), timestamp2.getSeconds());
        return (secondsDiff != 0) ? secondsDiff : Integer.compare(timestamp1.getNanos(), timestamp2.getNanos());
    }

    private static Collection<Inner> deduplicate(List<List<Inner>> lists) {
        return lists.stream()
                .flatMap(List::stream)
                .collect(Collectors.toMap(
                        Inner::getId,
                        Function.identity(),
                        (left, right) -> left.toBuilder().mergeFrom(right).build()
                ))
                .values();
    }
}
