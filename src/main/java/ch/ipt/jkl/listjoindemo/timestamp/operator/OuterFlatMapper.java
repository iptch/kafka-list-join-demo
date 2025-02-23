package ch.ipt.jkl.listjoindemo.timestamp.operator;

import ch.ipt.jkl.listjoindemo.proto.Outer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.List;

public class OuterFlatMapper implements KeyValueMapper<String, Outer, Iterable<KeyValue<String, Outer>>> {

    @Override
    public Iterable<KeyValue<String, Outer>> apply(String key, Outer value) {
        // forward tombstones
        if (value == null) {
            return List.of(KeyValue.pair(key, null));
        }

        // forward empty list records
        if (value.getInnerList().isEmpty()) {
            return List.of(KeyValue.pair(key, value));
        }

        // otherwise flat map
        return value.getInnerList().stream()
                .map(inner -> KeyValue.pair(
                        "%s$$%s".formatted(key, inner.getId()),
                        value.toBuilder()
                                .clearInner()
                                .addInner(inner)
                                .build()
                ))
                .toList();
    }
}
