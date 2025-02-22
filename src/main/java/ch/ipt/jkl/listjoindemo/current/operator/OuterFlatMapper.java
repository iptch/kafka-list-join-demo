package ch.ipt.jkl.listjoindemo.current.operator;

import ch.ipt.jkl.listjoindemo.proto.Outer;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.List;

public class OuterFlatMapper implements ValueMapper<Outer, List<Outer>> {
    @Override
    public List<Outer> apply(Outer value) {

        // returning an empty list from the flat mapper effectively drops the messages
        if (value == null || value.getInnerList().isEmpty()) {
            return List.of();
        }

        // all forwarded message contain exactly one inner element in the list
        return value.getInnerList().stream()
                .map(inner -> value.toBuilder()
                        .clearInner()
                        .addInner(inner)
                        .build()
                )
                .toList();
    }
}
