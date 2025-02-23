package ch.ipt.jkl.listjoindemo.timestamp.operator;

import ch.ipt.jkl.listjoindemo.proto.Inner;
import ch.ipt.jkl.listjoindemo.proto.Outer;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class OuterInnerJoiner implements ValueJoiner<Outer, Inner, Outer> {

    @Override
    public Outer apply(Outer outer, Inner inner) {
        // forward message unchanged if other side is not available
        if (inner == null) return outer;

        return outer.toBuilder()
                .clearInner()
                .addInner(inner)
                .build();
    }
}
