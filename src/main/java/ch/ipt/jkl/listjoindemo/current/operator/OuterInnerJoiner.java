package ch.ipt.jkl.listjoindemo.current.operator;

import ch.ipt.jkl.listjoindemo.proto.Inner;
import ch.ipt.jkl.listjoindemo.proto.Outer;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class OuterInnerJoiner implements ValueJoiner<Outer, Inner, Outer> {

    @Override
    public Outer apply(Outer outer, Inner inner) {
        // forward tombstones
        if (outer == null) return null;

        // forward empty list messages unchanged
        if (inner == null) return outer;

        // simply replace the inner (which previously may only have contained an id) with the joined inner
        // at this point the outer will contain exactly a single list element
        return outer.toBuilder()
                .clearInner()
                .addInner(inner)
                .build();
    }
}
