package ch.ipt.jkl.listjoindemo.current.operator;

import ch.ipt.jkl.listjoindemo.proto.Outer;
import org.apache.kafka.streams.kstream.Reducer;

public class OuterReducerAdder implements Reducer<Outer> {

    @Override
    public Outer apply(Outer currentValue, Outer newValue) {
        if (newValue.getInnerList().isEmpty()) {
            // if list is empty update all fields other than list
            return newValue.toBuilder()
                    .addAllInner(currentValue.getInnerList())
                    .build();
        } else {
            // otherwise add inner item to current list
            return currentValue.toBuilder()
                    .addAllInner(newValue.getInnerList())
                    .build();
        }
    }
}
