package ch.ipt.jkl.listjoindemo.preprocessor.operator;

import ch.ipt.jkl.listjoindemo.proto.Outer;
import org.apache.kafka.streams.kstream.Reducer;

public class OuterReducerAdder implements Reducer<Outer> {

    @Override
    public Outer apply(Outer currentValue, Outer newValue) {
        if (newValue.getInnerList().isEmpty()) {
            // if list is empty update all fields other than the list
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
