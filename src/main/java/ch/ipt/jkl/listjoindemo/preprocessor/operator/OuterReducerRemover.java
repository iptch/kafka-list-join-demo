package ch.ipt.jkl.listjoindemo.preprocessor.operator;

import ch.ipt.jkl.listjoindemo.proto.Inner;
import ch.ipt.jkl.listjoindemo.proto.Outer;
import org.apache.kafka.streams.kstream.Reducer;

import java.util.List;

public class OuterReducerRemover implements Reducer<Outer> {

    @Override
    public Outer apply(Outer currentValue, Outer oldValue) {
        // if oldValue inner list is empty, there is nothing to do. The adder handles empty list updates
        if (oldValue.getInnerList().isEmpty()) return currentValue;

        // Remove the single inner value from the current list (in this case we do it by id)
        Inner innerToRemove = oldValue.getInnerList().getFirst();

        List<Inner> innerList = currentValue.getInnerList().stream()
                .filter(inner -> !inner.getId().equals(innerToRemove.getId()))
                .toList();

        return currentValue.toBuilder()
                .clearInner()
                .addAllInner(innerList)
                .build();
    }
}
