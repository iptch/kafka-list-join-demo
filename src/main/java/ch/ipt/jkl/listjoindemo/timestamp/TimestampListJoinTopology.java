package ch.ipt.jkl.listjoindemo.timestamp;

import ch.ipt.jkl.listjoindemo.timestamp.operator.OuterFlatMapper;
import ch.ipt.jkl.listjoindemo.timestamp.operator.OuterInnerJoiner;
import ch.ipt.jkl.listjoindemo.timestamp.operator.OuterMapper;
import ch.ipt.jkl.listjoindemo.timestamp.operator.OuterReducer;
import ch.ipt.jkl.listjoindemo.proto.Inner;
import ch.ipt.jkl.listjoindemo.proto.Outer;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.function.BiFunction;

import static ch.ipt.jkl.listjoindemo.Util.buildStore;

@Configuration
@RequiredArgsConstructor
public class TimestampListJoinTopology {

    private final OuterMapper outerMapper = new OuterMapper();
    private final OuterFlatMapper outerFlatMapper = new OuterFlatMapper();
    private final OuterInnerJoiner outerInnerJoiner = new OuterInnerJoiner();
    private final OuterReducer outerReducer = new OuterReducer();

    private final KafkaProtobufSerde<Outer> outerSerde;

    @Bean
    public BiFunction<KStream<String, Outer>, KTable<String, Inner>, KStream<String, Outer>> previousListJoin() {
        return (outerKStream, innerKTable) -> {
            // We manually pass tombstones to the end, hence the split
            Map<String, KStream<String, Outer>> branches = outerKStream
                    .split(Named.as("split-"))
                    .branch((_, value) -> value == null, Branched.as("null"))
                    .defaultBranch(Branched.as("non-null"));

            return branches.get("split-non-null")
                    .mapValues(outerMapper)
                    .flatMap(outerFlatMapper)
                    .toTable(buildStore(outerSerde, "listJoinFlatStore"))
                    .leftJoin(
                            innerKTable,
                            outer -> outer.getInnerCount() == 0 ? null : outer.getInner(0).getId(),
                            outerInnerJoiner,
                            buildStore(outerSerde, "listJoinJoinerStore")
                    )
                    .toStream()
                    .groupBy(
                            (key, _) -> key.split("\\$\\$")[0],
                            Grouped.with(Serdes.String(), outerSerde)
                    )
                    .reduce(
                            outerReducer,
                            buildStore(outerSerde, "listJoinReducerStore")
                    )
                    .toStream()
                    .merge(branches.get("split-null"));
        };
    }
}
