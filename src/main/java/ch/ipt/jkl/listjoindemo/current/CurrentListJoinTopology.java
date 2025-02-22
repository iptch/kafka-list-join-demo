package ch.ipt.jkl.listjoindemo.current;

import ch.ipt.jkl.listjoindemo.current.operator.*;
import ch.ipt.jkl.listjoindemo.proto.Inner;
import ch.ipt.jkl.listjoindemo.proto.Outer;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.BiFunction;

import static ch.ipt.jkl.listjoindemo.Util.buildStore;

@Configuration
@RequiredArgsConstructor
public class CurrentListJoinTopology {

    private final KafkaProtobufSerde<Outer> outerSerde;

    private final OuterFlatMapper outerFlatMapper = new OuterFlatMapper();
    private final OuterInnerJoiner outerInnerJoiner = new OuterInnerJoiner();
    private final OuterReducerAdder listAdder = new OuterReducerAdder();
    private final OuterReducerRemover listRemover = new OuterReducerRemover();

    private final PreProcessorSupplier<Outer> preProcessorSupplier = new PreProcessorSupplier<>(
            outerFlatMapper,
            outer -> outer.getInner(0).getId(),
            outer -> outer.toBuilder().clearInner().build()
    );

    @Bean
    public BiFunction<KStream<String, Outer>, KTable<String, Inner>, KStream<String, Outer>> currentListJoin() {
        return (outerKStream, innerKTable) -> {

            // we forward tombstones and empty lists separately, but the PreProcessor needs
            // to receive those as well for internal state updates
            KStream<String, Outer> bypassKStream = outerKStream
                    .filter(((_, value) -> value == null || value.getInnerList().isEmpty()));

            return outerKStream
                    .process(preProcessorSupplier)
                    .toTable(buildStore(outerSerde, "listJoinFlatStore"))
                    .leftJoin(
                            innerKTable,
                            // if the inner list is empty the foreign key extractor should return null so the outer is
                            // joined with null
                            outer -> outer.getInnerCount() == 0 ? null : outer.getInner(0).getId(),
                            outerInnerJoiner,
                            buildStore(outerSerde, "listJoinJoinerStore")
                    )
                    .groupBy(
                            // group by first part of composite key
                            (key, value) -> KeyValue.pair(key.split("\\$\\$")[0], value),
                            Grouped.with(Serdes.String(), outerSerde)
                    )
                    .reduce(
                            listAdder,
                            listRemover,
                            buildStore(outerSerde, "listJoinReducerStore")
                    )
                    .toStream()
                    .merge(bypassKStream);
        };
    }

}
