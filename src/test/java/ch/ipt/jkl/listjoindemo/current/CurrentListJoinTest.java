package ch.ipt.jkl.listjoindemo.current;

import ch.ipt.jkl.listjoindemo.Util;
import ch.ipt.jkl.listjoindemo.proto.Inner;
import ch.ipt.jkl.listjoindemo.proto.Outer;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(classes = {Util.class, CurrentListJoinTopology.class})
class CurrentListJoinTest {

    private TopologyTestDriver topologyTestDriver;

    Serde<String> stringSerde = Serdes.String();
    @Autowired
    KafkaProtobufSerde<Outer> outerSerde;
    @Autowired
    KafkaProtobufSerde<Inner> innerSerde;

    @Autowired
    CurrentListJoinTopology currentListJoinTopology;

    private TestInputTopic<String, Outer> outerInputTopic;
    private TestInputTopic<String, Inner> innerInputTopic;
    private TestOutputTopic<String, Outer> outerOutputTopic;

    @BeforeEach
    void beforeEach() {
        StreamsBuilder builder = new StreamsBuilder();

        String outerInputTopicName = "outer-input-topic";
        String innerInputTopicName = "inner-input-topic";
        String outerOutputTopicName = "outer-output-topic";

        KStream<String, Outer> outerKStream = builder.stream(outerInputTopicName, Consumed.with(stringSerde, outerSerde));
        KTable<String, Inner> innerKTable = builder.table(innerInputTopicName, Consumed.with(stringSerde, innerSerde));

        KStream<String, Outer> outerOutputStream = currentListJoinTopology.currentListJoin()
                .apply(outerKStream, innerKTable);
        outerOutputStream.to(outerOutputTopicName, Produced.with(stringSerde, outerSerde));

        Topology topology = builder.build();
        topologyTestDriver = new TopologyTestDriver(topology);

        outerInputTopic = topologyTestDriver.createInputTopic(outerInputTopicName, stringSerde.serializer(), outerSerde.serializer());
        innerInputTopic = topologyTestDriver.createInputTopic(innerInputTopicName, stringSerde.serializer(), innerSerde.serializer());
        outerOutputTopic = topologyTestDriver.createOutputTopic(outerOutputTopicName, stringSerde.deserializer(), outerSerde.deserializer());

        log.info("Current ListJoin Topology description:\n{}", topology.describe().toString());
    }

    @AfterEach
    void afterEach() {
        topologyTestDriver.close();
    }

    @Test
    @DisplayName("Basic happy path")
    void testCurrentListJoin() {
        Inner inner1 = Inner.newBuilder()
                .setId("inner01")
                .setName("Alice")
                .setAge(94)
                .build();
        Inner inner2 = Inner.newBuilder()
                .setId("inner02")
                .setName("Bob")
                .setAge(95)
                .build();

        Outer outer = Outer.newBuilder()
                .setId("outer01")
                .addInner(Inner.newBuilder().setId(inner1.getId()).build())
                .addInner(Inner.newBuilder().setId(inner2.getId()).build())
                .build();

        innerInputTopic.pipeInput(inner1.getId(), inner1);
        innerInputTopic.pipeInput(inner2.getId(), inner2);
        outerInputTopic.pipeInput(outer.getId(), outer);

        Outer result = outerOutputTopic.readKeyValuesToMap().get(outer.getId());

        assertThat(result.getInnerList()).hasSize(2);
        assertThat(result.getInnerList().get(0).getName()).isEqualTo("Alice");
        assertThat(result.getInnerList().get(1).getName()).isEqualTo("Bob");
    }
}