package ch.ipt.jkl.listjoindemo.timestamp;

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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(classes = {Util.class, TimestampListJoinTopology.class})
class TimestampListJoinTopologyTest {

    private TopologyTestDriver topologyTestDriver;

    Serde<String> stringSerde = Serdes.String();
    @Autowired
    KafkaProtobufSerde<Outer> outerSerde;
    @Autowired
    KafkaProtobufSerde<Inner> innerSerde;

    @Autowired
    TimestampListJoinTopology timestampListJoinTopology;

    private TestInputTopic<String, Outer> outerInputTopic;
    private TestInputTopic<String, Inner> innerInputTopic;
    private TestOutputTopic<String, Outer> outerOutputTopic;

    // some static test data
    private final Inner INNER_01 = Inner.newBuilder()
                .setId("inner01")
                .setName("Alice")
                .setAge(94)
                .build();
    private final Inner INNER_02 = Inner.newBuilder()
            .setId("inner02")
            .setName("Bob")
            .setAge(95)
            .build();
    private final Inner INNER_03 = Inner.newBuilder()
            .setId("inner03")
            .setName("Charlie")
            .setAge(23)
            .build();
    private final Outer OUTER_01 = Outer.newBuilder()
            .setId("outer01")
            .setSomeText("this is some text")
            .setSomeNumber(42)
            .addInner(Inner.newBuilder().setId(INNER_01.getId()))
            .addInner(Inner.newBuilder().setId(INNER_02.getId()))
            .build();

    @BeforeEach
    void beforeEach() {
        StreamsBuilder builder = new StreamsBuilder();

        String outerInputTopicName = "outer-input-topic";
        String innerInputTopicName = "inner-input-topic";
        String outerOutputTopicName = "outer-output-topic";

        KStream<String, Outer> outerKStream = builder.stream(outerInputTopicName, Consumed.with(stringSerde, outerSerde));
        KTable<String, Inner> innerKTable = builder.table(innerInputTopicName, Consumed.with(stringSerde, innerSerde));

        KStream<String, Outer> outerOutputStream = timestampListJoinTopology.previousListJoin()
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
    @DisplayName("basic happy path")
    void testListJoin() {
        innerInputTopic.pipeInput(INNER_01.getId(), INNER_01);
        innerInputTopic.pipeInput(INNER_02.getId(), INNER_02);
        innerInputTopic.pipeInput(INNER_03.getId(), INNER_03);
        outerInputTopic.pipeInput(OUTER_01.getId(), OUTER_01);

        Outer result = outerOutputTopic.readKeyValuesToMap().get(OUTER_01.getId());

        assertThat(result.getInnerList()).hasSize(2);
        assertThat(result.getInnerList().get(0).getId()).isEqualTo("inner01");
        assertThat(result.getInnerList().get(0).getName()).isEqualTo("Alice");
        assertThat(result.getInnerList().get(1).getId()).isEqualTo("inner02");
        assertThat(result.getInnerList().get(1).getName()).isEqualTo("Bob");
    }

    @Test
    @DisplayName("inner becomes available later")
    void testListJoinInnerUnavailable() {
        outerInputTopic.pipeInput(OUTER_01.getId(), OUTER_01);

        Outer result = outerOutputTopic.readKeyValuesToMap().get(OUTER_01.getId());

        assertThat(result.getInnerList()).hasSize(2);
        assertThat(result.getInnerList().get(0).getId()).isEqualTo("inner01");
        assertThat(result.getInnerList().get(0).getName()).isEmpty();
        assertThat(result.getInnerList().get(1).getId()).isEqualTo("inner02");
        assertThat(result.getInnerList().get(1).getName()).isEmpty();

        innerInputTopic.pipeInput(INNER_01.getId(), INNER_01);
        innerInputTopic.pipeInput(INNER_02.getId(), INNER_02);

        result = outerOutputTopic.readKeyValuesToMap().get(OUTER_01.getId());

        assertThat(result.getInnerList()).hasSize(2);
        assertThat(result.getInnerList().get(0).getId()).isEqualTo("inner01");
        assertThat(result.getInnerList().get(0).getName()).isEqualTo("Alice");
        assertThat(result.getInnerList().get(1).getId()).isEqualTo("inner02");
        assertThat(result.getInnerList().get(1).getName()).isEqualTo("Bob");
    }

    @Test
    @DisplayName("handle empty list")
    void testListJoinEmptyList() {
        Outer outer = OUTER_01.toBuilder()
                .clearInner()
                .build();

        outerInputTopic.pipeInput(outer.getId(), outer);

        Outer result = outerOutputTopic.readKeyValuesToMap().get(OUTER_01.getId());

        assertThat(result.getInnerList()).isEmpty();
        assertThat(result.getSomeNumber()).isEqualTo(42);
    }

    @Test
    @DisplayName("handle and forward tombstone")
    void testListJoinTombstone() {
        outerInputTopic.pipeInput(OUTER_01.getId(), null);

        Map<String, Outer> resultMap = outerOutputTopic.readKeyValuesToMap();

        assertThat(resultMap)
                .hasSize(1)
                .containsKey(OUTER_01.getId())
                .containsValue(null);
    }

    @Test
    @DisplayName("update of fields other than the list")
    void testListJoinUpdateOther() {
        innerInputTopic.pipeInput(INNER_01.getId(), INNER_01);
        innerInputTopic.pipeInput(INNER_02.getId(), INNER_02);
        outerInputTopic.pipeInput(OUTER_01.getId(), OUTER_01);

        Outer result = outerOutputTopic.readKeyValuesToMap().get(OUTER_01.getId());

        assertThat(result.getInnerList()).hasSize(2);
        assertThat(result.getSomeNumber()).isEqualTo(42);

        Outer updatedOuter = OUTER_01.toBuilder()
                .setSomeNumber(43)
                .build();

        outerInputTopic.pipeInput(updatedOuter.getId(), updatedOuter);

        result = outerOutputTopic.readKeyValuesToMap().get(OUTER_01.getId());

        assertThat(result.getInnerList()).hasSize(2);
        assertThat(result.getSomeNumber()).isEqualTo(43);
    }

    @Test
    @DisplayName("addition and removal of list elements")
    void testListJoinUpdateList() {
        innerInputTopic.pipeInput(INNER_01.getId(), INNER_01);
        innerInputTopic.pipeInput(INNER_02.getId(), INNER_02);
        innerInputTopic.pipeInput(INNER_03.getId(), INNER_03);

        outerInputTopic.pipeInput(OUTER_01.getId(), OUTER_01);

        Outer result = outerOutputTopic.readKeyValuesToMap().get(OUTER_01.getId());

        assertThat(result.getInnerList()).hasSize(2);
        assertThat(result.getInnerList().get(0).getId()).isEqualTo("inner01");
        assertThat(result.getInnerList().get(0).getName()).isEqualTo("Alice");
        assertThat(result.getInnerList().get(1).getId()).isEqualTo("inner02");
        assertThat(result.getInnerList().get(1).getName()).isEqualTo("Bob");

        Outer updatedOuter = OUTER_01.toBuilder()
                .clearInner()
                .addInner(Inner.newBuilder().setId(INNER_01.getId()))
                .addInner(Inner.newBuilder().setId(INNER_03.getId()))
                .build();

        outerInputTopic.pipeInput(updatedOuter.getId(), updatedOuter);

        result = outerOutputTopic.readKeyValuesToMap().get(OUTER_01.getId());

        assertThat(result.getInnerList()).hasSize(2);
        assertThat(result.getInnerList().get(0).getId()).isEqualTo("inner01");
        assertThat(result.getInnerList().get(0).getName()).isEqualTo("Alice");
        assertThat(result.getInnerList().get(1).getId()).isEqualTo("inner03");
        assertThat(result.getInnerList().get(1).getName()).isEqualTo("Charlie");
    }
}