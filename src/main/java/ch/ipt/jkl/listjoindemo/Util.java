package ch.ipt.jkl.listjoindemo;

import ch.ipt.jkl.listjoindemo.proto.Inner;
import ch.ipt.jkl.listjoindemo.proto.Outer;
import com.google.protobuf.Message;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class Util {

    @Value("${spring.cloud.stream.kafka.streams.binder.configuration.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KafkaProtobufSerde<Outer> outerSerde() {
        return createSerde(Outer.class);
    }

    @Bean
    public KafkaProtobufSerde<Inner> innerSerde() {
        return createSerde(Inner.class);
    }

    public static <T> Materialized<String, T, KeyValueStore<Bytes, byte[]>> buildStore(
            Serde<T> serde,
            String storeName
    ) {
        return Materialized.<String, T, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(serde);
    }

    private <T extends Message> KafkaProtobufSerde<T> createSerde(Class<T> clazz) {
        KafkaProtobufSerde<T> serde = new KafkaProtobufSerde<>(clazz);
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);

        return serde;
    }
}
