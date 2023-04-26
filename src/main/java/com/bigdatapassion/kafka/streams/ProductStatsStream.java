package com.bigdatapassion.kafka.streams;

import com.bigdatapassion.kafka.dto.CountAndSum;
import com.bigdatapassion.kafka.dto.ProductMessageAvro;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.*;


public class ProductStatsStream extends KafkaStreamsApp {

    private static final String WINDOW_STORE_NAME = "window-store";

    public static void main(final String[] args) {
        new ProductStatsStream().run();
    }

    @Override
    protected Properties createStreamProperties() {
        Properties properties = super.createStreamProperties();
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.setProperty("schema.registry.url", KAFKA_SCHEMA_REGISTRY);
        return properties;
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", KAFKA_SCHEMA_REGISTRY);

        final Serde<ProductMessageAvro> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values

        KStream<String, ProductMessageAvro> inputStream = builder.stream(TOPIC_PRODUCT_AVRO, Consumed.with(Serdes.String(), valueSpecificAvroSerde));

        inputStream.print(Printed.<String, ProductMessageAvro>toSysOut().withLabel("input"));


        KGroupedStream<String, Double> productByMaterial = inputStream
                .map(((key, value) -> new KeyValue<>(value.getProduct().getMaterial(), Double.parseDouble(value.getProduct().getPrice().replace(",", ".")))))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()));

        KTable<Windowed<String>, CountAndSum> productCountAndSum = productByMaterial
                .windowedBy(TimeWindows.of(Duration.ofSeconds(30)))
                .aggregate(CountAndSum::new,
                        (key, value, aggregate) -> {
                            aggregate.setCount(aggregate.getCount() + 1);
                            aggregate.setSum(aggregate.getSum() + value);
                            return aggregate;
                        }, buildWindowPersistentStore()
                );

        KStream<String, Double> averageStream = productCountAndSum
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value.getSum() / value.getCount()));

        averageStream.print(Printed.<String, Double>toSysOut().withLabel("avg"));

        averageStream.to(TOPIC_PRODUCT_AVG, Produced.with(Serdes.String(), Serdes.Double()));
    }

    private Materialized<String, CountAndSum, WindowStore<Bytes, byte[]>> buildWindowPersistentStore() {

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", KAFKA_SCHEMA_REGISTRY);

        final Serde<CountAndSum> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        valueSpecificAvroSerde.configure(serdeConfig, false); // `false` for record values

        return Materialized
                .<String, CountAndSum, WindowStore<Bytes, byte[]>>as(WINDOW_STORE_NAME)
                .withKeySerde(Serdes.String())
                .withValueSerde(valueSpecificAvroSerde);
    }

}