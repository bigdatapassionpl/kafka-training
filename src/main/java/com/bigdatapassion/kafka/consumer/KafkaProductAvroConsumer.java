package com.bigdatapassion.kafka.consumer;

import com.bigdatapassion.kafka.dto.ProductMessageAvro;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.KAFKA_SCHEMA_REGISTRY;
import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_PRODUCT;

/**
 * The consumer is designed to be run in its own thread!!!
 */
public class KafkaProductAvroConsumer extends KafkaConsumerApp<String, ProductMessageAvro> {

    protected KafkaProductAvroConsumer() {
        super(TOPIC_PRODUCT);
    }

    public static void main(String[] args) throws Exception {
        new KafkaProductAvroConsumer().run();
    }

    @Override
    protected Properties getConsumerProperties() {
        Properties consumerConfig = super.getConsumerProperties();
        consumerConfig.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        consumerConfig.setProperty("schema.registry.url", KAFKA_SCHEMA_REGISTRY);
        return consumerConfig;
    }

}
