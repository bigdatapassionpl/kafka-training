package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.datafactory.ProductMessageAvroFactory;
import com.bigdatapassion.kafka.dto.ProductMessageAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.KAFKA_SCHEMA_REGISTRY;
import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TEST_PRODUCT;

public class KafkaProductAvroProducer extends KafkaProducerApp<String, ProductMessageAvro> {

    private ProductMessageAvroFactory factory = new ProductMessageAvroFactory();

    public static void main(String[] args) {
        new KafkaProductAvroProducer().run();
    }

    @Override
    protected Properties getProducerProperties() {
        Properties properties = super.getProducerProperties();
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", KAFKA_SCHEMA_REGISTRY);
        return properties;
    }

    @Override
    protected ProducerRecord<String, ProductMessageAvro> createRecord(long messageId) {

        ProductMessageAvro productMessage = factory.generateNextMessage(messageId);

        return new ProducerRecord<>(TEST_PRODUCT, productMessage.getId().toString(), productMessage);
    }

}
