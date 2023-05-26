package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.datafactory.ProductMessageFactory;
import com.bigdatapassion.kafka.dto.ProductMessage;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.*;

public class ProductJsonSchemaRegistryProducer extends KafkaProducerApp<String, ProductMessage> {

    private ProductMessageFactory factory = new ProductMessageFactory();

    public static void main(String[] args) {
        new ProductJsonSchemaRegistryProducer().run();
    }

    @Override
    protected Properties getProducerProperties() {
        Properties properties = super.getProducerProperties();
        properties.setProperty("value.serializer", KafkaJsonSchemaSerializer.class.getName());
        properties.setProperty("schema.registry.url", KAFKA_SCHEMA_REGISTRY);
        properties.setProperty("json.oneof.for.nullables", "false");
        return properties;
    }

    @Override
    protected ProducerRecord<String, ProductMessage> createRecord(long messageId) {

        ProductMessage productMessage = factory.generateNextMessage(messageId);

        return new ProducerRecord<>(TOPIC_PRODUCT_JSON_SCHEMA_REGISTRY, productMessage.getId().toString(), productMessage);
    }

}
