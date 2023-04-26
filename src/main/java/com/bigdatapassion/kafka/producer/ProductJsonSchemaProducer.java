package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.datafactory.ProductMessageFactory;
import com.bigdatapassion.kafka.dto.ProductMessage;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.*;

public class ProductJsonSchemaProducer extends KafkaProducerApp<String, ProductMessage> {

    private ProductMessageFactory factory = new ProductMessageFactory();

    public static void main(String[] args) {
        new ProductJsonSchemaProducer().run();
    }

    @Override
    protected Properties getProducerProperties() {
        Properties properties = super.getProducerProperties();
        properties.setProperty("value.serializer", KafkaJsonSchemaSerializer.class.getName());
        properties.setProperty("schema.registry.url", KAFKA_SCHEMA_REGISTRY);
        return properties;
    }

    @Override
    protected ProducerRecord<String, ProductMessage> createRecord(long messageId) {

        ProductMessage productMessage = factory.generateNextMessage(messageId);

        return new ProducerRecord<>(TOPIC_PRODUCT_JSON_SCHEMA, productMessage.getId().toString(), productMessage);
    }

}
