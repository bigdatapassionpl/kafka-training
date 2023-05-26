package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.datafactory.ProductMessageFactory;
import com.bigdatapassion.kafka.dto.ProductMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_PRODUCT_JSON;

public class ProductJsonProducer extends KafkaProducerApp<String, ProductMessage> {

    private final ProductMessageFactory messageFactory = new ProductMessageFactory();

    public static void main(String[] args) {
        new ProductJsonProducer().run();
    }

    @Override
    protected ProducerRecord<String, ProductMessage> createRecord(long messageId) {
        ProductMessage value = messageFactory.generateNextMessage(messageId);
        return new ProducerRecord<>(TOPIC_PRODUCT_JSON, value.getId().toString(), value);
    }

    @Override
    protected Properties getProducerProperties() {
        Properties properties = super.getProducerProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return properties;
    }

}
