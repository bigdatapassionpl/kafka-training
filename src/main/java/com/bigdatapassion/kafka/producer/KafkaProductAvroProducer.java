package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.dto.ProductAvro;
import com.bigdatapassion.kafka.dto.ProductMessageAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.KAFKA_SCHEMA_REGISTRY;
import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_AVRO;
import static org.apache.commons.math3.util.Precision.round;

public class KafkaProductAvroProducer extends KafkaProducerApp<String, ProductMessageAvro> {

    private Random random = new Random(System.currentTimeMillis());

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

        String key = "key-" + messageId;
        double price = round(100 * random.nextDouble(), 2);

        ProductAvro productAvro = new ProductAvro();
        productAvro.setProductName("Product " + messageId);
        productAvro.setPrice(String.valueOf(price));

        ProductMessageAvro value = ProductMessageAvro.newBuilder()
                .setId(messageId)
                .setProduct(productAvro)
                .setCreationDate("")
                .build();

        return new ProducerRecord<>(TOPIC_AVRO, key, value);
    }

}
