package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.callback.LoggerCallback;
import com.bigdatapassion.kafka.dto.ProductAvro;
import com.bigdatapassion.kafka.dto.ProductMessageAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.*;
import static org.apache.commons.math3.util.Precision.round;

public class KafkaAvroProducerExample {

    private static final Logger LOGGER = Logger.getLogger(KafkaAvroProducerExample.class);
    private static final AtomicInteger MESSAGE_ID = new AtomicInteger(1);

    public static void main(String[] args) {

        Properties producerConfig = createProducerConfig();
        producerConfig.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        producerConfig.setProperty("schema.registry.url", KAFKA_SCHEMA_REGISTRY);
        Producer<String, ProductMessageAvro> producer = new KafkaProducer<>(producerConfig);

        LoggerCallback callback = new LoggerCallback();
        Random random = new Random(System.currentTimeMillis());

        try {
            while (true) {

                for (long i = 0; i < MESSAGE_BATCH_COUNT; i++) {

                    int id = MESSAGE_ID.getAndIncrement();
                    String key = "key-" + id;
                    double price = round(100 * random.nextDouble(), 2);

                    ProductAvro productAvro = new ProductAvro();
                    productAvro.setProductName("Product " + id);
                    productAvro.setPrice(String.valueOf(price));

                    ProductMessageAvro value = ProductMessageAvro.newBuilder()
                            .setId((long) id)
                            .setProduct(productAvro)
                            .setCreationDate("")
                            .build();
                    ProducerRecord<String, ProductMessageAvro> data = new ProducerRecord<>(TOPIC_AVRO, key, value);

                    producer.send(data, callback); // async with callback
                    // producer.send(data); // async without callback
                    // producer.send(data).get(); // sync send
                }

                LOGGER.info("Sended messages");
                Thread.sleep(SLEEP);
            }
        } catch (Exception e) {
            LOGGER.error("Błąd...", e);
        } finally {
            producer.flush();
            producer.close();
        }

    }

}
