package com.bigdatapassion.avro.prodcon;

import com.bigdatapassion.Product;
import com.bigdatapassion.callback.LoggerCallback;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bigdatapassion.KafkaConfigurationFactory.*;

public class KafkaAvroProducerExample {

    private static final Logger LOGGER = Logger.getLogger(KafkaAvroProducerExample.class);
    private static final AtomicInteger MESSAGE_ID = new AtomicInteger(1);

    public static void main(String[] args) {

        Properties producerConfig = createProducerConfig();
        producerConfig.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        producerConfig.setProperty("schema.registry.url", "http://schema-registry:8085/");
        Producer<String, Product> producer = new KafkaProducer<>(producerConfig);

        LoggerCallback callback = new LoggerCallback();
        Random random = new Random(System.currentTimeMillis());

        try {
            while (true) {

                for (long i = 0; i < MESSAGE_BATCH_COUNT; i++) {

                    int id = MESSAGE_ID.getAndIncrement();
                    String key = "key-" + id;
                    Product value = Product.newBuilder()
                            .setName("Product " + id)
                            .setPrice(random.nextDouble())
                            .build();
                    ProducerRecord<String, Product> data = new ProducerRecord<>(TOPIC_AVRO, key, value);

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
