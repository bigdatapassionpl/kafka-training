package com.bigdatapassion.avro.prodcon;

import com.bigdatapassion.Product;
import com.bigdatapassion.listener.ConsumerRebalanceLoggerListener;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static com.bigdatapassion.KafkaConfigurationFactory.*;

/**
 * The consumer is designed to be run in its own thread!!!
 */
public class KafkaAvroConsumerExample {

    private static final Logger LOGGER = Logger.getLogger(KafkaAvroConsumerExample.class);

    public static void main(String[] args) {

        Properties consumerConfig = createConsumerConfig();
        consumerConfig.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        consumerConfig.setProperty("schema.registry.url", KAFKA_SCHEMA_REGISTRY);
        KafkaConsumer<String, Product> consumer = new KafkaConsumer<>(consumerConfig);

        consumer.subscribe(Collections.singletonList(TOPIC_AVRO), new ConsumerRebalanceLoggerListener());

        try {
            while (true) {

                ConsumerRecords<String, Product> records = consumer.poll(Duration.of(TIMEOUT, ChronoUnit.MILLIS));
                if (records.count() > 0) {
                    LOGGER.info("Poll records: " + records.count());

                    for (ConsumerRecord<String, Product> record : records) {
                        System.out.printf("Received Message topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                }

                consumer.commitAsync(); // async commit (+ callback)
            }
        } catch (Exception e) {
            LOGGER.error("Błąd...", e);
        } finally {
            consumer.commitSync(); // sync commit
            consumer.close();
        }
    }

}
