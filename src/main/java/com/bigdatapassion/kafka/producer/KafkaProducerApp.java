package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.callback.LoggerCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.*;

public abstract class KafkaProducerApp<K, V> {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducerApp.class);
    private static final AtomicInteger MESSAGE_ID = new AtomicInteger(1);

    protected abstract ProducerRecord<K, V> createRecord(long messageId);

    protected Properties getProducerProperties() {
        return createProducerConfig();
    }

    protected void run() {
        System.out.println();

        Properties producerConfig = getProducerProperties();

        Producer<K, V> producer = new KafkaProducer<>(producerConfig);

        LoggerCallback callback = new LoggerCallback();

        try {
            while (true) {

                for (long i = 0; i < MESSAGE_BATCH_COUNT; i++) {

                    int messageId = MESSAGE_ID.getAndIncrement();

                    ProducerRecord<K, V> producerRecord = createRecord(messageId);

                    LOGGER.info(String.format("Sending message to topic:%s key: %s value:%s",
                            producerRecord.topic(), producerRecord.key(), producerRecord.value()));

                    producer.send(producerRecord, callback); // async with callback
                    // producer.send(producerRecord); // async without callback
                    // producer.send(producerRecord).get(); // sync send
                }

                Thread.sleep(SLEEP);
            }
        } catch (Exception e) {
            LOGGER.error("Error sending Kafka Records", e);
        } finally {
            producer.flush();
            producer.close();
        }
    }

}
