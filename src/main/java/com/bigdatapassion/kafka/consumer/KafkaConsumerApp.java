package com.bigdatapassion.kafka.consumer;

import com.bigdatapassion.kafka.listener.ConsumerRebalanceLoggerListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TIMEOUT;
import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.createConsumerConfig;

/**
 * The consumer is designed to be run in its own thread!!!
 */
public abstract class KafkaConsumerApp<K, V> {

    private static final Logger LOGGER = Logger.getLogger(KafkaConsumerApp.class);

    private Collection<String> topics;

    protected KafkaConsumerApp(String topic) {
        this.topics = Collections.singletonList(topic);
    }

    protected KafkaConsumerApp(Collection<String> topics) {
        this.topics = topics;
    }

    protected void run() {

        Properties consumerConfig = getConsumerProperties();

        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig);

        consumer.subscribe(topics, new ConsumerRebalanceLoggerListener());

        try {
            while (true) {

                ConsumerRecords<K, V> records = consumer.poll(Duration.of(TIMEOUT, ChronoUnit.MILLIS));
                if (records.count() > 0) {
                    LOGGER.info("Poll records: " + records.count());

                    for (ConsumerRecord<K, V> record : records) {
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

    protected Properties getConsumerProperties() {
        return createConsumerConfig();
    }

}
