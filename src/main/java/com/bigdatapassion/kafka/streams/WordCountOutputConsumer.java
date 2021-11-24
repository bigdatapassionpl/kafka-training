package com.bigdatapassion.kafka.streams;

import com.bigdatapassion.kafka.listener.ConsumerRebalanceLoggerListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.*;

/**
 * The consumer is designed to be run in its own thread!!!
 */
public class WordCountOutputConsumer {

    private static final Logger LOGGER = Logger.getLogger(WordCountOutputConsumer.class);

    public static void main(String[] args) {

        Properties consumerConfig = createConsumerConfig();
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP + new Random(System.currentTimeMillis()).nextInt());


        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerConfig);

        consumer.subscribe(Collections.singletonList(TOPIC_OUT), new ConsumerRebalanceLoggerListener());

        try {
            while (true) {

                ConsumerRecords<String, Long> records = consumer.poll(TIMEOUT);
                if (records.count() > 0) {
                    LOGGER.info("Poll records: " + records.count());

                    for (ConsumerRecord<String, Long> record : records) {
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
