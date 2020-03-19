package com.bigdatapassion.prodcon;

import com.bigdatapassion.listener.ConsumerRebalanceLoggerListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static com.bigdatapassion.KafkaConfigurationFactory.*;

/**
 * The consumer is designed to be run in its own thread!!!
 */
public class KafkaConsumerManualOffset {

    private static final Logger LOGGER = Logger.getLogger(KafkaConsumerManualOffset.class);

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createConsumerConfig());

        consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceLoggerListener());
        // consumer.subscribe(Arrays.asList(TOPIC, TOPIC2), new ConsumerRebalanceLoggerListener());

        Map<TopicPartition, Long> nextPositions = new HashMap<>();
        try {
            while (true) {

                // Moving to offset
                Set<TopicPartition> assignment = consumer.assignment();
                for (TopicPartition partition : assignment) {
                    Long nextPosition = nextPositions.get(partition);
                    consumer.seek(partition, nextPosition);
                }

                // Reading partitions
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(TIMEOUT, ChronoUnit.MILLIS));
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {

                        System.out.printf("Received Message topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    long nextOffset = lastOffset;
//                    System.out.println(partition + " : " + nextOffset);
                    nextPositions.put(partition, nextOffset);
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(nextOffset)));
                }

            }
        } catch (Exception e) {
            LOGGER.error("Błąd...", e);
        } finally {
            consumer.commitSync(); // sync commit
            consumer.close();
        }
    }

}
