package com.bigdatapassion.kafka.prodcon;

import com.bigdatapassion.kafka.listener.ConsumerRebalanceLoggerListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.*;

/**
 * The consumer is designed to be run in its own thread!!!
 */
public class KafkaConsumerManualOffset {

    private static final Logger LOGGER = Logger.getLogger(KafkaConsumerManualOffset.class);

    private static final long gapTimeMilis = 10 * 1000;
    private static final long windowTimeMilis = 30 * 1000;

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createConsumerConfig());

        consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceLoggerListener());
        // consumer.subscribe(Arrays.asList(TOPIC, TOPIC2), new ConsumerRebalanceLoggerListener());

        Map<TopicPartition, Long> nextPositions = new HashMap<>();
        try {
            while (true) {

                // Window end
                long currentTimeMillis = System.currentTimeMillis();

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
                    long nextOffset = 0;
                    for (ConsumerRecord<String, String> record : partitionRecords) {

                        System.out.printf("Received Message topic = %s, partition = %s, offset = %d, time = %s, key = %s, value = %s\n",
                                record.topic(), record.partition(), record.offset(), printDateFromTimestamp(record.timestamp()), record.key(), record.value());

                        long distance = currentTimeMillis - record.timestamp();
                        if (distance <= gapTimeMilis) {
                            // new events
                            System.out.println("New");
                        } else if (distance <= windowTimeMilis) {
                            // window events
                            System.out.println("Window");
                            nextOffset = record.offset();
                        } else {
                            // very old events ...
                            System.out.println("History events");
                        }

                    }

                    if (nextOffset <= 0) {
                        nextOffset = partitionRecords.get(partitionRecords.size() - 1).offset() + 1;
                    }

                    nextPositions.put(partition, nextOffset);
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(nextOffset)));
                }

                Thread.sleep(gapTimeMilis);
            }
        } catch (Exception e) {
            LOGGER.error("Błąd...", e);
        } finally {
            consumer.commitSync(); // sync commit
            consumer.close();
        }
    }

    private static String printDateFromTimestamp(final long timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getDefault());
        cal.setTimeInMillis(timestamp);
        return (cal.get(Calendar.YEAR) + "-"
                + (cal.get(Calendar.MONTH) + 1) + "-"
                + cal.get(Calendar.DAY_OF_MONTH) + " "
                + cal.get(Calendar.HOUR_OF_DAY) + ":"
                + cal.get(Calendar.MINUTE) + ":"
                + cal.get(Calendar.SECOND));
    }

}