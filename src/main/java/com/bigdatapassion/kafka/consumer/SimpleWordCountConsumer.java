package com.bigdatapassion.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_SIMPLE_WORDCOUNT;

public class SimpleWordCountConsumer extends KafkaConsumerApp<String, Long> {

    protected SimpleWordCountConsumer() {
        super(TOPIC_SIMPLE_WORDCOUNT);
    }

    @Override
    protected Properties getConsumerProperties() {
        Properties consumerConfig = super.getConsumerProperties();
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        return consumerConfig;
    }

    public static void main(String[] args) throws Exception {
        new SimpleWordCountConsumer().run();
    }

}
