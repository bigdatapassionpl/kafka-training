package com.bigdatapassion.kafka.consumer;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_SIMPLE;

public class SimpleConsumer extends KafkaConsumerApp<String, String> {

    protected SimpleConsumer() {
        super(TOPIC_SIMPLE);
    }

    public static void main(String[] args) throws Exception {
        new SimpleConsumer().run();
    }

}
