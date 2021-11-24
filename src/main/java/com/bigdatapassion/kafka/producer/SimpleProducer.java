package com.bigdatapassion.kafka.producer;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC;

public class SimpleProducer extends KafkaProducerApp<String, String> {

    private final Random random = new Random(System.currentTimeMillis());
    private final String[] messages;

    public static void main(String[] args) throws Exception {
        new SimpleProducer().run();
    }

    public SimpleProducer() throws Exception {
        String message = IOUtils.resourceToString("message.txt", StandardCharsets.UTF_8, SimpleProducer.class.getClassLoader());
        messages = message.split("\\R");
    }

    @Override
    protected ProducerRecord<String, String> createRecord(long messageId) {
        int id = random.nextInt(messages.length);
        String key = "key-" + id;
        String value = messages[id];
        return new ProducerRecord<>(TOPIC, key, value);
    }

}
