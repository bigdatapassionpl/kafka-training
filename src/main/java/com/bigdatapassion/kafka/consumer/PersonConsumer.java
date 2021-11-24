package com.bigdatapassion.kafka.consumer;

import com.bigdatapassion.kafka.dto.PersonMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_PERSON;

public class PersonConsumer extends KafkaConsumerApp<String, PersonMessage> {

    protected PersonConsumer() {
        super(TOPIC_PERSON);
    }

    public static void main(String[] args) throws Exception {
        new PersonConsumer().run();
    }

    @Override
    protected Properties getConsumerProperties() {
        Properties properties = super.getConsumerProperties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return properties;
    }
}
