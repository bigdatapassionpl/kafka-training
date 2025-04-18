package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.datafactory.PersonMessageFactory;
import com.bigdatapassion.kafka.dto.PersonMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_PERSON;

public class PersonJsonProducer extends KafkaProducerApp<String, PersonMessage> {

    private PersonMessageFactory messageFactory = new PersonMessageFactory();

    public static void main(String[] args) {
        new PersonJsonProducer().run();
    }

    @Override
    protected ProducerRecord<String, PersonMessage> createRecord(long messageId) {
        PersonMessage value = messageFactory.generateNextMessage(messageId);
        return new ProducerRecord<>(TOPIC_PERSON, value.getId().toString(), value);
    }

    @Override
    protected Properties getProducerProperties() {
        Properties properties = super.getProducerProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return properties;
    }

}
