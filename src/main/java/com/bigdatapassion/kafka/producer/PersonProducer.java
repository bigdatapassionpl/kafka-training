package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.datafactory.PersonMessage;
import com.bigdatapassion.kafka.datafactory.PersonMessageFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC;

public class PersonProducer extends KafkaProducerApp<String, PersonMessage> {

    private PersonMessageFactory messageFactory = new PersonMessageFactory();

    public static void main(String[] args) {
        new PersonProducer().run();
    }

    @Override
    protected ProducerRecord<String, PersonMessage> createRecord(long messageId) {
        PersonMessage value = messageFactory.generateNextMessage(messageId);
        return new ProducerRecord<>(TOPIC, value.getId().toString(), value);
    }

    @Override
    protected Properties getProducerProperties() {
        Properties properties = super.getProducerProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return properties;
    }

}
