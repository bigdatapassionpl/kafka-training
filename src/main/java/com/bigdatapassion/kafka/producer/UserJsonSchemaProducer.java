package com.bigdatapassion.kafka.producer;

import com.bigdatapassion.kafka.datafactory.UserFactory;
import com.bigdatapassion.kafka.json.User;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.KAFKA_SCHEMA_REGISTRY;

public class UserJsonSchemaProducer extends KafkaProducerApp<String, User> {

    private UserFactory factory = new UserFactory();

    public static void main(String[] args) {
        new UserJsonSchemaProducer().run();
    }

    @Override
    protected Properties getProducerProperties() {
        Properties properties = super.getProducerProperties();
        properties.setProperty("value.serializer", KafkaJsonSchemaSerializer.class.getName());
        properties.setProperty("schema.registry.url", KAFKA_SCHEMA_REGISTRY);
        properties.setProperty("json.oneof.for.nullables", "false");
        return properties;
    }

    @Override
    protected ProducerRecord<String, User> createRecord(long messageId) {

        User productMessage = factory.generateNextMessage(messageId);

        return new ProducerRecord<>("DLH-PayBM-TransactionStatus-OnPremise", String.valueOf(messageId), productMessage);
    }

}
