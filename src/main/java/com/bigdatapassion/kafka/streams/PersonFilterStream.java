package com.bigdatapassion.kafka.streams;

import com.bigdatapassion.kafka.dto.PersonMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_PERSON;
import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_PERSON_FILTERED;

public class PersonFilterStream extends KafkaStreamsApp {

    public static void main(final String[] args) {
        new PersonFilterStream().run();
    }

    @Override
    protected Properties createStreamProperties() {
        Properties properties = super.createStreamProperties();
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
//        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return properties;
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        JsonSerde<PersonMessage> jsonSerde = new JsonSerde<>();
        JsonDeserializer<PersonMessage> deserializer = (JsonDeserializer<PersonMessage>) jsonSerde.deserializer();
        deserializer.addTrustedPackages("*");

//        KStream<String, PersonMessage> inputStream = builder.stream(TOPIC_PERSON);
        KStream<String, PersonMessage> inputStream = builder.stream(TOPIC_PERSON, Consumed.with(Serdes.String(), jsonSerde));

        inputStream.print(Printed.<String, PersonMessage>toSysOut().withLabel("input"));

        KStream<String, PersonMessage> filteredStream = inputStream.
                filter((key, value) -> Integer.parseInt(value.getPerson().getNumber()) % 2 == 0);

        filteredStream.print(Printed.<String, PersonMessage>toSysOut().withLabel("filtered"));

//        filteredStream.to(TOPIC_PERSON_FILTERED);
        filteredStream.to(TOPIC_PERSON_FILTERED, Produced.with(Serdes.String(), jsonSerde));
    }

}