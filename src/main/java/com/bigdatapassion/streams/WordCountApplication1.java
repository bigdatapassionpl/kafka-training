package com.bigdatapassion.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

import static com.bigdatapassion.KafkaConfigurationFactory.*;

public class WordCountApplication1 {

    private static final String WORD_SPLIT_PATTERN = "\\W+";
    private static final String APPLICATION_NAME = "wordcount-application";
    private static final String COUNTS_STORE = "wordcount-store";

    public static void main(final String[] args) {

        Properties config = getStreamConfig();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStreamWithMessages = builder.stream(TOPIC);

        KTable<String, Long> wordCountTable = inputStreamWithMessages
                .mapValues((ValueMapper<String, String>) String::toLowerCase) // --> to lower case
                .flatMapValues(textLine -> Arrays.asList(textLine.split(WORD_SPLIT_PATTERN))) // -> to words very simple convert
//                .map((key, value) -> new KeyValue<>(value, value)) // --> mapping value as key (like below)
                .selectKey((key, value) -> value) // --> mapping value as key (like above)
//                .through(TOPIC_THROUGH) // --> writing curent values to topic
//                .peek((key, value) -> System.out.println(String.format("(key:%s -> value:%s)", key, value))) // --> printing current values, debug purpose
                .groupByKey() // --> group stream by keys
                .count(Materialized.as(COUNTS_STORE)); // --> saving state to topic: ${applicationId}-${internalStoreName}-changelog

        wordCountTable.toStream().to(TOPIC_OUT, Produced.with(Serdes.String(), Serdes.Long())); // --> konwerting KTable to regular stream and save in output topic

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);

        // Delete the application's local state.
        // Note: In real application you'd call `cleanUp()` only under certain conditions.
        // See Confluent Docs for more details:
        // https://docs.confluent.io/current/streams/developer-guide/app-reset-tool.html#step-2-reset-the-local-environments-of-your-application-instances
//        streams.cleanUp();

        streams.start(); // start Kafka Streams App

        // Print topology
        System.out.println("TOPOLOGY:");
        System.out.println(topology.describe());

        System.out.println("TOPOLOGY:");
        System.out.println(streams.toString());

        System.out.println("TOPOLOGY:");
        streams.localThreadsMetadata().forEach(System.out::println);

        // jeśli chcemy zamknać aplikację po jakimś czasie to najprościej dajemy sleep i close
        // Thread.sleep(5000L);
        // streams.close();

        // Eleganckie zamknięcie
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
