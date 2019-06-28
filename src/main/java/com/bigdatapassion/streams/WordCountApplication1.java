package com.bigdatapassion.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

import static com.bigdatapassion.KafkaConfigurationFactory.*;

public class WordCountApplication1 {

    private static final String PATTERN = "\\W+";

    public static void main(final String[] args) {

        Properties config = getStreamConfig();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream(TOPIC);

        KTable<String, Long> wordCountTable = textLines
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(textLine -> Arrays.asList(textLine.split(PATTERN)))
                //.map((key, value) -> new KeyValue<>(value, value))
                .selectKey((key, value) -> value)
//                .peek((key, value) -> System.out.println(String.format("(key:%s -> value:%s)", key, value)))
                .groupByKey()
                .count(Materialized.as("counts-store"));

        KStream<String, Long> wordCountStream = wordCountTable.toStream();

        wordCountStream.to(TOPIC_OUT, Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);

        // Nie używać na produkcji, czyści stan strumieni
        // streams.cleanUp();

        // start Kafka Streams
        streams.start();

        // Print topology
        System.out.println("TOPOLOGY:");
        System.out.println(topology.describe());
        while (true) {
//            System.out.println("TOPOLOGY:");
//            System.out.println(topology.describe());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
        }

        // jeśli chcemy zamknać aplikację po jakimś czasie to najprościej dajemy sleep i close
        // Thread.sleep(5000L);
        // streams.close();

        // Eleganckie zamknięcie
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
