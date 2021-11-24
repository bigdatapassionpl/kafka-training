package com.bigdatapassion.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_SIMPLE;
import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.getStreamConfig;

public class WordCountWindowApplication {

    private static final Pattern PATTERN = Pattern.compile("\\W+");

    public static void main(String[] args) {

        Properties config = getStreamConfig();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream(TOPIC_SIMPLE);

        KTable<Windowed<String>, Long> wordCountTable = textLines
                .flatMapValues(value -> Arrays.asList(PATTERN.split(value.toLowerCase())))
                .filter((key, value) -> value.contains("psa"))
                .groupBy((key, word) -> word)
                .windowedBy(TimeWindows.of(10))
                .count(Materialized.as("counts-store"));

        KStream<Windowed<String>, Long> wordCountStream = wordCountTable.toStream();

        wordCountStream.print(Printed.<Windowed<String>, Long>toSysOut().withLabel("Counted Parts"));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // Nie używać na produkcji, czyści stan strumieni
        // streams.cleanUp();

        // start Kafka Streams
        streams.start();

        // jeśli chcemy zamknać aplikację po jakimś czasie to najprościej dajemy sleep i close
        // Thread.sleep(5000L);
        // streams.close();

        // Eleganckie zamknięcie
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}