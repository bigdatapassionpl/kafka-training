package com.bigdatapassion.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.bigdatapassion.KafkaConfigurationFactory.*;

public class WordCountApplication2 {

    private static final Pattern PATTERN = Pattern.compile("\\W+");

    public static void main(String[] args) {

        Properties config = getStreamConfig();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream(TOPIC);

        KTable<String, Long> wordCountTable = textLines
                .flatMapValues(value -> Arrays.asList(PATTERN.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count(Materialized.as("counts-store"));

        KStream<String, Long> wordCountStream = wordCountTable.toStream();

        wordCountStream.to(TOPIC_OUT, Produced.with(Serdes.String(), Serdes.Long()));

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