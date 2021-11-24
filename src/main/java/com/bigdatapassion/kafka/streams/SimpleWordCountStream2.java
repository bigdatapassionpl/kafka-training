package com.bigdatapassion.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.regex.Pattern;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_SIMPLE;
import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.TOPIC_SIMPLE_WORDCOUNT;

public class SimpleWordCountStream2 extends KafkaStreamsApp {

    private static final String COUNT_STORE = "count-store";

    private static final Pattern PATTERN = Pattern.compile("\\W+");

    public static void main(String[] args) {
        new SimpleWordCountStream2().run();
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        KStream<String, String> textLines = builder.stream(TOPIC_SIMPLE);

        KTable<String, Long> wordCountTable = textLines
                .flatMapValues(value -> Arrays.asList(PATTERN.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count(Materialized.as(COUNT_STORE));

        KStream<String, Long> wordCountStream = wordCountTable.toStream();

        wordCountStream.to(TOPIC_SIMPLE_WORDCOUNT, Produced.with(Serdes.String(), Serdes.Long()));
    }

}