package com.bigdatapassion.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.*;

public class SimpleWordCountStream extends KafkaStreamsApp {

    private static final String COUNT_STORE = "count-store";

    private static final String WORD_SPLIT_PATTERN = "\\W+";

    public static void main(final String[] args) {
        new SimpleWordCountStream().run();
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        KStream<String, String> inputStreamWithMessages = builder.stream(TOPIC_SIMPLE);

        KTable<String, Long> wordCountTable = inputStreamWithMessages
                .mapValues((ValueMapper<String, String>) String::toLowerCase) // --> to lower case
                .flatMapValues(textLine -> Arrays.asList(textLine.split(WORD_SPLIT_PATTERN))) // -> to words very simple convert
//                .map((key, value) -> new KeyValue<>(value, value)) // --> mapping value as key (like below)
                .selectKey((key, value) -> value) // --> mapping value as key (like above)
//                .through(TOPIC_THROUGH) // --> writing curent values to topic
//                .peek((key, value) -> System.out.println(String.format("(key:%s -> value:%s)", key, value))) // --> printing current values, debug purpose
                .groupByKey() // --> group stream by keys
                .count(Materialized.as(COUNT_STORE)); // --> saving state to topic: ${applicationId}-${internalStoreName}-changelog

        wordCountTable.toStream().to(TOPIC_SIMPLE_WORDCOUNT, Produced.with(Serdes.String(), Serdes.Long())); // --> konwerting KTable to regular stream and save in output topic
    }

}
