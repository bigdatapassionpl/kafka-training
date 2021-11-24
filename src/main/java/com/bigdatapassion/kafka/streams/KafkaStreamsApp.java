package com.bigdatapassion.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.getStreamConfig;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_HYPHEN;

public abstract class KafkaStreamsApp {

    protected void run() {

        StreamsBuilder builder = new StreamsBuilder();

        Properties streamConfig = createStreamsProperties();

        // Building topology (data flow)
        createTopology(builder);

        // Running topology
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamConfig);

        // Delete the application's local state.
        // Note: In real application you'd call `cleanUp()` only under certain conditions.
        // See Confluent Docs for more details:
        // https://docs.confluent.io/current/streams/developer-guide/app-reset-tool.html#step-2-reset-the-local-environments-of-your-application-instances
        // streams.cleanUp();

        streams.start(); // start Kafka Streams App

        // Print topology
        System.out.println("TOPOLOGY:");
        System.out.println(topology.describe());

        System.out.println("TOPOLOGY:");
        System.out.println(streams.toString());

        System.out.println("TOPOLOGY:");
        streams.localThreadsMetadata().forEach(System.out::println);

        // To close topology after some time...
        // Thread.sleep(5000L);
        // streams.close();

        // Best way to close stream application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    protected Properties createStreamsProperties() {
        Properties streamConfig = getStreamConfig();
        String applicationId = LOWER_CAMEL.to(LOWER_HYPHEN, SimpleWordCountStream.class.getSimpleName());
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        return streamConfig;
    }

    protected abstract void createTopology(StreamsBuilder builder);

}
