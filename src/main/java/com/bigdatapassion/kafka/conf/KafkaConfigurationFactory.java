package com.bigdatapassion.kafka.conf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaConfigurationFactory {

    public static final String KAFKA_SERVER = "bootstrap.mytestkafkacluster.europe-west3.managedkafka.bigdataworkshops.cloud.goog:9092";
    public static final String KAFKA_SCHEMA_REGISTRY = "http://localhost:8081/";

    public static final String TOPIC_SIMPLE = "test-simple";
    public static final String TOPIC_SIMPLE_WORDCOUNT = "test-simple-wordcount";
    public static final String TOPIC_THROUGH = "test-wordcount-through";

    public static final String TOPIC_PERSON = "test-person"; //json topic
    public static final String TOPIC_PERSON_FILTERED = "test-person-filtered"; //json topic

    public static final String TOPIC_PRODUCT_AVRO = "test-product-avro";
    public static final String TOPIC_PRODUCT_JSON = "test-product-json";
    public static final String TOPIC_PRODUCT_JSON_SCHEMA = "test-product-json-schema";
    public static final String TOPIC_PRODUCT_JSON_SCHEMA_REGISTRY = "test-product-json-schema-registry";
    public static final String TOPIC_PRODUCT_AVG = "test-product-avg"; //avro topic

    public static final String CONSUMER_GROUP = "my-group";

    public static final int TIMEOUT = 10000;
    public static final int SLEEP = 1000;
    public static final int MESSAGE_BATCH_COUNT = 1;

    /**
     * https://kafka.apache.org/documentation/#newconsumerconfigs
     */
    public static Properties createConsumerConfig() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);

        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // if true manual commit is no needed

        return consumerConfig;
    }

    /**
     * https://kafka.apache.org/documentation/#producerconfigs
     */
    public static Properties createProducerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        // producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "");

        producerConfig.put("security.protocol", "SASL_SSL"); // or "SASL_PLAINTEXT"
        producerConfig.put("sasl.mechanism", "PLAIN"); // or SCRAM-SHA-256, GSSAPI, etc.

        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // no duplicated messages
        // producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        // producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        // producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        // producerConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760"); // 10MB

        return producerConfig;
    }

    /**
     * https://kafka.apache.org/documentation/#streamsconfigs
     */
    public static Properties createStreamConfig() {
        Properties streamConfig = new Properties();
        streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "unknown-application-id");

        streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        streamConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        // streamConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        // consumer streamConfig
        streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // turn off cache, not recommended in prod
        // streamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        return streamConfig;
    }

}
