package com.bigdatapassion.kafka.prodcon;

import com.bigdatapassion.kafka.callback.LoggerCallback;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.*;

public class KafkaProducerFromFile {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducerFromFile.class);
    private static final AtomicInteger MESSAGE_ID = new AtomicInteger(1);

    public static void main(String[] args) throws IOException {

        Producer<String, String> producer = new KafkaProducer<>(createProducerConfig());

        LoggerCallback callback = new LoggerCallback();

        String message = IOUtils.resourceToString("message.txt", StandardCharsets.UTF_8, KafkaProducerFromFile.class.getClassLoader());

        try {
            while (true) {

                for (long i = 0; i < 10; i++) {

                    String key = "key-" + MESSAGE_ID;
                    ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, key, message);

                    producer.send(data, callback); // async with callback
                    // producer.send(data); // async without callback
                    // producer.send(data).get(); // sync send

                    MESSAGE_ID.getAndIncrement();
                }

                LOGGER.info("Sended messages");
                Thread.sleep(SLEEP);
            }
        } catch (Exception e) {
            LOGGER.error("Błąd...", e);
        } finally {
            producer.flush();
            producer.close();
        }

    }

}
