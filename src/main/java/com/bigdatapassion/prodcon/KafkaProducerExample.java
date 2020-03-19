package com.bigdatapassion.prodcon;

import com.bigdatapassion.callback.LoggerCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bigdatapassion.KafkaConfigurationFactory.*;

public class KafkaProducerExample {

    private static final Logger LOGGER = Logger.getLogger(KafkaProducerExample.class);
    private static final AtomicInteger MESSAGE_ID = new AtomicInteger(1);
    private static final String[] MESSAGES = {"Ala ma kota, Ela ma psa", "W Szczebrzeszynie chrzaszcz brzmi w trzcinie", "Byc albo nie byc"};

    public static void main(String[] args) {

        Producer<String, String> producer = new KafkaProducer<>(createProducerConfig());

        LoggerCallback callback = new LoggerCallback();
        Random random = new Random(System.currentTimeMillis());

        try {
            while (true) {

                for (long i = 0; i < MESSAGE_BATCH_COUNT; i++) {

                    int id = random.nextInt(MESSAGES.length);
                    String key = "key-" + id;
                    String value = MESSAGES[id];
                    ProducerRecord<String, String> data = new ProducerRecord<>(TOPIC, key, value);

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
