package com.bigdatapassion.kafka

import com.bigdatapassion.kafka.callback.LoggerCallback
import com.bigdatapassion.kafka.conf.KafkaConfigurationFactory.{SLEEP, TOPIC, createProducerConfig}
import com.bigdatapassion.kafka.prodcon.KafkaProducerExample
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger

object KafkaProducerScalaExample {

  private val LOGGER = Logger.getLogger(classOf[KafkaProducerExample])

  def main(args: Array[String]): Unit = {

    val producer = new KafkaProducer[String, String](createProducerConfig)
    val callback = new LoggerCallback

    var messageId = 1

    try
        while (true) {
          var i = 0
          for (i <- 1 to 10) {
            val data = new ProducerRecord[String, String](TOPIC, "key-" + messageId, "message-" + messageId + " Ala ma kota")
            producer.send(data, callback)
            messageId += 1
          }
          LOGGER.info("Sended messages")
          Thread.sleep(SLEEP)
        }
    catch {
      case e: Exception => LOGGER.error("Błąd...", e)
    } finally {
      producer.flush()
      producer.close()
    }

  }

}
