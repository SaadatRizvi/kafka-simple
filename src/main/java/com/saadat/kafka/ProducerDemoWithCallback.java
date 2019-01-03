package com.saadat.kafka;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class ProducerDemoWithCallback {

  public static void main(String[] args) {

    String bootstrapServer = "127.0.0.1:9092";
    // create Properties
    Properties properties = new Properties();

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create Producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

    for (Integer i = 0; i < 10; i++) {
      // create producer record
      ProducerRecord<String, String> record =
          new ProducerRecord<String, String>("second_topic", "hello kafka world" + i);

      // send data - asynchronous
      kafkaProducer.send(
          record,
          (RecordMetadata recordMetadata, Exception e) -> {
            // this callback function is executed everytime a record is sent successfully or an
            // exception is thrown
            if (e == null) {
              // record was sent successfully
              log.info(
                  "Recieved metadata: \n"
                      + "Topic: "
                      + recordMetadata.topic()
                      + "\n"
                      + "Partion: "
                      + recordMetadata.partition()
                      + "\n"
                      + "Offset: "
                      + recordMetadata.offset()
                      + "\n"
                      + "Timestamp: "
                      + recordMetadata.timestamp());
            } else {
              log.error("Error while producing", e);
            }
          });
    }
    // flush data
    kafkaProducer.flush();

    // flush and close producer
    kafkaProducer.close();
  }
}
