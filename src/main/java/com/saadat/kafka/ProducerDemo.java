package com.saadat.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

  public static void main(String[] args) {

    String bootstrapServer = "127.0.0.1:9092";
    //create Properties
    Properties properties = new Properties();

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //create Producer
    KafkaProducer<String,String> kafkaProducer= new KafkaProducer<String,String>(properties);

    //create producer record
    ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","hello kafka world");

    //send data - asynchronous
    kafkaProducer.send(record);

    //flush data
    kafkaProducer.flush();

    //flush and close producer
    kafkaProducer.close();
  }
}
