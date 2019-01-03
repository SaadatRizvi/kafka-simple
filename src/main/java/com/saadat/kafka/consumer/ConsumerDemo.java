package com.saadat.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ConsumerDemo {

  public static void main(String[] args) {
    String bootstrapServer = "127.0.0.1:9092";
    String groupId = "first-java-consumer";
    String topic = "first_topic";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

    // subscribe consumer to topic(s)
    kafkaConsumer.subscribe(Arrays.asList(topic));

    // poll for new data
    while (true) {
      ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100L));

      for (ConsumerRecord<String, String> record : consumerRecords) {
        log.info(
            "Key: {},\nValue: {},\nPartition: {},\nOffset: {}",
            record.key(),
            record.value(),
            record.partition(),
            record.offset());
      }
    }
  }
}
