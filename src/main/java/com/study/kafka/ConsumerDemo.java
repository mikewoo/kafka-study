package com.study.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class ConsumerDemo {
    
    private static KafkaConsumer<String, String> consumer;

    private static  List<String> topics;

    public static void main(String[] args) {
        String topic = args[0];
        String groupId = args[1];
        String consumerid = args[2];
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.33.101:9092");
        properties.put("group.id", groupId);// consumer的分组id
        properties.put("auto.commit.interval.ms", "1000");// 每隔1s，自动提交offsets
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        topics = Arrays.asList(topic);
        consumer = new KafkaConsumer<>(properties);

        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String message = String.format(
                            "Topic:%s, GroupID:%s, Consumer ID:%s, PartitionID:%s, Offset:%s, Message Key:%s, Message Payload: %s",
                            record.topic(), groupId, consumerid, record.partition(),
                            record.offset(), new String(record.key()),
                            new String(record.value()));
                    System.out.println(message);
                }
            }
        } catch (WakeupException e) {
        } finally {
            consumer.close();
        }
    
    }
}
