package com.study.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerDemo {

	static private final String TOPIC = "topic-safe";

	public static void main(String[] args) throws InterruptedException {
		final KafkaProducer<String, String> producer = initProducer();

		for(int i = 1; i <= 20; i++) {
			send(producer, TOPIC, "key" + i, "this is the " + i + " message");
			Thread.sleep(1000);
		}

	}

	private static KafkaProducer<String, String> initProducer() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "192.168.33.100:9092");
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);// send message without delay
		properties.put(ProducerConfig.ACKS_CONFIG, "1");// 对应partition的leader写到本地后即返回成功。极端情况下，可能导致失败
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 序列化的方式，ByteArraySerializer或者StringSerializer
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("partitioner.class", com.study.kafka.partition.RoundRobinPartitioner.class);
		return new KafkaProducer<String, String>(properties);
	}

	public static void send(KafkaProducer<String, String> producer, String topic, String key, String message) {
		final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);
		producer.send(record, new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null) {
					e.printStackTrace();
					System.out.println("The offset of the record we just sent is: " + metadata);
				} else {
					System.out.println("send message " + record.value() + " success, " + "Sent to partition: "
							+ metadata.partition() + ", offset: " + metadata.offset());
				}
			}
		});
	}

}
