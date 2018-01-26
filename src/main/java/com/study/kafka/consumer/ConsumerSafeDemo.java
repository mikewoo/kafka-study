package com.study.kafka.consumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class ConsumerSafeDemo {

	private KafkaConsumer<String, String> consumer;

	private List<String> topics;
	
	/**
	 * singleton instance
	 */
	private static ConsumerSafeDemo instance = new ConsumerSafeDemo();

	private ConsumerSafeDemo() {}
	
	/**
	 * get instance
	 * @return
	 */
	public static ConsumerSafeDemo getInstance() {
		return instance;
	}

	public static void main(String[] args) {
		String topic = "topic-safe";
		String groupId = "consumer-safe";
		
		ConsumerSafeDemo.getInstance().init(topic, groupId);
		ConsumerSafeDemo.getInstance().run(groupId);

	}
	
	public void init(String topic, String groupId) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "192.168.33.100:9092");
		properties.put("group.id", groupId);// consumer的分组id
		properties.put("auto.commit.interval.ms", "1000");// 每隔1s，自动提交offsets
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		topics = Arrays.asList(topic);
		consumer = new KafkaConsumer<>(properties);
	}

	private void run(String groupId) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println("Consumer线程ID："+Thread.currentThread().getId());
				consumer(groupId, "consumer01");
			}
		}).start();
		new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println("Consumer线程ID："+Thread.currentThread().getId());
				consumer(groupId, "consumer02");
			}
		}).start();
	}

	private void consumer(String groupId, String consumerid) {
		try {
			consumer.subscribe(topics);
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					String message = String.format(
							"Topic:%s, GroupID:%s, Consumer ID:%s, PartitionID:%s, Offset:%s, Message Key:%s, Message Payload: %s",
							record.topic(), groupId, consumerid, record.partition(), record.offset(),
							new String(record.key()), new String(record.value()));
					System.out.println(message);
				}
			}
		} catch (WakeupException e) {
		} finally {
			consumer.close();
		}
	}
}
