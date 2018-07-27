package com.bd.kafka.client;

import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author yuan.li
 *
 */
@Component
public class KafkaConsumer1 {

	@Value("${kafka.topic.bd.testtopic}")
	private String topic;

	@Autowired
	private KafkaConsumer<String, String> kafkaConsumer;

	// @PostConstruct
	public void listen() {

		kafkaConsumer.subscribe(Arrays.asList(topic));
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
			records.forEach(record -> {
				System.out.printf("topic:%s,partition:%d,offset:%d,key:%s,value:%s%n", record.topic(), record.partition(), record.offset(),
						record.key(), record.value());
			});
		}
	}
}
