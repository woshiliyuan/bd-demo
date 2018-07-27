package com.bd.kafka.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author yuan.li
 *
 */
@Component
public class KafkaProducer1 {

	@Autowired
	private KafkaProducer<String, String> kafkaProducer;

	public void send(String topic, String key, String value) {

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

		kafkaProducer.send(
				record,
				(metadata, exception) -> {
					if (metadata != null) {
						System.out.printf("send record partition:%d,offset:%d,keysize:%d,valuesize:%d %n", metadata.partition(),
								metadata.offset(), metadata.serializedKeySize(), metadata.serializedValueSize());
					}
					if (exception != null) {
						exception.printStackTrace();
					}
				});
		// kafkaProducer.close();
	}
}
