package com.bd.kafka.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * @author yuan.li
 * 
 */
@Component
public class KafkaProducer {

	@Autowired
	@Qualifier("kafkaTemplate")
	private KafkaTemplate<String, Object> kafkaTemplate;

	public void send(String topic, Object basic) {
		kafkaTemplate.send(topic, basic);
	}

}
