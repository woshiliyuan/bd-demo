package com.bd.kafka.spring;

import org.springframework.stereotype.Component;

/**
 * @author yuan.li
 * 
 */
@Component
public class KafkaConsumer {

	// @KafkaListener(topics = "${kafka.topic.bd.testtopic}", containerFactory = "concurrentKafkaListenerContainerFactory")
	// public void listen(String str, Object data) {
	//
	// // System.out.println(data.toString());
	// }
}
