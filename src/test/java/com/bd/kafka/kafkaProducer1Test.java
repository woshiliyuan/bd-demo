package com.bd.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.fastjson.JSON;
import com.bd.common.CustModel;
import com.bd.kafka.client.KafkaConsumer1;
import com.bd.kafka.client.KafkaProducer1;

/**
 * @author yuan.li
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class kafkaProducer1Test {
	@Autowired
	private KafkaProducer1 kafkaProducer1;

	@Autowired
	private KafkaConsumer1 kafkaConsumer1;

	@Value("${kafka.topic.bd.testtopic}")
	private String topic;

	@Test
	public void send() {
		// for (int i = 0; i < 1; i++) {
		CustModel custModel = new CustModel();
		custModel.setFirstname("li");
		custModel.setSecondname("yuan");
		custModel.setSex("m");
		custModel.setAge(0);
		kafkaProducer1.send(topic, "1", JSON.toJSONString(custModel));
		// }
		kafkaConsumer1.listen();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
