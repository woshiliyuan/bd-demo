package com.bd.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.bd.common.CustModel;
import com.bd.kafka.spring.KafkaProducer;

/**
 * @author yuan.li
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/applicationContext.xml" })
public class KafkaProducerTest {

	@Autowired 
	private KafkaProducer kafkaProducer;

	@Value("${kafka.topic.bd.testtopic}")
	private String topic;

	@Test
	public void send() {
		CustModel custModel = new CustModel();
		custModel.setFirstname("li");
		custModel.setSecondname("yuan");
		custModel.setSex("m");
		custModel.setAge(18);
		kafkaProducer.send(topic, custModel);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	// cd /opt/cloudera/parcels/KAFKA-3.0.0-1.3.0.0.p0.40/lib/kafka/bin/
	// ./kafka-console-producer.sh --broker-list 172.29.3.20:9092,172.29.3.21:9092,172.29.3.22:9092 --topic BD.TEST_TOPIC
	// ./kafka-console-consumer.sh --zookeeper 172.29.3.20:2181,172.29.3.21:2181,172.29.3.22:2181 --topic BD.TEST_TOPIC --from-beginning
	// ./kafka-topics.sh --zookeeper 172.29.3.20:2181,172.29.3.21:2181,172.29.3.22:2181 --list

	// cd /opt/cloudera/parcels/CDH-5.13.0-1.cdh5.13.0.p0.29/lib/zookeeper/bin
	// ./zkCli.sh
	// ls /brokers/topics
	// rmr /brokers/topics/BD.TEST_TOPIC

	// ./kafka-topics -create -zookeeper 172.29.3.20:2181,172.29.3.21:2181,172.29.3.22:2181 -replication-factor 3 -partitions 6 -topic
	// BD.TEST_TOPIC

	// ./kafka-topics.sh --zookeeper 172.29.3.20:2181,172.29.3.21:2181,172.29.3.22:2181 --topic BD.TEST_TOPIC --describe
}
