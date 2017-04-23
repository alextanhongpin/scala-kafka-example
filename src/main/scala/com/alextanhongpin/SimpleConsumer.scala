package com.alextanhongpin

import java.util
import java.util.Properties

import kafka.consumer.KafkaStream
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._

import java.util.concurrent.ExecutorService;

object SimpleConsumerExample {


	def main(args: Array[String]): Unit = {
		val zookeeper = args(0)
		val groupId = args(1)
		val topic = args(2)

		println("Zookeeper port - " + zookeeper)
		println("groupId - " + groupId)
		println("topic - " + topic)

		val simpleHLConsumer = new SimpleConsumer(zookeeper, groupId, topic)
		simpleHLConsumer.testConsumer()
	}
}

class SimpleConsumer(zookeeper: String, groupId: String, private val topic:String) {
	private val consumer = new KafkaConsumer[String, String](createConsumerConfig(zookeeper, groupId))
	private val executor: ExecutorService = null
	private def createConsumerConfig(zookeeper: String, groupId: String): Properties = {
		val props = new Properties()
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.185:9093, 192.168.1.185:9094, 192.168.1.185:9095")
		props.put("zookeeper.connect", zookeeper)
		props.put("group.id", groupId)
		props.put("zookeeper.session.timeout.ms", "500")
		props.put("zookeeper.sync.time.ms", "250")
		props.put("auto.commit.interval.ms", "1000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
		// new ConsumerConfig(props)
		props
	}

	def testConsumer() {
		//val topicMap = new util.HashMap[String, Integer]()
		//topicMap.put(topic, 1)
		//val consumerStreamsMap = consumer.createMessageStreams(topicMap)
		//val streamList = consumerStreamsMap.get(topic)
		//for (stream <- streamList; aStream <- stream)
		//	println("Message from a single topic :: " + new String(aStream.message()))
		consumer.subscribe(util.Collections.singletonList(topic))

		while(true) {
			val records = consumer.poll(1000)

			for (record <- records) {
				System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
			}
		}
		if (consumer != null) {
			consumer.close()
		}
		if (executor != null) {
			executor.shutdown()
		}
	}
}