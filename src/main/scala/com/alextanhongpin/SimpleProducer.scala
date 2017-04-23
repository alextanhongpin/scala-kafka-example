package com.alextanhongpin

import java.util.{Date, Properties}
//import com.alextanhongpin.SimpleProducer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SimpleProducerExample {

	def main(args: Array[String]): Unit = {

		val argsCount = args.length
		if (argsCount == 0 || argsCount == 1) {
			throw new IllegalArgumentException("Provide topic name and message count as arguments")
		}
		// Topic name and message count to be published is passed from the command line
		val topic = args(0)
		var count = args(1)

		val messageCount = java.lang.Integer.parseInt(count)
		println("Topic name - " + topic)
		println("Message count - " + messageCount)
		val simpleProducer = new SimpleProducer()
		simpleProducer.publishMessage(topic, messageCount) 
	}
}

class SimpleProducer {

	private var producer:KafkaProducer[String, String] = _
	val props = new Properties()
	// Specifies the list of brokers that connect to the producer
	// in the format [node:port, node:port,...]
	// Here we have three brokers connecting to the producer
	props.put("bootstrap.servers",
		"192.168.1.185:9093, 192.168.1.185:9094, 192.168.1.185:9095")

	// Specifies the serializer used while preparing the message for
	// transmission from the producer to the broker.
	// Here we use the string encoder provided by kafka.
	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

	// Indicates to the broker to send an acknowledgement to the producer
	// when a message is received.
	props.put("request.required.acks", "1")

	producer = new KafkaProducer(props)

	def publishMessage(topic: String, messageCount: Int) {
		for (mCount <- 0 until messageCount) {
			val runtime = new Date().toString
			val msg = "Message publishing time - " + runtime
			println(msg)

			// Create a message
			val data = new ProducerRecord[String, String](topic, msg)

			// Publish the message
			producer.send(data)
		}

		// Close producer connection with broker.
		producer.close()
	}

}