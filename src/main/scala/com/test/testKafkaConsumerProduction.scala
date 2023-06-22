package com.test
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._

object testKafkaConsumerProduction {
  def main(args: Array[String]): Unit = {

    // Set up Kafka connection
    val props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "data-node11:9992,data-node12:9992,data-node09:9992");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    val consumer = new KafkaConsumer[String, String](props);
    consumer.subscribe(List("", "").asJava);

    // Handle data of each log/row from Kafka
    while (true) {
      val records = consumer.poll(Duration.ofMillis(100));
      records.asScala.foreach { record =>
        var value = record.value();
        println(value)
        }
      }
    }
}

//{"subject":"avrotest1-value","version":4,"id":104,"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"int\"}]}","deleted":false}