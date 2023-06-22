package com.test

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._

object testKafkaProducerProduction {
  def main(args: Array[String]): Unit = {

    // Set up Kafka connection
    val props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "data-ingestion01:9992,data-ingestion02:9992,data-ingestion03:9992");
    props.put("bootstrap.servers", "data-ingestion01:9992,data-ingestion02:9992,data-ingestion03:9992")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)

    val keyJson = """{"subject":"waka-rd-fact-v2_logreader-value","version":1,"magic":1,"keytype":"SCHEMA"}"""
    val valueJson = """{"subject":"waka-rd-fact-v2_logreader-value","version":1,"id":98,"schema":"{\"type\":\"record\",\"name\":\"UserActivity1\",\"fields\":[{\"name\":\"user_id\",\"type\":\"int\"},{\"name\":\"vega_id\",\"type\":\"int\"},{\"name\":\"user_name\",\"type\":\"string\"},{\"name\":\"email\"}]}", "deleted":false}"""

    val record = new ProducerRecord[String, String]("_schemas", keyJson, valueJson)
    producer.send(record)

    producer.close()
    }
}

//{"subject":"avrotest1-value","version":4,"id":104,"schema":"{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"int\"}]}","deleted":false}