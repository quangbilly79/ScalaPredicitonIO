package com.test
import org.apache.kafka.clients.producer._
import java.util.Properties

object testKafkaProducerSandbox extends App{

  // Tạo properties dạng key,value
  val prop = new Properties()
  prop.setProperty("bootstrap.servers", "vftsandbox-node02:9092,vftsandbox-node03:9092,vftsandbox-snamenode:9092")
  prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  // Tạo 1 instance Kafka producer tu Properties bên trên
  val kafkaProducer = new KafkaProducer[String, String](prop)

  for (i <- 1 to 5) {
    // Tạo 1 record vs tên topic và value, key có thể có hoặc không
    val record = new ProducerRecord[String, String]("test", s"i--: $i")
    println(s"record: $record") // record: ProducerRecord(topic=test, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=null, value=i: 20, timestamp=null)
    // Gửi record
    kafkaProducer.send(record)
  }

  // Nhớ close
  kafkaProducer.close()


}
