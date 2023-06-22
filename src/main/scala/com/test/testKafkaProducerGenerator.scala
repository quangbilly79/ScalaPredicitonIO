package com.test
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import com.github.javafaker.Faker
import scala.util.Random

object testKafkaProducerGenerator extends App {
  // Tạo Kafka Properties, chủ yếu là list các broker
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "vftsandbox-node02:9092,vftsandbox-node03:9092,vftsandbox-snamenode:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") //io.confluent.kafka.serializers.KafkaJsonSerializer
  val topic = "kafkaGenerator"

  // Tạo 1 producer từ props bên trên
  val producer = new KafkaProducer[String, String](props)

  // Tạo 1 instace từ class Faker, cái class này hỗ trợ gen dl fake kiểu tên ng, tên TP,...
  val faker = new Faker()

  // Function gen random message dùng class Faker, trả v 1 chuỗi String dạng Json
  def generateRandomMessage(): String = {
    val randomName = faker.name().fullName()
    val randomAge = faker.number().numberBetween(18, 60)
    val randomGender = if (Random.nextBoolean()) "Male" else "Female"
    val randomLocation = faker.address().cityName()
    return s"""{"user": "$randomName", "age": $randomAge, "gender": "$randomGender", "location": "$randomLocation"}"""
  }

  // Produce record liên tục, mỗi record random dựa trên hàm bên trên
  while (true) {
    val randomMessage = generateRandomMessage()
    val record = new ProducerRecord[String, String](topic, randomMessage)
    producer.send(record)
    println(record)
    Thread.sleep(5000) // Delay 5s mỗi record
  }

}
