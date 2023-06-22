package com.test
import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer._
import java.time.Duration
import scala.collection.JavaConverters._
import java.util.Properties

object testKafkaConsumerSandbox extends App {
  // Khá tương tự producer, khác ở đoạn key.deser và value.deser và StringDerser
  // Thêm ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" để đọc từ đầu
  // (lưu ý đổi ms group.id nếu cần, mỗi group sẽ lưu giá trị offset (vị trí đọc hiện tại) tương ứng)
  val prop = new Properties()
  prop.setProperty("bootstrap.servers", "vftsandbox-node02:9092,vftsandbox-node03:9092,vftsandbox-snamenode:9092")
  prop.setProperty("group.id", "group2")
  prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  // Tạo Kafka Consumer từ prop bên trên
  val kafkaConsumer = new KafkaConsumer[String, String](prop)

  // Subscribe đến 1 / 1 list các topic
  kafkaConsumer.subscribe(List("test").asJava)

  // Đọc dl như bên dưới
  // records có dạng java iterable, cần convert sang scala iterable. Lấy "value" thôi
  // poll(100ms) có nghĩa là sau khi gọi hàm poll, consumer sẽ gửi request lấy dl đến broker
  // consumer sẽ đợi 100ms để nhận dl về, nếu k có gì thì trả về rỗng
  while (true) {
    val records = kafkaConsumer.poll(Duration.ofMillis(100))
    for (record <- records.asScala) {
      val value = record.value()
      println(value) // testValllll
    }
  }


}
