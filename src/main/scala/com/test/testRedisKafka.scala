package com.test
import com.redis._
import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._
import java.security.MessageDigest

object testRedisKafka {
  def main(args: Array[String]): Unit = {
    // Connect to Redis server
    var REDIS_HOST: String = "172.25.0.109"
    var REDIS_PORT: Int = 6379
    val redisClient = new RedisClient(REDIS_HOST, REDIS_PORT)

    // Write data to Redis
    redisClient.select(2)


    // Set up Kafka connection
    val props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "data-ingestion01:9992,data-ingestion02:9992,data-ingestion03:9992");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    val consumer = new KafkaConsumer[String, String](props);
    consumer.subscribe(List("waka_events").asJava);

    while (true) {
      val records = consumer.poll(Duration.ofMillis(100));
      records.asScala.foreach { record =>
        // get the desired data from kafka (content_id field)
        var value = record.value();
        var jsonValue = new JsonParser().parse(value).getAsJsonObject;
        var event = jsonValue.get("event").getAsString;

        if (event == "read" || event == "reading") {
          var content_id = jsonValue.get("properties").getAsJsonObject.get("content_id").getAsString;
          var query = s"SELECT status FROM content_dim where content_id = ${content_id} and content_type_key = 1 limit 1";
          println(query)
          // Mã hóa redisKey dựa trên câu lệnh SQL
          val redisKey = s"wakarec:${MessageDigest.getInstance("MD5").digest(s"${query}".getBytes).map("%02x".format(_)).mkString}"
          println(redisKey)
          // Write ra redis key va value
          redisClient.set(key = redisKey, value = 1)

        }
      }
    }


  }
}

