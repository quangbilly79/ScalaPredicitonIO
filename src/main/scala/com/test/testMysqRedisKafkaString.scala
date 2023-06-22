package com.test
import com.google.gson.JsonParser

import java.sql.{Connection, DriverManager, ResultSet}
import com.redis._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.predictionio.sdk.java.{Event, EventClient}

import scala.collection.JavaConverters._
import java.time.Duration
import java.util.Properties
import scala.concurrent.duration._

object testMysqRedisKafkaString {
  def main(args: Array[String]): Unit = {

    //------------------------------------------ Tạo event client cho PredicitonIO
    val accessKey = "bfLleHuY0jIHUDlYZyyiHu3Ss0p80iW4CVeUb6MFSnJKItTUJ2AbUf3kgRpyI4iQ"
    // sandbox: "lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7"
    // production: "bfLleHuY0jIHUDlYZyyiHu3Ss0p80iW4CVeUb6MFSnJKItTUJ2AbUf3kgRpyI4iQ"
    val eventUrl = "http://172.25.0.124:7070"
    // sandbox: "http://172.25.48.219:7070"
    // production: "http://172.25.0.105:7070"
    val client = new EventClient(accessKey, eventUrl)

    //------------------------------------------ Khởi tạo Mysql connection
    val url = "jdbc:mysql://172.25.0.113:3306/waka"
    val user = "etl"
    val password = "Vega123312##"
    // Tạo 1 connection duy nhất
    val connection = DriverManager.getConnection(url, user, password)
    // Dùng preparedStatement vì sql query khá giống nhau, chỉ khác 1 tham số (content_id)
    // Có thể dùng createStatement cũng được, những sẽ phải đặt trong vòng lặp
    val preparedStatement = connection.prepareStatement("SELECT status FROM content_dim where content_id = ? and content_type_key = 1 limit 1")

    //------------------------------------------ Khởi tạo Redis connection
    // Nhớ chọn host master để write
    val REDIS_HOST: String = "172.25.0.109"
    val REDIS_PORT: Int = 6379
    val redisClient = new RedisClient(REDIS_HOST, REDIS_PORT)
    // 30 ngày sẽ xóa cache cũ đi update lại, đề phòng TH 1 cuốn sách status từ ACT => INA hoặc ng lại
    val duration = scala.concurrent.duration.Duration(30, DAYS)
    // Chọn db số 2
    redisClient.select(2)


    //------------------------------------------ Khởi tạo Kafka
    // Tạo config
    val props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "data-node09:9992,data-node11:9992,data-node12:9992");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // Tạo 1 Kafka Consumer
    val consumer = new KafkaConsumer[String, String](props);
    // Subscribe 1 hoặc nh topic
    consumer.subscribe(List("waka_events").asJava);

    //------------------------------------------ Xử lý dữ liệu Kafka và đẩy lên API
    // Vòng lặp đọc từng Batch dl input
    while (true) {
      // poll timeout là thời gian nghỉ / chuyển sang topic khác nếu k có log nào ms
      val records = consumer.poll(Duration.ofMillis(100));

      // Vơi mỗi log (record), bđ xử lý dl
      records.asScala.foreach { record =>
        // Xử lý trường value (Json String) vs thư viện Gson
        var valueKafka = record.value();
        var jsonValue = new JsonParser().parse(valueKafka).getAsJsonObject;

        // Bđ lấy những trường cần thiết để upload lên PredicitonIO
        // Chỉ lấy event "reading"
        var event = jsonValue.get("event").getAsString;
        if (event == "reading") {
          try {
            // Lấy trường content_type_key (loại sách). Chỉ lấy content_type_key=1 (book), còn magazine, comic,... k lấy
            var content_type_key = jsonValue.get("properties").getAsJsonObject.get("content_type").getAsString;
            if (content_type_key == "1") {
              // Lấy trường vega_id (account_id ~ id ng dùng) và trường content_id (id sách), trường content_name để debug
              var vega_id = jsonValue.get("properties").getAsJsonObject.get("vega_id").getAsString;
              var content_id = jsonValue.get("properties").getAsJsonObject.get("content_id").getAsString;
              var content_name = jsonValue.get("properties").getAsJsonObject.get("content_title").getAsString
              //println(s"Content_id ban dau. vega_id: ${vega_id}, content_id: ${content_id}, content_name: ${content_name}")

              // redis_key là sql query luôn, có thể mã hóa cx đc. Check testRedisKafka.scala
              var redis_key = s"SELECT status FROM content_dim where content_id = ${content_id} and content_type_key = 1 limit 1"

              // Nếu tồn tịa cache trong redis và status = ACT, đẩy trực tiếp vào Engine luôn
              if (redisClient.get(redis_key).getOrElse() == "ACT") {
                //println(s"Co san trong redis, status = ACT. vega_id: ${vega_id}, content_id: ${content_id}, content_name: ${content_name}")
                //println(s"redis_key: ${redis_key}")
                // Tạo event và gửi lên PredicitonIO
                val readEvent = new Event()
                  .event("read")
                  .entityType("user")
                  .entityId(vega_id)
                  .targetEntityType("item")
                  .targetEntityId(content_id)
                println(s"Co san trong redis, status = ACT. Gui len predicitonIO Engine ${readEvent}");
                client.createEvent(readEvent);


                // Nếu không tồn tại cache trong Redis, thì chọc vào Mysql, và viết lại dl (status) vào redis
                // Để lần sau k phải chọc vào mysql nữa
              } else if (redisClient.get(redis_key).isEmpty){

                // Chạy SQL query tương ứng, lấy trg status với content_id tg ứng
                // SELECT status FROM content_dim where content_id = ? and content_type_key = 1 limit 1
                preparedStatement.setString(1, content_id)
                val resultSet = preparedStatement.executeQuery()

                // Check nếu tồn tại content_id trong bảng content_dim với if (resultSet.next())
                // (nhiều lúc sách mới trong ngày chưa đc cập nhập vào mysql dwh sẽ k có data => skip)
                if (resultSet.next()) {
                  // Lấy trường status
                  var status = resultSet.getString("status")
                  // Lưu cache: SQL query và status vào redis, duration là 1 tháng
                  redisClient.set(key = redis_key, value = status, expire = duration)

                  // nếu status = ACT thì ms đẩy vào Engine
                  if (status == "ACT") {
                    //println(s"K co san trong redis, status = ACT. content_id: ${content_id}, content_name: ${content_name}, status: ${status}")
                    val readEvent = new Event()
                      .event("read")
                      .entityType("user")
                      .entityId(vega_id)
                      .targetEntityType("item")
                      .targetEntityId(content_id)
                    println(s"Khong co san trong redis, status = ACT. Gui len predicitonIO Engine ${readEvent}");
                    client.createEvent(readEvent);
                  }
                  else { // status = INA thì skip k import len nua
                    //println(s"K co san trong redis, status = INA. content_id: ${content_id}, content_name: ${content_name}, status: ${status}")
                  }
                }
              }
            }
          }
            // Nếu chẳng may bị lỗi bất kì (kiểu field trong log k tồn tại,...) thì tiếp tục vòng lặp
          catch {
            case e: Exception =>
              println(s"Error processing record: ${e.getMessage}");
          }

        }
        //println("-------------Het 1 log---------------------")

      }
    }
  }
}
