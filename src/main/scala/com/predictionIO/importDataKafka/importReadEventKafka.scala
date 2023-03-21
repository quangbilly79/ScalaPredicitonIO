package com.predictionIO.importDataKafka

import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.predictionio.sdk.java.{Event, EventClient}

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._

object importReadEventKafka {
  def main(args: Array[String]): Unit = {
    // Tạo event client cho PredicitonIO
    val client = new EventClient("bfLleHuY0jIHUDlYZyyiHu3Ss0p80iW4CVeUb6MFSnJKItTUJ2AbUf3kgRpyI4iQ", "http://172.25.0.105:7070")

    // Tạo Properties cho Kafka Client Consumer
    val props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "data-ingestion01:9992,data-ingestion02:9992,data-ingestion03:9992");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    // Tạo 1 Kafka Consumer
    val consumer = new KafkaConsumer[String, String](props);
    // Subscribe 1 hoặc nh topic
    consumer.subscribe(List("waka_events").asJava);

    // Vòng lặp đọc từng Batch dl input
    while (true) {
      // Đọc từng log (records ~ log).
      // poll timeout là thời gian nghỉ / chuyển sang topic khác nếu k có log nào ms
      val records = consumer.poll(Duration.ofMillis(100));

      // Vơi mỗi log (records), bđ xử lý dl
      records.asScala.foreach { record =>
        // Xử lý trg value (Json String) vs thư viện Gson
        var value = record.value();
        var jsonValue = new JsonParser().parse(value).getAsJsonObject;

        // Bđ lấy những trường cần thiết để upload lên PredicitonIO
        var event = jsonValue.get("event").getAsString;
        if (event == "read" || event == "reading") {
          try {
            var content_type_key = jsonValue.get("properties").getAsJsonObject.get("content_type").getAsString;
            if (content_type_key == "1") {
              var vega_id = jsonValue.get("properties").getAsJsonObject.get("vega_id").getAsString;
              var content_id = jsonValue.get("properties").getAsJsonObject.get("content_id").getAsString;

              // Tạo event và gửi lên PredicitonIO
              val readEvent = new Event()
                .event("read")
                .entityType("user")
                .entityId(vega_id)
                .targetEntityType("item")
                .targetEntityId(content_id)
              println(readEvent);
              client.createEvent(readEvent);
            }
          }
          // Nếu bị lỗi thì tiếp tục vòng lặp
          catch {
            case e: Exception =>
              println(s"Error processing record: ${e.getMessage}");
          }

        }

      }
    }
  }
}

// Chạy code với
//export SPARK_KAFKA_VERSION=0.10;
//nohup spark2-submit  --master yarn --deploy-mode client \
//--keytab vega.keytab --principal vega@BI.VEGA.COM \
//--class com.predictionIO.importData.importReadEventKafka \
//  --driver-memory 2G \
//--executor-memory 1G \
//--num-executors 5 \
//  --conf spark.ui.port=4043 \
//  --files=jaas.conf,krb5.conf \
//  /home/vgdata/quang/predicitionIO/predictionIO.jar &> /home/vgdata/quang/predicitionIO/importReadDebug.txt &
