package com.test
import com.google.gson.JsonParser
import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._

object testMysqlKafka {
  def main(args: Array[String]): Unit = {
    // Set up mysql connection, create 1 connection at start
    val url = "jdbc:mysql://172.25.0.101:3306/waka"
    val user = "etl"
    val password = "Vega123312##"
    val connection = DriverManager.getConnection(url, user, password)
    val statement = connection.prepareStatement("SELECT * FROM content_dim where content_id = ? limit 1")

    // Set up Kafka connection
    val props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "data-ingestion01:9992,data-ingestion02:9992,data-ingestion03:9992");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    val consumer = new KafkaConsumer[String, String](props);
    consumer.subscribe(List("waka_events").asJava);

    // Handle data of each log/row from Kafka
    while (true) {
      val records = consumer.poll(Duration.ofMillis(100));
      records.asScala.foreach { record =>
        // get the desired data from kafka (content_id field)
        var value = record.value();
        var jsonValue = new JsonParser().parse(value).getAsJsonObject;
        var event = jsonValue.get("event").getAsString;

        if (event == "read" || event == "reading") {
          var content_id = jsonValue.get("properties").getAsJsonObject.get("content_id").getAsString;

          statement.setString(1, content_id)
          val resultSet = statement.executeQuery()
          if (resultSet.next) {
            val content_id = resultSet.getString("content_id")
            val content_name = resultSet.getString("content_name")
            val status = resultSet.getString("status")
            // print the desired output
            println(s"content_id: ${content_id}, content_name: ${content_name}, status: ${status}")
          }

          // I can't close connection here inside for loop.
          // connection.close()
        }
      }
    }
    // Put here won't have any effect
    //connection.close()
  }
}
