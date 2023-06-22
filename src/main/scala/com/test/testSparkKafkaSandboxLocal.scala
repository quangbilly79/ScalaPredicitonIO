package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object testSparkKafkaSandboxLocal extends App {
  // Set up log level nếu cần
  //Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local").getOrCreate()
  //spark.sparkContext.setLogLevel("ERROR")

  // dùng spark.readStream vs format("kafka") để đọc dl stream từ kafka topic (consumer)
  // tham số là "kafka.bootstrap.servers" cho list broker host:port, "subscribe" cho tên topic
  val inputKafkaDF = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "vftsandbox-node02:9092,vftsandbox-node03:9092,vftsandbox-snamenode:9092")
    .option("subscribe", "kafkaGenerator")
    .load()
  // Dl nằm trong cột "value", cast từ byte sang String
  val stringValueDF = inputKafkaDF.selectExpr("CAST(value AS STRING)")
  // +------------------------------------------------------------------------------------------+
  //|value                                                                                     |
  //+------------------------------------------------------------------------------------------+
  //|{"user": "Dr. Terresa Huels", "age": 34, "gender": "Female", "location": "North Berttown"}|
  //+------------------------------------------------------------------------------------------+

  // Khai báo Schema để xử lý dl dạng Json String nếu cần với from_json
  val schema = StructType(Array(StructField("user", StringType), StructField("age", IntegerType),
    StructField("gender", StringType), StructField("location", StringType)))
  val fromJsonDF = stringValueDF.withColumn("json", from_json(col("value"), schema))
  //root
  // |-- value: string (nullable = true)
  // |-- json: struct (nullable = true)
  // |    |-- user: string (nullable = true)
  // |    |-- age: integer (nullable = true)
  // |    |-- gender: string (nullable = true)
  // |    |-- location: string (nullable = true)
  //+-----------------------------------------------
  //|json                                          |
  //+-----------------------------------------------
  //|[Sharolyn Bahringer Jr., 46, Male, Rippinport]|
  //+-----------------------------------------------
  val structDF = fromJsonDF.withColumn("user_name", col("json.user"))
    .withColumn("user_age", col("json.age"))
    .withColumn("user_gender", col("json.gender"))
    .withColumn("user_location", col("json.location"))
    .select("user_name", "user_age", "user_gender", "user_location")
  //+----------------+--------+-----------+-------------+
  //|user_name       |user_age|user_gender|user_location|
  //+----------------+--------+-----------+-------------+
  //|Shawanda Monahan|50      |Female     |Port Maxwell |
  //+----------------+--------+-----------+-------------+

  // Test sử dụng groupBy khi xử ly dl Stream
  val genderGroupByDF = structDF.groupBy(col("user_gender"))
    .agg(count("*").as("count"), avg(col("user_age")).as("avg"))
  //+-----------+-----+------------------+
  //|user_gender|count|avg               |
  //+-----------+-----+------------------+
  //|Female     |3    |42.666666666666664|
  //|Male       |4    |34.5              |
  //+-----------+-----+------------------+

  // Write ra console, lưu ý outputMode
//  val query = genderGroupByDF
//    .writeStream
//    .format("console")
//    .option("truncate", "false")
//    .outputMode("Complete") //Append: K aggregate; Complete: Có aggreagate
//    .start()

  // Write ra Kafka topic (~ producer), cần các field/cột Key cx như Value
  val keyValueDF = genderGroupByDF.withColumn("value",
    concat(col("user_gender"), lit("-"), col("count"), lit("-"), col("avg")))
    .select("value")
  //gender-count-avgAge
  //Female-139-38.12230215827338
  //Male-127-36.93700787401575

  // Lưu ý các option
  val query = keyValueDF
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "vftsandbox-node02:9092,vftsandbox-node03:9092,vftsandbox-snamenode:9092")
    .option("topic", "sparkKafkaSandbox")
    .option("checkpointLocation", "/user/root/checkpoint/") // bắt buộc, lưu index record cuối cg mà đc đọc lần trc
    .outputMode("Complete") //Append: K aggregate; Complete: Có aggreagate
    .start()

  // Phải đánh dấu Terminate
  query.awaitTermination()
}
