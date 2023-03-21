package com.test
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object timestampConvert extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  var df = Seq("2023-03-13T15:18:14+0700").toDF("time")

  df = df.withColumn("timestamp_utc", to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm:ssZ"))
  df.show()

}