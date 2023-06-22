package com.test
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.predictionio.sdk.java.{Event, EventClient}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object sparksubmitStop {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()


    // Convert current date (yyyy-MM-dd) to data_date_key (yyyyMMdd)
    val currentDateTime = LocalDate.now();
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val data_date_key = currentDateTime.format(formatter).toInt
    println("---------------Before spark stop")
    spark.stop()

    // Tạo event client cho PredicitonIO. Cái này làm cho spark-submit k end đc dù đặt ở đâu
    val accessKey = "lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7"
    // sandbox: "lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7"
    // production: "bfLleHuY0jIHUDlYZyyiHu3Ss0p80iW4CVeUb6MFSnJKItTUJ2AbUf3kgRpyI4iQ"
    val eventUrl = "http://172.25.48.219:7070"
    // sandbox: "http://172.25.48.219:7070"
    // production: "http://172.25.0.105:7070"
    // val client = new EventClient(accessKey, eventUrl)
    println("-------------After spark stop")
    sys.exit(0)

  }
}
