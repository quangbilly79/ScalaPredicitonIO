package com.test
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDate;
import java.time.format.DateTimeFormatter
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object timestampConvert extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  // Convert string "2023-03-13T15:18:14+0700" to spark timestamp() "2023-03-13 15:18:14"
  var df = Seq("2023-03-13T15:18:14+0700").toDF("time")
  df = df.withColumn("timestamp_utc", to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm:ssZ"))
  df.show()

  //Convert string "2022-03-21T00:00:00.000+07:00" to org.joda.time.DateTime
  val stringDateTime = "2022-03-21T00:00:00.000+07:00"
  val formatStringDateTime = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  val jodaDateTime = DateTime.parse(stringDateTime, formatStringDateTime)
  println(jodaDateTime) //2022-03-21T00:00:00.000+07:00
  println(jodaDateTime.getClass) // class org.joda.time.DateTime

  // Convert current date (yyyy-MM-dd) to data_date_key (yyyyMMdd)
  // 2023-03-21 to 20230321
  val localDate = LocalDate.now(); //java.time.LocalDate = 2023-03-21
  val formatLocalDate = DateTimeFormatter.ofPattern("yyyyMMdd")
  val formatedLocalDate = localDate.format(formatLocalDate);
  val intLocalDate = formatedLocalDate.toInt;
  print(intLocalDate)


  // Convert data_date_key (yyyyMMdd) to a timestamp GMT column yyyy-MM-dd'T'HH:mm:ss.SSSXXX
  // 20220120 to 2022-01-20T00:00:00.000+07:00
  val df1 = Seq(20220120, 20220310).toDF("date_int")
  val dfTimestamp = df1
    .withColumn("date_str_unix_time",
      unix_timestamp($"date_int".cast("string"), "yyyyMMdd"))
    .withColumn("date_str_from_unixtime",
      from_unixtime(
        unix_timestamp($"date_int".cast("string"), "yyyyMMdd"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

  // show the result
  dfTimestamp.show(truncate = false)
//  +--------+------------------+-----------------------------+
//  |date_int|date_str_unix_time|date_str_from_unixtime       |
//  +--------+------------------+-----------------------------+
//  |20220120|1642611600        |2022-01-20T00:00:00.000+07:00|
//  |20220310|1646845200        |2022-03-10T00:00:00.000+07:00|
//  +--------+------------------+-----------------------------+
}