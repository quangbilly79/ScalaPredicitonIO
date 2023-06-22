package com.predictionIO.importDataCDC

import org.apache.predictionio.sdk.java.{Event, EventClient}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object importWishlistEventCDC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()


    // Convert current date (yyyy-MM-dd) to data_date_key (yyyyMMdd)
    val currentDateTime = LocalDate.now();
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val data_date_key = currentDateTime.format(formatter).toInt

    //Về cơ bản ý tưởng là convert ngày/tháng/năm hiện tại (03/24/2023) thành data_date_key - 1 (20230323)
    // (trễ 1 ngày để dl cập nhập lên impala dwh table) và query theo data_date_key.
    // Query tương tự Initial Import
    var sqlWishlist =
      s"""
         |select su.account_id, fw.content_id, fw.data_date_key from waka.waka_pd_fact_wishlist as fw
         |join (select content_id from waka.content_dim where content_type_key = 1 and status = "ACT")
         |as cd on fw.content_id = cd.content_id
         |join waka.sqoop_user as su on fw.user_id = su.id
         |where fw.data_date_key = ${data_date_key-1} and fw.content_type = 1
         |and su.account_id is not null and su.account_id != 0 and fw.user_id != 0
         |order by su.account_id
         |""".stripMargin
    var dfWishlist = spark.sql(sqlWishlist)
    val rateEventDF = dfWishlist
      .withColumn("event", lit("wishlist"))
      .withColumn("entityType", lit("user"))
      .withColumn("entityId", col("account_id").cast(StringType))
      .withColumn("targetEntityType", lit("item"))
      .withColumn("targetEntityId", col("content_id").cast(StringType))
      .withColumn("eventTime", from_unixtime( //lấy eventTime dựa trên data_date_key của event 20220131 => 2022-01-31'T'00:00:00.000+07:00
        unix_timestamp(col("data_date_key").cast("string"), "yyyyMMdd"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "eventTime")

    val rateEventList = rateEventDF.collect()
    spark.stop()

    // Tạo event client cho PredicitonIO
    val accessKey = "bfLleHuY0jIHUDlYZyyiHu3Ss0p80iW4CVeUb6MFSnJKItTUJ2AbUf3kgRpyI4iQ"
    // sandbox: "lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7"
    // production: "bfLleHuY0jIHUDlYZyyiHu3Ss0p80iW4CVeUb6MFSnJKItTUJ2AbUf3kgRpyI4iQ"
    val eventUrl = "http://172.25.0.124:7070"
    // sandbox: "http://172.25.48.219:7070"
    // production: "http://172.25.0.124:7070"
    val client = new EventClient(accessKey, eventUrl)

    val formatStringDateTime = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

    for (elem <- rateEventList) {
      val event = new Event().event("wishlist")
        .entityType("user").entityId(elem.getAs[String]("entityType"))
        .targetEntityType("item").targetEntityId(elem.getAs[String]("entityId"))
        .eventTime(DateTime.parse(elem.getAs[String]("eventTime"), formatStringDateTime)) // Cần kdl joda.time.DateTime
      client.createEvent(event)
      println(s"$data_date_key: ${event}")
    }
    println("----------------------------------------")

    sys.exit(0)

  }
}
