package com.predictionIO.importDataCDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.predictionio.sdk.java.{Event, EventClient}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConversions.mapAsJavaMap
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


object importRateEventCDC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()


    // Convert current date (yyyy-MM-dd) to data_date_key (yyyyMMdd)
    val currentDateTime = LocalDate.now();
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val data_date_key = currentDateTime.format(formatter).toInt
    //Về cơ bản ý tưởng là convert ngày/tháng/năm hiện tại (03/24/2023) thành data_date_key - 1 (20230323)
    // (trễ 1 ngày để dl cập nhập lên impala dwh table) và query theo data_date_key.
    // Query tương tự Initial Import
    val sqlRate =
      s"""
         |select su.account_id, fr.content_id, fr.rate, fr.data_date_key from waka.waka_pd_fact_rate as fr
         |join (select content_id from waka.content_dim where content_type_key = 1 and status = "ACT")
         |as cd on fr.content_id = cd.content_id
         |join waka.sqoop_user as su on fr.user_id = su.id
         |where fr.data_date_key = ${data_date_key - 1} and fr.content_type = 1
         |and su.account_id is not null and su.account_id != 0 and fr.user_id != 0
         |order by su.account_id
         |""".stripMargin
    val dfRate = spark.sql(sqlRate)
    val rateEventDF = dfRate
      .withColumn("event", lit("rate"))
      .withColumn("entityType", lit("user"))
      .withColumn("entityId", col("account_id").cast(StringType))
      .withColumn("targetEntityType", lit("item"))
      .withColumn("targetEntityId", col("content_id").cast(StringType))
      .withColumn("properties", map(lit("rate"), col("rate")))
      .withColumn("eventTime", from_unixtime( //lấy eventTime dựa trên data_date_key của event 20220131 => 2022-01-31'T'00:00:00.000+07:00
        unix_timestamp(col("data_date_key").cast("string"), "yyyyMMdd"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "properties", "eventTime")

    val rateEventList = rateEventDF.collect()
    // Sau khi collect dl event từ df về dạng list thì dừng Spark đc rồi
    spark.stop()

    // Tạo event client cho PredicitonIO
    val accessKey = "bfLleHuY0jIHUDlYZyyiHu3Ss0p80iW4CVeUb6MFSnJKItTUJ2AbUf3kgRpyI4iQ"
    // sandbox: "lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7"
    // production: "bfLleHuY0jIHUDlYZyyiHu3Ss0p80iW4CVeUb6MFSnJKItTUJ2AbUf3kgRpyI4iQ"
    val eventUrl = "http://172.25.0.124:7070"
    // sandbox: "http://172.25.48.219:7070"
    // production: "http://172.25.0.124:7070"
    val client = new EventClient(accessKey, eventUrl)
    //println(s"elem.getAs(properties) get class: ${rateEventList(0).getAs("properties").getClass}")
    //Map(rate -> 5) class scala.collection.immutable.Map$Map1
    //println(s"mapAsJavaMap(elem.getAs(properties)) get class: ${mapAsJavaMap(rateEventList(0).getAs("properties")).getClass}")
    //{rate=5}  get class: class scala.collection.convert.Wrappers$MapWrapper

    // Với mỗi event/elem, chuyển về đúng format/kdl và import lên Engine sử dụng SDK
    for (elem <- rateEventList) {
      //getAs có thể get theo index getInt(1) hoặc fieldname getAs[Int]("Id")

      val entityId = elem.getAs[String]("entityId") // "111111"
      //println(entityId.getClass) // String

//      val properties = elem.getAs[Map[String, Object]]("properties") // Map(rate -> 5)
//      // Object Java ~ AnyRef Scala. Scala Any thì bao gồm Java Object và Java Primitive Type, Nothing
//      println(properties.getClass) //class scala.collection.immutable.Map$Map1

      val properties = elem.getJavaMap[String, Object](5)
      // Object Java ~ AnyRef Scala. Scala Any thì bao gồm Java Object và Java Primitive Type, Nothing
      //println(properties.getClass) // class scala.collection.convert.Wrappers$MapWrapper (java.utils.map)

      val eventTime = elem.getAs[String]("eventTime") //"2023-03-22T00:00:00.000+07:00"
      val formatStringDateTime = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      //println(eventTime.getClass) // String, K convert đc trực tiếp sang Joda Date Time



      val event = new Event().event("rate")
        .entityType("user").entityId(elem.getAs[String]("entityId"))
        .targetEntityType("item").targetEntityId(elem.getAs[String]("targetEntityId"))
        .properties(properties) // Cần kdl java.utils.map[String, Object]
        .eventTime(DateTime.parse(eventTime, formatStringDateTime)) // Cần kdl joda.time.DateTime

      client.createEvent(event)
      println(s"$data_date_key: ${event}")

      //event {"event":"rate","entityType":"user","entityId":"38140700","targetEntityType":"item","targetEntityId":"43748",
      // "properties":{"rate":5},"eventTime":"2023-03-22T00:00:00.000+07:00"}
    }
    println("----------------------------------------")
    sys.exit(0)

  }
}
