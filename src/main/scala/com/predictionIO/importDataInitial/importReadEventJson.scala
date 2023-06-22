package com.predictionIO.importDataInitial

import org.apache.predictionio.sdk.java.{Event, EventClient, FileExporter}
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}
import org.joda.time._

object importReadEventJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()
    def importReadJson(): Unit = {
      // event read.
      // Lấy account_id (vegaid_id) chứ k phải user_id
      // Vì bảng fact_read cột content_key bị lỗi nên không join theo content_key đc
      // chỉ lấy book(content_type_key = 1) và status = ACT và content_type_key = 1
      // cũng như k lấy user_id/vegaid_id = 0 (k có account) để tránh loãng dl
      // Import lần đầu từ 01 / 01 / 2022 đến tháng 20 / 10 / 2022 (dừng update)
      var sqlRead =
        """
          |select fr.vegaid_id, fr.content_id, fr.data_date_key from waka.waka_pd_fact_reader as fr
          |join (select content_id from waka.content_dim where status = "ACT" and content_type_key = 1)
          |as cd on fr.content_id = cd.content_id
          |where fr.data_date_key >= 20220101 and fr.data_date_key <= 20231231 and fr.content_type_key = 1
          |and fr.vegaid_id != 0 and fr.user_id != 0
          |order by fr.vegaid_id
          |""".stripMargin
      var dfRead = spark.sql(sqlRead)
      val readEventJson = dfRead
        .withColumn("event", lit("read"))
        .withColumn("entityType", lit("user"))
        .withColumn("entityId", col("vegaid_id").cast(StringType))
        .withColumn("targetEntityType", lit("item"))
        .withColumn("targetEntityId", col("content_id").cast(StringType))
        .withColumn("eventTime", from_unixtime( //lấy eventTime dựa trên data_date_key của event 20220131 => 2022-01-31'T'00:00:00.000+07:00
          unix_timestamp(col("data_date_key").cast("string"), "yyyyMMdd"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
        .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "eventTime")

      readEventJson.write.json("readEvent.json")
    }
    importReadJson()
    spark.stop()
  }
}
// Luu trong hdfs hdfs://vftsandbox-namenode:8020/user/vgdata/readEvent.json
// hadoop fs -cat /user/vgdata/readEvent.json/* | hadoop fs -put - /user/vgdata/mergedReadEvent.json
// hadoop fs -get /user/vgdata/mergedReadEvent.json /home/vgdata/universal/importEventJson/
// pio import --appid 4 --input /home/vgdata/universal/importEventJson/mergedReadEvent.json