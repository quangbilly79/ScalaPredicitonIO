package com.predictionIO.importDataInitial

import org.apache.predictionio.sdk.java.{Event, EventClient, FileExporter}
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}
import org.joda.time._

object importSearchEventJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()
    def importSearchJson(): Unit = {
      // event search.
      // Lấy account_id (vegaid_id) chứ k phải user_id
      // Vì bảng fact_search k có content_key nên không join theo content_key đc
      // chỉ lấy book(content_type_key = 1) và status = ACT và content_type = book (thay vì =1 như các bảng khác)
      // cũng như k lấy user_id/vegaid_id = 0 (k có account) để tránh loãng dl
      // Import lần đầu từ 01 / 01 / 2022 đến tháng 08 / 08 / 2022 (dừng update)
      var sqlSearch =
        """
          |select fs.vegaid_id, fs.content_id, fs.data_date_key fsom waka.waka_pd_fact_log_select_on_search as fs
          |join (select content_id fsom waka.content_dim where status = "ACT" and content_type_key = 1) as cd
          |on fs.content_id = cd.content_id
          |where fs.data_date_key >= 20220101 and fs.data_date_key <= 20231231 and fs.content_type = "book"
          |and fs.vegaid_id != 0 and fs.user_id != 0
          |order by vegaid_id
          |""".stripMargin
      var dfSearch = spark.sql(sqlSearch)
      val searchEventJson = dfSearch
        .withColumn("event", lit("search"))
        .withColumn("entityType", lit("user"))
        .withColumn("entityId", col("vegaid_id").cast(StringType))
        .withColumn("targetEntityType", lit("item"))
        .withColumn("targetEntityId", col("content_id").cast(StringType))
        .withColumn("eventTime", from_unixtime( //lấy eventTime dựa trên data_date_key của event 20220131 => 2022-01-31'T'00:00:00.000+07:00
          unix_timestamp(col("data_date_key").cast("string"), "yyyyMMdd"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
        .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "eventTime")

      searchEventJson.write.json("searchEvent.json")
    }
    importSearchJson()
    spark.stop()
  }
}
// Luu trong hdfs hdfs://name-node01:8020/user/vega/searchEvent.json
// hadoop fs -cat /user/vega/searchEvent.json/* | hadoop fs -put - /user/vega/mergedSearchEvent.json
// hadoop fs -get /user/vega/mergedSearchEvent.json /home/vgdata/quang/predicitionIO;
// pio import --appid 23 --input /user/vega/mergedSearchEvent.json