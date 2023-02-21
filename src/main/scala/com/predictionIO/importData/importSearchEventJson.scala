package com.predictionIO.importData

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
      var sqlSearch =
        """
          select user_id, fr.content_id from waka.waka_pd_fact_log_select_on_search as fr
          |join waka.content_dim as cd on fr.content_id = cd.content_id
          |where data_date_key >= 20220101 and data_date_key < 20220701
          |and cd.status = "ACT"
          |""".stripMargin
      var dfSearch = spark.sql(sqlSearch)
      val searchEventJson = dfSearch
        .withColumn("event", lit("search"))
        .withColumn("entityType", lit("user"))
        .withColumn("entityId", col("user_id").cast(StringType))
        .withColumn("targetEntityType", lit("item"))
        .withColumn("targetEntityId", col("content_id").cast(StringType))
        .withColumn("eventTime", lit(current_timestamp()))
        .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "eventTime")

      searchEventJson.write.json("searchEvent.json")
    }
    importSearchJson()
    spark.stop()
  }
}
// Luu trong hdfs hdfs://vftsandbox-namenode:8020/user/vgdata/searchEvent.json
// hadoop fs -cat /user/vgdata/searchEvent.json/* | hadoop fs -put - /user/vgdata/mergedSearchEvent.json
// hadoop fs -get /user/vgdata/mergedSearchEvent.json /home/vgdata/universal/importEventJson/
// pio import --appid 4 --input /home/vgdata/universal/importEventJson/mergedSearchEvent.json