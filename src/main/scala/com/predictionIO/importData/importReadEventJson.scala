package com.predictionIO.importData

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
      var sqlRead =
        """
          |select user_id, fr.content_id from waka.waka_pd_fact_reader as fr
          |join waka.content_dim as cd on fr.content_id = cd.content_id
          |where data_date_key >= 20220101 and data_date_key < 20220701
          |and cd.status = "ACT"
          |""".stripMargin
      var dfRead = spark.sql(sqlRead)
      val readEventJson = dfRead
        .withColumn("event", lit("read"))
        .withColumn("entityType", lit("user"))
        .withColumn("entityId", col("user_id").cast(StringType))
        .withColumn("targetEntityType", lit("item"))
        .withColumn("targetEntityId", col("content_id").cast(StringType))
        .withColumn("eventTime", lit(current_timestamp()))
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