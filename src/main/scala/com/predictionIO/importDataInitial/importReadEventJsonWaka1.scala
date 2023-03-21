package com.predictionIO.importDataInitial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object importReadEventJsonWaka1 {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()
    def importReadJson(): Unit = {
      var sqlRead =
        """
          |select user_id, content_id from waka.waka_pd_fact_reader
          |where data_date_key >= 20220106 and data_date_key < 20220701
          |and content_id in (select distinct content_id from waka.content_dim)""".stripMargin
      var dfRead = spark.sql(sqlRead)
      val readEventJson = dfRead
        .withColumn("event", lit("read"))
        .withColumn("entityType", lit("user"))
        .withColumn("entityId", col("user_id").cast(StringType))
        .withColumn("targetEntityType", lit("item"))
        .withColumn("targetEntityId", col("content_id").cast(StringType))
        .withColumn("eventTime", lit(current_timestamp()))
        .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "eventTime")

      readEventJson.write.json("readEventWaka1.json")
    }
    importReadJson()
    spark.stop()
  }
}
// Luu trong hdfs hdfs://vftsandbox-namenode:8020/user/vgdata/readEvent.json
// hadoop fs -cat /user/vgdata/readEvent.json/* | hadoop fs -put - /user/vgdata/mergedReadEvent.json
// hadoop fs -get /user/vgdata/mergedReadEvent.json /home/vgdata/universal/importEventJson/
// pio import --appid 4 --input /home/vgdata/universal/importEventJson/mergedReadEvent.json