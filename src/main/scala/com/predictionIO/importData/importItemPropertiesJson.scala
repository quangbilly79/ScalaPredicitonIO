package com.predictionIO.importData

import org.apache.predictionio.sdk.java.{Event, EventClient, FileExporter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}
import org.joda.time._

object importItemPropertiesJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()

    def importItemProperties(): Unit = {
      var sqlItemProperties =
        """
          |select cd.content_id, collect_set(cast(cab.author_id as string)) as author, collect_set(cast(ccb.category_id as string)) as category
          |from waka.content_dim as cd
          |left join waka.content_category_brid as ccb on cd.content_id = ccb.content_id
          |left join waka.content_author_brid as cab on cd.content_id = cab.content_id
          |where cd.status = "ACT" and ccb.status = "ACT"
          |group by cd.content_id
          |order by cd.content_id""".stripMargin
      val dfItemProperities = spark.sql(sqlItemProperties)
//      var EventSchema = StructType(Array(
//        StructField("event",StringType),
//        StructField("entityType",StringType),
//        StructField("entityId",StringType),
//        StructField("targetEntityType",StringType),
//        StructField("targetEntityId",StringType),
//        StructField("properties",MapType(StringType, StringType)),
//        StructField("eventTime",DateType)
//        )
//      )
      val colToMap = List("author", "category") // Turn 2 columns into a Map
        .flatMap(colName => List(lit(colName), col(colName)))

      val itemPropertiesEventJson = dfItemProperities
        .withColumn("event", lit("$set"))
        .withColumn("entityType", lit("item"))
        .withColumn("entityId", col("content_id").cast(StringType))
        .withColumn("properties", map(colToMap: _ *)) //sql map func (key1, val1, key2, val2,...) all must be column. _* mean extract all elems
        .withColumn("eventTime", lit(current_timestamp()))
        .select("event", "entityType", "entityId", "properties", "eventTime")

      itemPropertiesEventJson.write.json("propertiesEvent.json")
      // Luu trong hdfs hdfs://vftsandbox-namenode:8020/user/vgdata/propertiesEvent.json
      // hadoop fs -cat /user/vgdata/propertiesEvent.json/* | hadoop fs -put - /user/vgdata/mergedPropertiesEvent.json
      // hadoop fs -get /user/vgdata/mergedPropertiesEvent.json /home/vgdata/universal/importEventJson/
      // pio import --appid 4 --input /home/vgdata/universal/importEventJson/mergedPropertiesEvent.json
    }

    importItemProperties()
    spark.stop()
  }

}
