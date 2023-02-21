package com.predictionIO.importData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object importItemPropertiesCategoryJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()

    def importItemProperties(): Unit = {
      var sqlItemProperties =
        """
          |select cd.content_id, collect_set(cast(ccb.category_id as string)) as category
          |from waka.content_dim as cd
          |left join waka.content_category_brid as ccb on cd.content_id = ccb.content_id
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

      val itemPropertiesEventJson = dfItemProperities
        .withColumn("event", lit("$set"))
        .withColumn("entityType", lit("item"))
        .withColumn("entityId", col("content_id").cast(StringType))
        .withColumn("properties", map(lit("category"), col("category"))) //sql map func (key1, val1, key2, val2,...) all must be column. _* mean extract all elems
        .withColumn("eventTime", lit(current_timestamp()))
        .select("event", "entityType", "entityId", "properties", "eventTime")

      itemPropertiesEventJson.write.json("propertiesEventCategory.json")
      // Luu trong hdfs hdfs://vftsandbox-namenode:8020/user/vgdata/propertiesEventCategory.json
      // hadoop fs -cat /user/vgdata/propertiesEventCategory.json/* | hadoop fs -put - /user/vgdata/mergedPropertiesEventCategory.json
      // hadoop fs -get /user/vgdata/mergedPropertiesEventCategory.json /home/vgdata/universal/importEventJson/
      // pio import --appid 6 --input /home/vgdata/universal/importEventJson/mergedPropertiesEventCategory.json
    }

    importItemProperties()
    spark.stop()
  }

}
