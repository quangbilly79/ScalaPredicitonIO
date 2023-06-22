package com.predictionIO.importDataInitial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object importPropertiesCategoryEventJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()

    def importItemProperties(): Unit = {
      // Import properties cho Engine itemSim
      // Chỉ lấy category cũng như chỉ lấy status = ACT
      // Vì là backup Engine nên cx k quá quan trọng
      var sqlItemProperties =
        """
          |select cd.content_id,
          |collect_set(distinct cast(ccb.category_id as string)) as category
          |from waka.content_dim as cd
          |left join waka.content_category_brid ccb on cd.content_key = ccb.content_key
          |where cd.content_type_key = 1 and cd.status = "ACT" and ccb.status = "ACT"
          |group by cd.content_id
          |order by content_id""".stripMargin
      val dfItemProperities = spark.sql(sqlItemProperties)

      val itemPropertiesEventJson = dfItemProperities
        .withColumn("event", lit("$set"))
        .withColumn("entityType", lit("item"))
        .withColumn("entityId", col("content_id").cast(StringType))
        .withColumn("properties", map(lit("category"), col("category"))) //sql map func (key1, val1, key2, val2,...) all must be column. _* mean extract all elems
        .withColumn("eventTime", lit(current_timestamp()))
        .select("event", "entityType", "entityId", "properties", "eventTime")
      itemPropertiesEventJson.write.json("propertiesCategoryEvent.json")
    }
    importItemProperties()
    spark.stop()
  }

}
