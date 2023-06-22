package com.predictionIO.importDataInitial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, _}


object importPropertiesEventJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()

    def importItemProperties(): Unit = {
      // event Properties ($set)
      // Lấy status, ds tác giả, ds thể loại, ds tag của từng cuốn sách
      // Status lấy theo kdl list để cùng data type, lấy cả ACT và INA
      // join theo content_key để tránh bị trùng, chỉ lấy book (content_type_key = 1)
      var sqlItemProperties =
        """
          |select cd.content_id, collect_set(cd.status) as status,
          |collect_set(distinct cast(ccb.category_id as string)) as category_list,
          |collect_set(distinct cast(cab.author_id as string)) as author_list,
          |collect_set(distinct cast(ctb.tag_id as string)) as tag_list
          |from waka.content_dim as cd
          |left join waka.content_category_brid ccb on cd.content_key = ccb.content_key
          |left join waka.content_author_brid as cab on cd.content_key = cab.content_key
          |left join waka.content_tag_brid as ctb on cd.content_key = ctb.content_key
          |where cd.content_type_key = 1
          |group by cd.content_id
          |order by content_id""".stripMargin

      val dfItemProperities = spark.sql(sqlItemProperties)
      // flatMap: Map 1 ptử trong List thành nh ptu, sau đó gộp tất cả các ptu thành 1 list
      // List("author_list", "category_list", "status")
      // => List(lit("author_list"), col("author_lit"), lit("category_list"), col("category_list"), lit("status"), col("status"))
      val colToMap = List("author_list", "category_list", "tag_list", "status")
        .flatMap(colName => List(lit(colName), col(colName)))

      val itemPropertiesEventJson = dfItemProperities
        .withColumn("event", lit("$set"))
        .withColumn("entityType", lit("item"))
        .withColumn("entityId", col("content_id").cast(StringType))
        .withColumn("properties", map(colToMap: _ *)) //Tạo 1 map column từ ds các column (key1, value1, key2, value2,...)
        .withColumn("eventTime", lit(current_timestamp())) //Event time là current timestamp
        .select("event", "entityType", "entityId", "properties", "eventTime")

      itemPropertiesEventJson.write.json("propertiesEvent.json")
      // Luu trong hdfs hdfs://name-node01:8020/user/vega/propertiesEvent.json
      // hadoop fs -cat /user/vega/propertiesEvent.json/* | hadoop fs -put - /user/vega/mergedPropertiesEvent.json

      // pio import --appid 23  --input /user/vega/mergedPropertiesEvent.json;
    }

    importItemProperties()
    spark.stop()
  }

}
