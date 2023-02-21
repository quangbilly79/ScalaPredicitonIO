package com.predictionIO.importData

import org.apache.predictionio.sdk.java.FileExporter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object importWishlistEventJson {

  def main(args: Array[String]) {
    var exporter = new FileExporter("readEvents.json")
    var spark = SparkSession.builder.getOrCreate()
    def importWishlistJson(): Unit = {
      var sqlWishlist =
        """
          |select user_id, fr.content_id from waka.waka_pd_fact_wishlist as fr
          |join waka.content_dim as cd on fr.content_id = cd.content_id
          |where data_date_key >= 20220101 and data_date_key < 20220701
          |and cd.status = "ACT"
          """.stripMargin
      var dfWishlist = spark.sql(sqlWishlist)
      val wishlistEventJson = dfWishlist
        .withColumn("event", lit("wishlist"))
        .withColumn("entityType", lit("user"))
        .withColumn("entityId", col("user_id").cast(StringType))
        .withColumn("targetEntityType", lit("item"))
        .withColumn("targetEntityId", col("content_id").cast(StringType))
        .withColumn("eventTime", lit(current_timestamp()))
        .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "eventTime")

      wishlistEventJson.write.json("wishlistEvent.json")
    }
    importWishlistJson()
    spark.stop()
  }
}
// Luu trong hdfs hdfs://vftsandbox-namenode:8020/user/vgdata/wishlistEvent.json
// hadoop fs -cat /user/vgdata/wishlistEvent.json/* | hadoop fs -put - /user/vgdata/mergedWishlistEvent.json
// hadoop fs -get /user/vgdata/mergedWishlistEvent.json /home/vgdata/universal/importEventJson/
// pio import --appid 4 --input /home/vgdata/universal/importEventJson/mergedWishlistEvent.json
