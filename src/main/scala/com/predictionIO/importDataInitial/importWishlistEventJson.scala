package com.predictionIO.importDataInitial


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object importWishlistEventJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()

    // event wishlist.
    // Lấy account_id (vega_id) chứ k phải user_id. Ngoài ra lấy content_id, eventTime (dựa trên data_date_key)
    // Vì bảng fact_wishlist k có content_key nên không join theo content_key đc
    // chỉ lấy book(content_type_key = 1) và status = ACT và content_type = 1
    // cũng như k lấy user_id/vegaid_id = 0 (k có account) để tránh loãng dl
    // Import lần đầu từ 01 / 01 / 2022 đến hiện tại
    def importWishlistJson(): Unit = {
      var sqlWishlist =
        """
          |select su.account_id, fw.content_id, fw.data_date_key from waka.waka_pd_fact_wishlist as fw
          |join (select content_id from waka.content_dim where content_type_key = 1 and status = "ACT")
          |as cd on fw.content_id = cd.content_id
          |join waka.sqoop_user as su on fw.user_id = su.id
          |where fw.data_date_key >= 20220101 and fw.data_date_key <= 20231231 and fw.content_type = 1
          |and su.account_id is not null and su.account_id != 0 and fw.user_id != 0
          |order by su.account_id
          """.stripMargin
      var dfWishlist = spark.sql(sqlWishlist)
      val wishlistEventJson = dfWishlist
        .withColumn("event", lit("wishlist"))
        .withColumn("entityType", lit("user"))
        .withColumn("entityId", col("account_id").cast(StringType))
        .withColumn("targetEntityType", lit("item"))
        .withColumn("targetEntityId", col("content_id").cast(StringType))
        .withColumn("eventTime", from_unixtime( //lấy eventTime dựa trên data_date_key của event 20220131 => 2022-01-31'T'00:00:00.000+07:00
          unix_timestamp(col("data_date_key").cast("string"), "yyyyMMdd"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
        .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "eventTime")

      wishlistEventJson.write.json("wishlistEvent.json")
    }
    importWishlistJson()
    spark.stop()
  }
}
// Luu trong hdfs hdfs://name-node01:8020/user/vega/wishlistEvent.json
// hadoop fs -cat /user/vega/wishlistEvent.json/* | hadoop fs -put - /user/vega/mergedWishlistEvent.json
// hadoop fs -get /user/vega/mergedWishlistEvent.json /home/vgdata/quang/predicitionIO;
// pio import --appid 23 --input /user/vega/mergedWishlistEvent.json