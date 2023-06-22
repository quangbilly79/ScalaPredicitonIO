package com.predictionIO.importDataInitial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object importRateEventJson {

  def main(args: Array[String]) {
    var spark = SparkSession.builder.getOrCreate()
    def importRateJson(): Unit = {
      // event rate.
      // Lấy account_id (vega_id) chứ k phải user_id. Ngoài ra lấy content_id, rate, eventTime (dựa trên data_date_key)
      // Vì bảng fact_rate k có content_key nên không join theo content_key đc
      // chỉ lấy book(content_type_key = 1) và status = ACT và content_type = 1
      // cũng như k lấy user_id/vegaid_id = 0 (k có account) để tránh loãng dl
      // Import lần đầu từ 01 / 01 / 2022 đến hiện tại
      var sqlRate =
        """
          |select su.account_id, fr.content_id, fr.rate, fr.data_date_key from waka.waka_pd_fact_rate as fr
          |join (select content_id from waka.content_dim where content_type_key = 1 and status = "ACT")
          |as cd on fr.content_id = cd.content_id
          |join waka.sqoop_user as su on fr.user_id = su.id
          |where fr.data_date_key >= 20220101 and fr.data_date_key <= 20231231 and fr.content_type = 1
          |and su.account_id is not null and su.account_id != 0 and fr.user_id != 0
          |order by su.account_id
          |""".stripMargin
      var dfRate = spark.sql(sqlRate)
      val rateEventJson = dfRate
        .withColumn("event", lit("rate"))
        .withColumn("entityType", lit("user"))
        .withColumn("entityId", col("account_id").cast(StringType))
        .withColumn("targetEntityType", lit("item"))
        .withColumn("targetEntityId", col("content_id").cast(StringType))
        .withColumn("properties", map(lit("rate"), col("rate")))
        .withColumn("eventTime", from_unixtime( //lấy eventTime dựa trên data_date_key của event 20220131 => 2022-01-31'T'00:00:00.000+07:00
          unix_timestamp(col("data_date_key").cast("string"), "yyyyMMdd"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
        .select("event", "entityType", "entityId", "targetEntityType", "targetEntityId", "properties", "eventTime")

      // write ra hdfs
      rateEventJson.write.json("rateEvent.json")
    }
    importRateJson()
    spark.stop()
  }
}
// Luu trong hdfs hdfs://name-node01:8020/user/vega/rateEvent.json
// hadoop fs -cat /user/vega/rateEvent.json/* | hadoop fs -put - /user/vega/mergedRateEvent.json
// hadoop fs -get /user/vega/mergedRateEvent.json /home/vgdata/quang/predicitionIO;
// pio import --appid 23 --input /user/vega/mergedRateEvent.json