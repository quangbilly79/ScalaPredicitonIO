package com.test

import org.apache.spark.sql._

object testSparkHiveProductionServerNode extends App {
  val spark = SparkSession.builder().getOrCreate()
  val df = spark.sql("select * from waka.content_dim limit 10")
  df.show()
}
