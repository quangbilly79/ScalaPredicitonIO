package com.test
import org.apache.spark.sql._

object testSparkHiveSandboxServerNode extends App {
  val spark = SparkSession.builder().getOrCreate()
  val df = spark.sql("select * from vega_data.tmp")
  df.show()
}
