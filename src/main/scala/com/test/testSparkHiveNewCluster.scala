package com.test
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object testSparkHiveNewCluster {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val df = spark.sql("select * from test.test1")
    df.show()
  }
}

