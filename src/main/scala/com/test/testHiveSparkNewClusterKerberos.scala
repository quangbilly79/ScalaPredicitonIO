package com.test

import org.apache.spark.sql.SparkSession

object testHiveSparkNewClusterKerberos extends App {
  val spark = SparkSession.builder.getOrCreate()
  spark.sql("select * from test.test1").show()
  spark.sql("select * from default.default1").show()
}
