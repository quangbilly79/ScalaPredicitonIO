package com.test
import scala.App
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object testSparkHiveSandboxLocal extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    // nếu đã ném hdfs-site.xml và hive-site.xml thì k cần đoạn dưới này
    // vì trong file xml đã chứa các conf bên dưới
    .config("hive.metastore.uris", "thrift://vftsandbox-namenode:9083") //
    .config("spark.sql.warehouse.dir", "hdfs://vftsandbox-namenode:8020/user/hive/warehouse/") //
    .enableHiveSupport()
    .getOrCreate()

  val df = spark.sql("select * from vega_data.tmp")
  df.show()
}
