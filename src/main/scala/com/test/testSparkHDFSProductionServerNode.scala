package com.test

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object testSparkHDFSProductionServerNode extends App {
  // Giống hệt SandboxLocal, chỉ bỏ đi .master("local")
  val spark = SparkSession.builder().getOrCreate()
  // Định nghĩa Schema cho DF với StructType (cấu trúc tổng), gồm 1 List/Array các StructField
  // Tiếp theo dùng StructField định nghĩa các cột (tên, kdl, có thể null hay k,...)
  val mySchema = StructType(Array(
    StructField("col1", IntegerType, true),
    StructField("col2", StringType, true),
    StructField("col3", StringType, true)
  )
  )
  // Đọc từ file csv từ hdfs vs .format("csv"), thêm schema và option tương ứng, và cuối cùng là load(path)
  // Có thể dùng spark.read.csv cx đc
  // Các option có thể check trên mạng, tùy vào format nữa
  val inputDF = spark.read.format("csv")
    .schema(mySchema)
    .option("header", "true")
    .load("hdfs://vegadata/user/vgdata/test.csv") // Phải để là vegadata thì ms đc (dfs.nameservices)
  inputDF.show()

  // Write DF ra 1 file csv trên hdfs
  val outputDF = inputDF.withColumn("col4",
    concat(col("col1").cast(StringType), col("col2"), col("col3")))
  outputDF.show()
  // Lưu ý các option, mode tương ứng, và cuối cùng là save(path)
  outputDF.write.format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save("hdfs://vegadata/user/vgdata/test_out.csv")

//  val inputDF1 = spark.read.format("csv")
//    .option("inferSchema", "true")
//    .option("header", "true")
//    .load("hdfs://vftsandbox-namenode:8020/user/vgdata/test_out.csv")
//  inputDF1.show()

}
