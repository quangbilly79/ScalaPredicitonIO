package com.test
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

object testSparkMysqlSandboxLocal extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  // Read: Dùng format "jdbc" với option là jdbc url cx như tên bảng
  val inputMysqlDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://172.25.48.219:3306/testDbPentaho?user=vgdata&password=vgdata&zeroDateTimeBehavior=convertToNull")
    .option("dbtable", "test")
    .load()
  inputMysqlDF.printSchema()
  inputMysqlDF.show()
  //+---+---+---+-------------------+
  //| d1| d2|  v|              date1|
  //+---+---+---+-------------------+
  //|  a|  x|  5|2023-01-02 00:00:00|
  //|  a|  y|  5|2023-01-02 00:00:00|
  //|  a|  y| 10|2023-01-03 00:00:00|
  //|  b|  x| 20|2023-01-03 00:00:00|
  //|  a|  g|  9|2023-01-02 11:11:11|
  //+---+---+---+-------------------+
  val nextDF = inputMysqlDF.withColumn("nextWeek",  expr("cast(date_add(date1, 7) as date)"))
    .withColumn("date11", col("date1").cast(DateType)).select("d1","d2","v", "date11", "nextWeek")
  nextDF.printSchema()
  //root
  // |-- d1: string (nullable = true)
  // |-- d2: string (nullable = true)
  // |-- v: decimal(10,0) (nullable = true)
  // |-- date1: timestamp (nullable = true)
  // |-- nextWeek: timestamp (nullable = true)
  nextDF.show()
  //+---+---+---+-------------------+-------------------+
  //| d1| d2|  v|              date1|           nextWeek|
  //+---+---+---+-------------------+-------------------+
  //|  a|  x|  5|2023-01-02 00:00:00|2023-01-09 00:00:00|
  //|  a|  y|  5|2023-01-02 00:00:00|2023-01-09 00:00:00|
  //|  a|  y| 10|2023-01-03 00:00:00|2023-01-10 00:00:00|
  //|  b|  x| 20|2023-01-03 00:00:00|2023-01-10 00:00:00|
  //|  a|  g|  9|2023-01-02 11:11:11|2023-01-09 00:00:00|
  //+---+---+---+-------------------+-------------------+
  // Write: Gần giống như read. Sẽ tự động tạo bảng nếu k chưa có.
  // Nếu có sẵn bảng thì phải thêm mode("append") để thêm dl ms vào bảng hoặc mode("overwrite") để xóa đi tạo thêm ms lại
  nextDF.write.format("jdbc")
    .mode("overwrite") // append overwrite
    .option("url", "jdbc:mysql://172.25.48.219:3306/testDbPentaho?user=vgdata&password=vgdata")
    .option("dbtable", "testSparkMysql")
    .save()

}
