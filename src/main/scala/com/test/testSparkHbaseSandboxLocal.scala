//package com.test
//
//import org.apache.spark.sql.{DataFrameReader, SparkSession}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//
//import org.apache.hadoop.hbase.spark.HBaseContext
//import org.apache.hadoop.hbase._
//import org.apache.hadoop.hbase.client._
//import org.apache.hadoop.hbase.util.Bytes
//
//
//import scala.collection.JavaConverters._
//
//object testSparkHbaseSandboxLocal extends App {
//
//  val spark = SparkSession.builder().master("local").getOrCreate()
//  // Khởi tạo 1 hbaseConf, chứa ttin về zookeeper.quorum
//  val hbaseConf =  HBaseConfiguration.create()
//  hbaseConf.set("hbase.zookeeper.quorum", "vftsandbox-namenode,vftsandbox-snamenode,vftsandbox-node03")
//
//  // Khởi tạo 1 HbaseContext (tuy k dùng gì bên dưới nhưng phải có)
//  new HBaseContext(spark.sparkContext, hbaseConf)
//
//  // Thông tin về tên bảng, hệ thống column, source format
//  // columnMapping dạng "tênCộtTrongDF KDL columnFamily:column",
//  // ngoài ra khai báo cột chứa row_key sẽ đặc biệt chút
//  // KDL ban đầu nên để hết là String cho đỡ lỗi (vd lỗi offset(0)+length(4))
//  val hbaseTable = "mytable"
//  val columnMapping =
//    """id STRING :key,
//      mycfColumn1 STRING mycf:column1,
//      mycfColumn2 STRING mycf:column2,
//      mycfCol1 STRING mycf:col1,
//      mycfCol3 STRING mycf:col3
//      """
//  val hbaseSource = "org.apache.hadoop.hbase.spark"
//
//  // Đọc bảng hbase vs spark vs format là "org.apache.hadoop.hbase.spark"
//  // Các option là columnMapping và tên bảng đã khai báo bên trên
//  val hbaseDF = spark.read.format(hbaseSource)
//    .option("hbase.columns.mapping", columnMapping)
//    .option("hbase.table", hbaseTable)
//    .load()
//  hbaseDF.show()
//
////  +-----------+--------+-----------+--------+--------+
////  |mycfColumn1|mycfCol1|mycfColumn2|mycfCol3|      id|
////  +-----------+--------+-----------+--------+--------+
////  |          1|    null|          2|    null|       1|
////  |       null|      12|       null|    null|     123|
////  |       null|     456|       null|     789|row_key2|
////  +-----------+--------+-----------+--------+--------+
//
//  // Tạo 1 bảng mới để test write dl. Chi tiết check testHbaseProduction.scala
//  val connection = ConnectionFactory.createConnection(hbaseConf)
////  val admin = connection.getAdmin
////  val newTableName = TableDescriptorBuilder.newBuilder(TableName.valueOf("testSparkHbase"))
////  val colFam1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colFam1")).build()
////  val colFam2 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("colFam2")).build()
////  newTableName.setColumnFamilies(List(colFam1, colFam2).asJava)
////  admin.createTable(newTableName.build())
//
//
//  // Tạo 1 df để write vào bảng hbase
//  import spark.implicits._
//  val testSparkHbaseDF = Seq(
//    ("value998", "value998", "value998", "row_key2"),
//    ("value312", "value311", "value322", "row_key3")
//  ).toDF("colFam1col2", "colFam1col1", "colFam2col2", "id")
//
//  // Write vs các option tương tự như Read, tùy vào row_key mà sẽ insert hoặc overwrite
//  testSparkHbaseDF.write.format("org.apache.hadoop.hbase.spark")
//    .option("hbase.columns.mapping", "id string :key, colFam1col1 string colFam1:col1, " +
//      "colFam1col2 string colFam1:col2, colFam2col2 string colFam2:col2")
//    .option("hbase.table","testSparkHbase")
//    .save()
//
//}
