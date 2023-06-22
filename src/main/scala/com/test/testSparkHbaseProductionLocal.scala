//package com.test
//
//import org.apache.hadoop.hbase._
//import org.apache.hadoop.hbase.client._
//import org.apache.hadoop.hbase.spark.HBaseContext
//import org.apache.spark.sql.SparkSession
//
//object testSparkHbaseProductionLocal extends App {
//
//  val spark = SparkSession.builder().master("local").getOrCreate()
//  // Khởi tạo 1 hbaseConf, chứa ttin về zookeeper.quorum
//  val hbaseConf =  HBaseConfiguration.create()
//  hbaseConf.set("hbase.zookeeper.quorum", "data-node08,data-node06,name-node02")
//
//  // Khởi tạo 1 HbaseContext (tuy k dùng gì bên dưới nhưng phải có)
//  new HBaseContext(spark.sparkContext, hbaseConf)
//
//  // Thông tin về tên bảng, hệ thống column, source format
//  // columnMapping dạng "tênCộtTrongDF KDL columnFamily:column",
//  // ngoài ra khai báo cột chứa row_key sẽ đặc biệt chút
//  // KDL ban đầu nên để hết là String cho đỡ lỗi (vd lỗi offset(0)+length(4))
//  val hbaseTable = "testScala"
//  val columnMapping =
//    """id STRING :key,
//      colFam1col1 STRING colFam1:col1,
//      colFam1col2 STRING colFam1:col2,
//      colFam2col1 STRING colFam2:col1,
//      colFam2col2 STRING colFam2:col2,
//      colFam3col2 STRING colFam3:col2,
//      colFam3col3 STRING colFam3:col3
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
////  +-----------+-----------+-----------+-----------+-----------+-----------+--------+
////  |colFam1col2|colFam3col3|colFam2col2|colFam1col1|colFam3col2|colFam2col1|      id|
////  +-----------+-----------+-----------+-----------+-----------+-----------+--------+
////  |       null|       null|       null|   value111|   value132|   value121|row_key1|
////  |   value212|   value233|   value222|       null|       null|       null|row_key2|
////  +-----------+-----------+-----------+-----------+-----------+-----------+--------+
//
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
//    ("311", "312", "321", "322", "332", "333", "row_key3"),
//    ("411", "412", "421", "422", "432", "433", "row_key4")
//  ).toDF("colFam1col1", "colFam1col2", "colFam2col1", "colFam2col2",
//    "colFam3col2", "colFam3col3", "id")
//
//  // Write vs các option tương tự như Read, tùy vào row_key mà sẽ insert hoặc overwrite
//  testSparkHbaseDF.write.format(hbaseSource)
//    .option("hbase.columns.mapping", columnMapping)
//    .option("hbase.table", hbaseTable)
//    .save()
//
//}
