//package com.test
//
//import org.apache.hadoop.hbase.client._
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase._
//import scala.collection.JavaConverters._
//
//object testHbaseProduction extends App {
//  // Tạo conf cho Hbase, hbase.zookeeper.quorum lấy ở /etc/hbase/conf/hbase-site.xml
//  val conf = HBaseConfiguration.create()
//  conf.set("hbase.zookeeper.quorum", "data-node08,data-node06,name-node02")
//
//  // Tạo connection với config tương ứng
//  val connection = ConnectionFactory.createConnection(conf)
//
//  // Lấy list các bảng, cần tạo 1 Admin để lấy ttin này.
//  // Tg tự admin có thể tạo, xóa bảng,...
//  val admin = connection.getAdmin
//  val tableNames = admin.listTableNames()
//  tableNames.foreach(tableName => println(tableName.getNameAsString))
//
//  //// Tạo bảng mới tên "testScala"
//  // Tạo bảng vs tên "testScala" vs TableDescriptorBuilder.newBuilder
////  val newTableName = TableName.valueOf("testScala")
////  val newTable = TableDescriptorBuilder.newBuilder(newTableName)
////  // Tạo các column family vs ColumnFamilyDescriptorBuilder.newBuilder
////  val columnFamily1 = Bytes.toBytes("colFam1")
////  val columnFamily2 = Bytes.toBytes("colFam2")
////  val columnFamily3 = Bytes.toBytes("colFam3")
////  val newColumnFamily1 = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1)
////  val newColumnFamily2 = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily2)
////  val newColumnFamily3 = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily3)
////
////  // Thêm column family vào bảng vs setColumnFamily và tạo bảng vs createTable
////  // Note newBuilder trả về class ...DescriptorBuilder, cần .build() để trả về class Descriptor dùng làm tham số
////  newTable.setColumnFamily(newColumnFamily1.build()).setColumnFamily(newColumnFamily2.build()).setColumnFamily(newColumnFamily3.build())
////  admin.createTable(newTable.build())
////  println("created")
//
//  //// Truy vấn ttin từ 1 bảng
//  // Lấy tên bảng
////  val table = connection.getTable(TableName.valueOf("test_hbase"))
////  println(s"table: $table") // table: test_hbase;hconnection-0x7fe7c640
////  // Chọn row_key và get giá trị tg ứng vs row_key
////  val get = new Get(Bytes.toBytes("1"))
////  println(s"get: $get")
////  // get: {"cacheBlocks":true,"totalColumns":0,"row":"1","families":{},"maxVersions":1,"timeRange":[0,9223372036854775807]}
////
////  val result = table.get(get)
////  println(s"result: $result")
////  // result: keyvalues={1/e:e/1486978789628/Put/vlen=7/seqid=0/event_1, 1/f:ety/1486978789628/Put/vlen=12/seqid=0/entityType_1, 1/g:eid/1486978789628/Put/vlen=10/seqid=0/entityId_1}
////
////  // Lấy giá trị theo column family và column, lưu ý tham số hay kết quả đều là dạng byte array []
////  val value1byte = result.getValue(Bytes.toBytes("e"), Bytes.toBytes("e")) // byte array []
////  val value1 = new String(value1byte) // hoặc Bytes.toString(value1byte)
////  println(s"value1: $value1") // event_1
////  val value2byte = result.getValue(Bytes.toBytes("g"), Bytes.toBytes("eid")) // byte array []
////  val value2 = new String(value2byte) // hoặc Bytes.toString(value1byte)
////  println(s"value2: $value2") // entityId_1
////  val value3byte = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("ety")) // byte array []
////  val value3 = new String(value3byte) // hoặc Bytes.toString(value1byte)
////  println(s"value2: $value3") // entityType_1
//
//  //// Thêm giá trị vào 1 bảng
//  // Lấy ttin bảng theo tên
//  val table = connection.getTable(TableName.valueOf("testScala"))
//  // Chọn row_key và put giá trị tg ứng
//  val put1 = new Put(Bytes.toBytes("row_key1"))
//  // method addColumn cần 1 tham số: tên column family, tên column và giá trị muốn thêm
//  put1.addColumn(Bytes.toBytes("colFam1"), Bytes.toBytes("col1"), Bytes.toBytes("value111"))
//  put1.addColumn(Bytes.toBytes("colFam2"), Bytes.toBytes("col1"), Bytes.toBytes("value121"))
//  put1.addColumn(Bytes.toBytes("colFam3"), Bytes.toBytes("col2"), Bytes.toBytes("value132"))
//  val put2 = new Put(Bytes.toBytes("row_key2"))
//  put2.addColumn(Bytes.toBytes("colFam1"), Bytes.toBytes("col2"), Bytes.toBytes("value212"))
//  put2.addColumn(Bytes.toBytes("colFam2"), Bytes.toBytes("col2"), Bytes.toBytes("value222"))
//  put2.addColumn(Bytes.toBytes("colFam3"), Bytes.toBytes("col3"), Bytes.toBytes("value233"))
//  // Put các giá trị theo row_key bên trên vào bảng, có thể put nhiều row 1 lúc
//  table.put(List(put1, put2).asJava)
//  println("putted")
//
//}
