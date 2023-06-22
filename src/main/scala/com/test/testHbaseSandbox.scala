//package com.test
//import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
//import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
//import org.apache.hadoop.hbase.util.Bytes
//
//
//object testHbaseSandbox extends App {
//  // Tạo conf cho Hbase, hbase.zookeeper.quorum lấy ở /etc/hbase/conf/hbase-site.xml
//  val conf = HBaseConfiguration.create()
//  conf.set("hbase.zookeeper.quorum", "vftsandbox-namenode,vftsandbox-snamenode,vftsandbox-node03")
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
//  //// Truy vấn ttin từ 1 bảng
//  // Lấy tên bảng
//  val table = connection.getTable(TableName.valueOf("mytable"))
//  println(s"table: $table") // table: mytable;hconnection-0x19553973
//  // Chọn row_key và get giá trị tg ứng vs row_key
//  val get = new Get(Bytes.toBytes("row_key2"))
//  println(s"get: $get")
//  // get: {"cacheBlocks":true,"totalColumns":0,"row":"row_key2","families":{},"maxVersions":1,
//  // "timeRange":[0,9223372036854775807]}
//  val result = table.get(get)
//  println(s"result: $result")
//  // result: keyvalues={row_key2/mycf:col1/1670551917589/Put/vlen=3/seqid=0,
//  // row_key2/mycf:col3/1670551917589/Put/vlen=3/seqid=0}
//
//  // Lấy giá trị theo column family và column, lưu ý tham số hay kết quả đều là dạng byte array []
//  val value1byte = result.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("col1")) // byte array []
//  val value1 = new String(value1byte) // hoặc Bytes.toString(value1byte)
//  println(s"value1: $value1") // 456
//  val value2byte = result.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("col3")) // byte array []
//  val value2 = new String(value2byte) // hoặc Bytes.toString(value1byte)
//  println(s"value2: $value2") // 789
//}
