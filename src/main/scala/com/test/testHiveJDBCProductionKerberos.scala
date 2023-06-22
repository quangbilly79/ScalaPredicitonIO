package com.test

import java.sql.DriverManager
import java.util.Properties

object testHiveJDBCProductionKerberos extends App{
  val jdbcString = "jdbc:hive2://name-node02:10000/waka;principal=hive/name-node02@BI.VEGA.COM"
  val props = new Properties()
  props.put("user", None)
  props.put("password", None)
  // Phần dưới này Co thể có hoặc không
  Class.forName("org.apache.hive.jdbc.HiveDriver")
  // Tạo connection từ jdbc String, có thể kết hợp vs properties (prop cx có thể đặt trực tiếp vào jdbc str)
  val conn = DriverManager.getConnection(jdbcString,props)
  // Tương đg conn.cursor() trong Python
  val cursor = conn.createStatement()
  // Executed query và lưu kq vào 1 biến
  // Sau đó iterate qua từng dòng (trong Python sẽ là execute xong fetchall)
  val result = cursor.executeQuery("select * from content_dim limit 100")
  while (result.next()) {
    // getString/Int/Object(tên cột)
    println(result.getString("content_id"))
    println(result.getString("content_name"))
  }
  val result1 = cursor.executeQuery("show create table content_dim")
  while (result1.next()) {
    // getString/Int/Object(STT cột)
    println(result1.getString(1))
  }
  // Đóng connection
  conn.close()
}
