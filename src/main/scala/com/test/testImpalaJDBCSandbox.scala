package com.test

import java.sql.DriverManager
import java.util.Properties

object testImpalaJDBCSandbox extends App{
  // authen = None => K cần đoạn "auth=...", authen = NOSASL+doAS = False => Cần "auth=noSasl"
  // port 21050 => Impala
  val jdbcString = "jdbc:hive2://vftsandbox-namenode:21050/vega_data;auth=noSasl"
  val props = new Properties()
  props.put("user", None)
  props.put("password", None)

  // Tạo connection từ jdbc String, có thể kết hợp vs properties (prop cx có thể đặt trực tiếp vào jdbc str)
  val t2= Class.forName("org.apache.hive.jdbc.HiveDriver")
  val conn = DriverManager.getConnection(jdbcString,props)
  // Tương đg conn.cursor() trong Python
  val cursor = conn.createStatement()
  // Executed query và lưu kq vào 1 biến
  // Sau đó iterate qua từng dòng (trong Python sẽ là execute xong fetchall)
  val result = cursor.executeQuery("select * from tmp")
  while (result.next()) {
    // getString/Int/Object(tên cột)
    println(result.getString("id"))
    println(result.getString("name"))
  }
  val result1 = cursor.executeQuery("show create table tmp")
  while (result1.next()) {
    // getString/Int/Object(STT cột)
    println(result1.getString(1))
  }
  // Đóng connection
  conn.close()

}
