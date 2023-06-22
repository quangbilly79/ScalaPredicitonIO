package com.test
import java.net.URLEncoder
import java.sql.{Connection, DriverManager, ResultSet}
object testMysql {
  def main(args: Array[String]): Unit = {

    // Load các ttin cần thiết cho jdbc
    val url = "jdbc:mysql://172.25.0.113:3306/waka"
    val user = "etl"
    val password = "Vega123312##"

    // Load the MySQL JDBC driver. Phiên bản mới k cần nữa
    // Class.forName("com.mysql.cj.jdbc.Driver")

    val connection = DriverManager.getConnection(url, user, password)
    // Có thể gộp các thứ vào thành 1 jdbc chung
    // (jdbc:mysql://172.25.0.113:3306/waka?user=etl&password=Vega123312%23%23) (các ký tự đb như # phải đc URLencoded thành %23
    // val connection = DriverManager.getConnection("jdbc:mysql://172.25.0.113:3306/waka?user=etl&password=Vega123312%23%23")

    // Test connection, k cần dùng try/catch cx đc
    try {
      // Tạo 1 statement (~cursor trong Python, 1 object để chạy query)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM content_dim where content_id < 300 limit 10")

      // Lây từng dòng kq từ java.sql.ResultSet vs .next()
      while (resultSet.next()) {
        // Lấy giá trị từng cột vs getString/getInt/... (tên cột hoặc stt cột (1 index))

        val content_id = resultSet.getString("content_id")
        val content_name = resultSet.getString("content_name")
        val status = resultSet.getString("status")
        println(s"content_id: ${content_id}, content_name: ${content_name}, status: ${status}")
      }

    } finally {
      // Close the connection
      connection.close()
    }
  }
}
