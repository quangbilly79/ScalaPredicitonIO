package com.test
import java.sql.{Connection, DriverManager, ResultSet}
object testMysql {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://172.25.0.101:3306/waka"
    val user = "etl"
    val password = "Vega123312##"

    // Load the MySQL JDBC driver. Phiên bản mới k cần nữa
    // Class.forName("com.mysql.cj.jdbc.Driver")

    // Open a connection to the database
    val connection = DriverManager.getConnection(url, user, password)

    try {
      // Execute a query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM content_dim where content_id < 300 limit 1")

       //Print the results
//      while (resultSet.next()) {
//        val content_id = resultSet.getString("content_id")
//        val content_name = resultSet.getString("content_name")
//        val status = resultSet.getString("status")
      //  println(s"content_id: ${content_id}, content_name: ${content_name}, status: ${status}")
//      }
      resultSet.next()
      val content_id = resultSet.getString("content_id")
      val content_name = resultSet.getString("content_name")
      val status = resultSet.getString("status")
      println(s"content_id: ${content_id}, content_name: ${content_name}, status: ${status}")
    } finally {
      // Close the connection
      connection.close()
    }
  }
}
