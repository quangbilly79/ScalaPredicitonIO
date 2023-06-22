package com.SDB
import java.sql._
import scala.util.Random
object testMySQLSDB extends App {
  val alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val url = "jdbc:mysql://svc-efacc873-ecf7-41a3-a0ea-e442bdcc493c-dml.aws-jakarta-1.svc.singlestore.com:3306/s2_dataset_martech_e8540a6a"
  val user = "admin"
  val pass = "Oldbrother98"

  val connection = DriverManager.getConnection(url, user, pass)

  val sql = "INSERT INTO test VALUES (?, ?)"
  val statement: PreparedStatement = connection.prepareStatement(sql)

  //val result = statement.execute("create table test (id int, name text, shard key id_sk (id))")

  for (i <- 1 to 10000) {
    var id = i
    val name = alphabet.charAt(Random.nextInt(alphabet.length)).toString
    if ( i > 9990) {
      // Set the parameter values
      statement.setInt(1, id)
      statement.setString(2, name)

      // Execute the prepared statement
      statement.execute()
      println(">9990")
    } else {
      // Set the parameter values
      statement.setInt(1, 1)
      statement.setString(2, name)

      // Execute the prepared statement
      statement.execute()
      println("<9990")
    }
  }
  connection.close()

}
