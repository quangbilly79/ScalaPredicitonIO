package com.SDB

import java.sql._
import scala.collection.mutable
import scala.util.Random
import scala.collection.mutable.Seq

object testMySQLSDBSortKey extends App {
  // Taọ 1 list/seq có 10 phần tử, mỗi ptử là 1 chuỗi String vs length 40,
  // tạo thành bởi random printable char, bỏ ' và " đi

  var imeiSeq: mutable.Seq[String] = mutable.Seq()
  for (i <- 1 to 100) {
    var elem: String = ""
    while (elem.length <= 39) {
      val randomChar = Random.nextPrintableChar()
      if (randomChar != ''' && randomChar != '"' && randomChar != '\\' && randomChar != '`') {
        elem += randomChar
      }
    }
    imeiSeq = imeiSeq :+ elem
  }
  //imeiSeq.foreach(println)
  println("+++++++++++++")


  val url = "jdbc:mysql://svc-efacc873-ecf7-41a3-a0ea-e442bdcc493c-dml.aws-jakarta-1.svc.singlestore.com:3306/s2_dataset_martech_e8540a6a"
  val user = "admin"
  val pass = "Oldbrother98"

  val connection = DriverManager.getConnection(url, user, pass)

  val sql = "INSERT INTO testSortKey VALUES (?, ?)"
  val statement: PreparedStatement = connection.prepareStatement(sql)


  for (i <- 1 to 10000) {
    var id = i
    val imei = imeiSeq(Random.nextInt(10))
    statement.setInt(1, id)
    statement.setString(2, imei)
    // Execute the prepared statement
    statement.execute()
    println(s"id: ${id}")
    println(s"imei: ${imei}")
    println("*****************")
  }
  connection.close()

}
