package com.testLocal

import java.io.PrintWriter

object testReadWrite extends App {
  // get input path for file in resources folder
  // getClass.getResource => Lấy về url chuẩn => getFile để lấy về string tên file
  // Lưu ý rằng file sẽ bị chuyen sang folder target chứ k còn ở Resources folder
  val inputPath=getClass.getResource("/testRead.txt").getFile
  println(getClass.getResource("/testRead.txt")) // file:/C:/Users/quang/ScalaProjects/predictionIO/target/scala-2.11/classes/testRead.txt
  println(inputPath) // /C:/Users/quang/ScalaProjects/predictionIO/target/scala-2.11/classes/testRead.txt
  val outputPath=getClass.getResource("/testWrite.txt").getFile

  // read file from Resources folder Scala
  // Sử dụng scala.io.Source.fromFile(String path)
  // Sau đó dùng getLines() để lấy về 1 iterator chứa dl từng dòng, chuyển sang List vs toList
  // Nhớ close
  val lines = scala.io.Source.fromFile(inputPath)
  val listLine = lines.getLines().toList
  println(listLine)
  lines.close()

  // write file to Resources folder Scala
  // Sử dụng java.io.printWriter(String path)
  // Write vs write method. File vs dl ms sẽ ở Target folder
  // Nhớ close
  val writer = new PrintWriter(outputPath)
  for (line <- listLine) {
    println(line)
    writer.write(line + "\n")
  }
  writer.close()
}
