package com.test
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.PrintWriter
import java.net.URI
object testHDFSSandbox extends App {

  // Obtain a reference to the Hadoop file system
  // Dựa trên URI và Configuration (hiện conf để mặc định k chỉnh gì)
  val fs = FileSystem.get(new URI("hdfs://vftsandbox-namenode:8020/"),
    new Configuration())
  println(s"fs: $fs")
  // fs: DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_-1959781629_1, ugi=quang (auth:SIMPLE)]]

  // Lấy đg dẫn đến file trên hdfs với Path
  val inputPath = new Path("/user/vgdata/input.txt")
  println(s"inputPath: $inputPath")
  // inputPath: /user/vgdata/input.txt

  // Dùng fileSystem để mở đg dẫn đó, trả về FSDataInputStream
  // Túm lại là cần FileSystem + Path => InputStream
  val inputStream = fs.open(inputPath)
  println(s"inputStream: $inputStream")
  // inputStream: org.apache.hadoop.hdfs.client.HdfsDataInputStream@f316aeb

  // Đọc trực tiếp từ inputStream (đọc dl dạng byte)
//  val inputLines = new scala.collection.mutable.ListBuffer[String]()
//  val buffer = new Array[Byte](4096) // Buffer size
//  var bytesRead = inputStream.read(buffer)
//  println(s"bytesRead: $bytesRead")
//  while (bytesRead != -1) {
//    val line = new String(buffer, 0, bytesRead)
//    println(s"line: $line")
//    inputLines += line
//    println(s"inputLines: $inputLines")
//    bytesRead = inputStream.read(buffer)
//    println(s"bytesRead: $bytesRead")
//    println("----------------")
//  }

  // Sau đó mới dùng fromInputStream để trả ve BufferedSource
  // và getLines + toList để đưa về dạng list
  val inputLines = scala.io.Source.fromInputStream(inputStream).getLines().toList
  for (line <- inputLines) {
    println(line)
  }
  inputStream.close()

//  // Write to the output file in HDFS
//  val outputPath = new Path("/user/vgdata/output.txt")
//  // fs đã tạo bên trên + Path => OutputStream (FSDataOutputStream)
//  // lưu ý dùng create thay vì open để write
//  val outputStream = fs.create(outputPath)
//
//  // write trực tiếp từ outputStream (phải convert string sang byte[] với getBytes, hoặc dùng writeBytes0
//  inputLines.foreach(line => outputStream.write(line.getBytes))
//
//  // Tạo 1 PrintWriter từ 1 OutputStream, write ra file tương ứng
////  val writer = new PrintWriter(outputStream)
////  inputLines.foreach(line => writer.write(line))
//  outputStream.close()
//  println("File successfully written to HDFS.")

  // Test getFileStatus
  val inputPathForStatus = new Path("/user/vgdata/")
  val fileStatus = fs.getFileStatus(inputPathForStatus)
  println(s"fileStatus: ${fileStatus}")
  println(fileStatus.getLen)

}
