package com.test
import com.redis._
import scala.concurrent.duration._

object testRedis {
  def main(args: Array[String]): Unit = {
    // Connect to Redis server. Nhớ rằng nên connect đến master node để có thể write đc
    // Cụ thể truy cập vào server vs cli rồi chạy lệnh "role"
    var REDIS_HOST: String = "172.25.0.109"
    var REDIS_PORT: Int = 6379
    val redisClient = new RedisClient(REDIS_HOST, REDIS_PORT)

    // Select db
    redisClient.select(2)



    // Write data với duration. Cần import import scala.concurrent.duration._
    val duration = Duration(20, SECONDS)
    println("before")
    redisClient.set(key = "quang1", value = "INA") //, expire = duration
    println("after")

    // Read data: Lấy value của key với .get
    var value = redisClient.get("quang1")
    println(value.getClass) // Trả về Some() hoặc None
    println(value) // Some(value)

    // isDefined/isEmpty dùng cho Some type, ktra xem có phải None k.
    // getOrElse để lấy giá trị của Some nếu k empty, nếu empty thì trả về mặc định theo yêu cầu
    if (value.getOrElse() == "ACT") {
      println("exist va` status = ACT")
    } else if (value.isEmpty) {
      println("not exist")
    }


    // Read data: Lấy ds các key theo pattern vs .keys
//    var keys = redisClient.keys("*") // Trả về List(Some())
//    keys.foreach(println) // List(Some(quang), Some(history_mix_1),...)

  }
}


