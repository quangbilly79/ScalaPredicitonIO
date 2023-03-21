package com.test
import scala.concurrent.duration._
object test extends App {


  val duration = Duration(5, SECONDS) //this works
  val variable: Option[String] = None //Some("A")

  println(variable.getOrElse("C"))
  if (variable.getOrElse() == "A") {
    println("Exist")
  } else {
    println("None")
  }


//  class test(a: String, b: Int) {
//    def printing(): Unit = {
//      println(this.a)
//      println(this.b)
//    }
//  }
//  val testInstance = new test("a", 1);
//  testInstance.printing();


//  val seq: Seq[Int] = (1 to 20)
//  println(seq.getClass) //class scala.collection.immutable.Range$Inclusive
//
//  for ( i <- seq) {
//    val z = i
//    println(s"z: $z")
//  }

}
