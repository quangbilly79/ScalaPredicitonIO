package com.test
import scala.collection.JavaConversions.mapAsJavaMap
import java.util.{Map => JMap}
import scala.collection.JavaConverters._

object testScalaJavaMapConvert {
  def main(args: Array[String]): Unit = {
    def processMap(map: JMap[String, Any]): Unit = {
      println("Keys in the map:")
      map.keySet().asScala.foreach(println)
    }

    // create a java.util.Map object for testing
    val javaMap: JMap[String, Any] = new java.util.HashMap[String, Any]()
    javaMap.put("key1", "value1")
    javaMap.put("key2", 123)
    javaMap.put("key3", true)

    // create a scala.collection.immutable.Map object for testing
    val scalaMap: Map[String, Any] = Map("key1" -> "value1", "key2" -> 123, "key3" -> true)

    // convert scala map to java map
    val convertedMap: JMap[String, Any] = mapAsJavaMap(scalaMap)

    // test the function with the java map and the converted scala map
    processMap(javaMap)
    processMap(convertedMap)
  }
}
