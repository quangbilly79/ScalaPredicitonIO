package com.test
import org.apache.predictionio.sdk.java.{BaseClient, Event, EventClient, FutureAPIResponse}

object testPredictionIoImport {
  def main(args: Array[String]): Unit = {
    val client = new EventClient("lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7", "http://localhost:7070")
    val readEvent = new Event()
      .event("reader")
      .entityType("user")
      .entityId("u99")
      .targetEntityType("item")
      .targetEntityId("i99")
    client.createEvent(readEvent)
  }
}
