package com.test

import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig, RequestBuilder, Response, Request}

import java.util.concurrent.Future

object testNingHttpClient {
  def main(args: Array[String]): Unit = {


    val asyncHttpClient = new AsyncHttpClient(
      new AsyncHttpClientConfig.Builder()
        .setConnectTimeout(5000)
        .setRequestTimeout(5000)
        .build())

    val url = "http://localhost:7070/events.json?accessKey=lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7"
    val requestBuilder = new RequestBuilder("POST")
      .setUrl(url)
      .setHeader("Content-Type", "application/json")
      .setBody(
        """{
        "event" : "buy",
        "entityType" : "user",
        "entityId" : "u15",
        "targetEntityType" : "item",
        "targetEntityId" : "i2"
      }""")
    val request: Request = requestBuilder.build()
    asyncHttpClient.executeRequest(request)


  }
}
