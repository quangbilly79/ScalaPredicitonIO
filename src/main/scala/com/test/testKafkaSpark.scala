package com.test
import org.apache.spark.sql._
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.predictionio.sdk.java.{BaseClient, Event, EventClient, FutureAPIResponse}
import java.io.IOException
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig, RequestBuilder, Response, Request}

object testKafkaSpark{

  def main(args: Array[String]) {

    // Test gửi request với thư viện ning.http.client => K thành công vì thư viện này k Serilaziable đc với Spark
    val asyncHttpClient = new AsyncHttpClient(
      new AsyncHttpClientConfig.Builder()
        .setConnectTimeout(5000)
        .setRequestTimeout(5000)
        .build())
    val url = "http://localhost:7070/events.json?accessKey=lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7"

    // Tương tự thư viện build sẵn của PredictionIO cx k đc
    val client = new EventClient("lpFLJ5o83vW1B0LLGNQ7mOoZxdx43h2dUyAZpsjdkIYwwTDktM42p48gUosasnV7", "localhost")


    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    // Khai báo broker server và tên topic
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "data-ingestion01:9992,data-ingestion02:9992,data-ingestion03:9992")
      .option("subscribe", "waka_events")
      .load()

    // Thường Schmema sau khi đọc log sẽ có dạng key/value/... Dl cần đọc nằm trong value
    // Vì value ở dạng binary nên cần cast sang String sau đó sẽ đc kdl Json String
    val valueDF = df.selectExpr("CAST(value AS STRING)")
    // Lấy nhanh các trg cần thiết với get_json_object (có tể lấy trg nested).
    var dataDF = valueDF
      .select(get_json_object(col("value"), "$.user_id").alias("user_id"),
        get_json_object(col("value"), "$.event").alias("event"),
        get_json_object(col("value"), "$.properties.vega_id").alias("vega_id"),
        get_json_object(col("value"), "$.properties.content_id").alias("content_id"))

    // Hoặc có thể tạo Schema, rồi dùng from_json như bên dưới
//    val dataDF = valueDF.select(from_json(col("value"), schema).as("data"))
//      .select("data.*")
    dataDF.printSchema()
    //  root
    //  |-- user_id: string
    //  (nullable = true)
    //  |-- event: string
    //  (nullable = true)
    //  |-- vega_id: string
    //  (nullable = true)
    //  |-- content_id: string
    //  (nullable = true)
    //"time":"2023-03-13T15:18:14+0700"

    // Có thể tiếp tục xử lý thêm DF
    dataDF = dataDF.filter(col("event") === "reading")

    // Dl phải qua writeStream thì ms in ra hoặc xử lý được
    var query = dataDF.writeStream.foreach(
      // Dùng hàm foreach để xử lý từng dòng trong DF stream. Lưu ý cấu trúc phải giống như bên dưới
      // Hàm Process là nơi xử lý dl. Tuy vậy các hàm/object/class trong này phải Serializable (chia nhỏ) đc
      new ForeachWriter[Row] {
        def open(partitionId: Long, version: Long): Boolean = true
        def process(record: Row) = {
            // Test với PredicitonIO Class => K thành công
//          val readEvent = new Event()
//            .event("reader")
//            .entityType("user")
//            .entityId(record.getAs("user_id"))
//            .targetEntityType("item")
//            .targetEntityId(record.getAs("content_id"))
//          client.createEvent(readEvent)

            // Test với Ning Http Client => K thành công
//          val requestBuilder = new RequestBuilder("POST")
//            .setUrl(url)
//            .setHeader("Content-Type", "application/json")
//            .setBody(
//              s"""{
//              "event" : "buy",
//              "entityType" : "user",
//              "entityId" : ${record.getAs("user_id")},
//              "targetEntityType" : "item",
//              "targetEntityId" : ${record.getAs("content_id")}
//            }""")
//          val request: Request = requestBuilder.build()
//          asyncHttpClient.executeRequest(request)

        }
        def close(errorOrNull: Throwable): Unit = {}
      // mode append: chỉ cập nhập những dl ms, mode complete: hiển thị tất cả dl từ khi bđ chạy, chỉ dùng đc với groupBy
      // Trigger.ProcessingTime: Thời gian để xử lý từng Batch, cứ sau x giây sẽ hiển thị 1 batch dl
      }).outputMode("append").trigger(Trigger.ProcessingTime("10 seconds")).start()

    // Code bên ngoài sẽ k chạy
    println("++++++++++++++++++++++++++++++++++++++")
    // Ngắt khi có tín hiệu ngắt
    query.awaitTermination()
  }
}
//  +-------+-------+--------+----------+
//  |user_id|  event| vega_id|content_id|
//  +-------+-------+--------+----------+
//  |7788741|reading|37407379|     17698|
//  |4472581|reading|25378291|     43295|
//  +-------+-------+--------+----------+
