import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("DuplicateRemovalExample")
  .master("local")
  .getOrCreate()

import spark.implicits._
// Create original df
val df = Seq(
  ("227705102", "-4.4239483,48.30", "227580520", "-4.423927,48.301"),
  ("227705102", "-4.4239483,48.30", "227580520", "-4.4240417,48.30"),
  ("227705102", "-4.4239483,48.30", "227580520", "-4.42384,48.3015"),
  ("227705102", "-4.4239483,48.30", "227580520", "-4.4239235,48.30"),
  ("227705102", "-4.4239483,48.30", "227592820", "-4.4238935,48.30"),
  ("227705102", "-4.4239483,48.30", "227113100", "-4.423935,48.301"),
  ("227306100", "-4.4624915,48.34", "227113100", "-4.462385,48.349"),
  ("227306100", "-4.4624915,48.34", "227590030", "-4.462485,48.348"),
  ("259019000", "-4.514735,48.369", "205204000", "-4.51476,48.3693"),
  ("259019000", "-4.514735,48.369", "205204000", "-4.5147367,48.36"),
  ("259019000", "-4.514735,48.369", "205204000", "-4.5148015,48.36"),
  ("259019000", "-4.514735,48.369", "228064900", "-4.5147567,48.36"),
  ("259019000", "-4.514735,48.369", "228762000", "-4.514828,48.369"),
  ("259019000", "-4.514735,48.369", "228762000", "-4.514692,48.369"),
  ("259019000", "-4.514735,48.369", "228762000", "-4.5148416,48.36"),
  ("259019000", "-4.514735,48.369", "228762000", "-4.5148416,48.36"),
  ("259019000", "-4.514735,48.369", "205204000", "-4.5147867,48.36"),
  ("259019000", "-4.514735,48.369", "205204000", "-4.51473,48.3694"),
  ("259019000", "-4.514735,48.369", "205204000", "-4.51474,48.3694"),
  ("259019000", "-4.514735,48.369", "205204000", "-4.514735,48.369")
).toDF("sourcemmsi_1", "coord_1", "sourcemmsi_2", "coord_2")
df.show(false)

val df_1 = df.withColumn("coordinates_1", concat(lit("("), df("coord_1"), lit(")")))
  .withColumn("coordinates_2", concat(lit("("), df("coord_2"), lit(")")))
val parenthesis_df = df_1.select("sourcemmsi_1", "coordinates_1", "sourcemmsi_2", "coordinates_2")
val structed_df = parenthesis_df.groupBy("sourcemmsi_1", "sourcemmsi_2").agg(collect_list("coordinates_1").as("coordinates_1"),collect_list("coordinates_2").as("coordinates_2"))
structed_df.show(false)
val expressed_df = structed_df.selectExpr("concat('{', sourcemmsi_1, ',', concat_ws(',', coordinates_1),'}', '{', sourcemmsi_2, ',', concat_ws(',', coordinates_2),'}') as data")
//expressed_df.show(false)
expressed_df.coalesce(1).write.text("detected_stops_pairs")
