
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


val spark = SparkSession.builder().master("local[*]").getOrCreate()

// Sample data
import spark.implicits._
val df = Seq(("Vincent", 20090502), ("Lauren", 20170509))
  .toDF("name", "birth_date")
df.printSchema()

// Convert birth_date to string without scientific notation
val dfWithoutScientificNotation = df.withColumn("birth_date", col("birth_date").cast("String"))
// format_number(col("birth_date"), 0)
// Show the result
dfWithoutScientificNotation.show()