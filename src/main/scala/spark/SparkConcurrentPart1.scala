package spark

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SparkConcurrentPart1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Concurrent Part 1")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.scheduler.allocation.file", "./fairscheduler.xml")
      .getOrCreate()

    spark.sparkContext.setLocalProperty("spark.scheduler.pool","mypool")

    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"
    val path = s"${SparkConstants.OutputPath}$paName";
    //
    val paDF = spark.read
      .schema(SparkConstants.PASchema)
      .parquet(path)

    val distinctPaDF = paDF.select(paDF("id"), paDF("body"))

    val schema = StructType(List(
      StructField("id", LongType, nullable = true),
      StructField("body", StringType, nullable = true)
    ))

    // Create data
    val numRows = 1000
    val data = (1L to numRows.toLong).map(i => Row(i, s"Body_$i"))
    val dataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val combinedDF = distinctPaDF.union(dataFrame)
    combinedDF.show()

    val parquetPath = s"${SparkConstants.OutputPath}post_answers_temp";
    combinedDF.write.parquet(parquetPath)

    spark.stop()
  }
}
