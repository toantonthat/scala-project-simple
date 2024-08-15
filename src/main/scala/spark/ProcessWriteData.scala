package spark

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ProcessWriteData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Process Write Data")
      .getOrCreate()

    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"

    val paDF = spark.read.schema(SparkConstants.PASchema).parquet(SparkConstants.PAPath)
    val paFilter = paDF.filter(paDF("owner_user_id").isNotNull)
    paFilter.write
      .parquet(s"${SparkConstants.OutputPath}$paName")

    spark.stop()
  }
}
