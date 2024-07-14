package spark

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object PostAnswerRepartitionPart2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Post Answer Repartition Part 2").getOrCreate()
    val paDF = spark.read.schema(SparkConstants.PASchema).parquet(SparkConstants.PAPath)
    var paFilter = paDF;
    if (args.length > 0) {
      val score = args(0).toInt
      println("score" + score)
      if (score > 0) {
        paFilter = paFilter.filter(paDF("score") > score)
      }
    }
    if (args.length > 1) {
      val displayName = args(1)
      println("displayName" + displayName)
      if (displayName.nonEmpty) {
        paFilter = paFilter.filter(paDF("owner_display_name").like(s"%$displayName%"))
      }
    }
    if (args.length > 2) {
      val partition = args(2).toInt
      println("partition" + partition)
      if (partition > 0) {
        paFilter = paFilter.repartition(partition, paFilter("owner_user_id").isNotNull)
      }
    }

    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"

    // Start measuring time before writing parquet
    val startTime = System.nanoTime()

    paFilter.write.parquet(s"${SparkConstants.OutputPath}$paName")

    // End measuring time after writing parquet
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d  // convert to seconds
    println(s"Time taken to write parquet: $duration seconds")

    spark.stop()
  }
}
