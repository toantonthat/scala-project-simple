package spark

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SparkListenerTest2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Listener Part 2").getOrCreate()
    val sc = spark.sparkContext
    val sparkConf = sc.getConf
    val sparkMonitorListener = new SparkMonitorListener(sparkConf)
    spark.sparkContext.addSparkListener(sparkMonitorListener)

    val startTime = System.currentTimeMillis()

    val paDF = spark.read.schema(SparkConstants.PASchema).parquet(SparkConstants.PAPath)
    var paFilter = paDF;
    if (args.length > 0) {
      val score = args(0).toInt
      println("score" + score)
      paFilter = paFilter.filter(paDF("score") > score)
    }
    if (args.length > 1) {
      val displayName = args(1)
      println("displayName" + displayName)
      paFilter = paFilter.filter(paDF("owner_display_name").like(s"%$displayName%"))
    }

    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"
    paFilter.write.parquet(s"${SparkConstants.OutputPath}$paName")

    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 1000.0
    println(s"Job duration: $duration seconds")

    spark.stop()
  }
}
