package spark

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ProcessDeltaLakePart2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Process Delta Lake Part 2")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    var version = 1
    if (args.length > 0) {
      version = args(0).toInt
    }

    println("version " + version)
    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"

    val deltaPath = s"${SparkConstants.localDeltaOutputPath}$paName";

    val deltaDF = spark.read
      .format("delta")
      .option("versionAsOf", version)
      .load(deltaPath)

    deltaDF.show()

    spark.stop()
  }
}
