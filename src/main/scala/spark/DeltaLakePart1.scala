package spark

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DeltaLakePart1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Delta Lake Part 1")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    val paDF = spark.read.schema(SparkConstants.PASchema).parquet(SparkConstants.localPAPath)
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
      val coalesce = args(2).toInt
      println("coalesce" + coalesce)
      if (coalesce > 0) {
        paFilter = paFilter.coalesce(coalesce)
      }
    }

    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"

    paFilter.write.format("delta").mode("append").save(s"${SparkConstants.localDeltaOutputPath}$paName")

    spark.stop()
  }
}
