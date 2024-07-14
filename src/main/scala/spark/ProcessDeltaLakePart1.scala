package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ProcessDeltaLakePart1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Process Delta Lake Part 1")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    //    if (args.length > 1) {
    //      val displayName = args(1)
    //      println("displayName" + displayName)
    //      if (displayName.nonEmpty) {
    //        paFilter = paFilter.filter(paDF("owner_display_name").like(s"%$displayName%"))
    //      }
    //    }

    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"

    val deltaPath = s"${SparkConstants.localDeltaOutputPath}$paName";

    val deltaDF = spark.read.format("delta").load(deltaPath)

    val newDeltaDF = deltaDF.withColumn("version", lit("0"))

    newDeltaDF.write
      .format("delta")
      .mode("overwrite")
      .option("mergeSchema", value = true)
      .save(s"${SparkConstants.localDeltaOutputPath}$paName")

    newDeltaDF.show()

    spark.stop()
  }
}
