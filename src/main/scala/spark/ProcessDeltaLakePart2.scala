package spark

import io.delta.tables.DeltaTable
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

    var version = 2
    if (args.length > 0) {
      version = args(0).toInt
    }

    println("version " + version)
    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"

    val deltaPath = s"${SparkConstants.localDeltaOutputPath}$paName";

    val deltaDFV1 = spark.read
      .format("delta")
      .option("versionAsOf", 1)
      .load(deltaPath)

    val deltaDFV2 = spark.read
      .format("delta")
      .option("versionAsOf", 1)
      .load(deltaPath)

//    val v2 = spark.read
//      .format("delta")
//      .option("versionAsOf", 2)
//      .load(deltaPath)

    val deltaTable = DeltaTable.forPath(spark, deltaPath)

//    deltaTable.as("target")
//      .merge(
//        deltaDFV2.as("source"),
//        "target.id = source.id"
//      )
//      .whenMatched
//      .updateExpr(
//        Map(
//          "id" -> "source.id",
//        )
//      )
//      .execute()

    deltaTable.toDF.show()

//    val resultDF = spark.read.format("delta").load(deltaPath)
//    resultDF.show()
//
//    println("count " + resultDF.count())

    spark.stop()
  }
}
