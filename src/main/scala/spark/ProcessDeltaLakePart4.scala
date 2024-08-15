package spark

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ProcessDeltaLakePart4 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark Process Delta Lake Part 4")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

//    var version = 2
//    if (args.length > 0) {
//      version = args(0).toInt
//    }
//
//    println("version " + version)
//    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now)
//    val paName = s"post_answers_${currentTimestamp}"
//
//    val deltaPath = s"${SparkConstants.localDeltaOutputPath}$paName";
//
//    val deltaDF = spark.read
//      .format("delta")
//      .option("versionAsOf", version)
//      .load(deltaPath)
//
//    deltaDF.show()
//    println("count " + deltaDF.count())

    val paDF = spark.read.parquet("D:\\scala-maven-simple\\scala-project-simple\\src\\resources\\data\\post_answer_answer000000000000.parquet")

    paDF.write.format("delta").mode("overwrite").saveAsTable("post_answers")

    var df1 = spark.sql("select * from post_answers")
    df1.show()

    spark.stop()
  }
}
