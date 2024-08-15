package spark

import org.apache.spark.sql.SparkSession

object ProcessDeltaLakePart9 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark Process Delta Lake Merge Part 2")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val originalPath = "file:/D:/scala-maven-simple/scala-project-simple/post_answers_original";

    spark.sql(s"describe history delta.`$originalPath`").show()

    spark.stop()
  }
}
