package spark

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object DeltaLakeMain {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark Process Delta Lake Part 5")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val path = "D:\\scala-maven-simple\\scala-project-simple\\spark-warehouse\\post_answers"

    val df = spark.read.option("", "").format("delta").load(path)

    spark.stop()
  }
}
