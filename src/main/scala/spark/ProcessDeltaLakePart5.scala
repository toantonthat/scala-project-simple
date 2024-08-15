package spark

import org.apache.spark.sql.SparkSession

object ProcessDeltaLakePart5 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark Process Delta Lake Part 5")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val path = "D:\\scala-maven-simple\\scala-project-simple\\spark-warehouse\\post_answers";
    val paDF = spark.read.format("delta").option("versionAsOf", 0).load(path)


    spark.sql(s"describe history delta.`$path`").show()

    spark.sql(s"select * from delta.`$path` where id = '6669191'").show()

    spark.stop()
  }
}
