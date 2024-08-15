package deltaLake

import org.apache.spark.sql.SparkSession

object DeltaLakePart2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Listener Part 1").master("local").getOrCreate()
    spark.read.option("header", "true").csv("")
  }
}
