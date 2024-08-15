package spark

import io.delta.tables._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkConcurrentPart4 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Concurrent Part 4")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val deltaTablePath = "D:\\scala-maven-simple\\scala-project-simple\\spark-warehouse\\delta-table"

    // Load the Delta table
    val deltaTable = DeltaTable.forPath(deltaTablePath)

    deltaTable.as("t")
      .update(
        condition = $"id".between(1, 30000),  // UPDATE operation on id = 1
        set = Map("name" -> concat(lit("UpdatedName"), $"id"))
      )

    // Show the final state of the table
    deltaTable.toDF.show()
    println(deltaTable.toDF.count())
    spark.stop()
  }
}
