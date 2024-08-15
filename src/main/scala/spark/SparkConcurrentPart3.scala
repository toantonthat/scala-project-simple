package spark

import io.delta.tables._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkConcurrentPart3 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Concurrent Part 3")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val deltaTablePath = "D:\\scala-maven-simple\\scala-project-simple\\spark-warehouse\\delta-table"

    val deltaTable = DeltaTable.forPath(deltaTablePath)

    deltaTable.as("t").delete($"id".between(1, 25000))
    deltaTable.toDF.write
      .format("delta")
      .mode("overwrite")
      .save(deltaTablePath)

    deltaTable.toDF.show()
    println(deltaTable.toDF.count())
    spark.stop()
  }
}
