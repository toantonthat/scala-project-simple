package spark

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SparkConcurrentPart2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Concurrent Part 2")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    // Create a Delta table with some initial data
    val data = (1 to 30000).map(id => (id, s"Name$id")).toDF("id", "name")

    val deltaTablePath = "D:\\scala-maven-simple\\scala-project-simple\\spark-warehouse\\delta-table"

    data.write
      .format("delta")
      .mode("overwrite")
      .save(deltaTablePath)

    // Load the Delta table
    val deltaTable = DeltaTable.forPath(deltaTablePath)
//
//    // Start two concurrent operations on the same table
//    val t1 = new Thread(() => {
//      deltaTable.as("t")
//        .delete($"id".between(1, 20000))  // DELETE operation on id = 1
//    })
//
//    val t2 = new Thread(() => {
//      deltaTable.as("t")
//        .update(
//          condition = $"id".between(1, 20000),  // UPDATE operation on id = 1
//          set = Map("name" -> concat(lit("UpdatedName"), $"id"))
//        )
////      Thread.sleep(1000)
//    })
//
////    // Start both threads
//    t1.start()
//    t2.start()
//
////    // Wait for both threads to finish
//    t1.join()
//    t2.join()

    // Show the final state of the table
    deltaTable.toDF.show()
    println(deltaTable.toDF.count())
    spark.stop()
  }
}
