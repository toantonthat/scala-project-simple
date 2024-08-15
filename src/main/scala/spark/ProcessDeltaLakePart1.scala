package spark

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.types.{LongType, StringType, StructType}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ProcessDeltaLakePart1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Process Delta Lake Part 1")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .master("local")
      .getOrCreate()

    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"

    val deltaPath = s"${SparkConstants.localDeltaOutputPath}$paName";

    val deltaDF = spark.read.format("delta").load(deltaPath)

    val newDeltaDFV1 = deltaDF

//    newDeltaDFV1
//      .withColumn("last_update", current_timestamp())
//      .write
//      .format("delta")
//      .mode("overwrite")
//      .option("mergeSchema", value = true)
//      .save(s"${SparkConstants.localDeltaOutputPath}$paName")

    val newDeltaDFV2 = deltaDF.filter(deltaDF("owner_user_id").isNotNull)

//    newDeltaDFV2.write
//      .format("delta")
//      .mode("overwrite")
//      //.option("mergeSchema", value = true)
//      .save(s"${SparkConstants.localDeltaOutputPath}$paName")

    val newDeltaDFV3 = deltaDF.filter(deltaDF("title").isNotNull)

//    newDeltaDFV3.write
//      .format("delta")
//      .mode("overwrite")
//      //.option("mergeSchema", value = true)
//      .save(s"${SparkConstants.localDeltaOutputPath}$paName")


//    val v1 = spark.read
//      .format("delta")
////      .option("versionAsOf", 1)
//      .load(deltaPath)
//    v1.show()

//    val v2 = spark.read
//      .format("delta")
//      .option("versionAsOf", 2)
//      .load(deltaPath)
//
//    val merge = v1.join(v2, v1("id") === v2("id"), "inner");
//    merge.show()

    // Define the schema of the DataFrame
//    val schema = new StructType()
//      .add("id2", LongType, nullable = false)
//      .add("name", StringType, nullable = true)
//      .add("role", StringType, nullable = true)

    // Create a row with the data to insert
//    val row = Row(4L, "David", "Analyst")

    // Create a DataFrame with the row
//    val insertDF = spark.createDataFrame(
//      spark.sparkContext.parallelize(Seq(row)),
//      schema
//    )

    // Append the DataFrame to the Delta table
//    insertDF.write
//      .format("delta")
//      .option("mergeSchema", "true")
//      .mode("append")
//      .save(deltaPath)

        val v1 = spark.read
          .format("delta")
          .load(deltaPath)
        v1.printSchema()
        v1.select("id2").filter(v1("id2") === 4).show()
    spark.stop()
  }
}
