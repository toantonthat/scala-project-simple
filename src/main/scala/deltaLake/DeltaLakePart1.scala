package deltaLake

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object DeltaLakePart1 {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("Spark Listener Part 1").master("local").getOrCreate()

    val schema = StructType(Array(
      StructField("name", StringType, nullable = true)
    ))
    val data = Seq(
      Row("ToanDN1"),
      Row("ToanDN2")
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.show()
    println("spark " + spark)
  }

}
