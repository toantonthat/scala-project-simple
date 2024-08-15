package spark

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object ProcessDeltaLakePart7 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark Process Delta Lake Part 7")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val path = "D:\\scala-maven-simple\\scala-project-simple\\spark-warehouse\\post_answers";

    val originalDF = spark.read.format("delta")
      .option("versionAsOf", 0)
      .load(path)

    originalDF.write.format("delta").save("post_answers_original")

    val latestDF = spark.read.format("delta")
      .option("versionAsOf", 4)
      .load(path)

    latestDF.write.format("delta").save("post_answers_latest")

//    println(originalDF.count())
//    println(latestDF.count())


    //paDF.createOrReplaceTempView("post_answer")
    //spark.sql(s"select * from delta.`$path`@v0").show()

    //spark.sql(s"update delta.`$path` set title = id")

//    spark.sql(s"insert into delta.`$path` (id, title) values ('6669191', '123')")

//    val count = spark.sql(s"describe history delta.`$path`").count()
//
//    println(count)

//    spark.sql(s"describe history delta.`$path`").show()
//
//    spark.sql(s"select * from delta.`$path` where id = '6669191'").show()

    spark.stop()
  }
}
