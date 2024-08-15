package spark

import org.apache.spark.sql.SparkSession

object ProcessDeltaLakePart6 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark Process Delta Lake Part 6")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val path = "D:\\scala-maven-simple\\scala-project-simple\\spark-warehouse\\post_answers";
    val paDF = spark.read.format("delta").option("versionAsOf", 0).load(path)

    val count = spark.sql(s"select * from delta.`$path`").count()
    var query = s"insert into delta.`$path` (id, title) values "
    println(count)
    for(i <- 1 to 1000) {
      val num = count + i;
      val row = s"('${num}', '${num}')";

      query +=  row;
      if (i != 1000) {
        query += ","
      }
    }
    println(query)
    spark.sql(query)
    spark.sql(s"describe history delta.`$path`").show()

    spark.stop()
  }
}
