package spark

import org.apache.spark.sql.SparkSession

object SparkTest {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("Spark Simple")
      .master("local")
      .getOrCreate()

    val data = spark.sparkContext.parallelize(Seq("Hello World", "This is some text", "Hello text"))
    val map = data.flatMap(e => e.split(" ")).map(word => (word, 1))

    val sumCounts = (count1: Int, count2: Int) => count1 + count2
    val counts = map.reduceByKey(sumCounts).repartition(1)

    println( "counts " +counts.count())


    spark.stop()
  }
}
