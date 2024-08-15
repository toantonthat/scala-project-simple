package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object SparkGroupPart1 {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Group Part 1").master("local").getOrCreate()

    val paDF = spark.read.parquet("D:\\scala-maven-simple\\scala-project-simple\\src\\resources\\data\\post_answer_answer000000000000.parquet")
    val groupPaDF = paDF.groupBy(paDF("owner_user_id")).agg(count(paDF("owner_user_id")).alias("Total Users"))
    groupPaDF.show()
    println(groupPaDF.count())
  }
}
