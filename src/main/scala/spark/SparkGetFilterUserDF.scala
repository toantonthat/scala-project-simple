package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import spark.SparkFunctions.generateStrings

object SparkGetFilterUserDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Get Filter Users Part 1").getOrCreate()
    val usrName = "users"
    var userPaths = generateStrings(usrName, s"${usrName}000000000000", s"${usrName}000000000005", SparkConstants.UserPath)
    if (args.length > 1) {
      println(s"path: " + args(0), args(1))
      userPaths = generateStrings(usrName, s"$usrName${args(0)}", s"$usrName${args(1)}", SparkConstants.UserPath)
    }
    var userDF = spark.read.schema(SparkConstants.UserSchema).parquet(userPaths: _*)
    if (args.length > 2) {
      println("Usage: SparkGetFilterUsers <display_name>")
      val displayName = args(2)
      println(s"displayName: $displayName")
      userDF = userDF.filter(col("display_name").like(s"%$displayName%"))
    }
    println("count " + userDF.count())
    userDF.show()

    spark.stop()
  }
}
