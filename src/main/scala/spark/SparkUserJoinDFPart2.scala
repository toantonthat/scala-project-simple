package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import spark.SparkFunctions.generateStrings

object SparkUserJoinDFPart2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark User Join Part 2").getOrCreate()
    val usrName = "users"
    var userPaths = generateStrings(usrName, s"${usrName}000000000000", s"${usrName}000000000002", SparkConstants.UserPath)
    if (args.length > 1) {
      userPaths = generateStrings(usrName, s"$usrName${args(0)}", s"$usrName${args(1)}", SparkConstants.UserPath)
      println(s"path: " + s"$usrName${args(0)}", s"$usrName${args(1)}")
    }
    var userDF = spark.read.schema(SparkConstants.UserSchema).parquet(userPaths: _*)
    if (args.length > 2) {
      println("Usage: SparkGetFilterUsers <display_name>")
      val displayName = args(2)
      println(s"displayName: $displayName")
      userDF = userDF.filter(col("display_name").like(s"%$displayName%"))
    }
    userDF.createOrReplaceTempView("users")

    val paName = "answer"
    var paPaths = generateStrings(paName, s"${paName}000000000000", s"${paName}000000000002", SparkConstants.PAPath)
    if (args.length > 1) {
      paPaths = generateStrings(paName, s"$paName${args(0)}", s"$paName${args(1)}", SparkConstants.PAPath)
      println(s"path: " + s"$paName${args(0)}", s"$paName${args(1)}")
    }
    val paDF = spark.read.schema(SparkConstants.PASchema).parquet(paPaths: _*)
    paDF.createOrReplaceTempView("post_answers")

    val userJoinDF = userDF.join(paDF, paDF("last_editor_user_id") === userDF("id"), "inner")
      .select(userDF("id"), userDF("display_name"), userDF("about_me"), userDF("age"), userDF("location"), userDF("creation_date"),
        paDF("title"), paDF("body"), paDF("owner_user_id"), paDF("owner_display_name"))
    println("user join count " + userJoinDF.count())

    val userJoinName = "user_post_answers"
    userJoinDF.write.parquet(s"${SparkConstants.OutputPath}$userJoinName")

    spark.stop()
  }
}
