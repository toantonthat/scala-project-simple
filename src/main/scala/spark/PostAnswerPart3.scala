package spark

import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object PostAnswerPart3 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Post Answer Part 3").getOrCreate()
    val paDF = spark.read.schema(SparkConstants.PASchema).parquet(SparkConstants.PAPath)
    var paFilter = paDF;
    if (args.length > 0) {
      val score = args(0).toInt
      println("score" + score)
      paFilter = paFilter.filter(paDF("score") > score)
    }
    if (args.length > 1) {
      val displayName = args(1)
      println("displayName" + displayName)
      paFilter = paFilter.filter(paDF("owner_display_name").like(s"%$displayName%"))
    }

    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
    val paName = s"post_answers_${currentTimestamp}"
    paFilter = paFilter.select(paFilter("id"), paFilter("title"), paFilter("body"), paFilter("owner_display_name"))
    paFilter
      .write.parquet(s"${SparkConstants.OutputPath}$paName")
    println("pa count " + paFilter.count())
    paFilter.show()


    val userDF = spark.read.schema(SparkConstants.UserSchema).parquet(SparkConstants.UserPath)
    val joinDF = paDF.join(userDF, userDF("id") === paDF("owner_user_id")).select(paDF("id"), paDF("title"), paDF("body"), userDF("id").alias("user_id"), paDF("owner_display_name"))
    val joinName = s"post_answers_join_users_${currentTimestamp}"
    joinDF
      .write.parquet(s"${SparkConstants.OutputPath}$joinName")
    println("joinDF count " + joinDF.count())
    joinDF.show()

    spark.stop()
  }
}
