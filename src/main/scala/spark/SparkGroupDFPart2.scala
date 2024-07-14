package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}

object SparkGroupDFPart2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Group Part 2").getOrCreate()
    val userPaths = SparkConstants.UserPath
    var userDF = spark.read.schema(SparkConstants.UserSchema).parquet(userPaths)
    if (args.length > 2) {
      println("Usage: SparkGetFilterUsers <display_name>")
      val displayName = args(2)
      println(s"displayName: $displayName")
      userDF = userDF.filter(col("display_name").like(s"%$displayName%"))
    }

    val commentPaths = SparkConstants.CommentPath
    val commentDF = spark.read.schema(SparkConstants.commentSchema).parquet(commentPaths)

    val userCommentJoinDF = userDF.join(commentDF, userDF("id") === commentDF("user_id"), "inner")
      .select(userDF("id"), userDF("display_name"), userDF("location"),
        commentDF("id").alias("comment_id"), commentDF("user_id"), commentDF("user_display_name"), commentDF("text"), commentDF("post_id"))

    val aggGroupUserAndCountComments = userCommentJoinDF
      .select(userCommentJoinDF("id"), userCommentJoinDF("display_name"), userCommentJoinDF("location"), userCommentJoinDF("comment_id"))
      .groupBy(userCommentJoinDF("id"), userCommentJoinDF("display_name"), userCommentJoinDF("location"))
      .agg(count(userCommentJoinDF("comment_id")).alias("TotalComments"))
      .orderBy(col("TotalComments").desc)
    aggGroupUserAndCountComments.show()

//    val userVotesAndCommentName = "user_and_comments"
//    aggGroupUserAndCountComments.write.parquet(s"${SparkConstants.OutputPath}$userVotesAndCommentName")

    spark.stop()
  }
}
