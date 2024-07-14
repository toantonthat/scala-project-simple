package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, count, lit}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SparkGroupDFPart1 {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Spark Group Part 1").getOrCreate()
    var userDF = spark.read.schema(SparkConstants.UserSchema).parquet(SparkConstants.UserPath)
    if (args.length > 2) {
      println("Usage: SparkGetFilterUsers <display_name>")
      val displayName = args(2)
      println(s"displayName: $displayName")
      userDF = userDF.filter(col("display_name").like(s"%$displayName%"))
    }

    val paDF = spark.read.schema(SparkConstants.PASchema).parquet(SparkConstants.PAPath)
    paDF.createOrReplaceTempView("post_answers")

    val voteDF = spark.read.schema(SparkConstants.VoteSchema).parquet(SparkConstants.VotePath)

    val commentDF = spark.read.schema(SparkConstants.commentSchema).parquet(SparkConstants.CommentPath)

    val userCommentJoinDF = userDF.join(commentDF, userDF("id") === commentDF("user_id"), "inner")
      .select(userDF("id"), userDF("display_name"), userDF("location"),
        commentDF("id").alias("comment_id"), commentDF("user_id"), commentDF("user_display_name"), commentDF("text"), commentDF("post_id"))

    val userPostJoinVoteDF = userCommentJoinDF.join(voteDF, userCommentJoinDF("post_id") === voteDF("post_id"), "left")
      .select(userCommentJoinDF("id"), userCommentJoinDF("display_name"), userCommentJoinDF("location"),
        userCommentJoinDF("comment_id"), userCommentJoinDF("user_id"), userCommentJoinDF("user_display_name"), userCommentJoinDF("text"), userCommentJoinDF("post_id"),
        voteDF("id").alias("vote_id"), voteDF("vote_type_id"), voteDF("post_id").alias("vote_post_id"))

    val aggGroupUserAndCountComments = userPostJoinVoteDF
      .select(userPostJoinVoteDF("id"), userPostJoinVoteDF("display_name"), userPostJoinVoteDF("location"), userPostJoinVoteDF("comment_id"))
      .groupBy(userPostJoinVoteDF("id"), userPostJoinVoteDF("display_name"), userPostJoinVoteDF("location"))
      .agg(count(userPostJoinVoteDF("comment_id")).alias("TotalComments"))
      .orderBy(col("TotalComments").desc)

    val aggGroupUserAndCountVotes = userPostJoinVoteDF
      .select(userPostJoinVoteDF("id"), userPostJoinVoteDF("display_name"), userPostJoinVoteDF("location"), userPostJoinVoteDF("vote_id"))
      .filter(userPostJoinVoteDF("vote_id").isNotNull)
      .groupBy(userPostJoinVoteDF("id"), userPostJoinVoteDF("display_name"), userPostJoinVoteDF("location"))
      .agg(count(userPostJoinVoteDF("vote_id")).alias("TotalVotes"))
      .orderBy(col("TotalVotes").desc)

    val aggUserVotesAndComments = aggGroupUserAndCountComments
      .join(aggGroupUserAndCountVotes,
        aggGroupUserAndCountVotes("id") === aggGroupUserAndCountComments("id"),
        "left")
      .select(aggGroupUserAndCountComments("id"),
        aggGroupUserAndCountComments("display_name"),
        aggGroupUserAndCountComments("location"),
        aggGroupUserAndCountComments("TotalComments"),
        coalesce(aggGroupUserAndCountVotes("TotalVotes"), lit(0)).alias("TotalVotes"))
      .orderBy(col("TotalComments").desc, col("TotalVotes").desc)
    aggUserVotesAndComments.show()

    val currentTimestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now)
    val userVotesAndCommentName = s"user_and_comments_${currentTimestamp}"
    aggGroupUserAndCountComments.write.parquet(s"${SparkConstants.OutputPath}$userVotesAndCommentName")

    spark.stop()
  }
}
