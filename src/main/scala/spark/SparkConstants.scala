package spark

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object SparkConstants {
  private val localRoot = "src/resources/data/"
  private val RootPath: String = "gs://stackoverflow_input"
  val localUserPath = s"$localRoot/users/"
  val UserPathName = ""
  val UserPath: String = s"$RootPath/users/"
  val localVotePath = s"$localRoot/votes/"
  val VotePath: String = s"$RootPath/votes/"
  val localCommentPath = s"$localRoot/comments/"
  val CommentPath: String = s"$RootPath/comments/"
  val UserSchema = StructType(Array(
    StructField("id", LongType, nullable = true),
    StructField("display_name", StringType, nullable = true),
    StructField("about_me", StringType, nullable = true),
    StructField("age", StringType, nullable = true),
    StructField("creation_date", TimestampType, nullable = true),
    StructField("last_access_date", TimestampType, nullable = true),
    StructField("location", StringType, nullable = true),
    StructField("reputation", LongType, nullable = true),
    StructField("up_votes", LongType, nullable = true),
    StructField("down_votes", LongType, nullable = true),
    StructField("views", LongType, nullable = true),
    StructField("profile_image_url", StringType, nullable = true),
    StructField("website_url", StringType, nullable = true),
  ))
  val PAPath: String = s"$RootPath/post_answer/"
  val localPAPath = s"$localRoot/post_answer/"
  val PASchema = StructType(Array(
    StructField("id", LongType, nullable = true),
    StructField("title", StringType, nullable = true),
    StructField("body", StringType, nullable = true),
    StructField("last_editor_display_name", StringType, nullable = true),
    StructField("last_editor_user_id", LongType, nullable = true),
    StructField("owner_display_name", StringType, nullable = true),
    StructField("owner_user_id", LongType, nullable = true),
    StructField("parent_id", LongType, nullable = true),
    StructField("score", LongType, nullable = true),
    StructField("view_count", StringType, nullable = true),
  ))
  val VoteSchema = StructType(Array(
    StructField("id", LongType, nullable = true),
    StructField("creation_date", TimestampType, nullable = true),
    StructField("post_id", LongType, nullable = true),
    StructField("vote_type_id", LongType, nullable = true),
  ))
  val commentSchema = StructType(Array(
    StructField("id", LongType, nullable = true),
    StructField("user_display_name", StringType, nullable = true),
    StructField("post_id", LongType, nullable = true),
    StructField("user_id", LongType, nullable = true),
    StructField("score", LongType, nullable = true),
    StructField("text", StringType, nullable = true),
    StructField("creation_date", TimestampType, nullable = true),
  ))
  val localOutputPath: String = s"$localRoot/output/"
  val OutputPath: String = s"$RootPath/output/"
}
