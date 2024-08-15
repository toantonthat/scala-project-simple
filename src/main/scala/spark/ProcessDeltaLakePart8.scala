package spark

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object ProcessDeltaLakePart8 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark Process Delta Lake Merge Part 1")
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val originalPath = "file:/D:/scala-maven-simple/scala-project-simple/post_answers_original";
    val latestPath = "file:/D:/scala-maven-simple/scala-project-simple/post_answers_latest"
    spark.sql(s"CREATE TABLE post_answers_original USING DELTA LOCATION '$originalPath'")
    spark.sql(s"CREATE TABLE post_answers_latest USING DELTA LOCATION '$latestPath'")

    val originalTbl = DeltaTable.forName(spark, "post_answers_original")
    val latestTbl = DeltaTable.forName(spark, "post_answers_latest")
    val latestDf = latestTbl.toDF

    println("originalTbl count: " + originalTbl.toDF.count())
//    println(latestTbl.toDF.count())

    originalTbl
      .as("original")
      .merge(latestDf.as("updates"), "original.id = updates.id")
      .whenMatched
      .updateExpr(
        Map(
          "id" -> "updates.id",
          "title" -> "updates.title",
          "body" -> "updates.body",
          "accepted_answer_id" -> "updates.accepted_answer_id",
          "answer_count" -> "updates.answer_count",
          "comment_count" -> "updates.comment_count",
          "community_owned_date" -> "updates.community_owned_date",
          "creation_date" -> "updates.creation_date",
          "favorite_count" -> "updates.favorite_count",
          "last_activity_date" -> "updates.last_activity_date",
          "last_edit_date" -> "updates.last_edit_date",
          "last_editor_display_name" -> "updates.last_editor_display_name",
          "last_editor_user_id" -> "updates.last_editor_user_id",
          "owner_display_name" -> "updates.owner_display_name",
          "owner_user_id" -> "updates.owner_user_id",
          "parent_id" -> "updates.parent_id",
          "post_type_id" -> "updates.post_type_id",
          "score" -> "updates.score",
          "tags" -> "updates.tags",
          "view_count" -> "updates.view_count",
        )
      )
      .whenNotMatched
      .insertExpr(
        Map(
          "id" -> "updates.id",
          "title" -> "updates.title",
          "body" -> "updates.body",
          "accepted_answer_id" -> "updates.accepted_answer_id",
          "answer_count" -> "updates.answer_count",
          "comment_count" -> "updates.comment_count",
          "community_owned_date" -> "updates.community_owned_date",
          "creation_date" -> "updates.creation_date",
          "favorite_count" -> "updates.favorite_count",
          "last_activity_date" -> "updates.last_activity_date",
          "last_edit_date" -> "updates.last_edit_date",
          "last_editor_display_name" -> "updates.last_editor_display_name",
          "last_editor_user_id" -> "updates.last_editor_user_id",
          "owner_display_name" -> "updates.owner_display_name",
          "owner_user_id" -> "updates.owner_user_id",
          "parent_id" -> "updates.parent_id",
          "post_type_id" -> "updates.post_type_id",
          "score" -> "updates.score",
          "tags" -> "updates.tags",
          "view_count" -> "updates.view_count",
        )
      )
      .execute()

    val originalDF = spark.read.format("delta").load(originalPath)
    originalDF.show()
    println("originalDF count: " + originalDF.count())

    spark.stop()
  }
}
