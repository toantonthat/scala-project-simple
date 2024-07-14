package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object sparkDFJoin {

  def main(args: Array[String]) {
    val rootPath = "gs://stackoverflow_input"
    val spark = SparkSession.builder().appName("Spark SQL Part 1").master("local").getOrCreate()


    //val path = "D:\\scala-maven-simple\\scala-project-simple\\src\\data\\users000000000000";
    val userPath = s"$rootPath/users/users000000000001";
    System.out.println("userPath " + userPath)
//    val fileNames = Seq(
//      "users000000000001",
//      "users000000000002",
//      "users000000000003"
//    )
//    val userFullPath = fileNames.map(name => s"$userPath$name")
    spark.read.schema(SparkConstants.UserSchema).parquet(userPath).createOrReplaceTempView("users")
    val userDF = spark.sql("select * from users")
    userDF.show()

//    val postAnswerPath = "D:\\scala-maven-simple\\scala-project-simple\\src\\data\\post_answer_answer000000000000.parquet";
//    val paSchema = StructType(Array(
//      StructField("id", LongType, nullable = true),
//      StructField("title", StringType, nullable = true),
//      StructField("body", StringType, nullable = true),
//      StructField("last_editor_display_name", StringType, nullable = true),
//      StructField("last_editor_user_id", LongType, nullable = true),
//      StructField("owner_display_name", StringType, nullable = true),
//      StructField("owner_user_id", LongType, nullable = true),
//      StructField("parent_id", LongType, nullable = true),
//    ))
//    val postAnswerDF = spark.read.schema(paSchema).parquet(postAnswerPath)
//    postAnswerDF.createOrReplaceTempView("post_answers")
//
//    val joinDF = spark.sql("select u.*, pa.* from users u inner join post_answers pa on pa.last_editor_user_id == u.id")
//    joinDF.show()

    spark.stop();
  }
}
