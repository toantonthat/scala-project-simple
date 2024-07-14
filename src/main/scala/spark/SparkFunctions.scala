package spark

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object SparkFunctions {
  def getFilesFromDirectory(spark: SparkSession, directory: String): Array[String] = {
    val path = new Path(directory)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val status = fs.listStatus(path)
    var files: Array[String] = Array.empty[String]

    for (fileStatus <- status if fileStatus.isFile) {
      files :+= fileStatus.getPath.toString
    }
    files
  }

  def main(args: Array[String]): Unit = {
    val strings = generateStrings("users", "users000000000000", "users000000000016", SparkConstants.UserPath)
    strings.foreach(println)
  }

  def generateStrings(subName: String, start: String, end: String, directory: String): Array[String] = {
    var list: Array[String] = Array.empty[String]
    var i = start.substring(subName.length).toLong
    val n = end.substring(subName.length).toLong

    while (i <= n) {
      val str = s"$directory$subName"//{"%012d".format(i)}"
      list :+= str
      i += 1
    }

    list
  }
}
