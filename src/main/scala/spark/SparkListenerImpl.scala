package spark

import org.apache.commons.lang3.StringUtils
import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskStart}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import spark.UIData.StageUIData

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet, LinkedHashMap, ListBuffer}
import scala.util.Try


class SparkListenerImpl extends SparkListener with Logging {
  type JobId = Int
  type JobGroupId = String
  type StageId = Int
  type StageAttemptId = Int

  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  val startTime: Long = System.currentTimeMillis()
  val stageIdToData = new mutable.HashMap[(StageId, StageAttemptId), StageUIData]
  private val appId: String = SparkSession.getActiveSession match {
    case Some(sparkSession) => sparkSession.sparkContext.applicationId
    case _ => "noAppId"
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val executorInfo = executorAdded.executorInfo
    val epochMillis = System.currentTimeMillis()
    val metrics = Map[String, Any](
      "name" -> "executors_started",
      "appId" -> appId,
      "executorId" -> executorAdded.executorId,
      "host" -> executorInfo.executorHost,
      "totalCores" -> executorInfo.totalCores,
      "startTime" -> executorAdded.time,
      "epochMillis" -> epochMillis
    )
    report(metrics)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val submissionTime = stageSubmitted.stageInfo.submissionTime.getOrElse(0L);
    val attemptNumber = stageSubmitted.stageInfo.attemptNumber()
    val stageId = stageSubmitted.stageInfo.stageId.toString
    val epochMillis = System.currentTimeMillis()

    val metrics = Map[String, Any](
      "name" -> "stages_started",
      "appId" -> appId,
      "stageId" -> stageId,
      "attemptNumber" -> attemptNumber,
      "submissionTime" -> submissionTime,
      "epochMillis" -> epochMillis
    )
    report(metrics)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println(s"[Generic Listener] Stage completed: ${stageCompleted.stageInfo.stageId}")
    val submissionTime = stageCompleted.stageInfo.submissionTime.getOrElse(0L);
    val attemptNumber = stageCompleted.stageInfo.attemptNumber()
    val stageId = stageCompleted.stageInfo.stageId.toString
    val epochMillis = System.currentTimeMillis()

    val metrics = Map[String, Any](
      "name" -> "stages_started",
      "appId" -> appId,
      "stageId" -> stageId,
      "attemptNumber" -> attemptNumber,
      "submissionTime" -> submissionTime,
      "epochMillis" -> epochMillis
    )
    report(metrics)
  }


  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val startTime = jobStart.time
    val jobId = jobStart.jobId.toString
    val epochMillis = System.currentTimeMillis()

    val jobStartMetrics = Map[String, Any](
      "name" -> "jobs_started",
      "appId" -> appId,
      "jobId" -> jobId,
      "startTime" -> startTime,
      "epochMillis" -> epochMillis
    )
    report(jobStartMetrics)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val completionTime = jobEnd.time
    val jobId = jobEnd.jobId.toString
    val epochMillis = System.currentTimeMillis()

    val jobEndMetrics = Map[String, Any](
      "name" -> "jobs_ended",
      "appId" -> appId,
      "jobId" -> jobId,
      "completionTime" -> completionTime,
      "epochMillis" -> epochMillis
    )
    report(jobEndMetrics)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    super.onTaskStart(taskStart)
    val executor = taskStart.taskInfo.executorId
    val host = taskStart.taskInfo.host
    val time = prettyTime(taskStart.taskInfo.launchTime)
    val taskId = taskStart.taskInfo.taskId
    val stageId = taskStart.taskInfo.taskId
    // Get TaskInfos for this stage to compute number of tasks

    print("Stage #%d: Started task #%d on host %s, executor %s at %s".format(stageId, taskId, host, executor, time))
  }

  private def prettyTime(time: Long) = time - startTime

  protected def report[T <: Any](metrics: Map[String, T]): Unit = Try {
    logger.info("metrics " + metrics)
  }.recover {
    case ex: Throwable => logger.error(s"error on reporting metrics details=${ex.getMessage}", ex)
  }

  def print(msg: String): Unit = {
    try {
      println(msg)
    } catch {
      case exception: Throwable => println(s"error on reporting metrics details=${exception.getMessage}" + exception)
    }
  }
}
