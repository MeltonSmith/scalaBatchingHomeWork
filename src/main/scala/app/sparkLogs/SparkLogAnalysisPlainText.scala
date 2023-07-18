package app.sparkLogs

import org.apache.spark.sql.functions.{col, lit, regexp_extract, to_timestamp, trim}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Melton Smith
 * @since 17.07.2023
 */
object SparkLogAnalysisPlainText {

  def readPlainLogs(implicit sparkSession: SparkSession): DataFrame = {


    val sparkDriverLogFilePath = "/Users/ian_rakhmatullin/Desktop/Datatech/RWA/tasks/fixLogging/120034120120/arrow-spark-9ea7838944d08a83-driver-spark-kubernetes-driver.log"
    val sparkExec4ogFilePath = "/Users/ian_rakhmatullin/Desktop/Datatech/RWA/tasks/fixLogging/120034120120/ian-k8s-exec-exec-4-spark-kubernetes-executor.log"
    val sparkExec5ogFilePath = "/Users/ian_rakhmatullin/Desktop/Datatech/RWA/tasks/fixLogging/120034120120/ian-k8s-exec-exec-5-spark-kubernetes-executor.log"
    val sparkExec6ogFilePath = "/Users/ian_rakhmatullin/Desktop/Datatech/RWA/tasks/fixLogging/120034120120/ian-k8s-exec-exec-6-spark-kubernetes-executor.log"


    val plainTextLogsDf=  sparkDriverLogFilePath.readSparkLogs union sparkExec4ogFilePath.readSparkLogs union sparkExec5ogFilePath.readSparkLogs union sparkExec6ogFilePath.readSparkLogs
    plainTextLogsDf

  }

  implicit class ReadSparkLog(path: String)(implicit spark: SparkSession) {
    def readSparkLogs: DataFrame =  {
      val apacheSparkLogRegex = """(\d+\/\d+\/\d+\s\d*:\d*:\d*)\s(DEBUG|INFO|WARN|FATAL|ERROR|TRACE)\s([a-zA-Z$]+:|$)(.*)"""

      var frame = spark.read
        .text(path)
        .withColumn("localTime", to_timestamp(regexp_extract(col("value"), apacheSparkLogRegex, 1), "dd/MM/yy HH:mm:ss"))
        .withColumn("level", regexp_extract(col("value"), apacheSparkLogRegex, 2))
        .withColumn("loggerName", regexp_extract(col("value"), apacheSparkLogRegex, 3))
        .withColumn("text", trim(regexp_extract(col("value"), apacheSparkLogRegex, 4)))
        .drop("value")

      //todo hardcode
      "ian-k8s-exec-exec-\\d+".r.findFirstIn(path) match {
        case Some(value) => frame = frame.withColumn("podName", lit(value)).filter(col("text") =!= "")
        case _ => frame = frame.withColumn("podName", lit("arrow-spark-9ea7838944d08a83-driver"))
      }
      frame
    }
  }
}
