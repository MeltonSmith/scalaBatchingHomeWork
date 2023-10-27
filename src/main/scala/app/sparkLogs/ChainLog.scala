package app.sparkLogs

import org.apache.spark.sql.functions.{col, regexp_extract, split, to_timestamp, trim}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Melton Smith
 * @since 16.08.2023
 */
object ChainLog {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("chain log analysis")
      .getOrCreate()

    val log = readChainLog
    log.show(1000,false)

  }

  def readChainLog(implicit spark: SparkSession): DataFrame = {
    val apacheSparkLogRegex = """(\d+\/\d+\/\d+\s\d*:\d*:\d*,\d*)\s(DEBUG|INFO|WARN|FATAL|ERROR|TRACE)\s([a-zA-Z.]+:|$)(.*)"""


    var frame = spark.read
      .text("/Users/ian_rakhmatullin/Desktop/Datatech/RWA/tasks/playthrough/chainLog.log")
      .withColumn("localTime", to_timestamp(regexp_extract(col("value"), apacheSparkLogRegex, 1), "dd/MM/yy HH:mm:ss,zzz"))
      .withColumn("level", regexp_extract(col("value"), apacheSparkLogRegex, 2))
      .withColumn("loggerName", regexp_extract(col("value"), apacheSparkLogRegex, 3))
      .withColumn("text", trim(regexp_extract(col("value"), apacheSparkLogRegex, 4)))
      .withColumn("df", regexp_extract(col("text"), """(=\s[a-zA-Z]+.*)""", 1))
      .select("df")
      .distinct()
      .drop("value")

    //todo hardcode
//    "ian-k8s-exec-exec-\\d+".r.findFirstIn(path) match {
//      case Some(value) => frame = frame.withColumn("podName", lit(value)).filter(col("text") =!= "")
//      case _ => frame = frame.withColumn("podName", lit("arrow-spark-9ea7838944d08a83-driver"))
//    }
    frame
  }
}
