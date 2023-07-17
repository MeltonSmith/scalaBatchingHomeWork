package app.sparkLogs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * @author MS
 * @since 14.07.2023
 */
object SparkLogAnalysis {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("log analysis")
      .getOrCreate()

    val fluentBitLogs = spark.read
      .option("multiline", "true")
      .json("/Users/ian_rakhmatullin/Desktop/Datatech/RWA/tasks/fixLogging/120034120120/spark_cleansed.json")
      .withColumn("localTime", to_timestamp(col("localTime"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

//    fluentBitLogs.groupBy("podName")
//      .count().show()

    //    fluentBitLogs
    //      .groupBy("sparkAppType", "podName", "loggerName")
    //      .count()
    //      .select("loggerName", "count")
    //      .show(10000000,false )

    //load
    val fluentBitLogsDfGropedBySec = fluentBitLogs
      .groupBy(hour(col("localTime").alias("hour")),
        minute(col("localTime").alias("minute")),
        second(col("localTime").alias("second")),
        col("podName"))
      .agg(count("*").alias("count_per_sec"))
      .groupBy("podName")
      .agg("count_per_sec" -> "avg",
        "count_per_sec" -> "max",
        "count_per_sec" -> "min",
        "count_per_sec" -> "stddev_pop")

    val plainTextLogs = SparkLogAnalysisPlainText.readPlainLogs
      .groupBy(hour(col("localTime")).alias("hour"),
              minute(col("localTime")).alias("minute"),
              second(col("localTime")).alias("second"),
              col("podName"))
      .agg(count("*").as("count_per_sec_pt"))


    val fbGroupedLogs = fluentBitLogs
      .groupBy(hour(col("localTime")).alias("hour"),
              minute(col("localTime")).alias("minute"),
              second(col("localTime")).alias("second"),
              col("podName"))
      .agg(count("*").as("count_per_sec_fb"))

    fbGroupedLogs.as("fb")
      .join(plainTextLogs.as("pt"), Seq("hour", "minute", "second", "podName"), "left")
      .withColumn("diff", col("count_per_sec_fb").minus(col("count_per_sec_pt")))
      .filter("diff > 0")
      .show()



  }


  private def getTslgLogSchema = {
    List(
      StructField("appName", StringType, nullable = false),
      StructField("projectCode", StringType, nullable = false),
      StructField("risCode", StringType, nullable = false),
      StructField("localTime", DateType, nullable = false),
      StructField("level", StringType, nullable = false),
      StructField("text", StringType, nullable = false),
      StructField("eventId", DoubleType, nullable = false),
      StructField("encProvider", StringType, nullable = false),
      StructField("argType", StringType, nullable = false),
      StructField("traceId", StringType, nullable = false),
      StructField("spanId", StringType, nullable = false),
      StructField("appType", StringType, nullable = false),
      StructField("levelInt", StringType, nullable = false),
      StructField("sparkAppType", StringType, nullable = false),
      StructField("loggerName", StringType, nullable = false),
      StructField("envType", StringType, nullable = false),
      StructField("esIndexLevelSuffix", StringType, nullable = false),
      StructField("es_index_name_suffix", DoubleType, nullable = false),
      StructField("namespace", StringType, nullable = false),
      StructField("podName", StringType, nullable = false),
      StructField("tec", StructType(Seq(
        StructField("nodeName", StringType, nullable = false),
        StructField("podIp", StringType, nullable = false),
      )), nullable = false),
      StructField("threadName", StringType, nullable = false),
      StructField("tslgClientVersion", StringType, nullable = false)
    )
  }

}
