package app.sparkLogs

import org.apache.spark.sql.{DataFrame, SparkSession}
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

    val execTextRegex = """(.+\d\))"""

    val fluentBitLogs = spark.read
      .option("multiline", "true")
      .json("/Users/ian_rakhmatullin/Desktop/Datatech/RWA/tasks/fixLogging/120034120120/spark_cleansed.json")
      .withColumn("localTime", to_timestamp(col("localTime"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
      .withColumn("hour", hour(col("localTime")))
      .withColumn("minute", minute(col("localTime")))
      .withColumn("second", second(col("localTime")))
      .withColumn("text", trim(col("text")))
      .withColumn("text", when(col("text").startsWith(lit("1 block locks were not")), regexp_extract(col("text"), execTextRegex, 1))
        .otherwise(col("text")))

    //just count
    //    showFluentBitLogsGroupedByPodAndLogName(fluentBitLogs)

    //load
//    getLoadStatsFor(fluentBitLogs)
//      .show
//    getLoadStatsFor(getReadPlainTextLogsWithDateColumns)
//      .show

//    getLoadStatsForOnWindow(getReadPlainTextLogsWithDateColumns)

    getStatsHardCounted(getReadPlainTextLogsWithDateColumns)
      .show()


  }

  def getStatsHardCounted(targetDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    targetDf
      .groupBy("podName")
      .agg(max("localTime").alias("maxTime"), min("localTime").alias("minTime"), count("*").alias("logcount"))
      .withColumn("diffInSeconds", col("maxTime").cast("long") - col("minTime").cast("long"))
      .withColumn("averagehard", col("logcount") / col("diffInSeconds"))
  }

  def getLoadStatsForOnWindow(targetDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    targetDf
      .groupBy(window($"localTime", "1 second"), $"podName")
      .agg(count("*").alias("count_per_sec"),
        avg(length(col("text"))).as("avg_size_per_sec"),
      )
      .groupBy("podName")
      .agg("count_per_sec" -> "avg",
        "count_per_sec" -> "max",
        "count_per_sec" -> "min",
        "count_per_sec" -> "stddev_pop",
        "avg_size_per_sec" -> "avg",
        "avg_size_per_sec" -> "max")
  }


  def getLoadStatsFor(targetDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    targetDf
      .groupBy(col("hour"), col("minute"), col("second"), col("podName"))
      .agg(count("*").alias("count_per_sec"),
        avg(length(col("text"))).as("avg_size_per_sec"),
      )
      .groupBy("podName")
      .agg("count_per_sec" -> "avg",
        "count_per_sec" -> "max",
        "count_per_sec" -> "min",
        "count_per_sec" -> "stddev_pop",
        "avg_size_per_sec" -> "avg",
        "avg_size_per_sec" -> "max")
  }

  def showFluentBitLogsGroupedByPodAndLogName(fluentBitLogs: DataFrame): Unit = {
    fluentBitLogs.groupBy("podName", "loggerName")
      .count()
      .orderBy(desc("count"))
      .show(10000, false)
  }

  /**
   * Diffs between console and fluent bit
 *
   * @param fluentBitLogs -df with logs
   * @param plainTextLogs - df with logs
   */
  private def resolveDiffsBetweenTheseTwo(fluentBitLogs: DataFrame, plainTextLogs: DataFrame): Unit = {
    val plainGroupTextLogs = plainTextLogs
      .groupBy("hour", "minute", "second", "podName")
      .agg(count("*").as("count_pt"))


    val fbGroupedLogs = fluentBitLogs
      .groupBy(col("hour"), col("minute"), col("second"), col("podName"))
      .agg(count("*").as("count_fb"))


    val diffsBySecAndPodDf = getDiffsBySecAndPod(plainGroupTextLogs, fbGroupedLogs)


    printLogsCount(fluentBitLogs, plainTextLogs)


    //    println(frame2
    //      .filter(col("podName") === "ian-k8s-exec-exec-4")
    //      .filter(col("text") === "")
    //      .count())

    //    val except = frame1.except(frame2)

    //    val except = frame2.except(frame1)

    //6 for whole stage gen + code from fluent side. Its better idea to set CodeGenerator to WARN
    //same from plain side + null null null

    //    except
    //      .join(plainTextLogs.as("self"), Seq("hour", "minute", "second", "podName", "text"), "left")
    //      .select("hour", "minute", "second", "podName", "text", "self.loggerName")
    //      .show()

  }

  private def getReadPlainTextLogsWithDateColumns(implicit spark: SparkSession) = {
    val plainTextLogs = SparkLogAnalysisPlainText.readPlainLogs
      .withColumn("hour", hour(col("localTime")))
      .withColumn("minute", minute(col("localTime")))
      .withColumn("second", second(col("localTime")))
    plainTextLogs
  }

  private def printLogsCount(fluentBitLogs: DataFrame, plainTextLogs: DataFrame): Unit = {
    val frame1 = fluentBitLogs.select("hour", "minute", "second", "podName", "text")
      .filter(col("hour").isNotNull)
    val frame2 = plainTextLogs.select("hour", "minute", "second", "podName", "text")
      .filter(col("hour").isNotNull)

    println(frame1.count() + " " + frame2.count())
  }

  private def getDiffsBySecAndPod(plainTextLogs: DataFrame, fbGroupedLogs: DataFrame): DataFrame = {
    fbGroupedLogs.as("fb")
      .join(plainTextLogs.as("pt"), Seq("hour", "minute", "second", "podName"), "left")
      .withColumn("diff", col("count_fb").minus(col("count_pt")))
      .filter("diff == 0")
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
