package hotelWeatherReading

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]) {

    //    val logFile = "/Users/Ian_Rakhmatullin/Desktop/README.md"
    val spark = SparkSession.builder.appName("HotelWeather Reading Application").getOrCreate()
    //    val logData = spark.read.textFile(logFile).cache()

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option("maxOffsetsPerTrigger", 12334342328L)
      .option("subscribe", "daysUnique")
      .load();

    println(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .count())
    //      .foreach(row => println(row))


    //
    //    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //      .encoder
    //
    //    df.show()

    //    val numAs = logData.filter(line => line.contains("a")).count()
    //    val numBs = logData.filter(line => line.contains("b")).count()
    //    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

}
