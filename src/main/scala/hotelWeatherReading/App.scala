package hotelWeatherReading

import org.apache.spark.sql.{Encoders, SparkSession}

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("HotelWeather Reading Application").getOrCreate()

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option("maxOffsetsPerTrigger", 123389L)
      .option("subscribe", "weather")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "timestamp")
      .foreach(row => println(row.getString(1)))

    spark.stop()
  }

}
