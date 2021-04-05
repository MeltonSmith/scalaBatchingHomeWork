package bookingDataFiltering

import model.BookingData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Encoder, Row, SparkSession}


/**
 * Created by: Ian_Rakhmatullin
 * Date: 04.04.2021
 */
object BookingDataApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Booking Data Application").getOrCreate()

    import spark.implicits._

    val expedia = spark.read
      .format("avro")
      .load("/201 HW Dataset/expedia")
      .as[BookingData]
      //        .filter("hotel_id == 2680059592710")


//    println("Before: " + expedia.count())

    val hotelsKafka = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option("maxOffsetsPerTrigger", 123389L)
      .option("subscribe", "hotels")
      .load()

    val hotels = spark.read
                      .json(hotelsKafka.selectExpr("CAST(value as STRING) as value")
                                        .map(row => row.toString()))
//                      .filter("Country == 'US'")
//                      .foreach(row => println(row))



    val w = Window.partitionBy("hotel_id").orderBy("srch_ci")

    val invalidData = expedia
                  .withColumn("previousDate", lag("srch_ci", 1).over(w))
                  .withColumn("idle_days", datediff(col("srch_ci"), col("previousDate")))
                  .join(hotels.as("hotels"), expedia.col("hotel_id").equalTo(hotels.col("id")))
                  .where("idle_days >= 2 AND idle_days < 30")
                  .select(hotels.col("Id"), hotels.col("Name"), hotels.col("Address"), hotels.col("Country"), col("idle_days"))

//    invalidData
//      .select(hotels.col("Id"), hotels.col("Name"), hotels.col("Address"), hotels.col("Country"), col("idle_days"))
//      .show(false)

    val l = expedia.as("expedia")
      .join(invalidData.as("invalidData"), expedia.col("hotel_id").equalTo(invalidData.col("Id")), "leftanti")
      .count()

//    println("After: " + l)




    spark.close()
  }
}
