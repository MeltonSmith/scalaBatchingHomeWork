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

    val w = Window.partitionBy("hotel_id").orderBy("srch_ci")

    val expediaWithHotels = expedia
                              .withColumn("previousDate", lag("srch_ci", 1).over(w))
                              .withColumn("idle_days", datediff(col("srch_ci"), col("previousDate")))
                              .join(hotels.as("hotels"), expedia.col("hotel_id").equalTo(hotels.col("id")))

    val invalidData = expediaWithHotels
                                .where("idle_days >= 2 AND idle_days < 30")
                                .select(hotels.col("Id"), hotels.col("Name"), hotels.col("Address"), hotels.col("Country"), col("idle_days"))

    val validExpediaWithHotels = expediaWithHotels.as("validExp")
                      .join(invalidData.as("invalidData"),
                        $"validExp.hotel_id" === $"invalidData.Id",
                        "leftanti")


//    grouping remaning data
    validExpediaWithHotels
      .groupBy("Country")
      .agg(count("*").as("booking count per country"))
      .show(false)

    val groupedByCity = validExpediaWithHotels
                .groupBy("Country", "City")
                .agg(count("*").as("booking count per city"))
    val countInt = groupedByCity.count().asInstanceOf[Int]
    groupedByCity.show(countInt)


    val validExpediaToSave = expedia.as("expedia")
                                  .join(invalidData.as("invalidData"),
                                    expedia.col("hotel_id").equalTo(invalidData.col("Id")),
                                    "leftanti").as[BookingData]

    validExpediaToSave
                      .withColumn("check_in_year", year(col("srch_ci")))
                      .write
                      .partitionBy("check_in_year")
                      .format("avro")
                      .save("/201 HW Dataset/validExpedia")

    spark.close()
  }
}
