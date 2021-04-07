package bookingDataFiltering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY


/**
 * Created by: Ian_Rakhmatullin
 * Date: 04.04.2021
 */
object BookingDataApp {
  val check_in_column = "srch_ci"
  val country_column = "Country"
  val id_column = "Id"
  val hotel_id_column = "hotel_id"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Booking Data Application").getOrCreate()
    import spark.implicits._
    //reading expedia from hdfs
    val expedia = spark.read
                        .format("avro")
                        .load("/201 HW Dataset/expedia")
    //hotels from kafka for joining
    val hotelsKafka = spark.read
                            .format("kafka")
                            .option("kafka.bootstrap.servers", "localhost:9094")
                            .option("maxOffsetsPerTrigger", 123389L)
                            .option("subscribe", "hotels")
                            .load()
    //transforming to have only a "payload" of value
    val hotels = spark.read
                      .json(hotelsKafka.selectExpr("CAST(value as STRING) as value")
                      .map(row => row.toString()))

    hotels.persist(MEMORY_ONLY)
    expedia.persist(MEMORY_ONLY)

    val validExpediaToSave = getValidExpediaData(spark, expedia, hotels)

    //saving to hdfs
    validExpediaToSave
                      .write
                      .partitionBy("check_in_year")
                      .format("avro")
                      .save("/201 HW Dataset/validExpedia")

    spark.close()
  }

  /**
   * @param spark spark session
   * @param expedia input expedia
   * @param hotels input hotels
   * @return valid expedia data ready for writing
   */
  def getValidExpediaData(spark: SparkSession, expedia: DataFrame, hotels: DataFrame): DataFrame = {
    import spark.implicits._

    val w = Window.partitionBy(hotel_id_column).orderBy(check_in_column)

    val expediaWithHotels = expedia
                              .withColumn("previousDate", lag(check_in_column, 1).over(w))
                              .withColumn("idle_days", datediff(col(check_in_column), col("previousDate")))
                              .join(hotels.as("hotels"), expedia.col(hotel_id_column).equalTo(hotels.col("id")))

    val invalidData = expediaWithHotels
                            .where("idle_days >= 2 AND idle_days < 30")
                            .select(hotels.col(id_column),
                                    hotels.col("Name"),
                                    hotels.col("Address"),
                                    hotels.col(country_column),
                                    col("idle_days"))
    invalidData.persist(MEMORY_ONLY)

    val validExpediaWithHotels = expediaWithHotels.as("validExp")
      .join(invalidData.as("invalidData"),
        $"validExp.hotel_id" === $"invalidData.Id",
        "leftanti")
    validExpediaWithHotels.persist(MEMORY_ONLY)

    //    grouping remaining data for console printing
    validExpediaWithHotels
                    .groupBy(country_column)
                    .agg(count("*").as("booking count per country"))
                    .show(false)

    val groupedByCity = validExpediaWithHotels
                    .groupBy(country_column, "City")
                    .agg(count("*").as("booking count per city"))
    groupedByCity.persist(MEMORY_ONLY)

    val countInt = groupedByCity.count().asInstanceOf[Int]
    groupedByCity.show(countInt)

    val validExpediaToSave = expedia.as("expedia")
      .join(invalidData.as("invalidData"),
        expedia.col(hotel_id_column).equalTo(invalidData.col(id_column)),
        "leftanti")
      .withColumn("check_in_year", year(col(check_in_column)))

    validExpediaToSave.persist(MEMORY_ONLY)
  }
}
