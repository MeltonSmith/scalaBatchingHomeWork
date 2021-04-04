package bookingDataFiltering

import model.BookingData
import org.apache.spark.sql.{Encoder, SparkSession}


/**
 * Created by: Ian_Rakhmatullin
 * Date: 04.04.2021
 */
object BookingDataApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Booking Data Application").getOrCreate()

    import spark.implicits._

    spark.read
        .format("avro")
        .load("/201 HW Dataset/expedia")
        .as[BookingData]
//        .filter("hotel_id == 2680059592710")
        .filter(data => data.hotel_id == 2680059592710L)
        .foreach(row => println(row))

//    spark.read
//      .parquet("13232")
//      .as[BookingData]


    spark.close()
  }
}
