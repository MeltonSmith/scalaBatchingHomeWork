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

    val frame = spark.read
                      .json(hotelsKafka.selectExpr("CAST(value as STRING) as value")
                                        .map(row => row.toString()))
//                      .filter("Country == 'US'")
//                      .foreach(row => println(row))



    val w = Window.partitionBy("hotel_id").orderBy("srch_ci")

    expedia
      .withColumn("previousDate", lag("srch_ci", 1).over(w))
      .withColumn("idle_days", datediff(col("srch_ci"), col("previousDate")))
      .join(frame.as("hotels"), expedia.col("hotel_id").equalTo(frame.col("id")))
      .where("idle_days >= 2 AND idle_days < 30")
//      .where(expedia.col("idle_days").geq(2).and(expedia.col("idle_days").lt(30)))
//      .selectExpr("hotels.Id", "hotels.Name", "idle_days")
      .select(frame.col("Id"), frame.col("Name"), frame.col("Address"),frame.col("Country"), col("idle_days"))
      .show(false)




    spark.close()
  }
}
