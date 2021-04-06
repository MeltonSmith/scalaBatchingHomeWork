package bookingDataTest

import bookingDataFiltering.BookingDataApp
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import model.BookingData
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper

/**
 * Created by: Ian_Rakhmatullin
 * Date: 06.04.2021
 */
class BookingDataTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{

  import spark.implicits._



  it("aliases a DataFrame") {

    val hotelsTest = Seq(
      (8589934594L, "Holiday Inn Express", "US", "Ashland", "555 Clover Ln", 42.183544, -122.663345, "9r2z"),
      (25769803777L, "Travelodge", "US", "Oswego", "309 W Seneca St", 43.45161, -76.53235, "dr9x"),
      (25769803782L, "Ibis Wuerzburg City", "US", "Gresham", "Veitshoechheimer Str 5b", 49.802514, 9.921491, "u0z5"),
    ).toDF("Id", "Name", "Country", "City", "Address", "Latitude", "Longitude", "GeoHash")

    //I'm using not all fields
    val expediaTest = Seq(
      (1L, "2020-06-19", "2020-06-22", 8589934594L),
      (2L, "2020-06-20", "2020-06-24", 25769803777L),
      (3L, "2020-06-23", "2020-06-26", 25769803782L),
    ).toDF("id", "srch_ci", "srch_co", "hotel_id")

//    val actualDF = hotelsTest.select(col("Name").alias("Name"))
//
//    val expectedDF = Seq(
//      ("Holiday Inn Express"),
//      ("Travelodge"),
//      ("Ibis Wuerzburg City")
//    ).toDF("Name")

    val frame = BookingDataApp.getValidExpediaData(spark, expediaTest, hotelsTest)

    frame.show()

//    assertSmallDatasetEquality(actualDF, expectedDF)

    info("Seems to work")

  }
}
