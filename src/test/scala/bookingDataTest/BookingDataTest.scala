package bookingDataTest

import bookingDataFiltering.BookingDataApp
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Testing of getValidExpediaData of BookingDataApp
 *
 * Created by: Ian_Rakhmatullin
 * Date: 06.04.2021
 */
class BookingDataTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{

  it("should contain all the records") {
    val hotels = Seq(
      Row(8589934594L, "Holiday Inn Express", "US", "Ashland", "555 Clover Ln", 42.183544, -122.663345, "9r2z"),
      Row(25769803777L, "Travelodge", "US", "Oswego", "309 W Seneca St", 43.45161, -76.53235, "dr9x"),
      Row(25769803782L, "Ibis Wuerzburg City", "US", "Gresham", "Veitshoechheimer Str 5b", 49.802514, 9.921491, "u0z5"),
      Row(32134412313L, "CanYouHearMe", "RU", "Moscow", "1st Street", 55.751244, 37.618423, "sas4"),
    )

    val expediaTest = Seq(
      Row (1L, "2020-06-19", "2020-06-22", 8589934594L),
      Row(2L, "2020-06-20", "2020-06-21", 8589934594L),
      Row(3L, "2020-06-21", "2020-06-22", 8589934594L),

      Row(4L, "2020-06-20", "2020-06-24", 25769803777L),

      Row(5L, "2018-06-23", "2018-06-26", 25769803782L),

      Row(6L, null, "2018-03-22", 32134412313L)
    )

    val expectedResult = getCorrectExpediaById(expediaTest, List(1L, 2L, 3L, 4L, 5L, 6L))

    //creating DFs with schema
    val expediaInputDF: DataFrame = createDF(expediaTest,getExpediaInputSchema)
    val hotelsInputDF: DataFrame = createDF(hotels, getHotelSchema)
    val expectedResultDF: DataFrame = createDF(expectedResult, getResultSchema)

    val actualResult = BookingDataApp.getValidExpediaData(spark, expediaInputDF, hotelsInputDF)
    assertSmallDatasetEquality(actualResult, expectedResultDF)

    info("should contain all the records test is passed")
  }


  it("should filter invalid data") {

    val hotels = Seq(
      Row(8589934594L, "Holiday Inn Express", "US", "Ashland", "555 Clover Ln", 42.183544, -122.663345, "9r2z"),
      Row(25769803777L, "Travelodge", "US", "Oswego", "309 W Seneca St", 43.45161, -76.53235, "dr9x"),
      Row(25769803782L, "Ibis Wuerzburg City", "US", "Gresham", "Veitshoechheimer Str 5b", 49.802514, 9.921491, "u0z5"),
      Row(32134412313L, "CanYouHearMe", "RU", "Moscow", "1st Street", 55.751244, 37.618423, "sas4"),
    )

    val expediaTest = Seq(
      //invalid hotel
      Row(1L, "2020-06-19", "2020-06-22", 8589934594L),
      Row(2L, "2020-06-24", "2020-06-25", 8589934594L),
      Row(3L, "2020-06-25", "2020-06-30", 8589934594L),

      //should be ok idle > 30
      Row(4L, "2020-06-20", "2020-06-24", 25769803777L),
      Row(5L, "2020-07-28", "2020-06-24", 25769803777L),

      //invalid
      Row(6L, "2018-06-23", "2018-06-26", 25769803782L),
      Row(7L, "2018-07-10", "2018-06-26", 25769803782L),

      Row(8L, null, "2018-03-22", 32134412313L)
    )

    val expectedResult = getCorrectExpediaById(expediaTest, List(4L, 5L, 8L))

    //creating DFs with schema
    val expediaInputDF: DataFrame = createDF(expediaTest,getExpediaInputSchema)
    val hotelsInputDF: DataFrame = createDF(hotels, getHotelSchema)
    val expectedResultDF: DataFrame = createDF(expectedResult, getResultSchema)

    val actualResult = BookingDataApp.getValidExpediaData(spark, expediaInputDF, hotelsInputDF)
    assertSmallDatasetEquality(actualResult, expectedResultDF)

    info("should filter invalid data test is passed")
  }

  /**
   * Util
   * Filters passed expedia data by correctIds list.
   * Adds the last column for a row -> check_in_year
   * @param seq input expedia seq
   * @param correctIds Ids of expedia which considered to be correct
   */
  private def getCorrectExpediaById(seq: Seq[(Row)], correctIds: List[Long]): Seq[(Row)] = {
    val format = new SimpleDateFormat("yyyy-MM-DD")
    val calendar = Calendar.getInstance()

    seq.filter(p => correctIds.contains(p.getLong(0)))
      .map(p => {
        var mayBeYear: Option[Int] = None

        val check_in_date = p.getString(1) //check in date
        if (check_in_date != null) {
          val date = format.parse(check_in_date)
          calendar.setTime(date)
          mayBeYear = Some(calendar.get(Calendar.YEAR))
        }

        Row(p.getLong(0), check_in_date, p.getString(2), p.getLong(3), mayBeYear.orNull)
      })
  }

  private def createDF(expediaTest: Seq[Row], schema: Seq[StructField]) = {
    val expediaInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expediaTest),
      StructType(schema)
    )
    expediaInputDF
  }

  private def getExpediaInputSchema = {
    List(
      StructField("id", LongType),
      StructField("srch_ci", StringType),
      StructField("srch_co", StringType),
      StructField("hotel_id", LongType),
    )
  }

  private def getResultSchema = {
    getExpediaInputSchema :+ StructField("check_in_year", IntegerType)
  }

  private def getHotelSchema = {
    List(
      StructField("Id", LongType, nullable = false),
      StructField("Name", StringType, nullable = false),
      StructField("Country", StringType, nullable = false),
      StructField("City", StringType, nullable = false),
      StructField("Address", StringType, nullable = false),
      StructField("Latitude", DoubleType, nullable = false),
      StructField("Longitude", DoubleType, nullable = false),
      StructField("GeoHash", StringType, nullable = false)
    )
  }
}
