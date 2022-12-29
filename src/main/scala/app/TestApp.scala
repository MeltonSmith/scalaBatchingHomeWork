package app

import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author Yan R
 * @since 28.12.2022
 */
object TestApp {

  def main(args: Array[String]): Unit ={
    implicit val spark = SparkSession
                  .builder
                  .appName("Kubernetes test app")
                  .getOrCreate()

    val hotels = Seq(
      Row(8589934594L, "Holiday Inn Express", "US", "Ashland", "555 Clover Ln", 42.183544, -122.663345, "9r2z"),
      Row(25769803777L, "Travelodge", "US", "Oswego", "309 W Seneca St", 43.45161, -76.53235, "dr9x"),
      Row(25769803782L, "Ibis Wuerzburg City", "US", "Gresham", "Veitshoechheimer Str 5b", 49.802514, 9.921491, "u0z5"),
      Row(32134412313L, "CanYouHearMe", "RU", "Moscow", "1st Street", 55.751244, 37.618423, "sas4"),
    )

    val hotelsInputDF: DataFrame = createDF(hotels, getHotelSchema)

    hotelsInputDF.show()
  }


  private def createDF(expediaTest: Seq[Row], schema: Seq[StructField])(implicit spark: SparkSession) = {
    val expediaInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expediaTest),
      StructType(schema)
    )
    expediaInputDF
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
