package bookingDataTest

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper

/**
 * Created by: Ian_Rakhmatullin
 * Date: 06.04.2021
 */
class BookingDataTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{

  import spark.implicits._

  it("aliases a DataFrame") {

    val sourceDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")
    ).toDF("name")

    val actualDF = sourceDF.select(col("name").alias("student"))

    val expectedDF = Seq(
      ("jose"),
      ("li"),
      ("luisa")
    ).toDF("student")



    assertSmallDatasetEquality(actualDF, expectedDF)

  }
}
