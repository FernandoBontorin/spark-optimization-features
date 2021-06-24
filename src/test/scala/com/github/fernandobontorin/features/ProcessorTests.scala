package com.github.fernandobontorin.features

import com.github.fernandobontorin.env.SparkSessionUnitTestWrapper
import com.github.fernandobontorin.features.Processor.{
  BUY_ORDER_DISTANCE,
  CARD_FLAG,
  CITY_SIZE
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ProcessorTests
    extends AnyFlatSpec
    with should.Matchers
    with SparkSessionUnitTestWrapper {

  import spark.implicits._

  "Age feature" should "extract current age from birth date" in {
    val df  = Seq("1995-01-15").toDF("dob")
    val age = df.select(Processor.AGE).head.getAs[Int]("age")
    age should be > 23
  }

  "Distance feature" should "calculate distance of buy order and merchant place in km" in {
    val df = Seq((33.9659, -80.9355, 33.986391, -81.200714)).toDF(
      "lat",
      "long",
      "merch_lat",
      "merch_long"
    )
    val distance =
      df.select(BUY_ORDER_DISTANCE).head.getAs[Double]("buy_order_distance")
    distance should be > 25d
    distance should be < 35d
  }

  "Card Flag feature" should "provide a financial institution label based on credit card number" in {
    val df = (0 to 7).toDF("cc_num")
    val flags =
      df.select(CARD_FLAG).collect.map(row => row.getAs[String]("card_flag"))
    flags should be(
      Array(
        "UNKNOWN",
        "UNKNOWN",
        "MASNER CARD",
        "FISA",
        "BRAZILIAN EXPRESS",
        "UNKNOWN",
        "ELU",
        "UNKNOWN"
      )
    )
  }

  "City Size feature" should "label city size accordingly population amount" in {
    val df = Seq(-1, 700, 8000, 16000, 50000, 120000, 210000, 300000, 500000,
      900000, 3000000).toDF("city_pop")
    val sizes = df
      .select(CITY_SIZE)
      .collect
      .map(row => row.getAs[String]("city_size_label"))
    sizes should be(
      Array(
        "NA",
        "PP",
        "PM",
        "PG",
        "MP",
        "MM",
        "MG",
        "GP",
        "GM",
        "GG",
        "MEGA"
      )
    )
  }

}
