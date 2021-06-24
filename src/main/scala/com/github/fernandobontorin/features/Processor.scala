package com.github.fernandobontorin.features

import com.github.fernandobontorin.features.Source._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

object Processor {
  def AGE: Column = {
    months_between(current_date(), to_date(BIRTH_DATE)) / 12
  }.cast(IntegerType)
    .alias("age")

  def CITY_SIZE: Column =
    when(CITY_POPULATION < 0, "NA")
      .when(CITY_POPULATION < 800, "PP")
      .when(CITY_POPULATION < 9000, "PM")
      .when(CITY_POPULATION < 32000, "PG")
      .when(CITY_POPULATION < 80000, "MP")
      .when(CITY_POPULATION < 160000, "MM")
      .when(CITY_POPULATION < 220000, "MG")
      .when(CITY_POPULATION < 320000, "GP")
      .when(CITY_POPULATION < 600000, "GM")
      .when(CITY_POPULATION < 1000000, "GG")
      .otherwise("MEGA")
      .cast(StringType)
      .alias("city_size_label")

  def CARD_FLAG: Column =
    when(CREDIT_CARD_NUMBER.startsWith("2"), "MASNER CARD")
      .when(CREDIT_CARD_NUMBER.startsWith("3"), "FISA")
      .when(CREDIT_CARD_NUMBER.startsWith("4"), "BRAZILIAN EXPRESS")
      .when(CREDIT_CARD_NUMBER.startsWith("6"), "ELU")
      .otherwise("UNKNOWN")
      .cast(StringType)
      .alias("card_flag")

  def BUY_ORDER_DISTANCE: Column = {
    sqrt(
      ((MERCHANT_LATITUDE - LATITUDE) * (MERCHANT_LATITUDE - LATITUDE)) +
        ((MERCHANT_LONGITUDE - LONGITUDE) * (MERCHANT_LONGITUDE - LONGITUDE))
    ) * 111.0
  }.cast(DoubleType)
      .alias("buy_order_distance")
}
