package com.github.fernandobontorin.features

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

object Source {
  def ID: Column = col("_c0").cast(IntegerType).as("id")
  def TRANSACTION_DATETIME: Column =
    col("trans_date_trans_time").cast(StringType).as("transaction_datetime")
  def CREDIT_CARD_NUMBER: Column =
    col("cc_num").cast(StringType).as("credit_card_number")
  def MERCHANT_NAME: Column =
    col("merchant").cast(StringType).as("merchant_name")
  def MERCHANT_CATEGORY: Column =
    col("category").cast(StringType).as("merchant_category")
  def AMOUNT: Column     = col("amt").cast(DoubleType).as("amount")
  def FIRST_NAME: Column = col("first").cast(StringType).as("first_name")
  def LAST_NAME: Column  = col("last").cast(StringType).as("last_name")
  def GENDER: Column     = col("gender").cast(StringType).as("gender")
  def STREET: Column     = col("street").cast(StringType).as("street")
  def CITY: Column       = col("city").cast(StringType).as("city")
  def STATE: Column      = col("state").cast(StringType).as("state")
  def ZIP: Column        = col("zip").cast(StringType).as("zip")
  def LATITUDE: Column   = col("lat").cast(DoubleType).as("latitude")
  def LONGITUDE: Column  = col("long").cast(DoubleType).as("longitude")
  def CITY_POPULATION: Column =
    col("city_pop").cast(IntegerType).as("city_population")
  def JOB: Column        = col("job").cast(IntegerType).as("job")
  def BIRTH_DATE: Column = col("dob").cast(StringType).as("birth_date")
  def TRANSACTION_NUMBER: Column =
    col("trans_num").cast(StringType).as("transaction_number")
  def MERCHANT_LATITUDE: Column =
    col("merch_lat").cast(DoubleType).as("merchant_latitude")
  def MERCHANT_LONGITUDE: Column =
    col("merch_long").cast(DoubleType).as("merchant_longitude")
  def IS_FRAUD: Column = col("is_fraud").cast(IntegerType).as("is_fraud")
}
