package com.github.fernandobontorin.jobs

import com.github.fernandobontorin.args.Parameters
import com.github.fernandobontorin.env.SparkSessionWrapper
import com.github.fernandobontorin.features.Processor._
import com.github.fernandobontorin.features.Source._
import org.apache.spark.sql.SaveMode

object FraudBookProcessor extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    val params = Parameters.parse(args)
    val df = params.dataframes.par
      .map(spark.read.option("header", "true").csv)
      .reduce(_ union _)
    val features = Seq(
      ID,
      TRANSACTION_DATETIME,
      CREDIT_CARD_NUMBER,
      MERCHANT_NAME,
      MERCHANT_CATEGORY,
      AMOUNT,
      FIRST_NAME,
      LAST_NAME,
      GENDER,
      STREET,
      CITY,
      STATE,
      ZIP,
      LATITUDE,
      LONGITUDE,
      CITY_POPULATION,
      JOB,
      BIRTH_DATE,
      TRANSACTION_NUMBER,
      MERCHANT_LATITUDE,
      MERCHANT_LONGITUDE,
      IS_FRAUD,
      AGE,
      CITY_SIZE,
      CARD_FLAG,
      BUY_ORDER_DISTANCE
    )

    df.select(features: _*)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(params.output)
  }

}
