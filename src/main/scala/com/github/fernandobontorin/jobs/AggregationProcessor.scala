package com.github.fernandobontorin.jobs

import com.github.fernandobontorin.args.Parameters
import com.github.fernandobontorin.env.SparkSessionWrapper
import com.github.fernandobontorin.features.Processor.AGE
import com.github.fernandobontorin.features.Source.{AMOUNT, CREDIT_CARD_NUMBER}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object AggregationProcessor extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    val params = Parameters.parse(args)
    val df = params.dataframes.par
      .map(spark.read.option("header", "true").csv)
      .reduce(_ union _)

    val df_age = df
      .select(AGE, AMOUNT)
      .groupBy("age")
      .agg(
        sum("amount").as("amount_sum"),
        min("amount").as("amount_min"),
        max("amount").as("amount_max"),
        mean("amount").as("amount_mean"),
        avg("amount").as("amount_avg")
      )

    val df_ccnum = df
      .select(CREDIT_CARD_NUMBER, AMOUNT)
      .groupBy("credit_card_number")
      .agg(
        sum("amount").as("amount_sum"),
        min("amount").as("amount_min"),
        max("amount").as("amount_max"),
        mean("amount").as("amount_mean"),
        avg("amount").as("amount_avg")
      )

    df_age.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(params.output + "_age")

    df_ccnum.write
      .mode(SaveMode.Overwrite)
      .parquet(params.output + "_credit_card_number")
  }

}
